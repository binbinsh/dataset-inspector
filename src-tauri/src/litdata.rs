use hex::encode as hex_encode;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tauri::async_runtime::spawn_blocking;
use thiserror::Error;

const PREVIEW_BYTES: usize = 2048;
const MAX_CACHE_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Default)]
pub struct ChunkCache {
    inner: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl ChunkCache {
    fn fetch(&self, key: &str) -> Option<Vec<u8>> {
        self.inner.lock().ok()?.get(key).cloned()
    }

    fn maybe_store(&self, key: &str, data: Vec<u8>) {
        if data.len() <= MAX_CACHE_BYTES {
            if let Ok(mut guard) = self.inner.lock() {
                guard.insert(key.to_string(), data);
            }
        }
    }
}

pub type AppResult<T> = Result<T, AppError>;

#[derive(Error, Debug, Serialize)]
#[serde(tag = "code", content = "message")]
pub enum AppError {
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("not found: {0}")]
    Missing(String),
    #[error("unsupported compression: {0}")]
    UnsupportedCompression(String),
    #[error("malformed chunk")]
    MalformedChunk,
    #[error("io error: {0}")]
    Io(String),
    #[error("task error: {0}")]
    Task(String),
    #[error("open error: {0}")]
    Open(String),
}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        AppError::Io(value.to_string())
    }
}

fn read_le_u32(bytes: &[u8]) -> AppResult<u32> {
    let buf: [u8; 4] = bytes.try_into().map_err(|_| AppError::MalformedChunk)?;
    Ok(u32::from_le_bytes(buf))
}

#[derive(Deserialize)]
struct IndexFile {
    chunks: Vec<RawChunk>,
    config: IndexConfig,
}

#[derive(Deserialize, Clone, Serialize)]
struct IndexConfig {
    compression: Option<String>,
    chunk_size: Option<u32>,
    chunk_bytes: Option<u64>,
    data_format: Option<Vec<String>>,
    data_spec: Option<String>,
}

#[derive(Deserialize)]
struct RawChunk {
    filename: String,
    chunk_bytes: u64,
    chunk_size: u32,
    dim: Option<u32>,
}

struct ParsedIndex {
    root_dir: PathBuf,
    source: PathBuf,
    config: IndexConfig,
    config_raw: serde_json::Value,
    chunks: Vec<RawChunk>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkSummary {
    filename: String,
    path: String,
    chunk_size: u32,
    chunk_bytes: u64,
    dim: Option<u32>,
    exists: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexSummary {
    index_path: String,
    root_dir: String,
    data_format: Vec<String>,
    compression: Option<String>,
    chunk_size: Option<u32>,
    chunk_bytes: Option<u64>,
    config_raw: serde_json::Value,
    chunks: Vec<ChunkSummary>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldMeta {
    field_index: usize,
    size: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemMeta {
    item_index: u32,
    total_bytes: u64,
    fields: Vec<FieldMeta>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldPreview {
    preview_text: Option<String>,
    hex_snippet: String,
    guessed_ext: Option<String>,
    is_binary: bool,
    size: u32,
}

enum ChunkAccess {
    File(PathBuf),
    Memory(Vec<u8>),
}

impl ChunkAccess {
    fn read_exact_at(&self, offset: u64, len: usize) -> AppResult<Vec<u8>> {
        match self {
            ChunkAccess::File(path) => {
                let mut fp = File::open(path)?;
                fp.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; len];
                fp.read_exact(&mut buf)?;
                Ok(buf)
            }
            ChunkAccess::Memory(buf) => {
                let end = offset
                    .checked_add(len as u64)
                    .ok_or(AppError::MalformedChunk)? as usize;
                if end > buf.len() {
                    return Err(AppError::MalformedChunk);
                }
                Ok(buf[offset as usize..end].to_vec())
            }
        }
    }
}

fn parse_index(index_path: &Path) -> AppResult<ParsedIndex> {
    if is_chunk_path(index_path) {
        if let Some(found) = find_neighbor_index(index_path) {
            return parse_index(&found);
        }
        return parse_chunk_only(index_path);
    }

    let resolved = resolve_index_path(index_path)?;
    let content = read_index_file(&resolved)?;
    let parsed: IndexFile = serde_json::from_str(&content)
        .map_err(|e| AppError::Invalid(format!("index.json parse error: {e}")))?;
    let config = parsed.config;
    let config_raw = serde_json::to_value(&config).unwrap_or(serde_json::Value::Null);
    let root_dir = resolved
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    Ok(ParsedIndex {
        root_dir,
        source: resolved,
        config,
        config_raw,
        chunks: parsed.chunks,
    })
}

fn parse_chunk_only(index_path: &Path) -> AppResult<ParsedIndex> {
    let root_dir = index_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let mut file = File::open(index_path)?;
    let size = file.metadata()?.len();

    let mut num_buf = [0u8; 4];
    file.read_exact(&mut num_buf)?;
    let num_items = read_le_u32(&num_buf)?;

    let offsets_len = (num_items as usize + 1) * 4;
    let mut offsets = vec![0u8; offsets_len];
    file.read_exact(&mut offsets)?;

    let chunk = RawChunk {
        filename: index_path
            .file_name()
            .and_then(|f| f.to_str())
            .unwrap_or("chunk.bin")
            .to_string(),
        chunk_bytes: size,
        chunk_size: num_items.max(1),
        dim: None,
    };
    let fallback_config = IndexConfig {
        compression: None,
        chunk_size: Some(num_items.max(1)),
        chunk_bytes: Some(size),
        data_format: Some(vec!["bytes".into()]),
        data_spec: None,
    };
    Ok(ParsedIndex {
        root_dir,
        source: index_path.to_path_buf(),
        config: fallback_config.clone(),
        config_raw: serde_json::to_value(fallback_config).unwrap_or(serde_json::Value::Null),
        chunks: vec![chunk],
    })
}

fn is_chunk_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("bin") || ext.eq_ignore_ascii_case("zst"))
        .unwrap_or(false)
        || path
            .file_name()
            .and_then(|f| f.to_str())
            .map(|name| name.contains(".bin"))
            .unwrap_or(false)
}

fn find_neighbor_index(chunk_path: &Path) -> Option<PathBuf> {
    let parent = chunk_path.parent()?;
    let candidates = [
        "index.json",
        "index.json.zstd",
        "index.json.zst",
        "0.index.json",
        "0.index.json.zstd",
        "0.index.json.zst",
    ];
    for name in candidates {
        let candidate = parent.join(name);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    let mut globbed: Vec<PathBuf> = std::fs::read_dir(parent)
        .ok()?
        .filter_map(|e| e.ok().map(|e2| e2.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|f| f.to_str())
                .map(|name| name.ends_with(".index.json") || name.contains(".index.json."))
                .unwrap_or(false)
        })
        .collect();
    globbed.sort();
    globbed.into_iter().next()
}

fn resolve_index_path(path: &Path) -> AppResult<PathBuf> {
    if path.is_file() {
        return Ok(path.to_path_buf());
    }
    if path.is_dir() {
        let candidates = [
            "index.json",
            "index.json.zstd",
            "index.json.zst",
            "0.index.json",
            "0.index.json.zstd",
            "0.index.json.zst",
        ];
        let mut globbed: Vec<PathBuf> = std::fs::read_dir(path)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok().map(|e2| e2.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|f| f.to_str())
                    .map(|name| name.ends_with(".index.json") || name.contains(".index.json."))
                    .unwrap_or(false)
            })
            .collect();
        globbed.sort();
        for name in candidates {
            let candidate = path.join(name);
            if candidate.exists() {
                return Ok(candidate);
            }
        }
        if let Some(first) = globbed.first() {
            return Ok(first.to_path_buf());
        }
    } else if let Some(parent) = path.parent() {
        let base = path.file_stem().and_then(|s| s.to_str()).unwrap_or("index");
        let candidates = [
            path.to_path_buf(),
            path.with_extension("json"),
            path.with_extension("json.zstd"),
            path.with_extension("json.zst"),
            parent.join(format!("{base}.json")),
            parent.join(format!("{base}.json.zstd")),
            parent.join(format!("{base}.json.zst")),
        ];
        for candidate in candidates {
            if candidate.exists() {
                return Ok(candidate);
            }
        }
    }
    Err(AppError::Missing(path.display().to_string()))
}

fn read_index_file(path: &Path) -> AppResult<String> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    if ext.contains("zst") {
        let file = File::open(path)?;
        let mut decoder = zstd::stream::Decoder::new(file)?;
        let mut s = String::new();
        decoder.read_to_string(&mut s)?;
        Ok(s)
    } else {
        Ok(fs::read_to_string(path)?)
    }
}

fn parse_index_file(path: &Path) -> AppResult<ParsedIndex> {
    let content = read_index_file(path)?;
    let parsed: IndexFile = serde_json::from_str(&content)
        .map_err(|e| AppError::Invalid(format!("index.json parse error: {e}")))?;
    let config = parsed.config.clone();
    let config_raw = serde_json::to_value(&config).unwrap_or(serde_json::Value::Null);
    let root_dir = path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    Ok(ParsedIndex {
        root_dir,
        source: path.to_path_buf(),
        config,
        config_raw,
        chunks: parsed.chunks,
    })
}

#[tauri::command]
pub async fn load_index(index_path: String) -> AppResult<IndexSummary> {
    let path = PathBuf::from(index_path);
    spawn_blocking(move || load_index_sync(path))
        .await
        .map_err(|e| AppError::Task(e.to_string()))?
}

fn load_index_sync(index_path: PathBuf) -> AppResult<IndexSummary> {
    parse_index(&index_path).and_then(
        |ParsedIndex {
             root_dir,
             source,
             config,
             config_raw,
             chunks,
         }| {
            let data_format = config.data_format.clone().unwrap_or_default();
            let mut summaries = Vec::with_capacity(chunks.len());
            for c in chunks {
                let full = root_dir.join(&c.filename);
                let exists = full.exists();
                summaries.push(ChunkSummary {
                    filename: c.filename,
                    path: full.display().to_string(),
                    chunk_size: c.chunk_size,
                    chunk_bytes: c.chunk_bytes,
                    dim: c.dim,
                    exists,
                });
            }
            Ok(IndexSummary {
                index_path: source.display().to_string(),
                root_dir: root_dir.display().to_string(),
                data_format,
                compression: config.compression.clone(),
                chunk_size: config.chunk_size,
                chunk_bytes: config.chunk_bytes,
                config_raw,
                chunks: summaries,
            })
        },
    )
}

#[tauri::command]
pub async fn load_chunk_list(paths: Vec<String>) -> AppResult<IndexSummary> {
    spawn_blocking(move || load_chunk_list_sync(paths))
        .await
        .map_err(|e| AppError::Task(e.to_string()))?
}

fn load_chunk_list_sync(paths: Vec<String>) -> AppResult<IndexSummary> {
    if paths.is_empty() {
        return Err(AppError::Invalid("no chunk paths provided".into()));
    }
    let mut raw_chunks: Vec<RawChunk> = Vec::new();
    let mut root_dir = None;
    let mut name_to_path: HashMap<String, PathBuf> = HashMap::new();
    let mut index_path: Option<PathBuf> = None;
    let mut data_format: Vec<String> = vec!["bytes".into()];
    let mut compression: Option<String> = None;
    let mut chunk_size: Option<u32> = None;
    let mut chunk_bytes: Option<u64> = None;
    let mut config_raw: Option<serde_json::Value> = None;
    for p in &paths {
        let path = PathBuf::from(p);
        if root_dir.is_none() {
            root_dir = path.parent().map(|pp| pp.to_path_buf());
        }
        if let Some(name) = path
            .file_name()
            .and_then(|f| f.to_str())
            .map(|s| s.to_string())
        {
            name_to_path.insert(name, path.clone());
        }
    }
    if let Some(found_index_path) = find_neighbor_index(Path::new(&paths[0])) {
        let parsed = parse_index_file(&found_index_path)?;
        data_format = parsed
            .config
            .data_format
            .clone()
            .unwrap_or_else(|| data_format.clone());
        compression = parsed.config.compression.clone();
        chunk_size = parsed.config.chunk_size;
        chunk_bytes = parsed.config.chunk_bytes;
        config_raw = Some(parsed.config_raw.clone());
        index_path = Some(found_index_path);
        root_dir = Some(parsed.root_dir.clone());
        let selected: HashSet<String> = name_to_path.keys().cloned().collect();
        for c in parsed.chunks {
            if selected.contains(&c.filename) {
                raw_chunks.push(c);
            }
        }
    }

    let root_dir = root_dir.unwrap_or_else(|| PathBuf::from("."));

    let covered: HashSet<String> = raw_chunks.iter().map(|c| c.filename.clone()).collect();
    for (name, path) in &name_to_path {
        if covered.contains(name) {
            continue;
        }
        let info = fs::metadata(path)?;
        let size = info.len();
        let mut file = File::open(path)?;
        let mut num_buf = [0u8; 4];
        file.read_exact(&mut num_buf)?;
        let num_items = read_le_u32(&num_buf)?.max(1);
        let offsets_len = (num_items as usize + 1) * 4;
        let mut offsets = vec![0u8; offsets_len];
        file.read_exact(&mut offsets)?;
        raw_chunks.push(RawChunk {
            filename: name.clone(),
            chunk_bytes: size,
            chunk_size: num_items,
            dim: None,
        });
    }

    let config_raw_default_fmt = data_format.clone();
    let config_raw = config_raw.unwrap_or_else(|| {
        serde_json::json!({
            "source": "multi-bin",
            "data_format": config_raw_default_fmt,
        })
    });

    let resolved_index_path = index_path.unwrap_or_else(|| PathBuf::from(&paths[0]));

    Ok(IndexSummary {
        index_path: resolved_index_path.display().to_string(),
        root_dir: root_dir.display().to_string(),
        data_format,
        compression,
        chunk_size,
        chunk_bytes,
        config_raw,
        chunks: raw_chunks
            .into_iter()
            .map(|c| {
                let path = name_to_path
                    .get(&c.filename)
                    .cloned()
                    .unwrap_or_else(|| root_dir.join(&c.filename));
                ChunkSummary {
                    filename: c.filename,
                    path: path.display().to_string(),
                    chunk_size: c.chunk_size,
                    chunk_bytes: c.chunk_bytes,
                    dim: c.dim,
                    exists: true,
                }
            })
            .collect(),
    })
}

fn load_chunk_access(
    parsed: &ParsedIndex,
    chunk_filename: &str,
    cache: &ChunkCache,
) -> AppResult<ChunkAccess> {
    let chunk_path = parsed.root_dir.join(chunk_filename);
    if !chunk_path.exists() {
        return Err(AppError::Missing(chunk_path.display().to_string()));
    }
    match parsed.config.compression.as_ref().map(|c| c.to_lowercase()) {
        Some(ref c) if c == "zstd" => {
            let key = chunk_path.display().to_string();
            if let Some(buf) = cache.fetch(&key) {
                return Ok(ChunkAccess::Memory(buf));
            }
            let file = File::open(&chunk_path)?;
            let mut decoder = zstd::stream::Decoder::new(file)?;
            let mut buf = Vec::new();
            decoder
                .read_to_end(&mut buf)
                .map_err(|e| AppError::Invalid(format!("decompressing chunk: {e}")))?;
            cache.maybe_store(&key, buf.clone());
            Ok(ChunkAccess::Memory(buf))
        }
        Some(other) => Err(AppError::UnsupportedCompression(other)),
        None => Ok(ChunkAccess::File(chunk_path)),
    }
}

fn parse_offsets(access: &ChunkAccess) -> AppResult<(u32, Vec<u32>)> {
    let num_buf = access.read_exact_at(0, 4)?;
    let num_items = read_le_u32(&num_buf)?;
    let offsets_len = (num_items as usize + 1) * 4;
    let offsets_buf = access.read_exact_at(4, offsets_len)?;
    let mut offsets = Vec::with_capacity(num_items as usize + 1);
    for chunk in offsets_buf.chunks_exact(4) {
        offsets.push(read_le_u32(chunk)?);
    }
    Ok((num_items, offsets))
}

#[tauri::command]
pub async fn list_chunk_items(
    index_path: String,
    chunk_filename: String,
    cache: tauri::State<'_, ChunkCache>,
) -> AppResult<Vec<ItemMeta>> {
    let path = PathBuf::from(index_path);
    let cache_handle = (*cache).clone();
    spawn_blocking(move || list_chunk_items_sync(path, chunk_filename, &cache_handle))
        .await
        .map_err(|e| AppError::Task(e.to_string()))?
}

fn list_chunk_items_sync(
    index_path: PathBuf,
    chunk_filename: String,
    cache: &ChunkCache,
) -> AppResult<Vec<ItemMeta>> {
    let parsed = parse_index(&index_path)?;
    let access = load_chunk_access(&parsed, &chunk_filename, cache)?;
    let format_len = parsed
        .config
        .data_format
        .as_ref()
        .map(|v| v.len())
        .unwrap_or(0);
    let header_len = format_len * 4;
    let (num_items, offsets) = parse_offsets(&access)?;
    let mut items = Vec::with_capacity(num_items as usize);
    for item_idx in 0..num_items {
        let start = offsets[item_idx as usize];
        let end = offsets[item_idx as usize + 1];
        if end < start {
            return Err(AppError::MalformedChunk);
        }
        let mut sizes = Vec::new();
        if header_len > 0 {
            let head = access.read_exact_at(start as u64, header_len)?;
            for j in 0..format_len {
                let pos = j * 4;
                sizes.push(read_le_u32(&head[pos..pos + 4])?);
            }
        }
        items.push(ItemMeta {
            item_index: item_idx,
            total_bytes: (end - start) as u64,
            fields: sizes
                .into_iter()
                .enumerate()
                .map(|(idx, size)| FieldMeta {
                    field_index: idx,
                    size,
                })
                .collect(),
        });
    }
    Ok(items)
}

#[tauri::command]
pub async fn peek_field(
    index_path: String,
    chunk_filename: String,
    item_index: u32,
    field_index: usize,
    cache: tauri::State<'_, ChunkCache>,
) -> AppResult<FieldPreview> {
    let cache_handle = (*cache).clone();
    spawn_blocking(move || {
        preview_field(
            &index_path,
            &chunk_filename,
            item_index,
            field_index,
            &cache_handle,
        )
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

fn preview_field(
    index_path: &str,
    chunk_filename: &str,
    item_index: u32,
    field_index: usize,
    cache: &ChunkCache,
) -> AppResult<FieldPreview> {
    let parsed = parse_index(Path::new(index_path))?;
    let fmt = parsed.config.data_format.clone().unwrap_or_default();
    let access = load_chunk_access(&parsed, chunk_filename, cache)?;
    let (data, size) = read_field_bytes(
        &access,
        item_index,
        field_index,
        fmt.len(),
        Some(PREVIEW_BYTES),
    )?;
    let text = String::from_utf8(data.clone()).ok();
    let guessed_ext = guess_ext(fmt.get(field_index), &data);
    let hex_snippet = hex_encode(data.iter().take(48).copied().collect::<Vec<u8>>());
    Ok(FieldPreview {
        preview_text: text.as_ref().map(|s| s.chars().take(400).collect()),
        hex_snippet,
        guessed_ext,
        is_binary: text.is_none(),
        size,
    })
}

#[tauri::command]
pub async fn open_leaf(
    index_path: String,
    chunk_filename: String,
    item_index: u32,
    field_index: usize,
    cache: tauri::State<'_, ChunkCache>,
) -> AppResult<String> {
    let cache_handle = (*cache).clone();
    spawn_blocking(move || {
        let path = PathBuf::from(&index_path);
        open_leaf_inner(
            &path,
            &chunk_filename,
            item_index,
            field_index,
            &cache_handle,
        )
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

fn open_leaf_inner(
    index_path: &Path,
    chunk_filename: &str,
    item_index: u32,
    field_index: usize,
    cache: &ChunkCache,
) -> AppResult<String> {
    let parsed = parse_index(index_path)?;
    let fmt = parsed.config.data_format.clone().unwrap_or_default();
    let access = load_chunk_access(&parsed, chunk_filename, cache)?;
    let (data, size) = read_field_bytes(&access, item_index, field_index, fmt.len(), None)?;
    let ext = guess_ext(fmt.get(field_index), &data).unwrap_or_else(|| "bin".into());
    let temp_dir = std::env::temp_dir().join("litdata-viewer");
    fs::create_dir_all(&temp_dir)?;
    let out = temp_dir.join(format!(
        "{}-i{}-f{}.{}",
        sanitize(chunk_filename),
        item_index,
        field_index,
        ext
    ));
    fs::write(&out, data)?;
    open::that_detached(&out).map_err(|e| AppError::Open(e.to_string()))?;
    Ok(format!("{} ({} bytes)", out.display(), size))
}

fn read_field_bytes(
    access: &ChunkAccess,
    item_index: u32,
    field_index: usize,
    format_len: usize,
    limit: Option<usize>,
) -> AppResult<(Vec<u8>, u32)> {
    let header_len = format_len * 4;
    let (num_items, offsets) = parse_offsets(access)?;
    if item_index >= num_items {
        return Err(AppError::Invalid("item index out of range".into()));
    }
    let start = offsets[item_index as usize];
    let end = offsets[item_index as usize + 1];
    if end < start {
        return Err(AppError::MalformedChunk);
    }
    let header = if header_len > 0 {
        Some(access.read_exact_at(start as u64, header_len)?)
    } else {
        None
    };
    let mut sizes = Vec::new();
    if let Some(head) = header {
        for j in 0..format_len {
            let pos = j * 4;
            sizes.push(read_le_u32(&head[pos..pos + 4])?);
        }
    }
    if field_index >= sizes.len() {
        return Err(AppError::Invalid("field index out of range".into()));
    }
    let mut cursor = start as u64 + header_len as u64;
    for (idx, sz) in sizes.iter().enumerate() {
        if idx == field_index {
            let desired = limit.map(|l| l.min(*sz as usize)).unwrap_or(*sz as usize);
            let data = access.read_exact_at(cursor, desired)?;
            return Ok((data, *sz));
        }
        cursor += *sz as u64;
    }
    Err(AppError::MalformedChunk)
}

fn guess_ext(data_format: Option<&String>, data: &[u8]) -> Option<String> {
    if let Some(fmt) = data_format {
        let fmt_lower = fmt.to_lowercase();
        if fmt_lower == "bytes" || fmt_lower == "bin" {
            if let Some(magic) = detect_magic_ext(data) {
                return Some(magic);
            }
            return Some("bin".into());
        }
    }
    if let Some(fmt) = data_format {
        if let Some((_, subtype)) = fmt.split_once(':') {
            if !subtype.is_empty() {
                return Some(subtype.trim().trim_start_matches('.').to_string());
            }
        }
        if let Some((_, ext)) = fmt.rsplit_once('.') {
            if !ext.is_empty() {
                return Some(ext.to_string());
            }
        }
        let fmt_lower = fmt.to_lowercase();
        let map = [
            ("jpeg", "jpg"),
            ("jpg", "jpg"),
            ("pil", "png"),
            ("png", "png"),
            ("tiff", "tiff"),
            ("str", "txt"),
            ("string", "txt"),
            ("int", "txt"),
            ("float", "txt"),
            ("bool", "txt"),
            ("bytes", "bin"),
            ("audio", "wav"),
        ];
        if let Some((_, ext)) = map.iter().find(|(k, _)| *k == fmt_lower) {
            return Some((*ext).into());
        }
        if fmt_lower.contains("wav") {
            return Some("wav".into());
        }
        if fmt_lower.contains("mp3") {
            return Some("mp3".into());
        }
        if fmt_lower.contains("flac") {
            return Some("flac".into());
        }
    }
    if let Some(magic_ext) = detect_magic_ext(data) {
        return Some(magic_ext);
    }
    if std::str::from_utf8(data)
        .map(|s| s.trim().len() > 0)
        .unwrap_or(false)
    {
        return Some("txt".into());
    }
    infer::get(data).map(|t| t.extension().to_string())
}

fn sanitize(input: &str) -> String {
    input
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

fn detect_magic_ext(data: &[u8]) -> Option<String> {
    if data.len() >= 12 && &data[0..4] == b"RIFF" && &data[8..12] == b"WAVE" {
        return Some("wav".into());
    }
    if data.len() >= 3 && &data[0..3] == b"ID3" {
        return Some("mp3".into());
    }
    if data.len() >= 2 && data[0] == 0xFF && (data[1] & 0xE0) == 0xE0 {
        return Some("mp3".into());
    }
    if data.len() >= 4 && &data[0..4] == b"fLaC" {
        return Some("flac".into());
    }
    None
}
