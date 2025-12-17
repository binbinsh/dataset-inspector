use hex::encode as hex_encode;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};
use tauri::async_runtime::spawn_blocking;

use crate::{
    app_error::{AppError, AppResult},
    audio,
    ipc_types::{
        ChunkSummary, FieldMeta, FieldPreview, IndexSummary, ItemMeta, OpenLeafResponse,
        PreparedFileResponse,
    },
    open_with,
};

const PREVIEW_BYTES: usize = 2048;
const MAX_LISTED_SAMPLES: u32 = 5_000;
const MAX_OPEN_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Deserialize)]
struct MdsIndexFile {
    shards: Vec<MdsShard>,
}

#[derive(Deserialize, Clone)]
struct MdsShard {
    column_encodings: Vec<String>,
    column_names: Vec<String>,
    column_sizes: Vec<Option<u32>>,
    compression: Option<String>,
    format: String,
    hashes: Vec<String>,
    raw_data: FileInfo,
    samples: u32,
    size_limit: Option<u64>,
    version: u32,
    zip_data: Option<FileInfo>,
}

#[derive(Deserialize, Clone)]
struct FileInfo {
    basename: String,
    bytes: u64,
    hashes: HashMap<String, String>,
}

fn resolve_index_path(path: &Path) -> AppResult<PathBuf> {
    if path.is_dir() {
        let candidates = ["index.json", "index.json.zstd", "index.json.zst"];
        for name in candidates {
            let candidate = path.join(name);
            if candidate.exists() {
                return Ok(candidate);
            }
        }
        return Err(AppError::Missing(format!(
            "no index.json found in {}",
            path.display()
        )));
    }
    if path.exists() {
        return Ok(path.to_path_buf());
    }
    Err(AppError::Missing(path.display().to_string()))
}

fn read_index_bytes(path: &Path) -> AppResult<Vec<u8>> {
    let file = File::open(path)?;
    let name = path
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("")
        .to_lowercase();
    if name.ends_with(".zst") || name.ends_with(".zstd") {
        let mut decoder = zstd::stream::Decoder::new(file)?;
        let mut buf = Vec::new();
        decoder
            .read_to_end(&mut buf)
            .map_err(|e| AppError::Invalid(format!("decompressing index: {e}")))?;
        return Ok(buf);
    }
    let mut buf = Vec::new();
    let mut reader = file;
    reader.read_to_end(&mut buf)?;
    Ok(buf)
}

fn parse_index(index_path: &Path) -> AppResult<(PathBuf, PathBuf, MdsIndexFile)> {
    let resolved = resolve_index_path(index_path)?;
    let bytes = read_index_bytes(&resolved)?;
    let parsed: MdsIndexFile = serde_json::from_slice(&bytes)
        .map_err(|e| AppError::Invalid(format!("index.json parse error: {e}")))?;
    let root_dir = resolved
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    Ok((root_dir, resolved, parsed))
}

fn shard_for_filename<'a>(
    index: &'a MdsIndexFile,
    shard_filename: &str,
) -> AppResult<&'a MdsShard> {
    let trimmed = shard_filename.trim();
    if trimmed.is_empty() {
        return Err(AppError::Invalid("missing shard filename".into()));
    }
    index
        .shards
        .iter()
        .find(|s| {
            s.raw_data.basename == trimmed
                || s.zip_data.as_ref().is_some_and(|z| z.basename == trimmed)
        })
        .ok_or_else(|| AppError::Missing(format!("unknown shard: {trimmed}")))
}

fn compression_kind(value: Option<&str>, filename: &str) -> Option<String> {
    let normalized = value.unwrap_or("").trim().to_lowercase();
    if normalized.starts_with("zstd") {
        return Some("zstd".into());
    }
    let lower = filename.to_lowercase();
    if lower.ends_with(".zst") || lower.ends_with(".zstd") {
        return Some("zstd".into());
    }
    None
}

fn temp_cache_dir() -> PathBuf {
    std::env::temp_dir()
        .join("dataset-inspector")
        .join("mds-cache")
}

fn hash_key_for_path(path: &Path) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.to_string_lossy().hash(&mut hasher);
    if let Ok(meta) = fs::metadata(path) {
        meta.len().hash(&mut hasher);
        if let Ok(modified) = meta.modified() {
            if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
                duration.as_nanos().hash(&mut hasher);
            }
        }
    }
    format!("{:016x}", hasher.finish())
}

fn decompress_zstd_to_temp(zip_path: &Path) -> AppResult<PathBuf> {
    let key = hash_key_for_path(zip_path);
    let out_dir = temp_cache_dir();
    fs::create_dir_all(&out_dir)?;
    let out_path = out_dir.join(format!("{key}.mds"));
    if out_path.exists() {
        return Ok(out_path);
    }
    let input = File::open(zip_path)?;
    let mut decoder = zstd::stream::Decoder::new(input)?;
    let mut output = File::create(&out_path)?;
    std::io::copy(&mut decoder, &mut output)
        .map_err(|e| AppError::Invalid(format!("decompressing shard: {e}")))?;
    Ok(out_path)
}

fn resolve_raw_shard_path(root_dir: &Path, shard: &MdsShard) -> AppResult<PathBuf> {
    let raw_path = root_dir.join(&shard.raw_data.basename);
    if raw_path.exists() {
        return Ok(raw_path);
    }

    if let Some(zip_info) = shard.zip_data.as_ref() {
        let zip_path = root_dir.join(&zip_info.basename);
        if zip_path.exists() {
            let kind = compression_kind(shard.compression.as_deref(), &zip_info.basename);
            match kind.as_deref() {
                Some("zstd") => return decompress_zstd_to_temp(&zip_path),
                Some(other) => return Err(AppError::UnsupportedCompression(other.into())),
                None => return Err(AppError::Invalid("missing compression metadata".into())),
            }
        }
    }

    let zstd_candidates = [
        format!("{}.zstd", shard.raw_data.basename),
        format!("{}.zst", shard.raw_data.basename),
    ];
    for candidate in zstd_candidates {
        let zip_path = root_dir.join(&candidate);
        if zip_path.exists() {
            return decompress_zstd_to_temp(&zip_path);
        }
    }

    Err(AppError::Missing(format!(
        "shard data file not found for {}",
        shard.raw_data.basename
    )))
}

fn read_le_u32(buf: &[u8]) -> AppResult<u32> {
    let raw: [u8; 4] = buf.try_into().map_err(|_| AppError::MalformedChunk)?;
    Ok(u32::from_le_bytes(raw))
}

fn read_sample_offsets(fp: &mut File, idx: u32) -> AppResult<(u32, u32)> {
    let offset = (1u64 + idx as u64) * 4;
    fp.seek(SeekFrom::Start(offset))?;
    let mut pair = [0u8; 8];
    fp.read_exact(&mut pair)?;
    let begin = read_le_u32(&pair[0..4])?;
    let end = read_le_u32(&pair[4..8])?;
    if end < begin {
        return Err(AppError::MalformedChunk);
    }
    Ok((begin, end))
}

fn read_variable_sizes(fp: &mut File, begin: u32, shard: &MdsShard) -> AppResult<Vec<u32>> {
    let mut sizes = Vec::with_capacity(shard.column_names.len());
    let var_cols = shard.column_sizes.iter().filter(|s| s.is_none()).count();
    let header_len = var_cols * 4;
    let mut header = vec![0u8; header_len];
    if header_len > 0 {
        fp.seek(SeekFrom::Start(begin as u64))?;
        fp.read_exact(&mut header)?;
    }
    let mut var_idx = 0usize;
    for fixed in shard.column_sizes.iter() {
        if let Some(sz) = fixed {
            sizes.push(*sz);
        } else {
            let start = var_idx * 4;
            let end = start + 4;
            let sz = read_le_u32(&header[start..end])?;
            sizes.push(sz);
            var_idx += 1;
        }
    }
    Ok(sizes)
}

fn field_start_offset(
    begin: u32,
    shard: &MdsShard,
    field_index: usize,
    sizes: &[u32],
) -> AppResult<(u64, u32)> {
    if field_index >= sizes.len() {
        return Err(AppError::Invalid("field index out of range".into()));
    }
    let var_cols = shard.column_sizes.iter().filter(|s| s.is_none()).count();
    let header_len = (var_cols * 4) as u64;
    let mut cursor = begin as u64 + header_len;
    for (idx, sz) in sizes.iter().enumerate() {
        if idx == field_index {
            return Ok((cursor, *sz));
        }
        cursor = cursor
            .checked_add(*sz as u64)
            .ok_or_else(|| AppError::Invalid("field offset overflow".into()))?;
    }
    Err(AppError::MalformedChunk)
}

fn mds_guess_ext(encoding: Option<&str>, data: &[u8]) -> Option<String> {
    let encoding = encoding.unwrap_or("").trim();
    if encoding.is_empty() {
        if let Some(magic) = detect_magic_ext(data) {
            return Some(magic);
        }
        return infer::get(data).map(|t| t.extension().to_string());
    }
    let enc_lower = encoding.to_lowercase();
    let map = [
        ("jpeg", "jpg"),
        ("jpg", "jpg"),
        ("pil", "png"),
        ("png", "png"),
        ("tiff", "tiff"),
        ("str", "txt"),
        ("str_int", "txt"),
        ("str_float", "txt"),
        ("str_decimal", "txt"),
        ("int", "txt"),
        ("int8", "txt"),
        ("int16", "txt"),
        ("int32", "txt"),
        ("int64", "txt"),
        ("uint8", "txt"),
        ("uint16", "txt"),
        ("uint32", "txt"),
        ("uint64", "txt"),
        ("float16", "txt"),
        ("float32", "txt"),
        ("float64", "txt"),
        ("json", "json"),
        ("bytes", "bin"),
        ("pkl", "pkl"),
    ];
    if let Some((_, ext)) = map.iter().find(|(k, _)| *k == enc_lower) {
        if *ext == "bin" {
            if let Some(magic) = detect_magic_ext(data) {
                return Some(magic);
            }
        }
        return Some((*ext).into());
    }
    if enc_lower == "audio" {
        if let Some(magic) = detect_magic_ext(data) {
            return Some(magic);
        }
        return Some("wav".into());
    }
    if let Some((_, subtype)) = enc_lower.split_once(':') {
        let trimmed = subtype.trim().trim_start_matches('.');
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Some(magic) = detect_magic_ext(data) {
        return Some(magic);
    }
    if std::str::from_utf8(data)
        .map(|s| s.trim().len() > 0)
        .unwrap_or(false)
    {
        return Some("txt".into());
    }
    infer::get(data).map(|t| t.extension().to_string())
}

fn detect_magic_ext(data: &[u8]) -> Option<String> {
    if audio::is_sphere_file(data) {
        return Some("sph".into());
    }
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

fn sanitize(input: &str) -> String {
    input
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

fn decode_scalar_to_text(encoding: &str, data: &[u8]) -> Option<String> {
    let enc = encoding.trim().to_lowercase();
    match enc.as_str() {
        "str" | "str_int" | "str_float" | "str_decimal" | "json" => {
            Some(String::from_utf8_lossy(data).to_string())
        }
        "int" | "int64" => {
            if data.len() != 8 {
                return None;
            }
            let raw: [u8; 8] = data.try_into().ok()?;
            Some(i64::from_le_bytes(raw).to_string())
        }
        "int32" => {
            if data.len() != 4 {
                return None;
            }
            let raw: [u8; 4] = data.try_into().ok()?;
            Some(i32::from_le_bytes(raw).to_string())
        }
        "int16" => {
            if data.len() != 2 {
                return None;
            }
            let raw: [u8; 2] = data.try_into().ok()?;
            Some(i16::from_le_bytes(raw).to_string())
        }
        "int8" => (data.len() == 1).then(|| (data[0] as i8).to_string()),
        "uint64" => {
            if data.len() != 8 {
                return None;
            }
            let raw: [u8; 8] = data.try_into().ok()?;
            Some(u64::from_le_bytes(raw).to_string())
        }
        "uint32" => {
            if data.len() != 4 {
                return None;
            }
            let raw: [u8; 4] = data.try_into().ok()?;
            Some(u32::from_le_bytes(raw).to_string())
        }
        "uint16" => {
            if data.len() != 2 {
                return None;
            }
            let raw: [u8; 2] = data.try_into().ok()?;
            Some(u16::from_le_bytes(raw).to_string())
        }
        "uint8" => (data.len() == 1).then(|| data[0].to_string()),
        "float64" => {
            if data.len() != 8 {
                return None;
            }
            let raw: [u8; 8] = data.try_into().ok()?;
            Some(f64::from_le_bytes(raw).to_string())
        }
        "float32" => {
            if data.len() != 4 {
                return None;
            }
            let raw: [u8; 4] = data.try_into().ok()?;
            Some(f32::from_le_bytes(raw).to_string())
        }
        _ => None,
    }
}

#[tauri::command]
pub async fn mosaicml_load_index(index_path: String) -> AppResult<IndexSummary> {
    spawn_blocking(move || mosaicml_load_index_sync(PathBuf::from(index_path)))
        .await
        .map_err(|e| AppError::Task(e.to_string()))?
}

fn mosaicml_load_index_sync(index_path: PathBuf) -> AppResult<IndexSummary> {
    let (root_dir, resolved, index) = parse_index(&index_path)?;
    let first = index
        .shards
        .get(0)
        .ok_or_else(|| AppError::Invalid("index.json contains no shards".into()))?;
    if first.version != 2 {
        return Err(AppError::Invalid(format!(
            "unsupported MDS version: {} (expected 2)",
            first.version
        )));
    }
    if first.format.to_lowercase() != "mds" {
        return Err(AppError::Invalid(format!(
            "unsupported dataset format: {} (expected mds)",
            first.format
        )));
    }
    let data_format = first.column_names.clone();
    let compression = first.compression.clone();
    let config_raw = serde_json::json!({
        "format": "mds",
        "version": first.version,
        "columnNames": first.column_names,
        "columnEncodings": first.column_encodings,
        "columnSizes": first.column_sizes,
        "compression": first.compression,
    });

    let chunks = index
        .shards
        .iter()
        .map(|shard| {
            let raw_path = root_dir.join(&shard.raw_data.basename);
            let mut exists = raw_path.exists();
            let mut bytes = shard.raw_data.bytes;
            if !exists {
                if let Some(zip) = shard.zip_data.as_ref() {
                    let zip_path = root_dir.join(&zip.basename);
                    if zip_path.exists() {
                        exists = true;
                        bytes = zip.bytes;
                    }
                }
                if !exists {
                    let zst = root_dir.join(format!("{}.zst", shard.raw_data.basename));
                    let zstd = root_dir.join(format!("{}.zstd", shard.raw_data.basename));
                    if zst.exists() || zstd.exists() {
                        exists = true;
                    }
                }
            }
            ChunkSummary {
                filename: shard.raw_data.basename.clone(),
                path: raw_path.display().to_string(),
                chunk_size: shard.samples,
                chunk_bytes: bytes,
                dim: None,
                exists,
            }
        })
        .collect();

    Ok(IndexSummary {
        index_path: resolved.display().to_string(),
        root_dir: root_dir.display().to_string(),
        data_format,
        compression,
        chunk_size: None,
        chunk_bytes: None,
        config_raw,
        chunks,
    })
}

#[tauri::command]
pub async fn mosaicml_list_samples(
    index_path: String,
    shard_filename: String,
) -> AppResult<Vec<ItemMeta>> {
    spawn_blocking(move || mosaicml_list_samples_sync(PathBuf::from(index_path), shard_filename))
        .await
        .map_err(|e| AppError::Task(e.to_string()))?
}

fn mosaicml_list_samples_sync(
    index_path: PathBuf,
    shard_filename: String,
) -> AppResult<Vec<ItemMeta>> {
    let (root_dir, _resolved, index) = parse_index(&index_path)?;
    let shard = shard_for_filename(&index, &shard_filename)?;
    let raw_path = resolve_raw_shard_path(&root_dir, shard)?;
    let mut fp = File::open(&raw_path)?;

    let mut num_buf = [0u8; 4];
    fp.seek(SeekFrom::Start(0))?;
    fp.read_exact(&mut num_buf)?;
    let num_in_file = read_le_u32(&num_buf)?;
    let expected = shard.samples;
    let total = expected.min(num_in_file);
    let limit = total.min(MAX_LISTED_SAMPLES);

    let mut items = Vec::with_capacity(limit as usize);
    for idx in 0..limit {
        let (begin, end) = read_sample_offsets(&mut fp, idx)?;
        let sizes = read_variable_sizes(&mut fp, begin, shard)?;
        let fields = sizes
            .iter()
            .enumerate()
            .map(|(field_index, size)| FieldMeta {
                field_index,
                size: *size,
            })
            .collect();
        items.push(ItemMeta {
            item_index: idx,
            total_bytes: (end - begin) as u64,
            fields,
        });
    }
    Ok(items)
}

#[tauri::command]
pub async fn mosaicml_peek_field(
    index_path: String,
    shard_filename: String,
    item_index: u32,
    field_index: usize,
) -> AppResult<FieldPreview> {
    spawn_blocking(move || {
        mosaicml_peek_field_sync(
            PathBuf::from(index_path),
            shard_filename,
            item_index,
            field_index,
        )
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

fn mosaicml_peek_field_sync(
    index_path: PathBuf,
    shard_filename: String,
    item_index: u32,
    field_index: usize,
) -> AppResult<FieldPreview> {
    let (root_dir, _resolved, index) = parse_index(&index_path)?;
    let shard = shard_for_filename(&index, &shard_filename)?;
    let raw_path = resolve_raw_shard_path(&root_dir, shard)?;
    let encoding = shard.column_encodings.get(field_index).map(|s| s.as_str());

    let mut fp = File::open(&raw_path)?;
    let (begin, end) = read_sample_offsets(&mut fp, item_index)?;
    let sizes = read_variable_sizes(&mut fp, begin, shard)?;
    let (field_start, field_size) = field_start_offset(begin, shard, field_index, &sizes)?;
    let available = (end as u64)
        .checked_sub(field_start)
        .ok_or_else(|| AppError::MalformedChunk)?;
    if available < field_size as u64 {
        return Err(AppError::MalformedChunk);
    }

    let should_read_full = matches!(
        encoding.map(|s| s.trim().to_lowercase()).as_deref(),
        Some(
            "int"
                | "int8"
                | "int16"
                | "int32"
                | "int64"
                | "uint8"
                | "uint16"
                | "uint32"
                | "uint64"
                | "float32"
                | "float64"
        )
    );
    let desired = if should_read_full {
        field_size as usize
    } else {
        PREVIEW_BYTES.min(field_size as usize)
    };

    fp.seek(SeekFrom::Start(field_start))?;
    let mut data = vec![0u8; desired];
    fp.read_exact(&mut data)?;

    let preview_text = if let Some(enc) = encoding {
        if should_read_full {
            decode_scalar_to_text(enc, &data).map(|s| s.chars().take(400).collect())
        } else {
            String::from_utf8(data.clone())
                .ok()
                .map(|s| s.chars().take(400).collect())
        }
    } else {
        String::from_utf8(data.clone())
            .ok()
            .map(|s| s.chars().take(400).collect())
    };

    let guessed_ext = mds_guess_ext(encoding, &data);
    let hex_snippet = hex_encode(data.iter().take(48).copied().collect::<Vec<u8>>());
    let is_binary = preview_text.is_none();
    Ok(FieldPreview {
        preview_text,
        hex_snippet,
        guessed_ext,
        is_binary,
        size: field_size,
    })
}

fn read_field_full(
    fp: &mut File,
    shard: &MdsShard,
    item_index: u32,
    field_index: usize,
) -> AppResult<(Vec<u8>, u32)> {
    let (begin, end) = read_sample_offsets(fp, item_index)?;
    let sizes = read_variable_sizes(fp, begin, shard)?;
    let (field_start, field_size) = field_start_offset(begin, shard, field_index, &sizes)?;
    let available = (end as u64)
        .checked_sub(field_start)
        .ok_or_else(|| AppError::MalformedChunk)?;
    if available < field_size as u64 {
        return Err(AppError::MalformedChunk);
    }
    if field_size as u64 > MAX_OPEN_BYTES {
        return Err(AppError::Invalid(format!(
            "field is too large to open ({} bytes, max {})",
            field_size, MAX_OPEN_BYTES
        )));
    }
    fp.seek(SeekFrom::Start(field_start))?;
    let mut data = vec![0u8; field_size as usize];
    fp.read_exact(&mut data)?;
    Ok((data, field_size))
}

#[tauri::command]
pub async fn mosaicml_open_leaf(
    index_path: String,
    shard_filename: String,
    item_index: u32,
    field_index: usize,
    opener_app_path: Option<String>,
) -> AppResult<OpenLeafResponse> {
    spawn_blocking(move || {
        mosaicml_open_leaf_sync(
            PathBuf::from(index_path),
            shard_filename,
            item_index,
            field_index,
            opener_app_path.as_deref(),
        )
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

fn mosaicml_open_leaf_sync(
    index_path: PathBuf,
    shard_filename: String,
    item_index: u32,
    field_index: usize,
    opener_app_path: Option<&str>,
) -> AppResult<OpenLeafResponse> {
    let (root_dir, _resolved, index) = parse_index(&index_path)?;
    let shard = shard_for_filename(&index, &shard_filename)?;
    let raw_path = resolve_raw_shard_path(&root_dir, shard)?;
    let encoding = shard.column_encodings.get(field_index).map(|s| s.as_str());

    let mut fp = File::open(&raw_path)?;
    let (mut data, size) = read_field_full(&mut fp, shard, item_index, field_index)?;
    let ext = mds_guess_ext(encoding, &data).unwrap_or_else(|| "bin".into());

    if let Some(enc) = encoding {
        if let Some(text) = decode_scalar_to_text(enc, &data) {
            if ext == "txt" || ext == "json" {
                data = text.into_bytes();
            }
        }
    }

    let temp_dir = std::env::temp_dir().join("dataset-inspector");
    fs::create_dir_all(&temp_dir)?;
    let base_name = format!(
        "{}-i{}-f{}",
        sanitize(&shard_filename),
        item_index,
        field_index
    );

    let mut out = temp_dir.join(format!("{base_name}.{ext}"));
    fs::write(&out, &data)?;

    let mut ext = ext;
    if ext == "sph" {
        let wav_out = temp_dir.join(format!("{base_name}.wav"));
        match audio::write_sph_as_wav_with_fallback(&data, &out, &wav_out) {
            Ok(()) => {
                out = wav_out;
                ext = "wav".into();
            }
            Err(err) => return Err(AppError::Invalid(format!("sph decode failed: {err}"))),
        }
    }

    let mut opened = false;
    let mut open_error: Option<String> = None;
    if let Some(app_path) = opener_app_path {
        match open_with::open_with_app_detached(&out, app_path) {
            Ok(()) => opened = true,
            Err(err) => open_error = Some(err),
        }
    }
    if !opened {
        if let Err(err) = open::that_detached(&out) {
            open_error = Some(err.to_string());
        } else {
            opened = true;
        }
    }

    let base = format!("{} ({} bytes)", out.display(), size);
    let mut message = base;
    let needs_opener = !opened && open_error.is_some();
    if needs_opener {
        message.push_str(" · no default app found, choose an app to open it");
    }

    Ok(OpenLeafResponse {
        path: out.display().to_string(),
        size,
        ext,
        opened,
        needs_opener,
        message,
    })
}

#[tauri::command]
pub async fn mosaicml_prepare_audio_preview(
    index_path: String,
    shard_filename: String,
    item_index: u32,
    field_index: usize,
) -> AppResult<PreparedFileResponse> {
    spawn_blocking(move || {
        mosaicml_prepare_audio_preview_sync(
            PathBuf::from(index_path),
            shard_filename,
            item_index,
            field_index,
        )
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

fn mosaicml_prepare_audio_preview_sync(
    index_path: PathBuf,
    shard_filename: String,
    item_index: u32,
    field_index: usize,
) -> AppResult<PreparedFileResponse> {
    let (root_dir, _resolved, index) = parse_index(&index_path)?;
    let shard = shard_for_filename(&index, &shard_filename)?;
    let raw_path = resolve_raw_shard_path(&root_dir, shard)?;
    let encoding = shard.column_encodings.get(field_index).map(|s| s.as_str());

    let mut fp = File::open(&raw_path)?;
    let (data, size) = read_field_full(&mut fp, shard, item_index, field_index)?;
    let ext = mds_guess_ext(encoding, &data).unwrap_or_else(|| "bin".into());

    let temp_dir = std::env::temp_dir().join("dataset-inspector");
    fs::create_dir_all(&temp_dir)?;
    let base_name = format!(
        "{}-i{}-f{}",
        sanitize(&shard_filename),
        item_index,
        field_index
    );
    let mut out = temp_dir.join(format!("{base_name}.{ext}"));
    fs::write(&out, &data)?;

    let mut ext = ext;
    if ext == "sph" {
        let wav_out = temp_dir.join(format!("{base_name}.wav"));
        audio::write_sph_as_wav_with_fallback(&data, &out, &wav_out)
            .map_err(|e| AppError::Invalid(format!("sph decode failed: {e}")))?;
        out = wav_out;
        ext = "wav".into();
    }

    Ok(PreparedFileResponse {
        path: out.display().to_string(),
        size,
        ext,
    })
}

pub fn detect_mds_index_path(path: &Path) -> Option<String> {
    let resolved = resolve_index_path(path).ok()?;
    let bytes = read_index_bytes(&resolved).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let shards = value.get("shards")?.as_array()?;
    let first = shards.get(0)?.as_object()?;
    let format = first.get("format")?.as_str()?.to_lowercase();
    if format != "mds" {
        return None;
    }
    Some(resolved.display().to_string())
}
