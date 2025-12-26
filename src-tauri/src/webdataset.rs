use hex::encode as hex_encode;
use serde::Serialize;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tauri::async_runtime::spawn_blocking;

use crate::app_error::{AppError, AppResult};
use crate::audio;
use crate::ipc_types::{FieldPreview, OpenLeafResponse, PreparedFileResponse};
use crate::mosaicml;
use crate::open_with;

const PREVIEW_BYTES: usize = 16 * 1024;
const PREVIEW_TEXT_CHARS: usize = 8 * 1024;
const MAX_LISTED_SAMPLES: usize = 5000;
const MAX_OPEN_BYTES: u64 = 256 * 1024 * 1024;
const MAX_TAR_META_BYTES: u64 = 1024 * 1024;

fn preview_utf8_text(data: &[u8]) -> Option<String> {
    let raw = match std::str::from_utf8(data) {
        Ok(text) => text,
        Err(err) if err.error_len().is_none() => {
            std::str::from_utf8(&data[..err.valid_up_to()]).ok()?
        }
        Err(_) => return None,
    };
    Some(raw.chars().take(PREVIEW_TEXT_CHARS).collect())
}

#[derive(Clone, Default)]
pub struct WdsScanCache {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<ShardScanState>>>>>,
}

impl WdsScanCache {
    fn get_or_create(&self, shard_path: &Path) -> AppResult<Arc<Mutex<ShardScanState>>> {
        let key = shard_path.display().to_string();
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| AppError::Task("wds scan cache lock poisoned".into()))?;
        if let Some(existing) = guard.get(&key) {
            return Ok(existing.clone());
        }
        let created = Arc::new(Mutex::new(ShardScanState::new(shard_path.to_path_buf())?));
        guard.insert(key, created.clone());
        Ok(created)
    }
}

struct ShardScanState {
    shard_path: PathBuf,
    tar: TarStream<Box<dyn Read + Send>>,
    done: bool,
    samples: Vec<WdsSampleInfo>,
    current_key: Option<String>,
    current_fields: Vec<WdsFieldInfo>,
    current_bytes: u64,
    current_sample_index: u32,
}

struct TarStream<R: Read> {
    reader: R,
    pending_longname: Option<String>,
    pending_pax_path: Option<String>,
}

struct TarFileMeta {
    path: String,
    size: u64,
}

impl<R: Read> TarStream<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            pending_longname: None,
            pending_pax_path: None,
        }
    }

    fn next_file(&mut self) -> io::Result<Option<TarFileMeta>> {
        loop {
            let Some(header) = read_tar_header_block(&mut self.reader)? else {
                return Ok(None);
            };
            if header.iter().all(|b| *b == 0) {
                // tar EOF marker: two consecutive 512-byte zero blocks.
                let Some(next) = read_tar_header_block(&mut self.reader)? else {
                    return Ok(None);
                };
                if next.iter().all(|b| *b == 0) {
                    return Ok(None);
                }
                if let Some(item) = self.process_header(next)? {
                    return Ok(Some(item));
                }
                continue;
            }
            if let Some(item) = self.process_header(header)? {
                return Ok(Some(item));
            }
        }
    }

    fn process_header(&mut self, header: [u8; 512]) -> io::Result<Option<TarFileMeta>> {
        let size = parse_tar_size(&header).unwrap_or(0);
        let typeflag = header[156];

        // GNU long name (next entry path stored in the data section).
        if typeflag == b'L' {
            if size > MAX_TAR_META_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "tar longname entry is too large",
                ));
            }
            let data = read_tar_data(&mut self.reader, size)?;
            self.pending_longname = Some(parse_tar_string(&data));
            skip_tar_padding(&mut self.reader, size)?;
            return Ok(None);
        }

        // PAX extended headers (path override for next entry).
        if typeflag == b'x' || typeflag == b'g' {
            if size > MAX_TAR_META_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "tar pax entry is too large",
                ));
            }
            let data = read_tar_data(&mut self.reader, size)?;
            if let Some(path) = parse_pax_path(&data) {
                self.pending_pax_path = Some(path);
            }
            skip_tar_padding(&mut self.reader, size)?;
            return Ok(None);
        }

        let mut path = if let Some(longname) = self.pending_longname.take() {
            longname
        } else {
            parse_ustar_path(&header)
        };
        if let Some(pax_path) = self.pending_pax_path.take() {
            path = pax_path;
        }
        let normalized = normalize_member_path_str(&path);

        // Skip entry data so the stream is positioned at the next header.
        skip_tar_data(&mut self.reader, size)?;

        if typeflag == b'5' {
            return Ok(None);
        }
        if normalized.is_empty() {
            return Ok(None);
        }

        Ok(Some(TarFileMeta {
            path: normalized,
            size,
        }))
    }
}

fn read_tar_header_block<R: Read>(reader: &mut R) -> io::Result<Option<[u8; 512]>> {
    let mut buf = [0u8; 512];
    match reader.read_exact(&mut buf) {
        Ok(()) => Ok(Some(buf)),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

fn read_tar_data<R: Read>(reader: &mut R, size: u64) -> io::Result<Vec<u8>> {
    let size: usize = size
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "tar entry is too large"))?;
    let mut out = vec![0u8; size];
    reader.read_exact(&mut out)?;
    Ok(out)
}

fn skip_tar_data<R: Read>(reader: &mut R, size: u64) -> io::Result<()> {
    if size == 0 {
        return Ok(());
    }
    io::copy(&mut reader.take(size), &mut io::sink())?;
    skip_tar_padding(reader, size)
}

fn skip_tar_padding<R: Read>(reader: &mut R, size: u64) -> io::Result<()> {
    let pad = (512 - (size % 512)) % 512;
    if pad == 0 {
        return Ok(());
    }
    io::copy(&mut reader.take(pad), &mut io::sink())?;
    Ok(())
}

fn parse_tar_size(header: &[u8; 512]) -> Option<u64> {
    parse_tar_octal(&header[124..136])
}

fn parse_tar_octal(slice: &[u8]) -> Option<u64> {
    let cleaned: Vec<u8> = slice
        .iter()
        .copied()
        .take_while(|b| *b != 0)
        .filter(|b| *b != b' ' && *b != b'\n' && *b != b'\r' && *b != b'\t')
        .collect();
    if cleaned.is_empty() {
        return Some(0);
    }
    let s = std::str::from_utf8(&cleaned).ok()?.trim();
    if s.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(s, 8).ok()
}

fn parse_tar_string(data: &[u8]) -> String {
    let trimmed = data
        .iter()
        .copied()
        .take_while(|b| *b != 0)
        .collect::<Vec<u8>>();
    String::from_utf8_lossy(&trimmed)
        .trim()
        .trim_end_matches('\n')
        .trim_end_matches('\r')
        .to_string()
}

fn parse_pax_path(data: &[u8]) -> Option<String> {
    let s = String::from_utf8_lossy(data);
    for line in s.lines() {
        let Some((_, rest)) = line.split_once(' ') else {
            continue;
        };
        let Some((key, value)) = rest.split_once('=') else {
            continue;
        };
        if key != "path" {
            continue;
        }
        let v = value.trim().trim_end_matches('\u{0}').to_string();
        if !v.is_empty() {
            return Some(v);
        }
    }
    None
}

fn parse_ustar_path(header: &[u8; 512]) -> String {
    let name = parse_tar_string(&header[0..100]);
    let prefix = parse_tar_string(&header[345..500]);
    if prefix.is_empty() {
        name
    } else if name.is_empty() {
        prefix
    } else {
        format!("{}/{}", prefix, name)
    }
}

impl ShardScanState {
    fn new(shard_path: PathBuf) -> AppResult<Self> {
        let reader = open_shard_reader(&shard_path)?;
        Ok(Self {
            shard_path,
            tar: TarStream::new(reader),
            done: false,
            samples: Vec::new(),
            current_key: None,
            current_fields: Vec::new(),
            current_bytes: 0,
            current_sample_index: 0,
        })
    }

    fn ensure_scanned(&mut self, target_count: u32, compute_total: bool) -> AppResult<()> {
        if self.done {
            return Ok(());
        }

        if !compute_total && (self.samples.len() as u32) >= target_count {
            return Ok(());
        }
        let mut stopped_early = false;

        while !self.done {
            let next = self
                .tar
                .next_file()
                .map_err(|e| AppError::Task(format!("wds tar scan failed: {e}")))?;
            let Some(next) = next else {
                self.done = true;
                break;
            };

            let member_path = next.path;
            let (key, field_name) = split_sample_key(&member_path);
            let size = next.size;

            if self.current_key.as_deref() != Some(&key) {
                flush_sample_parts(
                    self.current_key.take(),
                    &mut self.current_fields,
                    &mut self.current_bytes,
                    &mut self.current_sample_index,
                    &mut self.samples,
                );
                self.current_key = Some(key);
            }

            self.current_bytes = self.current_bytes.saturating_add(size);
            self.current_fields.push(WdsFieldInfo {
                name: field_name,
                member_path,
                size,
            });

            if !compute_total && (self.samples.len() as u32) >= target_count {
                stopped_early = true;
                break;
            }
        }

        if self.done && !stopped_early {
            flush_sample_parts(
                self.current_key.take(),
                &mut self.current_fields,
                &mut self.current_bytes,
                &mut self.current_sample_index,
                &mut self.samples,
            );
        }
        Ok(())
    }
}

fn flush_sample_parts(
    key: Option<String>,
    current_fields: &mut Vec<WdsFieldInfo>,
    current_bytes: &mut u64,
    current_sample_index: &mut u32,
    samples: &mut Vec<WdsSampleInfo>,
) {
    let Some(key) = key else {
        current_fields.clear();
        *current_bytes = 0;
        return;
    };

    let sample_index = *current_sample_index;
    let mut out_fields = std::mem::take(current_fields);
    out_fields.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then_with(|| a.member_path.cmp(&b.member_path))
    });
    samples.push(WdsSampleInfo {
        sample_index,
        key,
        total_bytes: *current_bytes,
        fields: out_fields,
    });
    *current_bytes = 0;
    *current_sample_index = (*current_sample_index).saturating_add(1);
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WdsShardSummary {
    pub filename: String,
    pub path: String,
    pub bytes: u64,
    pub exists: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WdsDirSummary {
    pub dir_path: String,
    pub shards: Vec<WdsShardSummary>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WdsFieldInfo {
    pub name: String,
    pub member_path: String,
    pub size: u64,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WdsSampleInfo {
    pub sample_index: u32,
    pub key: String,
    pub total_bytes: u64,
    pub fields: Vec<WdsFieldInfo>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WdsSampleListResponse {
    pub offset: u32,
    pub length: u32,
    pub num_samples_total: Option<u32>,
    pub partial: bool,
    pub samples: Vec<WdsSampleInfo>,
}

#[derive(Serialize)]
#[serde(tag = "kind")]
pub enum LocalDatasetDetectResponse {
    #[serde(rename = "litdata-index")]
    LitdataIndex {
        #[serde(rename = "indexPath")]
        index_path: String,
    },
    #[serde(rename = "mds-index")]
    MdsIndex {
        #[serde(rename = "indexPath")]
        index_path: String,
    },
    #[serde(rename = "webdataset-dir")]
    WebdatasetDir {
        #[serde(rename = "dirPath")]
        dir_path: String,
    },
}

#[tauri::command]
pub async fn detect_local_dataset(path: String) -> AppResult<LocalDatasetDetectResponse> {
    spawn_blocking(move || detect_local_dataset_sync(PathBuf::from(path)))
        .await
        .map_err(|e| AppError::Task(e.to_string()))?
}

fn detect_local_dataset_sync(path: PathBuf) -> AppResult<LocalDatasetDetectResponse> {
    let trimmed = path.to_string_lossy().trim().to_string();
    if trimmed.is_empty() {
        return Err(AppError::Invalid("path is empty".into()));
    }

    if path.is_file() {
        let filename = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if looks_like_wds_shard(filename) {
            let dir = path
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            return Ok(LocalDatasetDetectResponse::WebdatasetDir {
                dir_path: dir.display().to_string(),
            });
        }
        if looks_like_mds_shard(filename) {
            let dir = path
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            if let Some(index_path) = mosaicml::detect_mds_index_path(&dir) {
                return Ok(LocalDatasetDetectResponse::MdsIndex { index_path });
            }
            return Err(AppError::Missing(format!(
                "no MosaicML MDS index.json found next to {}",
                path.display()
            )));
        }
        if filename.to_lowercase().contains("index.json") {
            if let Some(index_path) = mosaicml::detect_mds_index_path(&path) {
                return Ok(LocalDatasetDetectResponse::MdsIndex { index_path });
            }
        }
        if looks_like_litdata_file(filename) {
            return Ok(LocalDatasetDetectResponse::LitdataIndex {
                index_path: path.display().to_string(),
            });
        }
    }

    if path.is_dir() {
        if let Some(index) = find_litdata_index_in_dir(&path) {
            if let Some(index_path) = mosaicml::detect_mds_index_path(&index) {
                return Ok(LocalDatasetDetectResponse::MdsIndex { index_path });
            }
            return Ok(LocalDatasetDetectResponse::LitdataIndex {
                index_path: index.display().to_string(),
            });
        }
        if has_wds_shards_in_dir(&path) {
            return Ok(LocalDatasetDetectResponse::WebdatasetDir {
                dir_path: path.display().to_string(),
            });
        }
        return Err(AppError::Missing(format!(
            "no LitData index.json, MDS index.json, or WebDataset shard found in {}",
            path.display()
        )));
    }

    Err(AppError::Missing(format!(
        "path does not exist: {}",
        path.display()
    )))
}

fn looks_like_litdata_file(filename: &str) -> bool {
    let name = filename.to_lowercase();
    if name.contains("index.json") {
        return true;
    }
    if name.ends_with(".bin") || name.contains(".bin.") {
        return true;
    }
    if name.ends_with(".zst") && !looks_like_wds_shard(&name) {
        return true;
    }
    false
}

fn find_litdata_index_in_dir(dir: &Path) -> Option<PathBuf> {
    let candidates = [
        "index.json",
        "index.json.zstd",
        "index.json.zst",
        "0.index.json",
        "0.index.json.zstd",
        "0.index.json.zst",
    ];
    for name in candidates {
        let p = dir.join(name);
        if p.exists() {
            return Some(p);
        }
    }
    let mut globbed: Vec<PathBuf> = std::fs::read_dir(dir)
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

fn has_wds_shards_in_dir(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok().map(|e2| e2.path()))
        .any(|p| {
            p.file_name()
                .and_then(|f| f.to_str())
                .map(looks_like_wds_shard)
                .unwrap_or(false)
        })
}

fn looks_like_wds_shard(filename: &str) -> bool {
    let name = filename.to_lowercase();
    name.ends_with(".tar")
        || name.ends_with(".tar.gz")
        || name.ends_with(".tgz")
        || name.ends_with(".tar.zst")
        || name.ends_with(".tar.zstd")
}

fn looks_like_mds_shard(filename: &str) -> bool {
    let name = filename.to_lowercase();
    name.ends_with(".mds") || name.ends_with(".mds.zst") || name.ends_with(".mds.zstd")
}

#[tauri::command]
pub async fn wds_load_dir(dir_path: String) -> AppResult<WdsDirSummary> {
    spawn_blocking(move || wds_load_dir_sync(PathBuf::from(dir_path)))
        .await
        .map_err(|e| AppError::Task(e.to_string()))?
}

fn wds_load_dir_sync(dir_path: PathBuf) -> AppResult<WdsDirSummary> {
    let (dir, shards) = resolve_shard_dir_and_list(&dir_path)?;
    Ok(WdsDirSummary {
        dir_path: dir.display().to_string(),
        shards,
    })
}

#[tauri::command]
pub async fn wds_list_samples(
    dir_path: String,
    shard_filename: String,
    offset: Option<u32>,
    length: Option<u32>,
    compute_total: Option<bool>,
    cache: tauri::State<'_, WdsScanCache>,
) -> AppResult<WdsSampleListResponse> {
    let cache_handle = (*cache).clone();
    spawn_blocking(move || {
        wds_list_samples_sync(
            PathBuf::from(dir_path),
            shard_filename,
            offset,
            length,
            compute_total,
            &cache_handle,
        )
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

fn wds_list_samples_sync(
    dir_path: PathBuf,
    shard_filename: String,
    offset: Option<u32>,
    length: Option<u32>,
    compute_total: Option<bool>,
    cache: &WdsScanCache,
) -> AppResult<WdsSampleListResponse> {
    let (dir, _) = resolve_shard_dir_and_list(&dir_path)?;
    let shard_filename = shard_filename.trim().to_string();
    if shard_filename.is_empty() {
        return Err(AppError::Invalid("shard filename is empty".into()));
    }
    let shard_path = dir.join(&shard_filename);
    if !shard_path.exists() {
        return Err(AppError::Missing(format!(
            "shard does not exist: {}",
            shard_path.display()
        )));
    }
    if !shard_path.is_file() {
        return Err(AppError::Invalid("shard path is not a file".into()));
    }
    if !shard_path
        .file_name()
        .and_then(|s| s.to_str())
        .map(looks_like_wds_shard)
        .unwrap_or(false)
    {
        return Err(AppError::Invalid(
            "file is not a supported WebDataset shard".into(),
        ));
    }

    let offset = offset.unwrap_or(0);
    let length = length.unwrap_or(200).max(1).min(MAX_LISTED_SAMPLES as u32);
    let compute_total = compute_total.unwrap_or(false);

    let state = cache.get_or_create(&shard_path)?;
    let mut guard = state
        .lock()
        .map_err(|_| AppError::Task("wds shard scan lock poisoned".into()))?;
    if guard.shard_path != shard_path {
        return Err(AppError::Task("wds shard scan cache mismatch".into()));
    }
    let target = offset.saturating_add(length);
    guard.ensure_scanned(target, compute_total)?;

    let total = if guard.done {
        Some(guard.current_sample_index)
    } else {
        None
    };
    let start = offset as usize;
    let end = (offset.saturating_add(length) as usize).min(guard.samples.len());
    let page = if start >= guard.samples.len() {
        Vec::new()
    } else {
        guard.samples[start..end].to_vec()
    };

    Ok(WdsSampleListResponse {
        offset,
        length,
        num_samples_total: total,
        partial: !guard.done,
        samples: page,
    })
}

#[tauri::command]
pub async fn wds_peek_member(
    dir_path: String,
    shard_filename: String,
    member_path: String,
) -> AppResult<FieldPreview> {
    spawn_blocking(move || {
        wds_peek_member_sync(PathBuf::from(dir_path), shard_filename, member_path)
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

fn wds_peek_member_sync(
    dir_path: PathBuf,
    shard_filename: String,
    member_path: String,
) -> AppResult<FieldPreview> {
    let shard_path = resolve_shard_path(&dir_path, &shard_filename)?;
    let member_path = member_path.trim().to_string();
    if member_path.is_empty() {
        return Err(AppError::Invalid("member path is empty".into()));
    }

    let reader = open_shard_reader(&shard_path)?;
    let mut archive = tar::Archive::new(reader);
    let normalized = normalize_member_path_str(&member_path);

    for entry in archive.entries()? {
        let entry = entry?;
        if entry.header().entry_type().is_dir() {
            continue;
        }
        let current = normalize_member_path(&entry.path()?);
        if current != normalized {
            continue;
        }
        let size = entry.size();
        let mut buf = Vec::new();
        entry.take(PREVIEW_BYTES as u64).read_to_end(&mut buf)?;

        let preview_text = preview_utf8_text(&buf);
        let is_binary = preview_text.is_none();
        let guessed_ext = guess_ext_from_member(&normalized, &buf);
        let hex_snippet = hex_encode(buf.iter().take(48).copied().collect::<Vec<u8>>());
        return Ok(FieldPreview {
            preview_text,
            hex_snippet,
            guessed_ext,
            is_binary,
            size: size.min(u32::MAX as u64) as u32,
        });
    }

    Err(AppError::Missing(format!(
        "member not found in shard: {member_path}"
    )))
}

#[tauri::command]
pub async fn wds_open_member(
    dir_path: String,
    shard_filename: String,
    member_path: String,
    opener_app_path: Option<String>,
) -> AppResult<OpenLeafResponse> {
    spawn_blocking(move || {
        wds_open_member_sync(
            PathBuf::from(dir_path),
            shard_filename,
            member_path,
            opener_app_path.as_deref(),
        )
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

fn wds_open_member_sync(
    dir_path: PathBuf,
    shard_filename: String,
    member_path: String,
    opener_app_path: Option<&str>,
) -> AppResult<OpenLeafResponse> {
    let shard_path = resolve_shard_path(&dir_path, &shard_filename)?;
    let member_path = member_path.trim().to_string();
    if member_path.is_empty() {
        return Err(AppError::Invalid("member path is empty".into()));
    }
    let normalized = normalize_member_path_str(&member_path);
    let (data, size) = read_member_bytes(&shard_path, &normalized, None)?;
    if size > MAX_OPEN_BYTES {
        return Err(AppError::Invalid(format!(
            "member too large to open ({} bytes)",
            size
        )));
    }
    let guessed_ext = guess_ext_from_member(&normalized, &data).unwrap_or_else(|| "bin".into());

    let temp_dir = std::env::temp_dir().join("dataset-inspector");
    fs::create_dir_all(&temp_dir)?;
    let base_name = format!("{}-{}", sanitize(&shard_filename), sanitize(&normalized));
    let mut out = temp_dir.join(format!("{base_name}.{guessed_ext}"));
    fs::write(&out, &data)?;

    // Default `.sph` support: decode to a WAV and open that.
    let mut ext = guessed_ext;
    if ext == "sph" {
        let wav_out = temp_dir.join(format!("{base_name}.wav"));
        match audio::write_sph_as_wav_with_fallback(&data, &out, &wav_out) {
            Ok(()) => {
                out = wav_out;
                ext = "wav".into();
            }
            Err(err) => {
                let base = format!("{} ({} bytes)", out.display(), size);
                return Ok(OpenLeafResponse {
                    path: out.display().to_string(),
                    size: size.min(u32::MAX as u64) as u32,
                    ext,
                    opened: false,
                    needs_opener: true,
                    message: format!(
                        "{base} · sph decode failed: {err} · choose an app to open it"
                    ),
                });
            }
        }
    }

    let mut opened = false;
    let mut open_error = None::<String>;
    if let Some(app_path) = opener_app_path {
        match open_with::open_with_app_detached(&out, app_path) {
            Ok(()) => opened = true,
            Err(err) => open_error = Some(err),
        }
    } else {
        match open::that_detached(&out) {
            Ok(()) => opened = true,
            Err(err) => open_error = Some(err.to_string()),
        }
    }

    let needs_opener = !opened;
    let message = if opened {
        format!("Opened {} ({})", out.display(), size)
    } else {
        let detail = open_error.unwrap_or_else(|| "unknown error".into());
        format!("Could not open {} · {detail}", out.display())
    };

    Ok(OpenLeafResponse {
        path: out.display().to_string(),
        size: size.min(u32::MAX as u64) as u32,
        ext,
        opened,
        needs_opener,
        message,
    })
}

#[tauri::command]
pub async fn wds_prepare_audio_preview(
    dir_path: String,
    shard_filename: String,
    member_path: String,
) -> AppResult<PreparedFileResponse> {
    spawn_blocking(move || {
        wds_prepare_audio_preview_sync(PathBuf::from(dir_path), shard_filename, member_path)
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

fn wds_prepare_audio_preview_sync(
    dir_path: PathBuf,
    shard_filename: String,
    member_path: String,
) -> AppResult<PreparedFileResponse> {
    let shard_path = resolve_shard_path(&dir_path, &shard_filename)?;
    let member_path = member_path.trim().to_string();
    if member_path.is_empty() {
        return Err(AppError::Invalid("member path is empty".into()));
    }
    let normalized = normalize_member_path_str(&member_path);
    let (data, size) = read_member_bytes(&shard_path, &normalized, None)?;
    if size > MAX_OPEN_BYTES {
        return Err(AppError::Invalid(format!(
            "member too large to preview ({} bytes)",
            size
        )));
    }
    let guessed_ext = guess_ext_from_member(&normalized, &data).unwrap_or_else(|| "bin".into());

    let temp_dir = std::env::temp_dir().join("dataset-inspector");
    fs::create_dir_all(&temp_dir)?;
    let base_name = format!("{}-{}", sanitize(&shard_filename), sanitize(&normalized));

    let mut out = temp_dir.join(format!("{base_name}.{guessed_ext}"));
    fs::write(&out, &data)?;

    let mut ext = guessed_ext;
    if ext == "sph" {
        let wav_out = temp_dir.join(format!("{base_name}.wav"));
        audio::write_sph_as_wav_with_fallback(&data, &out, &wav_out)
            .map_err(|e| AppError::Invalid(format!("sph decode failed: {e}")))?;
        out = wav_out;
        ext = "wav".into();
    }

    Ok(PreparedFileResponse {
        path: out.display().to_string(),
        size: size.min(u32::MAX as u64) as u32,
        ext,
    })
}

fn resolve_shard_dir_and_list(dir_path: &Path) -> AppResult<(PathBuf, Vec<WdsShardSummary>)> {
    if dir_path.is_file() {
        let filename = dir_path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if !looks_like_wds_shard(filename) {
            return Err(AppError::Invalid(
                "file is not a supported WebDataset shard".into(),
            ));
        }
        let dir = dir_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let meta = fs::metadata(dir_path)?;
        return Ok((
            dir,
            vec![WdsShardSummary {
                filename: filename.to_string(),
                path: dir_path.display().to_string(),
                bytes: meta.len(),
                exists: true,
            }],
        ));
    }

    if !dir_path.exists() {
        return Err(AppError::Missing(format!(
            "directory does not exist: {}",
            dir_path.display()
        )));
    }
    if !dir_path.is_dir() {
        return Err(AppError::Invalid("path is not a directory".into()));
    }

    let mut shards: Vec<WdsShardSummary> = fs::read_dir(dir_path)?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let p = e.path();
            let filename = p.file_name()?.to_str()?.to_string();
            if !looks_like_wds_shard(&filename) {
                return None;
            }
            let bytes = p.metadata().ok().map(|m| m.len()).unwrap_or(0);
            Some(WdsShardSummary {
                filename,
                path: p.display().to_string(),
                bytes,
                exists: p.exists(),
            })
        })
        .collect();
    shards.sort_by(|a, b| a.filename.cmp(&b.filename));

    Ok((dir_path.to_path_buf(), shards))
}

fn resolve_shard_path(dir_path: &Path, shard_filename: &str) -> AppResult<PathBuf> {
    let (dir, _) = resolve_shard_dir_and_list(dir_path)?;
    let shard_filename = shard_filename.trim();
    if shard_filename.is_empty() {
        return Err(AppError::Invalid("shard filename is empty".into()));
    }
    if !looks_like_wds_shard(shard_filename) {
        return Err(AppError::Invalid(
            "file is not a supported WebDataset shard".into(),
        ));
    }
    let shard_path = dir.join(shard_filename);
    if !shard_path.exists() {
        return Err(AppError::Missing(format!(
            "shard does not exist: {}",
            shard_path.display()
        )));
    }
    Ok(shard_path)
}

fn open_shard_reader(shard_path: &Path) -> AppResult<Box<dyn Read + Send>> {
    let file = File::open(shard_path)?;
    let filename = shard_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();

    if filename.ends_with(".tar.gz") || filename.ends_with(".tgz") {
        return Ok(Box::new(flate2::read::GzDecoder::new(file)));
    }
    if filename.ends_with(".tar.zst") || filename.ends_with(".tar.zstd") {
        let decoder = zstd::stream::read::Decoder::new(file)?;
        return Ok(Box::new(decoder));
    }
    Ok(Box::new(file))
}

fn normalize_member_path(path: &Path) -> String {
    normalize_member_path_str(&path.to_string_lossy())
}

fn normalize_member_path_str(path: &str) -> String {
    path.trim()
        .trim_start_matches("./")
        .trim_start_matches('/')
        .replace('\\', "/")
}

fn split_sample_key(member_path: &str) -> (String, String) {
    let normalized = normalize_member_path_str(member_path);
    let (dir, base) = match normalized.rsplit_once('/') {
        Some((d, b)) => (d, b),
        None => ("", normalized.as_str()),
    };
    let (base_prefix, suffix) = match base.split_once('.') {
        Some((prefix, rest)) if !prefix.is_empty() && !rest.is_empty() => (prefix, rest),
        _ => (base, ""),
    };
    let key = if dir.is_empty() {
        base_prefix.to_string()
    } else {
        format!("{dir}/{base_prefix}")
    };
    let field_name = if suffix.is_empty() {
        "bin".into()
    } else {
        suffix.to_lowercase()
    };
    (key, field_name)
}

fn guess_ext_from_member(member_path: &str, data: &[u8]) -> Option<String> {
    let ext = Path::new(member_path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.trim().trim_start_matches('.').to_lowercase())
        .filter(|s| !s.is_empty());
    if ext.is_some() {
        return ext;
    }
    detect_magic_ext(data).or_else(|| infer::get(data).map(|t| t.extension().to_string()))
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

fn read_member_bytes(
    shard_path: &Path,
    member_path: &str,
    limit: Option<usize>,
) -> AppResult<(Vec<u8>, u64)> {
    let reader = open_shard_reader(shard_path)?;
    let mut archive = tar::Archive::new(reader);
    let normalized = normalize_member_path_str(member_path);
    for entry in archive.entries()? {
        let entry = entry?;
        if entry.header().entry_type().is_dir() {
            continue;
        }
        let current = normalize_member_path(&entry.path()?);
        if current != normalized {
            continue;
        }
        let size = entry.size();
        let read_limit = limit.map(|v| v as u64).unwrap_or(size);
        let mut buf = Vec::new();
        entry.take(read_limit).read_to_end(&mut buf)?;
        return Ok((buf, size));
    }
    Err(AppError::Missing(format!(
        "member not found in shard: {member_path}"
    )))
}
