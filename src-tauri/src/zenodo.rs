use base64::Engine;
use hex::encode as hex_encode;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, io::Read};
use tauri::State;
use url::Url;

use crate::app_error::{AppError, AppResult};
use crate::ipc_types::{FieldPreview, InlineMediaResponse, OpenLeafResponse};
use crate::open_with;

const USER_AGENT: &str = "dataset-inspector/1.2.0 (tauri)";
const REQUEST_TIMEOUT_SECS: u64 = 30;
const PEEK_BYTES: usize = 64 * 1024;
const MAX_INLINE_DOWNLOAD_BYTES: u64 = 50 * 1024 * 1024;
const ZIP_TAIL_INITIAL_BYTES: u64 = 1024 * 1024;
const ZIP_TAIL_MAX_BYTES: u64 = 8 * 1024 * 1024;
const ZIP_MAX_CENTRAL_DIR_BYTES: u64 = 64 * 1024 * 1024;
const ZIP_PREVIEW_MAX_COMPRESSED_BYTES: u64 = 8 * 1024 * 1024;
const ZIP_INLINE_MEDIA_MAX_BYTES: u64 = 128 * 1024 * 1024;
const TAR_MAX_ENTRIES: usize = 250_000;
const TAR_INLINE_MEDIA_MAX_BYTES: u64 = 128 * 1024 * 1024;
const TAR_DEFAULT_PAGE_SIZE: u32 = 25;
const TAR_MAX_PAGE_SIZE: u32 = 200;
const MAX_TAR_META_BYTES: u64 = 1024 * 1024;
const TAR_MEDIA_CACHE_ITEM_MAX_BYTES: u64 = 32 * 1024 * 1024;
const TAR_MEDIA_CACHE_TOTAL_MAX_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone)]
pub struct ZenodoClient {
    http: reqwest::Client,
}

#[derive(Clone, Default)]
pub struct ZenodoZipIndexCache(Arc<Mutex<HashMap<String, Arc<ZipIndex>>>>);

#[derive(Clone)]
struct ZipIndex {
    entries: Vec<ZipEntryIndex>,
}

#[derive(Clone)]
struct ZipEntryIndex {
    name: String,
    method: u16,
    flags: u16,
    compressed_size: u64,
    uncompressed_size: u64,
    local_header_offset: u64,
    is_dir: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZenodoZipEntrySummary {
    name: String,
    method: u16,
    compressed_size: u64,
    uncompressed_size: u64,
    is_dir: bool,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZenodoTarEntrySummary {
    name: String,
    size: u64,
    is_dir: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZenodoTarEntryListResponse {
    offset: u32,
    length: u32,
    entries: Vec<ZenodoTarEntrySummary>,
    partial: bool,
    num_entries_total: Option<u32>,
}

#[derive(Clone, Default)]
pub struct ZenodoTarScanCache {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<ZenodoTarScanState>>>>>,
}

impl ZenodoTarScanCache {
    fn get_or_create(
        &self,
        content_url: &str,
        filename: &str,
    ) -> AppResult<Arc<Mutex<ZenodoTarScanState>>> {
        let key = content_url.trim().to_string();
        if key.is_empty() {
            return Err(AppError::Invalid("Missing content URL.".into()));
        }
        let filename = filename.trim().to_string();
        if filename.is_empty() {
            return Err(AppError::Invalid("Missing filename.".into()));
        }
        if !looks_like_tar(&filename) {
            return Err(AppError::Invalid(
                "Selected file is not a supported TAR archive.".into(),
            ));
        }

        let mut guard = self
            .inner
            .lock()
            .map_err(|_| AppError::Task("tar scan cache lock poisoned".into()))?;
        if let Some(existing) = guard.get(&key) {
            return Ok(existing.clone());
        }

        let url = Url::parse(&key)
            .map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
        if !allowed_content_url(&url) {
            return Err(AppError::Invalid("Blocked content URL.".into()));
        }

        let created = Arc::new(Mutex::new(ZenodoTarScanState::new(url, filename)?));
        guard.insert(key, created.clone());
        Ok(created)
    }
}

struct ZenodoTarScanState {
    tar: TarStream<Box<dyn Read + Send>>,
    done: bool,
    entries: Vec<ZenodoTarEntrySummary>,
    previews: HashMap<String, FieldPreview>,
    media_cache: HashMap<String, CachedMedia>,
    media_lru: std::collections::VecDeque<String>,
    media_total: u64,
}

impl ZenodoTarScanState {
    fn new(url: Url, filename: String) -> AppResult<Self> {
        let reader = open_remote_tar_reader(url, &filename)?;
        Ok(Self {
            tar: TarStream::new(reader),
            done: false,
            entries: Vec::new(),
            previews: HashMap::new(),
            media_cache: HashMap::new(),
            media_lru: std::collections::VecDeque::new(),
            media_total: 0,
        })
    }

    fn ensure_scanned_for_page(
        &mut self,
        target: usize,
        capture_start: usize,
        capture_end: usize,
    ) -> AppResult<()> {
        while !self.done && self.entries.len() < target {
            let idx = self.entries.len();
            let capture = idx >= capture_start && idx < capture_end;
            let next = self
                .tar
                .next_file_with_bytes(|meta| {
                    if !capture || meta.is_dir {
                        return None;
                    }
                    let ext = ext_from_filename(&meta.path).unwrap_or_default();
                    let is_media = matches!(
                        ext.as_str(),
                        "mp4" | "wav" | "mp3" | "flac" | "m4a" | "ogg" | "opus" | "aac"
                    );
                    if is_media && meta.size > 0 && meta.size <= TAR_MEDIA_CACHE_ITEM_MAX_BYTES {
                        return Some(meta.size);
                    }
                    Some(PEEK_BYTES as u64)
                })
                .map_err(|e| AppError::Invalid(format!("tar parse failed: {e}")))?;
            let Some((meta, maybe_bytes)) = next else {
                self.done = true;
                break;
            };

            let summary = ZenodoTarEntrySummary {
                name: meta.path.clone(),
                size: meta.size,
                is_dir: meta.is_dir,
            };
            self.entries.push(summary);
            if self.entries.len() >= TAR_MAX_ENTRIES {
                return Err(AppError::Invalid(
                    "TAR contains too many entries to list.".into(),
                ));
            }

            if let Some(bytes) = maybe_bytes {
                if !meta.is_dir {
                    let preview_bytes = bytes.iter().take(PEEK_BYTES).copied().collect::<Vec<u8>>();
                    let text = String::from_utf8(preview_bytes.clone()).ok();
                    let guessed_ext = ext_from_filename(&meta.path)
                        .or_else(|| infer::get(&preview_bytes).map(|t| t.extension().to_string()));
                    let hex_snippet =
                        hex_encode(preview_bytes.iter().take(48).copied().collect::<Vec<u8>>());
                    let preview = FieldPreview {
                        preview_text: text.as_ref().map(|s| s.chars().take(400).collect()),
                        hex_snippet,
                        guessed_ext,
                        is_binary: text.is_none(),
                        size: meta.size.min(u32::MAX as u64) as u32,
                    };
                    self.previews.insert(meta.path.clone(), preview);

                    if bytes.len() as u64 == meta.size
                        && meta.size <= TAR_MEDIA_CACHE_ITEM_MAX_BYTES
                    {
                        let ext = ext_from_filename(&meta.path).unwrap_or_else(|| "bin".into());
                        let mime = mime_for_ext(&ext).to_string();
                        self.cache_media(meta.path, ext, mime, bytes)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn cached_preview(&self, name: &str) -> Option<FieldPreview> {
        self.previews.get(name).cloned()
    }

    fn cached_media(&mut self, name: &str) -> Option<CachedMedia> {
        let Some(found) = self.media_cache.get(name).cloned() else {
            return None;
        };
        if let Some(pos) = self.media_lru.iter().position(|k| k == name) {
            self.media_lru.remove(pos);
        }
        self.media_lru.push_back(name.to_string());
        Some(found)
    }

    fn cache_media(
        &mut self,
        name: String,
        ext: String,
        mime: String,
        bytes: Vec<u8>,
    ) -> AppResult<()> {
        let size = bytes.len() as u64;
        if size == 0 || size > TAR_MEDIA_CACHE_ITEM_MAX_BYTES {
            return Ok(());
        }

        if let Some(existing) = self.media_cache.remove(&name) {
            self.media_total = self.media_total.saturating_sub(existing.bytes.len() as u64);
            if let Some(pos) = self.media_lru.iter().position(|k| k == &name) {
                self.media_lru.remove(pos);
            }
        }

        while self.media_total.saturating_add(size) > TAR_MEDIA_CACHE_TOTAL_MAX_BYTES {
            let Some(oldest) = self.media_lru.pop_front() else {
                break;
            };
            if let Some(evicted) = self.media_cache.remove(&oldest) {
                self.media_total = self.media_total.saturating_sub(evicted.bytes.len() as u64);
            }
        }

        self.media_total = self.media_total.saturating_add(size);
        self.media_cache
            .insert(name.clone(), CachedMedia { bytes, mime, ext });
        self.media_lru.push_back(name);
        Ok(())
    }
}

#[derive(Clone)]
struct CachedMedia {
    bytes: Vec<u8>,
    mime: String,
    ext: String,
}

struct TarStream<R: Read> {
    reader: R,
    pending_longname: Option<String>,
    pending_pax_path: Option<String>,
}

struct TarFileMeta {
    path: String,
    size: u64,
    is_dir: bool,
}

impl<R: Read> TarStream<R> {
    fn new(reader: R) -> Self {
        Self {
            reader,
            pending_longname: None,
            pending_pax_path: None,
        }
    }

    fn next_file_with_bytes<F>(
        &mut self,
        mut decide: F,
    ) -> std::io::Result<Option<(TarFileMeta, Option<Vec<u8>>)>>
    where
        F: FnMut(&TarFileMeta) -> Option<u64>,
    {
        loop {
            let Some(header) = read_tar_header_block(&mut self.reader)? else {
                return Ok(None);
            };
            if header.iter().all(|b| *b == 0) {
                let Some(next) = read_tar_header_block(&mut self.reader)? else {
                    return Ok(None);
                };
                if next.iter().all(|b| *b == 0) {
                    return Ok(None);
                }
                if let Some(item) = self.process_header(next, &mut decide)? {
                    return Ok(Some(item));
                }
                continue;
            }
            if let Some(item) = self.process_header(header, &mut decide)? {
                return Ok(Some(item));
            }
        }
    }

    fn process_header(
        &mut self,
        header: [u8; 512],
        decide: &mut dyn FnMut(&TarFileMeta) -> Option<u64>,
    ) -> std::io::Result<Option<(TarFileMeta, Option<Vec<u8>>)>> {
        let size = parse_tar_size(&header).unwrap_or(0);
        let typeflag = header[156];

        if typeflag == b'L' {
            if size > MAX_TAR_META_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "tar longname entry is too large",
                ));
            }
            let data = read_tar_data(&mut self.reader, size)?;
            self.pending_longname = Some(parse_tar_string(&data));
            skip_tar_padding(&mut self.reader, size)?;
            return Ok(None);
        }

        if typeflag == b'x' || typeflag == b'g' {
            if size > MAX_TAR_META_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
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
        if normalized.is_empty() {
            skip_tar_data(&mut self.reader, size)?;
            return Ok(None);
        }

        let is_dir = typeflag == b'5';
        let meta = TarFileMeta {
            path: normalized,
            size,
            is_dir,
        };
        let read_limit = decide(&meta);
        let bytes = if let Some(limit) = read_limit {
            if limit == 0 || meta.is_dir || size == 0 {
                skip_tar_data(&mut self.reader, size)?;
                None
            } else {
                let take = limit.min(size);
                let data = read_tar_data(&mut self.reader, take)?;
                let remaining = size.saturating_sub(take);
                if remaining > 0 {
                    std::io::copy(
                        &mut (&mut self.reader).take(remaining),
                        &mut std::io::sink(),
                    )?;
                }
                skip_tar_padding(&mut self.reader, size)?;
                Some(data)
            }
        } else {
            skip_tar_data(&mut self.reader, size)?;
            None
        };

        Ok(Some((meta, bytes)))
    }
}

fn read_tar_header_block<R: Read>(reader: &mut R) -> std::io::Result<Option<[u8; 512]>> {
    let mut buf = [0u8; 512];
    match reader.read_exact(&mut buf) {
        Ok(()) => Ok(Some(buf)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

fn read_tar_data<R: Read>(reader: &mut R, size: u64) -> std::io::Result<Vec<u8>> {
    let size: usize = size.try_into().map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "tar entry is too large")
    })?;
    let mut out = vec![0u8; size];
    reader.read_exact(&mut out)?;
    Ok(out)
}

fn skip_tar_data<R: Read>(reader: &mut R, size: u64) -> std::io::Result<()> {
    if size == 0 {
        return Ok(());
    }
    std::io::copy(&mut reader.take(size), &mut std::io::sink())?;
    skip_tar_padding(reader, size)
}

fn skip_tar_padding<R: Read>(reader: &mut R, size: u64) -> std::io::Result<()> {
    let pad = (512 - (size % 512)) % 512;
    if pad == 0 {
        return Ok(());
    }
    std::io::copy(&mut reader.take(pad), &mut std::io::sink())?;
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

impl Default for ZenodoClient {
    fn default() -> Self {
        let http = reqwest::Client::builder()
            .http1_only()
            .user_agent(USER_AGENT)
            .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { http }
    }
}

#[derive(Deserialize)]
struct ZenodoRecordResponse {
    id: u64,
    doi: Option<String>,
    doi_url: Option<String>,
    metadata: ZenodoMetadata,
    links: Option<ZenodoLinks>,
    files: Option<Vec<ZenodoFileEntry>>,
}

#[derive(Deserialize)]
struct ZenodoMetadata {
    title: String,
    publication_date: Option<String>,
    version: Option<String>,
    access_right: Option<String>,
    creators: Option<Vec<ZenodoCreator>>,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ZenodoCreator {
    name: String,
    affiliation: Option<String>,
    orcid: Option<String>,
}

#[derive(Deserialize)]
struct ZenodoLinks {
    self_html: Option<String>,
    preview_html: Option<String>,
    self_doi_html: Option<String>,
}

#[derive(Deserialize)]
struct ZenodoFileEntry {
    key: String,
    size: u64,
    checksum: Option<String>,
    links: ZenodoFileLinks,
}

#[derive(Deserialize)]
struct ZenodoFileLinks {
    #[serde(rename = "self")]
    content: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZenodoFileSummary {
    key: String,
    size: u64,
    checksum: Option<String>,
    content_url: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ZenodoRecordSummary {
    record_id: u64,
    title: String,
    doi: Option<String>,
    doi_url: Option<String>,
    publication_date: Option<String>,
    version: Option<String>,
    access_right: Option<String>,
    record_url: Option<String>,
    creators: Vec<ZenodoCreator>,
    files: Vec<ZenodoFileSummary>,
}

fn is_allowed_zenodo_host(host: &str) -> bool {
    let host = host.trim().to_ascii_lowercase();
    host == "zenodo.org" || host.ends_with(".zenodo.org")
}

fn validate_zenodo_url(url: &Url) -> bool {
    matches!(url.scheme(), "https" | "http") && url.host_str().is_some_and(is_allowed_zenodo_host)
}

fn extract_record_id_from_url(url: &Url) -> Option<u64> {
    let segments: Vec<_> = url
        .path_segments()
        .map(|it| it.filter(|s| !s.is_empty()).collect::<Vec<_>>())
        .unwrap_or_default();
    for i in 0..segments.len() {
        if segments[i] != "records" && segments[i] != "record" {
            continue;
        }
        let id = segments.get(i + 1)?;
        if !id.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        if let Ok(v) = id.parse::<u64>() {
            return Some(v);
        }
    }
    None
}

fn extract_record_id(input: &str) -> AppResult<(Url, u64)> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(AppError::Invalid(
            "Provide a Zenodo record URL like https://zenodo.org/records/<id>.".into(),
        ));
    }

    let url = Url::parse(trimmed).map_err(|_| {
        AppError::Invalid(
            "Unsupported input. Provide a Zenodo record URL like https://zenodo.org/records/<id>."
                .into(),
        )
    })?;
    if !validate_zenodo_url(&url) {
        return Err(AppError::Invalid(
            "Unsupported Zenodo host or scheme.".into(),
        ));
    }

    let record_id = extract_record_id_from_url(&url).ok_or_else(|| {
        AppError::Invalid(
            "Unsupported Zenodo URL. Expected https://zenodo.org/records/<id>.".into(),
        )
    })?;

    Ok((url, record_id))
}

fn api_record_url(base: &Url, record_id: u64) -> AppResult<Url> {
    let mut url = base.clone();
    url.set_path(&format!("/api/records/{record_id}"));
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn allowed_content_url(url: &Url) -> bool {
    if !validate_zenodo_url(url) {
        return false;
    }
    let segments: Vec<_> = url
        .path_segments()
        .map(|it| it.filter(|s| !s.is_empty()).collect::<Vec<_>>())
        .unwrap_or_default();
    if segments.len() < 6 {
        return false;
    }
    matches!(segments.get(0), Some(&"api"))
        && matches!(segments.get(1), Some(&"records"))
        && segments
            .get(2)
            .is_some_and(|s| s.chars().all(|c| c.is_ascii_digit()))
        && matches!(segments.get(3), Some(&"files"))
        && matches!(segments.last(), Some(&"content"))
}

fn record_id_from_content_url(url: &Url) -> Option<String> {
    let segments: Vec<_> = url
        .path_segments()
        .map(|it| it.filter(|s| !s.is_empty()).collect::<Vec<_>>())
        .unwrap_or_default();
    if segments.len() < 3 {
        return None;
    }
    if segments.get(0) != Some(&"api") || segments.get(1) != Some(&"records") {
        return None;
    }
    Some(segments[2].to_string())
}

async fn get_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: Url,
) -> AppResult<T> {
    let res = client
        .get(url.clone())
        .send()
        .await
        .map_err(|e| AppError::Remote(format!("request failed: {e}")))?;
    let status = res.status();
    let text = res
        .text()
        .await
        .map_err(|e| AppError::Remote(format!("read response failed: {e}")))?;
    if !status.is_success() {
        return Err(AppError::Remote(format!("HTTP {status} from {url}")));
    }
    serde_json::from_str(&text)
        .map_err(|e| AppError::Remote(format!("invalid JSON from {url}: {e}")))
}

fn ext_from_filename(name: &str) -> Option<String> {
    let trimmed = name.trim();
    let base = trimmed.split('/').last().unwrap_or(trimmed);
    let ext = base.rsplit_once('.').map(|(_, e)| e).unwrap_or("");
    let cleaned = ext.trim().trim_start_matches('.').to_ascii_lowercase();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn looks_like_tar(filename: &str) -> bool {
    let name = filename.trim().to_ascii_lowercase();
    name.ends_with(".tar")
        || name.ends_with(".tar.gz")
        || name.ends_with(".tgz")
        || name.ends_with(".tar.zst")
        || name.ends_with(".tar.zstd")
}

fn mime_for_ext(ext: &str) -> &'static str {
    match ext
        .trim()
        .trim_start_matches('.')
        .to_ascii_lowercase()
        .as_str()
    {
        "mp4" => "video/mp4",
        "wav" => "audio/wav",
        "mp3" => "audio/mpeg",
        "flac" => "audio/flac",
        "m4a" => "audio/mp4",
        "ogg" => "audio/ogg",
        "opus" => "audio/opus",
        "aac" => "audio/aac",
        _ => "application/octet-stream",
    }
}

fn normalize_member_path_str(path: &str) -> String {
    path.trim()
        .trim_start_matches("./")
        .trim_start_matches('/')
        .replace('\\', "/")
}

fn inflate_deflate_with_limit(compressed: &[u8], limit: u64) -> AppResult<Vec<u8>> {
    use std::io::Read;
    let mut decoder = flate2::read::DeflateDecoder::new(compressed);
    let mut out = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = decoder
            .read(&mut buf)
            .map_err(|e| AppError::Invalid(format!("ZIP inflate failed: {e}")))?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
        if out.len() as u64 > limit {
            return Err(AppError::Invalid(
                "ZIP entry expanded beyond the limit.".into(),
            ));
        }
    }
    Ok(out)
}

fn parse_content_range_total(value: &str) -> Option<u64> {
    let total = value.split('/').nth(1)?;
    if total == "*" {
        return None;
    }
    total.parse::<u64>().ok()
}

async fn range_request(
    client: &reqwest::Client,
    url: Url,
    start: u64,
    end_inclusive: u64,
) -> AppResult<(Vec<u8>, Option<u64>)> {
    let res = client
        .get(url.clone())
        .header(
            reqwest::header::RANGE,
            format!("bytes={start}-{end_inclusive}"),
        )
        .send()
        .await
        .map_err(|e| AppError::Remote(format!("request failed: {e}")))?;

    let status = res.status();
    if !(status.is_success() || status == reqwest::StatusCode::PARTIAL_CONTENT) {
        return Err(AppError::Remote(format!("HTTP {status} from {url}")));
    }

    let total_size = res
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_content_range_total);
    let bytes = res
        .bytes()
        .await
        .map_err(|e| AppError::Remote(format!("read response failed: {e}")))?;
    Ok((bytes.to_vec(), total_size))
}

fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    // Example: "bytes 0-255/8127399532"
    let value = value.trim();
    let value = value.strip_prefix("bytes ")?;
    let (range, total) = value.split_once('/')?;
    let total = total.parse::<u64>().ok()?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse::<u64>().ok()?;
    let end = end.parse::<u64>().ok()?;
    Some((start, end, total))
}

async fn suffix_range_request(
    client: &reqwest::Client,
    url: Url,
    suffix_len: u64,
) -> AppResult<(Vec<u8>, u64, u64)> {
    let suffix_len = suffix_len.max(1);
    let res = client
        .get(url.clone())
        .header(reqwest::header::RANGE, format!("bytes=-{suffix_len}"))
        .send()
        .await
        .map_err(|e| AppError::Remote(format!("request failed: {e}")))?;

    let status = res.status();
    if !(status.is_success() || status == reqwest::StatusCode::PARTIAL_CONTENT) {
        return Err(AppError::Remote(format!("HTTP {status} from {url}")));
    }

    let (start, _end, total) = res
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_content_range)
        .ok_or_else(|| AppError::Remote(format!("Missing Content-Range from {url}")))?;

    let bytes = res
        .bytes()
        .await
        .map_err(|e| AppError::Remote(format!("read response failed: {e}")))?;
    Ok((bytes.to_vec(), start, total))
}

fn sanitize(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '+') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn open_remote_tar_reader(url: Url, filename_hint: &str) -> AppResult<Box<dyn Read + Send>> {
    let client = reqwest::blocking::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .build()
        .map_err(|e| AppError::Task(format!("Failed to build HTTP client: {e}")))?;
    let res = client
        .get(url.clone())
        .send()
        .map_err(|e| AppError::Remote(format!("request failed: {e}")))?;
    let status = res.status();
    if !status.is_success() {
        return Err(AppError::Remote(format!("HTTP {status} from {url}")));
    }

    let name = filename_hint.trim().to_ascii_lowercase();
    let base: Box<dyn Read + Send> = Box::new(res);
    if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        return Ok(Box::new(flate2::read::GzDecoder::new(base)));
    }
    if name.ends_with(".tar.zst") || name.ends_with(".tar.zstd") {
        let decoder = zstd::stream::read::Decoder::new(base)?;
        return Ok(Box::new(decoder));
    }
    Ok(base)
}

fn read_u16_le(input: &[u8], offset: usize) -> AppResult<u16> {
    let slice = input
        .get(offset..offset + 2)
        .ok_or_else(|| AppError::Invalid("Unexpected EOF while parsing ZIP.".into()))?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}

fn read_u32_le(input: &[u8], offset: usize) -> AppResult<u32> {
    let slice = input
        .get(offset..offset + 4)
        .ok_or_else(|| AppError::Invalid("Unexpected EOF while parsing ZIP.".into()))?;
    Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_u64_le(input: &[u8], offset: usize) -> AppResult<u64> {
    let slice = input
        .get(offset..offset + 8)
        .ok_or_else(|| AppError::Invalid("Unexpected EOF while parsing ZIP.".into()))?;
    Ok(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}

fn find_zip_eocd(buf: &[u8]) -> Option<usize> {
    // EOCD can be at most 65535 + 22 bytes from the end of the file.
    const EOCD_SIG: [u8; 4] = [0x50, 0x4b, 0x05, 0x06];
    if buf.len() < 22 {
        return None;
    }
    let start = buf.len().saturating_sub(22 + 65535);
    for i in (start..=buf.len().saturating_sub(22)).rev() {
        if buf.get(i..i + 4) != Some(&EOCD_SIG) {
            continue;
        }
        let comment_len = u16::from_le_bytes([buf[i + 20], buf[i + 21]]) as usize;
        if i + 22 + comment_len == buf.len() {
            return Some(i);
        }
    }
    None
}

struct ZipCentralDirectory {
    total_entries: u64,
    central_dir_size: u64,
    central_dir_offset: u64,
}

async fn read_zip_central_directory_info(
    client: &reqwest::Client,
    url: Url,
) -> AppResult<ZipCentralDirectory> {
    let mut tail_len = ZIP_TAIL_INITIAL_BYTES;
    let (tail, tail_start, _total_size, eocd_rel) = loop {
        let (tail, tail_start, total_size) =
            suffix_range_request(client, url.clone(), tail_len).await?;
        let eocd_rel = find_zip_eocd(&tail)
            .ok_or_else(|| AppError::Invalid("Unable to locate ZIP EOCD in archive tail.".into()));
        if let Ok(eocd_rel) = eocd_rel {
            break (tail, tail_start, total_size, eocd_rel);
        }
        if tail_len >= ZIP_TAIL_MAX_BYTES {
            return Err(AppError::Invalid(
                "Unable to locate ZIP EOCD (tail too small).".into(),
            ));
        }
        tail_len = (tail_len * 2).min(ZIP_TAIL_MAX_BYTES);
    };

    let eocd_abs_offset = tail_start + eocd_rel as u64;
    let sig = read_u32_le(&tail, eocd_rel)?;
    if sig != 0x0605_4b50 {
        return Err(AppError::Invalid("Invalid ZIP EOCD signature.".into()));
    }

    let entries_u16 = read_u16_le(&tail, eocd_rel + 10)? as u64;
    let central_dir_size_u32 = read_u32_le(&tail, eocd_rel + 12)? as u64;
    let central_dir_offset_u32 = read_u32_le(&tail, eocd_rel + 16)? as u64;

    let needs_zip64 = entries_u16 == 0xFFFF
        || central_dir_size_u32 == 0xFFFF_FFFF
        || central_dir_offset_u32 == 0xFFFF_FFFF;

    if !needs_zip64 {
        return Ok(ZipCentralDirectory {
            total_entries: entries_u16,
            central_dir_size: central_dir_size_u32,
            central_dir_offset: central_dir_offset_u32,
        });
    }

    if eocd_abs_offset < 20 {
        return Err(AppError::Invalid("ZIP64 locator is out of bounds.".into()));
    }
    let locator_start = eocd_abs_offset - 20;
    let (locator, _total) =
        range_request(client, url.clone(), locator_start, eocd_abs_offset - 1).await?;
    if locator.len() < 20 || read_u32_le(&locator, 0)? != 0x0706_4b50 {
        return Err(AppError::Invalid("Missing ZIP64 locator.".into()));
    }
    let zip64_eocd_offset = read_u64_le(&locator, 8)?;
    let (zip64_eocd, _total) = range_request(
        client,
        url.clone(),
        zip64_eocd_offset,
        zip64_eocd_offset + 55,
    )
    .await?;
    if zip64_eocd.len() < 56 || read_u32_le(&zip64_eocd, 0)? != 0x0606_4b50 {
        return Err(AppError::Invalid("Missing ZIP64 EOCD record.".into()));
    }
    let total_entries = read_u64_le(&zip64_eocd, 32)?;
    let central_dir_size = read_u64_le(&zip64_eocd, 40)?;
    let central_dir_offset = read_u64_le(&zip64_eocd, 48)?;

    Ok(ZipCentralDirectory {
        total_entries,
        central_dir_size,
        central_dir_offset,
    })
}

fn parse_zip64_extra(
    extra: &[u8],
    need_uncompressed: bool,
    need_compressed: bool,
    need_local_offset: bool,
) -> AppResult<(Option<u64>, Option<u64>, Option<u64>)> {
    let mut pos = 0usize;
    while pos + 4 <= extra.len() {
        let header_id = u16::from_le_bytes([extra[pos], extra[pos + 1]]);
        let data_size = u16::from_le_bytes([extra[pos + 2], extra[pos + 3]]) as usize;
        pos += 4;
        if pos + data_size > extra.len() {
            break;
        }
        if header_id == 0x0001 {
            let mut cursor = pos;
            let mut uncompressed = None;
            let mut compressed = None;
            let mut local_offset = None;
            if need_uncompressed {
                uncompressed = Some(read_u64_le(extra, cursor)?);
                cursor += 8;
            }
            if need_compressed {
                compressed = Some(read_u64_le(extra, cursor)?);
                cursor += 8;
            }
            if need_local_offset {
                local_offset = Some(read_u64_le(extra, cursor)?);
            }
            return Ok((uncompressed, compressed, local_offset));
        }
        pos += data_size;
    }
    Ok((None, None, None))
}

fn parse_central_directory_entries(
    buf: &[u8],
    max_entries_hint: u64,
) -> AppResult<Vec<ZipEntryIndex>> {
    let mut entries = Vec::new();
    let mut pos = 0usize;
    while pos + 46 <= buf.len() {
        let sig = read_u32_le(buf, pos)?;
        if sig != 0x0201_4b50 {
            break;
        }
        let flags = read_u16_le(buf, pos + 8)?;
        let method = read_u16_le(buf, pos + 10)?;
        let crc32 = read_u32_le(buf, pos + 16)?;
        let compressed_size_u32 = read_u32_le(buf, pos + 20)?;
        let uncompressed_size_u32 = read_u32_le(buf, pos + 24)?;
        let name_len = read_u16_le(buf, pos + 28)? as usize;
        let extra_len = read_u16_le(buf, pos + 30)? as usize;
        let comment_len = read_u16_le(buf, pos + 32)? as usize;
        let local_header_offset_u32 = read_u32_le(buf, pos + 42)?;
        let header_end = pos
            .checked_add(46)
            .ok_or_else(|| AppError::Invalid("ZIP header overflow.".into()))?;
        let name_start = header_end;
        let name_end = name_start + name_len;
        let extra_start = name_end;
        let extra_end = extra_start + extra_len;
        let comment_end = extra_end + comment_len;
        let name_bytes = buf
            .get(name_start..name_end)
            .ok_or_else(|| AppError::Invalid("Malformed ZIP central directory entry.".into()))?;
        let extra_bytes = buf.get(extra_start..extra_end).unwrap_or(&[]);
        let name = String::from_utf8(name_bytes.to_vec())
            .unwrap_or_else(|_| String::from_utf8_lossy(name_bytes).to_string());
        let is_dir = name.ends_with('/');

        let need_zip64_uncompressed = uncompressed_size_u32 == 0xFFFF_FFFF;
        let need_zip64_compressed = compressed_size_u32 == 0xFFFF_FFFF;
        let need_zip64_local_offset = local_header_offset_u32 == 0xFFFF_FFFF;
        let (zip64_uncompressed, zip64_compressed, zip64_local_offset) = parse_zip64_extra(
            extra_bytes,
            need_zip64_uncompressed,
            need_zip64_compressed,
            need_zip64_local_offset,
        )?;

        let compressed_size = zip64_compressed.unwrap_or(compressed_size_u32 as u64);
        let uncompressed_size = zip64_uncompressed.unwrap_or(uncompressed_size_u32 as u64);
        let local_header_offset = zip64_local_offset.unwrap_or(local_header_offset_u32 as u64);
        let _ = crc32;

        entries.push(ZipEntryIndex {
            name,
            method,
            flags,
            compressed_size,
            uncompressed_size,
            local_header_offset,
            is_dir,
        });

        if max_entries_hint > 0 && entries.len() as u64 >= max_entries_hint {
            // Keep parsing safe even if EOCD entry count is wrong.
            // We'll still break once we hit buffer end or invalid signature.
        }
        pos = comment_end;
    }
    Ok(entries)
}

async fn build_zip_index(client: &reqwest::Client, url: Url) -> AppResult<ZipIndex> {
    let cd = read_zip_central_directory_info(client, url.clone()).await?;
    if cd.central_dir_size == 0 || cd.central_dir_size > ZIP_MAX_CENTRAL_DIR_BYTES {
        return Err(AppError::Invalid(
            "ZIP central directory is too large to parse.".into(),
        ));
    }
    let end = cd
        .central_dir_offset
        .checked_add(cd.central_dir_size)
        .and_then(|v| v.checked_sub(1))
        .ok_or_else(|| AppError::Invalid("ZIP central directory range overflow.".into()))?;
    let (buf, _total) = range_request(client, url, cd.central_dir_offset, end).await?;
    let entries = parse_central_directory_entries(&buf, cd.total_entries)?;
    Ok(ZipIndex { entries })
}

fn looks_like_zip(filename: &str) -> bool {
    ext_from_filename(filename)
        .map(|e| e == "zip")
        .unwrap_or(false)
}

async fn get_zip_index(
    client: &reqwest::Client,
    cache: &ZenodoZipIndexCache,
    content_url: &str,
) -> AppResult<Arc<ZipIndex>> {
    let trimmed = content_url.trim();
    if trimmed.is_empty() {
        return Err(AppError::Invalid("Missing content URL.".into()));
    }
    {
        let guard = cache
            .0
            .lock()
            .map_err(|_| AppError::Task("zip cache poisoned".into()))?;
        if let Some(found) = guard.get(trimmed) {
            return Ok(Arc::clone(found));
        }
    }

    let url =
        Url::parse(trimmed).map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
    if !allowed_content_url(&url) {
        return Err(AppError::Invalid("Blocked content URL.".into()));
    }

    let index = Arc::new(build_zip_index(client, url).await?);
    let mut guard = cache
        .0
        .lock()
        .map_err(|_| AppError::Task("zip cache poisoned".into()))?;
    guard.insert(trimmed.to_string(), Arc::clone(&index));
    Ok(index)
}

fn find_zip_entry<'a>(index: &'a ZipIndex, entry_name: &str) -> AppResult<&'a ZipEntryIndex> {
    let name = entry_name.trim();
    if name.is_empty() {
        return Err(AppError::Invalid("Missing ZIP entry name.".into()));
    }
    index
        .entries
        .iter()
        .find(|e| e.name == name)
        .ok_or_else(|| AppError::Missing(format!("Entry '{name}' not found in ZIP.")))
}

#[tauri::command]
pub async fn zenodo_record_summary(
    client: State<'_, ZenodoClient>,
    input: String,
) -> AppResult<ZenodoRecordSummary> {
    let (base_url, record_id) = extract_record_id(&input)?;
    let api_url = api_record_url(&base_url, record_id)?;
    let record: ZenodoRecordResponse = get_json(&client.http, api_url).await?;

    let creators = record.metadata.creators.unwrap_or_default();
    let record_url = record
        .links
        .as_ref()
        .and_then(|l| l.self_doi_html.clone())
        .or_else(|| record.links.as_ref().and_then(|l| l.preview_html.clone()))
        .or_else(|| record.links.as_ref().and_then(|l| l.self_html.clone()));

    let mut files = Vec::new();
    for f in record.files.unwrap_or_default() {
        if let Ok(url) = Url::parse(&f.links.content) {
            if allowed_content_url(&url) {
                files.push(ZenodoFileSummary {
                    key: f.key,
                    size: f.size,
                    checksum: f.checksum,
                    content_url: f.links.content,
                });
            }
        }
    }

    Ok(ZenodoRecordSummary {
        record_id: record.id,
        title: record.metadata.title,
        doi: record.doi,
        doi_url: record.doi_url,
        publication_date: record.metadata.publication_date,
        version: record.metadata.version,
        access_right: record.metadata.access_right,
        record_url,
        creators,
        files,
    })
}

#[tauri::command]
pub async fn zenodo_peek_file(
    client: State<'_, ZenodoClient>,
    content_url: String,
) -> AppResult<FieldPreview> {
    let trimmed = content_url.trim();
    let url =
        Url::parse(trimmed).map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
    if !allowed_content_url(&url) {
        return Err(AppError::Invalid("Blocked content URL.".into()));
    }

    let end = (PEEK_BYTES as u64).saturating_sub(1);
    let (data, total_size) = range_request(&client.http, url.clone(), 0, end).await?;
    let text = String::from_utf8(data.clone()).ok();

    let guessed_ext = url
        .path_segments()
        .and_then(|segments| {
            let segs = segments.filter(|s| !s.is_empty()).collect::<Vec<_>>();
            let idx = segs.iter().position(|s| *s == "files")?;
            ext_from_filename(segs.get(idx + 1).copied().unwrap_or(""))
        })
        .or_else(|| infer::get(&data).map(|t| t.extension().to_string()));

    let hex_snippet = hex_encode(data.iter().take(48).copied().collect::<Vec<u8>>());
    let size_u32 = total_size.unwrap_or(0).min(u32::MAX as u64) as u32;

    Ok(FieldPreview {
        preview_text: text.as_ref().map(|s| s.chars().take(400).collect()),
        hex_snippet,
        guessed_ext,
        is_binary: text.is_none(),
        size: size_u32,
    })
}

#[tauri::command]
pub async fn zenodo_open_file(
    client: State<'_, ZenodoClient>,
    content_url: String,
    filename: String,
    opener_app_path: Option<String>,
) -> AppResult<OpenLeafResponse> {
    let trimmed = content_url.trim();
    let url =
        Url::parse(trimmed).map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
    if !allowed_content_url(&url) {
        return Err(AppError::Invalid("Blocked content URL.".into()));
    }
    let filename = filename.trim().to_string();
    if filename.is_empty() {
        return Err(AppError::Invalid("Missing filename.".into()));
    }

    let (_head, total_size) = range_request(&client.http, url.clone(), 0, 0).await?;
    let total_size = total_size.unwrap_or(0);
    let ext = ext_from_filename(&filename)
        .or_else(|| {
            url.path_segments().and_then(|segments| {
                let segs = segments.filter(|s| !s.is_empty()).collect::<Vec<_>>();
                let idx = segs.iter().position(|s| *s == "files")?;
                ext_from_filename(segs.get(idx + 1).copied().unwrap_or(""))
            })
        })
        .unwrap_or_else(|| "bin".into());

    if total_size == 0 || total_size > MAX_INLINE_DOWNLOAD_BYTES {
        let opened = open::that_detached(trimmed).is_ok();
        let size_u32 = total_size.min(u32::MAX as u64) as u32;
        let message = if opened {
            format!("Opened download URL ({size_u32} bytes) in your browser.")
        } else {
            "Unable to open download URL.".into()
        };
        return Ok(OpenLeafResponse {
            path: trimmed.to_string(),
            size: size_u32,
            ext,
            opened,
            needs_opener: false,
            message,
        });
    }

    let res = client
        .http
        .get(url.clone())
        .send()
        .await
        .map_err(|e| AppError::Remote(format!("download failed: {e}")))?;
    let status = res.status();
    if !status.is_success() {
        return Err(AppError::Remote(format!(
            "download HTTP {status} from {url}"
        )));
    }
    let bytes = res
        .bytes()
        .await
        .map_err(|e| AppError::Remote(format!("download read failed: {e}")))?;
    let size_u32 = (bytes.len() as u64).min(u32::MAX as u64) as u32;

    let record_id = record_id_from_content_url(&url).unwrap_or_else(|| "unknown".into());
    let temp_dir = std::env::temp_dir()
        .join("dataset-inspector")
        .join("zenodo");
    std::fs::create_dir_all(&temp_dir)?;

    let sanitized = sanitize(&filename);
    let stem = sanitized
        .rsplit_once('.')
        .map(|(s, _)| s)
        .unwrap_or(&sanitized);
    let out = temp_dir.join(format!(
        "{}-r{}-{}.{}",
        sanitize(url.host_str().unwrap_or("zenodo")),
        sanitize(&record_id),
        stem,
        ext
    ));
    std::fs::write(&out, &bytes)?;

    let mut opened = false;
    let mut open_error = None::<String>;
    if let Some(app_path) = opener_app_path.as_deref() {
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

    let base = format!("{} ({} bytes)", out.display(), size_u32);
    let mut message = base;
    let needs_opener = !opened && open_error.is_some();
    if needs_opener {
        message.push_str(" · no default app found, choose an app to open it");
    }

    Ok(OpenLeafResponse {
        path: out.display().to_string(),
        size: size_u32,
        ext,
        opened,
        needs_opener,
        message,
    })
}

#[tauri::command]
pub async fn zenodo_zip_list_entries(
    client: State<'_, ZenodoClient>,
    cache: State<'_, ZenodoZipIndexCache>,
    content_url: String,
    filename: String,
) -> AppResult<Vec<ZenodoZipEntrySummary>> {
    let filename = filename.trim().to_string();
    if filename.is_empty() {
        return Err(AppError::Invalid("Missing filename.".into()));
    }
    if !looks_like_zip(&filename) {
        return Err(AppError::Invalid(
            "Selected file is not a ZIP archive.".into(),
        ));
    }
    let index = get_zip_index(&client.http, &cache, &content_url).await?;
    Ok(index
        .entries
        .iter()
        .cloned()
        .map(|e| ZenodoZipEntrySummary {
            name: e.name,
            method: e.method,
            compressed_size: e.compressed_size,
            uncompressed_size: e.uncompressed_size,
            is_dir: e.is_dir,
        })
        .collect())
}

fn local_header_data_offset(local_header: &[u8]) -> AppResult<u64> {
    if local_header.len() < 30 || read_u32_le(local_header, 0)? != 0x0403_4b50 {
        return Err(AppError::Invalid("Invalid ZIP local header.".into()));
    }
    let name_len = read_u16_le(local_header, 26)? as u64;
    let extra_len = read_u16_le(local_header, 28)? as u64;
    Ok(30 + name_len + extra_len)
}

async fn read_zip_entry_preview_bytes(
    client: &reqwest::Client,
    url: Url,
    entry: &ZipEntryIndex,
) -> AppResult<Vec<u8>> {
    if entry.is_dir {
        return Err(AppError::Invalid("ZIP entry is a directory.".into()));
    }
    if entry.flags & 1 == 1 {
        return Err(AppError::Invalid(
            "Encrypted ZIP entries are not supported.".into(),
        ));
    }
    let (local_header, _total) = range_request(
        client,
        url.clone(),
        entry.local_header_offset,
        entry.local_header_offset + 64,
    )
    .await?;
    let data_offset = local_header_data_offset(&local_header)?;
    let data_start = entry
        .local_header_offset
        .checked_add(data_offset)
        .ok_or_else(|| AppError::Invalid("ZIP offset overflow.".into()))?;

    if entry.compressed_size == 0 {
        return Ok(Vec::new());
    }

    if entry.method == 0 {
        let end = data_start
            .checked_add(entry.compressed_size.saturating_sub(1))
            .ok_or_else(|| AppError::Invalid("ZIP range overflow.".into()))?;
        let want_end = data_start
            .checked_add((PEEK_BYTES as u64).saturating_sub(1))
            .ok_or_else(|| AppError::Invalid("ZIP range overflow.".into()))?
            .min(end);
        let (data, _total) = range_request(client, url, data_start, want_end).await?;
        return Ok(data);
    }

    if entry.method != 8 {
        return Err(AppError::Invalid(format!(
            "Unsupported ZIP compression method: {}",
            entry.method
        )));
    }

    // Deflate (raw) decompression with bounded network usage.
    let mut decompressor = flate2::Decompress::new(false);
    let mut output: Vec<u8> = Vec::new();
    let mut fetched: u64 = 0;
    let mut chunk_start = data_start;
    let mut remaining = entry.compressed_size;

    while remaining > 0
        && (output.len() as u64) < PEEK_BYTES as u64
        && fetched < ZIP_PREVIEW_MAX_COMPRESSED_BYTES
    {
        let chunk_len = remaining
            .min(256 * 1024)
            .min(ZIP_PREVIEW_MAX_COMPRESSED_BYTES - fetched);
        let chunk_end = chunk_start
            .checked_add(chunk_len.saturating_sub(1))
            .ok_or_else(|| AppError::Invalid("ZIP range overflow.".into()))?;
        let (chunk, _total) = range_request(client, url.clone(), chunk_start, chunk_end).await?;
        if chunk.is_empty() {
            break;
        }
        fetched = fetched.saturating_add(chunk.len() as u64);
        chunk_start = chunk_start.saturating_add(chunk.len() as u64);
        remaining = remaining.saturating_sub(chunk.len() as u64);

        let mut input = chunk.as_slice();
        while !input.is_empty() && output.len() < PEEK_BYTES {
            let mut buf = [0u8; 8192];
            let before_in = decompressor.total_in();
            let before_out = decompressor.total_out();
            let status = decompressor
                .decompress(input, &mut buf, flate2::FlushDecompress::None)
                .map_err(|e| AppError::Invalid(format!("ZIP inflate failed: {e}")))?;
            let consumed = (decompressor.total_in() - before_in) as usize;
            let produced = (decompressor.total_out() - before_out) as usize;
            if produced > 0 {
                let take = (PEEK_BYTES - output.len()).min(produced);
                output.extend_from_slice(&buf[..take]);
            }
            if consumed == 0 && produced == 0 {
                break;
            }
            input = &input[consumed.min(input.len())..];
            if status == flate2::Status::StreamEnd {
                return Ok(output);
            }
        }
    }

    Ok(output)
}

#[tauri::command]
pub async fn zenodo_zip_peek_entry(
    client: State<'_, ZenodoClient>,
    cache: State<'_, ZenodoZipIndexCache>,
    content_url: String,
    filename: String,
    entry_name: String,
) -> AppResult<FieldPreview> {
    let filename = filename.trim().to_string();
    if filename.is_empty() {
        return Err(AppError::Invalid("Missing filename.".into()));
    }
    if !looks_like_zip(&filename) {
        return Err(AppError::Invalid(
            "Selected file is not a ZIP archive.".into(),
        ));
    }
    let index = get_zip_index(&client.http, &cache, &content_url).await?;
    let entry = find_zip_entry(index.as_ref(), &entry_name)?;
    let url = Url::parse(content_url.trim())
        .map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
    if !allowed_content_url(&url) {
        return Err(AppError::Invalid("Blocked content URL.".into()));
    }

    let data = read_zip_entry_preview_bytes(&client.http, url, entry).await?;
    let text = String::from_utf8(data.clone()).ok();
    let guessed_ext = ext_from_filename(&entry.name)
        .or_else(|| infer::get(&data).map(|t| t.extension().to_string()));
    let hex_snippet = hex_encode(data.iter().take(48).copied().collect::<Vec<u8>>());
    let size_u32 = entry.uncompressed_size.min(u32::MAX as u64) as u32;

    Ok(FieldPreview {
        preview_text: text.as_ref().map(|s| s.chars().take(400).collect()),
        hex_snippet,
        guessed_ext,
        is_binary: text.is_none(),
        size: size_u32,
    })
}

#[tauri::command]
pub async fn zenodo_zip_open_entry(
    client: State<'_, ZenodoClient>,
    cache: State<'_, ZenodoZipIndexCache>,
    content_url: String,
    filename: String,
    entry_name: String,
    opener_app_path: Option<String>,
) -> AppResult<OpenLeafResponse> {
    let filename = filename.trim().to_string();
    if filename.is_empty() {
        return Err(AppError::Invalid("Missing filename.".into()));
    }
    if !looks_like_zip(&filename) {
        return Err(AppError::Invalid(
            "Selected file is not a ZIP archive.".into(),
        ));
    }
    let index = get_zip_index(&client.http, &cache, &content_url).await?;
    let entry = find_zip_entry(index.as_ref(), &entry_name)?.clone();
    if entry.is_dir {
        return Err(AppError::Invalid("ZIP entry is a directory.".into()));
    }
    if entry.uncompressed_size > MAX_INLINE_DOWNLOAD_BYTES
        || entry.compressed_size > MAX_INLINE_DOWNLOAD_BYTES
    {
        return Err(AppError::Invalid(
            "ZIP entry is too large to extract locally.".into(),
        ));
    }
    let url = Url::parse(content_url.trim())
        .map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
    if !allowed_content_url(&url) {
        return Err(AppError::Invalid("Blocked content URL.".into()));
    }

    let (local_header, _total) = range_request(
        &client.http,
        url.clone(),
        entry.local_header_offset,
        entry.local_header_offset + 64,
    )
    .await?;
    let data_offset = local_header_data_offset(&local_header)?;
    let data_start = entry
        .local_header_offset
        .checked_add(data_offset)
        .ok_or_else(|| AppError::Invalid("ZIP offset overflow.".into()))?;

    let end = data_start
        .checked_add(entry.compressed_size.saturating_sub(1))
        .ok_or_else(|| AppError::Invalid("ZIP range overflow.".into()))?;
    let (compressed, _total) = range_request(&client.http, url.clone(), data_start, end).await?;

    let bytes: Vec<u8> = if entry.method == 0 {
        compressed
    } else if entry.method == 8 {
        inflate_deflate_with_limit(&compressed, MAX_INLINE_DOWNLOAD_BYTES)?
    } else {
        return Err(AppError::Invalid(format!(
            "Unsupported ZIP compression method: {}",
            entry.method
        )));
    };

    let record_id = record_id_from_content_url(&url).unwrap_or_else(|| "unknown".into());
    let temp_dir = std::env::temp_dir()
        .join("dataset-inspector")
        .join("zenodo");
    std::fs::create_dir_all(&temp_dir)?;

    let ext = ext_from_filename(&entry.name).unwrap_or_else(|| "bin".into());
    let base = format!(
        "{}-r{}-{}",
        sanitize(url.host_str().unwrap_or("zenodo")),
        sanitize(&record_id),
        sanitize(&filename)
    );
    let entry_filename = entry.name.split('/').last().unwrap_or(entry.name.as_str());
    let entry_stem_raw = entry_filename
        .rsplit_once('.')
        .map(|(s, _)| s)
        .unwrap_or(entry_filename);
    let entry_stem = sanitize(entry_stem_raw);
    let out_path = temp_dir.join(format!("{base}-{entry_stem}.{ext}"));
    std::fs::write(&out_path, &bytes)?;

    let mut opened = false;
    let mut open_error = None::<String>;
    if let Some(app_path) = opener_app_path.as_deref() {
        match open_with::open_with_app_detached(&out_path, app_path) {
            Ok(()) => opened = true,
            Err(err) => open_error = Some(err),
        }
    }
    if !opened {
        if let Err(err) = open::that_detached(&out_path) {
            open_error = Some(err.to_string());
        } else {
            opened = true;
        }
    }

    let size_u32 = (bytes.len() as u64).min(u32::MAX as u64) as u32;
    let base_msg = format!("{} ({} bytes)", out_path.display(), size_u32);
    let mut message = base_msg;
    let needs_opener = !opened && open_error.is_some();
    if needs_opener {
        message.push_str(" · no default app found, choose an app to open it");
    }

    Ok(OpenLeafResponse {
        path: out_path.display().to_string(),
        size: size_u32,
        ext,
        opened,
        needs_opener,
        message,
    })
}

#[tauri::command]
pub async fn zenodo_zip_inline_entry_media(
    client: State<'_, ZenodoClient>,
    cache: State<'_, ZenodoZipIndexCache>,
    content_url: String,
    filename: String,
    entry_name: String,
) -> AppResult<InlineMediaResponse> {
    let filename = filename.trim().to_string();
    if filename.is_empty() {
        return Err(AppError::Invalid("Missing filename.".into()));
    }
    if !looks_like_zip(&filename) {
        return Err(AppError::Invalid(
            "Selected file is not a ZIP archive.".into(),
        ));
    }
    let index = get_zip_index(&client.http, &cache, &content_url).await?;
    let entry = find_zip_entry(index.as_ref(), &entry_name)?.clone();
    if entry.is_dir {
        return Err(AppError::Invalid("ZIP entry is a directory.".into()));
    }
    if entry.uncompressed_size > ZIP_INLINE_MEDIA_MAX_BYTES {
        return Err(AppError::Invalid(
            "ZIP entry is too large for inline preview.".into(),
        ));
    }
    if entry.flags & 0x1 != 0 {
        return Err(AppError::Invalid(
            "Encrypted ZIP entries are not supported.".into(),
        ));
    }

    let url = Url::parse(content_url.trim())
        .map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
    if !allowed_content_url(&url) {
        return Err(AppError::Invalid("Blocked content URL.".into()));
    }

    let (local_header, _total) = range_request(
        &client.http,
        url.clone(),
        entry.local_header_offset,
        entry.local_header_offset + 64,
    )
    .await?;
    let data_offset = local_header_data_offset(&local_header)?;
    let data_start = entry
        .local_header_offset
        .checked_add(data_offset)
        .ok_or_else(|| AppError::Invalid("ZIP offset overflow.".into()))?;

    let end = data_start
        .checked_add(entry.compressed_size.saturating_sub(1))
        .ok_or_else(|| AppError::Invalid("ZIP range overflow.".into()))?;
    let (compressed, _total) = range_request(&client.http, url.clone(), data_start, end).await?;

    let bytes: Vec<u8> = if entry.method == 0 {
        compressed
    } else if entry.method == 8 {
        inflate_deflate_with_limit(&compressed, ZIP_INLINE_MEDIA_MAX_BYTES)?
    } else {
        return Err(AppError::Invalid(format!(
            "Unsupported ZIP compression method: {}",
            entry.method
        )));
    };

    let ext = ext_from_filename(&entry.name).unwrap_or_else(|| "bin".into());
    let mime = mime_for_ext(&ext).to_string();
    let base64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    Ok(InlineMediaResponse {
        base64,
        mime,
        size: (bytes.len() as u64).min(u32::MAX as u64) as u32,
        ext,
    })
}

fn read_tar_member_with_limit(
    url: Url,
    filename_hint: String,
    member_name: String,
    read_at_most: u64,
    hard_limit: Option<u64>,
) -> AppResult<(Vec<u8>, u64)> {
    let member_name = normalize_member_path_str(&member_name);
    if member_name.is_empty() {
        return Err(AppError::Invalid("Missing TAR entry name.".into()));
    }

    let reader = open_remote_tar_reader(url, &filename_hint)?;
    let mut archive = tar::Archive::new(reader);
    for entry in archive.entries()? {
        let entry = entry?;
        if entry.header().entry_type().is_dir() {
            continue;
        }
        let current = normalize_member_path_str(&entry.path()?.to_string_lossy());
        if current != member_name {
            continue;
        }
        let size = entry.size();
        if let Some(limit) = hard_limit {
            if size > limit {
                return Err(AppError::Invalid(format!(
                    "TAR entry is too large ({} bytes).",
                    size
                )));
            }
        }

        let mut buf = Vec::new();
        let cap = read_at_most.min(size);
        entry.take(cap).read_to_end(&mut buf)?;
        return Ok((buf, size));
    }

    Err(AppError::Missing(format!(
        "Entry '{member_name}' not found in TAR."
    )))
}

#[tauri::command]
pub async fn zenodo_tar_list_entries_paged(
    cache: State<'_, ZenodoTarScanCache>,
    content_url: String,
    filename: String,
    offset: Option<u32>,
    length: Option<u32>,
) -> AppResult<ZenodoTarEntryListResponse> {
    let filename = filename.trim().to_string();
    if filename.is_empty() {
        return Err(AppError::Invalid("Missing filename.".into()));
    }
    if !looks_like_tar(&filename) {
        return Err(AppError::Invalid(
            "Selected file is not a supported TAR archive.".into(),
        ));
    }

    let trimmed = content_url.trim();
    let url =
        Url::parse(trimmed).map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
    if !allowed_content_url(&url) {
        return Err(AppError::Invalid("Blocked content URL.".into()));
    }

    let offset = offset.unwrap_or(0);
    let length = length
        .unwrap_or(TAR_DEFAULT_PAGE_SIZE)
        .max(1)
        .min(TAR_MAX_PAGE_SIZE);

    let state = cache.get_or_create(&content_url, &filename)?;
    tauri::async_runtime::spawn_blocking(move || {
        let mut guard = state
            .lock()
            .map_err(|_| AppError::Task("tar scan lock poisoned".into()))?;
        let start = offset as usize;
        let end = start.saturating_add(length as usize);
        guard.ensure_scanned_for_page(end, start, end)?;

        let slice_end = end.min(guard.entries.len());
        let entries = if start >= guard.entries.len() {
            Vec::new()
        } else {
            guard.entries[start..slice_end].to_vec()
        };

        let partial = !guard.done && guard.entries.len() >= end;
        let num_entries_total = if guard.done {
            Some(guard.entries.len().min(u32::MAX as usize) as u32)
        } else {
            None
        };

        Ok(ZenodoTarEntryListResponse {
            offset,
            length,
            entries,
            partial,
            num_entries_total,
        })
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

#[tauri::command]
pub async fn zenodo_tar_peek_entry(
    cache: State<'_, ZenodoTarScanCache>,
    content_url: String,
    filename: String,
    entry_name: String,
) -> AppResult<FieldPreview> {
    let filename = filename.trim().to_string();
    if filename.is_empty() {
        return Err(AppError::Invalid("Missing filename.".into()));
    }
    if !looks_like_tar(&filename) {
        return Err(AppError::Invalid(
            "Selected file is not a supported TAR archive.".into(),
        ));
    }

    let trimmed = content_url.trim();
    let url =
        Url::parse(trimmed).map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
    if !allowed_content_url(&url) {
        return Err(AppError::Invalid("Blocked content URL.".into()));
    }
    let entry_name = entry_name.trim().to_string();
    if entry_name.is_empty() {
        return Err(AppError::Invalid("Missing TAR entry name.".into()));
    }

    if let Ok(state) = cache.get_or_create(&content_url, &filename) {
        let wanted = normalize_member_path_str(&entry_name);
        if let Ok(guard) = state.lock() {
            if let Some(hit) = guard.cached_preview(&wanted) {
                return Ok(hit);
            }
        }
    }

    tauri::async_runtime::spawn_blocking(move || {
        let (data, size) =
            read_tar_member_with_limit(url, filename, entry_name.clone(), PEEK_BYTES as u64, None)?;
        let text = String::from_utf8(data.clone()).ok();
        let guessed_ext = ext_from_filename(&entry_name)
            .or_else(|| infer::get(&data).map(|t| t.extension().to_string()));
        let hex_snippet = hex_encode(data.iter().take(48).copied().collect::<Vec<u8>>());
        Ok(FieldPreview {
            preview_text: text.as_ref().map(|s| s.chars().take(400).collect()),
            hex_snippet,
            guessed_ext,
            is_binary: text.is_none(),
            size: size.min(u32::MAX as u64) as u32,
        })
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

#[tauri::command]
pub async fn zenodo_tar_open_entry(
    content_url: String,
    filename: String,
    entry_name: String,
    opener_app_path: Option<String>,
) -> AppResult<OpenLeafResponse> {
    let filename = filename.trim().to_string();
    if filename.is_empty() {
        return Err(AppError::Invalid("Missing filename.".into()));
    }
    if !looks_like_tar(&filename) {
        return Err(AppError::Invalid(
            "Selected file is not a supported TAR archive.".into(),
        ));
    }

    let trimmed = content_url.trim();
    let url =
        Url::parse(trimmed).map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
    if !allowed_content_url(&url) {
        return Err(AppError::Invalid("Blocked content URL.".into()));
    }
    let entry_name = entry_name.trim().to_string();
    if entry_name.is_empty() {
        return Err(AppError::Invalid("Missing TAR entry name.".into()));
    }

    tauri::async_runtime::spawn_blocking(move || {
        let (bytes, size) = read_tar_member_with_limit(
            url.clone(),
            filename.clone(),
            entry_name.clone(),
            MAX_INLINE_DOWNLOAD_BYTES,
            Some(MAX_INLINE_DOWNLOAD_BYTES),
        )?;

        let record_id = record_id_from_content_url(&url).unwrap_or_else(|| "unknown".into());
        let temp_dir = std::env::temp_dir()
            .join("dataset-inspector")
            .join("zenodo");
        std::fs::create_dir_all(&temp_dir)?;

        let ext = ext_from_filename(&entry_name).unwrap_or_else(|| "bin".into());
        let base = format!(
            "{}-r{}-{}",
            sanitize(url.host_str().unwrap_or("zenodo")),
            sanitize(&record_id),
            sanitize(&filename)
        );
        let entry_filename = entry_name.split('/').last().unwrap_or(entry_name.as_str());
        let entry_stem_raw = entry_filename
            .rsplit_once('.')
            .map(|(s, _)| s)
            .unwrap_or(entry_filename);
        let entry_stem = sanitize(entry_stem_raw);
        let out_path = temp_dir.join(format!("{base}-{entry_stem}.{ext}"));
        std::fs::write(&out_path, &bytes)?;

        let mut opened = false;
        let mut open_error = None::<String>;
        if let Some(app_path) = opener_app_path.as_deref() {
            match open_with::open_with_app_detached(&out_path, app_path) {
                Ok(()) => opened = true,
                Err(err) => open_error = Some(err),
            }
        }
        if !opened {
            if let Err(err) = open::that_detached(&out_path) {
                open_error = Some(err.to_string());
            } else {
                opened = true;
            }
        }

        let size_u32 = size.min(u32::MAX as u64) as u32;
        let base_msg = format!("{} ({} bytes)", out_path.display(), size_u32);
        let mut message = base_msg;
        let needs_opener = !opened && open_error.is_some();
        if needs_opener {
            message.push_str(" · no default app found, choose an app to open it");
        }

        Ok(OpenLeafResponse {
            path: out_path.display().to_string(),
            size: size_u32,
            ext,
            opened,
            needs_opener,
            message,
        })
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

#[tauri::command]
pub async fn zenodo_tar_inline_entry_media(
    cache: State<'_, ZenodoTarScanCache>,
    content_url: String,
    filename: String,
    entry_name: String,
) -> AppResult<InlineMediaResponse> {
    let filename = filename.trim().to_string();
    if filename.is_empty() {
        return Err(AppError::Invalid("Missing filename.".into()));
    }
    if !looks_like_tar(&filename) {
        return Err(AppError::Invalid(
            "Selected file is not a supported TAR archive.".into(),
        ));
    }

    let trimmed = content_url.trim();
    let url =
        Url::parse(trimmed).map_err(|_| AppError::Invalid("Invalid Zenodo content URL.".into()))?;
    if !allowed_content_url(&url) {
        return Err(AppError::Invalid("Blocked content URL.".into()));
    }
    let entry_name = entry_name.trim().to_string();
    if entry_name.is_empty() {
        return Err(AppError::Invalid("Missing TAR entry name.".into()));
    }

    if let Ok(state) = cache.get_or_create(&content_url, &filename) {
        let wanted = normalize_member_path_str(&entry_name);
        if let Ok(mut guard) = state.lock() {
            if let Some(hit) = guard.cached_media(&wanted) {
                let base64 = base64::engine::general_purpose::STANDARD.encode(&hit.bytes);
                return Ok(InlineMediaResponse {
                    base64,
                    mime: hit.mime,
                    size: (hit.bytes.len() as u64).min(u32::MAX as u64) as u32,
                    ext: hit.ext,
                });
            }
        }
    }

    tauri::async_runtime::spawn_blocking(move || {
        let (bytes, size) = read_tar_member_with_limit(
            url,
            filename,
            entry_name.clone(),
            TAR_INLINE_MEDIA_MAX_BYTES,
            Some(TAR_INLINE_MEDIA_MAX_BYTES),
        )?;
        let ext = ext_from_filename(&entry_name).unwrap_or_else(|| "bin".into());
        let mime = mime_for_ext(&ext).to_string();
        let base64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        Ok(InlineMediaResponse {
            base64,
            mime,
            size: size.min(u32::MAX as u64) as u32,
            ext,
        })
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}
