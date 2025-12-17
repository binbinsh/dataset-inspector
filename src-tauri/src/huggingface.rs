use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::{fs, path::PathBuf};
use tauri::State;
use url::Url;

use crate::app_error::{AppError, AppResult};
use crate::ipc_types::OpenLeafResponse;
use crate::open_with;

const DATASETS_SERVER_BASE: &str = "https://datasets-server.huggingface.co/";
const DEFAULT_ROWS: usize = 25;
const MAX_ROWS: usize = 100;
const MAX_INLINE_TEXT: usize = 10 * 1024 * 1024;

#[derive(Clone)]
pub struct HfClient {
    http: reqwest::Client,
}

impl Default for HfClient {
    fn default() -> Self {
        let http = reqwest::Client::builder()
            .http1_only()
            .user_agent("dataset-inspector/0.6.0 (tauri)")
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { http }
    }
}

#[derive(Deserialize)]
struct SplitsResponse {
    splits: Vec<SplitEntry>,
}

#[derive(Deserialize)]
struct SplitEntry {
    config: String,
    split: String,
}

#[derive(Deserialize)]
struct RowsResponse {
    features: Vec<FeatureEntry>,
    rows: Vec<RowEntry>,
    num_rows_total: usize,
    partial: bool,
}

#[derive(Deserialize)]
struct FeatureEntry {
    name: String,
    #[serde(rename = "type")]
    ty: serde_json::Value,
}

#[derive(Deserialize)]
struct RowEntry {
    row: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HfConfigSummary {
    config: String,
    splits: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HfFeature {
    name: String,
    dtype: Option<String>,
    raw_type: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HfDatasetPreview {
    dataset: String,
    config: String,
    split: String,
    configs: Vec<HfConfigSummary>,
    offset: usize,
    length: usize,
    num_rows_total: usize,
    partial: bool,
    features: Vec<HfFeature>,
    rows: Vec<serde_json::Value>,
}

fn validate_repo_segment(segment: &str) -> bool {
    if segment.is_empty() {
        return false;
    }
    segment
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn extract_repo_id_from_url(url: &Url) -> Option<String> {
    let segments: Vec<_> = url
        .path_segments()
        .map(|it| it.filter(|s| !s.is_empty()).collect::<Vec<_>>())
        .unwrap_or_default();
    if segments.is_empty() {
        return None;
    }

    if url.scheme() == "hf" && url.host_str() == Some("datasets") {
        if segments.len() < 2 {
            return None;
        }
        let org = segments[0];
        let mut name = segments[1];
        if let Some((base, _rev)) = name.split_once('@') {
            name = base;
        }
        if validate_repo_segment(org) && validate_repo_segment(name) {
            return Some(format!("{org}/{name}"));
        }
        return None;
    }

    if matches!(url.host_str(), Some("huggingface.co") | Some("hf.co")) {
        let idx = segments.iter().position(|s| *s == "datasets")?;
        if segments.len() < idx + 3 {
            return None;
        }
        let org = segments[idx + 1];
        let name = segments[idx + 2];
        if validate_repo_segment(org) && validate_repo_segment(name) {
            return Some(format!("{org}/{name}"));
        }
    }

    None
}

fn extract_repo_id(input: &str) -> AppResult<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(AppError::Invalid(
            "Provide a Hugging Face dataset URL, or hf://datasets/<namespace>/<dataset-name>/<path>.".into(),
        ));
    }

    if let Ok(url) = Url::parse(trimmed) {
        if let Some(repo) = extract_repo_id_from_url(&url) {
            return Ok(repo);
        }
        return Err(AppError::Invalid(
            "Unsupported Hugging Face URL. Expected https://huggingface.co/datasets/<namespace>/<dataset-name> or https://hf.co/datasets/<namespace>/<dataset-name>."
                .into(),
        ));
    }

    Err(AppError::Invalid(
        "Unsupported input. Provide a dataset URL like https://huggingface.co/datasets/<namespace>/<dataset-name> or hf://datasets/<namespace>/<dataset-name>/<path>."
            .into(),
    ))
}

fn pick_default_split(splits: &BTreeSet<String>) -> String {
    if splits.contains("train") {
        return "train".into();
    }
    if let Some(found) = splits.iter().find(|s| s.starts_with("train")) {
        return found.to_string();
    }
    splits
        .iter()
        .next()
        .cloned()
        .unwrap_or_else(|| "train".into())
}

fn feature_dtype_label(ty: &serde_json::Value) -> Option<String> {
    ty.get("dtype")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            ty.get("_type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
}

async fn get_json<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: Url,
    token: Option<&str>,
) -> AppResult<T> {
    let mut req = client.get(url.clone());
    if let Some(t) = token.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        req = req.header(reqwest::header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let res = req
        .send()
        .await
        .map_err(|e| AppError::Remote(format!("request failed: {e}")))?;
    let status = res.status();
    let text = res
        .text()
        .await
        .map_err(|e| AppError::Remote(format!("read response failed: {e}")))?;

    let value: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| AppError::Remote(format!("invalid JSON from {url}: {e}")))?;

    if let Some(err) = value.get("error").and_then(|v| v.as_str()) {
        return Err(AppError::Invalid(err.to_string()));
    }
    if !status.is_success() {
        return Err(AppError::Remote(format!("HTTP {status} from {url}")));
    }

    serde_json::from_value(value).map_err(|e| AppError::Remote(format!("parse failed: {e}")))
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn allowed_asset_url(url: &Url) -> bool {
    if url.scheme() != "https" && url.scheme() != "http" {
        return false;
    }
    match url.host_str() {
        Some("datasets-server.huggingface.co") => true,
        Some("huggingface.co") => true,
        Some("hf.co") => true,
        Some("cdn-lfs.huggingface.co") => true,
        _ => false,
    }
}

fn ext_from_url(url: &Url) -> Option<String> {
    let name = url.path_segments()?.last()?.to_string();
    let ext = name.rsplit_once('.')?.1.trim();
    if ext.is_empty() {
        None
    } else {
        Some(ext.to_lowercase())
    }
}

fn ext_from_mime(mime: &str) -> Option<&'static str> {
    let m = mime.trim().to_lowercase();
    if m == "audio/wav" || m == "audio/x-wav" {
        return Some("wav");
    }
    if m == "audio/mpeg" || m == "audio/mp3" {
        return Some("mp3");
    }
    if m == "audio/flac" || m == "audio/x-flac" {
        return Some("flac");
    }
    if m == "audio/ogg" {
        return Some("ogg");
    }
    if m == "audio/opus" {
        return Some("opus");
    }
    if m == "audio/aac" {
        return Some("aac");
    }
    if m == "audio/mp4" {
        return Some("m4a");
    }
    if m == "image/jpeg" {
        return Some("jpg");
    }
    if m == "image/png" {
        return Some("png");
    }
    None
}

fn extract_asset(value: &serde_json::Value) -> Option<(Url, Option<String>)> {
    match value {
        serde_json::Value::Object(map) => {
            let src = map.get("src").and_then(|v| v.as_str())?.trim();
            if src.is_empty() {
                return None;
            }
            let url = Url::parse(src).ok()?;
            let mime = map
                .get("type")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_string());
            Some((url, mime))
        }
        serde_json::Value::Array(arr) => arr.iter().find_map(extract_asset),
        _ => None,
    }
}

async fn download_bytes(
    client: &reqwest::Client,
    url: Url,
    token: Option<&str>,
) -> AppResult<Vec<u8>> {
    if !allowed_asset_url(&url) {
        return Err(AppError::Invalid("Blocked asset URL host/scheme.".into()));
    }
    let mut req = client.get(url.clone());
    if let Some(t) = token.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        req = req.header(reqwest::header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let res = req
        .send()
        .await
        .map_err(|e| AppError::Remote(format!("asset request failed: {e}")))?;
    let status = res.status();
    if !status.is_success() {
        return Err(AppError::Remote(format!("asset HTTP {status} from {url}")));
    }
    res.bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| AppError::Remote(format!("asset read failed: {e}")))
}

#[tauri::command]
pub async fn hf_dataset_preview(
    client: State<'_, HfClient>,
    input: String,
    config: Option<String>,
    split: Option<String>,
    offset: Option<usize>,
    length: Option<usize>,
    token: Option<String>,
) -> AppResult<HfDatasetPreview> {
    let dataset = extract_repo_id(&input)?;
    let offset = offset.unwrap_or(0);
    let length = length.unwrap_or(DEFAULT_ROWS).clamp(1, MAX_ROWS);
    let token = token.as_deref();

    let mut splits_url = Url::parse(DATASETS_SERVER_BASE)
        .map_err(|e| AppError::Remote(format!("invalid datasets-server base url: {e}")))?;
    splits_url.set_path("splits");
    splits_url
        .query_pairs_mut()
        .append_pair("dataset", &dataset);
    let splits_resp: SplitsResponse = get_json(&client.http, splits_url, token).await?;

    let mut configs_map: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for entry in splits_resp.splits {
        configs_map
            .entry(entry.config)
            .or_default()
            .insert(entry.split);
    }
    if configs_map.is_empty() {
        return Err(AppError::Missing(format!(
            "No supported splits found for dataset {dataset}."
        )));
    }

    let selected_config = config
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| configs_map.keys().next().cloned().unwrap_or_default());
    let splits_for_config = configs_map.get(&selected_config).ok_or_else(|| {
        AppError::Invalid(format!(
            "Unknown config '{selected_config}' for dataset {dataset}."
        ))
    })?;

    let selected_split = split
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| pick_default_split(splits_for_config));
    if !splits_for_config.contains(&selected_split) {
        return Err(AppError::Invalid(format!(
            "Unknown split '{selected_split}' for config '{selected_config}'."
        )));
    }

    let mut rows_url = Url::parse(DATASETS_SERVER_BASE)
        .map_err(|e| AppError::Remote(format!("invalid datasets-server base url: {e}")))?;
    rows_url.set_path("rows");
    {
        let mut qp = rows_url.query_pairs_mut();
        qp.append_pair("dataset", &dataset);
        qp.append_pair("config", &selected_config);
        qp.append_pair("split", &selected_split);
        qp.append_pair("offset", &offset.to_string());
        qp.append_pair("length", &length.to_string());
    }
    let rows_resp: RowsResponse = get_json(&client.http, rows_url, token).await?;

    let mut configs: Vec<HfConfigSummary> = Vec::with_capacity(configs_map.len());
    for (config_name, splits) in configs_map {
        configs.push(HfConfigSummary {
            config: config_name,
            splits: splits.into_iter().collect(),
        });
    }

    let features = rows_resp
        .features
        .into_iter()
        .map(|f| HfFeature {
            name: f.name,
            dtype: feature_dtype_label(&f.ty),
            raw_type: f.ty,
        })
        .collect::<Vec<_>>();
    let rows = rows_resp.rows.into_iter().map(|r| r.row).collect();

    Ok(HfDatasetPreview {
        dataset,
        config: selected_config,
        split: selected_split,
        configs,
        offset,
        length,
        num_rows_total: rows_resp.num_rows_total,
        partial: rows_resp.partial,
        features,
        rows,
    })
}

#[tauri::command]
pub async fn hf_open_field(
    client: State<'_, HfClient>,
    input: String,
    config: String,
    split: String,
    row_index: usize,
    field_name: String,
    opener_app_path: Option<String>,
    token: Option<String>,
) -> AppResult<OpenLeafResponse> {
    let dataset = extract_repo_id(&input)?;
    let config = config.trim().to_string();
    let split = split.trim().to_string();
    let field_name = field_name.trim().to_string();
    let token = token.as_deref();
    if config.is_empty() {
        return Err(AppError::Invalid("Missing config.".into()));
    }
    if split.is_empty() {
        return Err(AppError::Invalid("Missing split.".into()));
    }
    if field_name.is_empty() {
        return Err(AppError::Invalid("Missing field name.".into()));
    }

    let mut rows_url = Url::parse(DATASETS_SERVER_BASE)
        .map_err(|e| AppError::Remote(format!("invalid datasets-server base url: {e}")))?;
    rows_url.set_path("rows");
    {
        let mut qp = rows_url.query_pairs_mut();
        qp.append_pair("dataset", &dataset);
        qp.append_pair("config", &config);
        qp.append_pair("split", &split);
        qp.append_pair("offset", &row_index.to_string());
        qp.append_pair("length", "1");
    }

    let rows_resp: RowsResponse = get_json(&client.http, rows_url, token).await?;
    let row = rows_resp
        .rows
        .into_iter()
        .next()
        .ok_or_else(|| AppError::Missing("No row returned for the requested offset.".into()))?
        .row;
    let row_obj = row
        .as_object()
        .ok_or_else(|| AppError::Invalid("Row is not a JSON object.".into()))?;
    let value = row_obj.get(&field_name).cloned().ok_or_else(|| {
        AppError::Missing(format!(
            "Field '{field_name}' not found in the requested row."
        ))
    })?;

    if let Some((asset_url, mime)) = extract_asset(&value) {
        let bytes = download_bytes(&client.http, asset_url.clone(), token).await?;
        let ext = ext_from_url(&asset_url)
            .or_else(|| {
                mime.as_deref()
                    .and_then(ext_from_mime)
                    .map(|s| s.to_string())
            })
            .or_else(|| infer::get(&bytes).map(|t| t.extension().to_string()))
            .unwrap_or_else(|| "bin".into());
        let size = bytes.len().min(u32::MAX as usize) as u32;
        let temp_dir = std::env::temp_dir()
            .join("dataset-inspector")
            .join("huggingface");
        fs::create_dir_all(&temp_dir)?;
        let base_name = format!(
            "{}-{}-{}-r{}-{}",
            sanitize(&dataset),
            sanitize(&config),
            sanitize(&split),
            row_index,
            sanitize(&field_name)
        );
        let out: PathBuf = temp_dir.join(format!("{base_name}.{ext}"));
        fs::write(&out, &bytes)?;

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

        let base = format!("{} ({} bytes)", out.display(), size);
        let mut message = base;
        let needs_opener = !opened && open_error.is_some();
        if needs_opener {
            message.push_str(" · no default app found, choose an app to open it");
        }

        return Ok(OpenLeafResponse {
            path: out.display().to_string(),
            size,
            ext,
            opened,
            needs_opener,
            message,
        });
    }

    let (bytes, ext): (Vec<u8>, String) = match value {
        serde_json::Value::String(s) => {
            if s.len() > MAX_INLINE_TEXT {
                return Err(AppError::Invalid("Text field is too large to open.".into()));
            }
            (s.into_bytes(), "txt".into())
        }
        serde_json::Value::Object(map) => {
            let bytes = serde_json::to_vec_pretty(&serde_json::Value::Object(map))
                .map_err(|e| AppError::Invalid(format!("JSON serialize failed: {e}")))?;
            (bytes, "json".into())
        }
        serde_json::Value::Array(_) => {
            let bytes = serde_json::to_vec_pretty(&value)
                .map_err(|e| AppError::Invalid(format!("JSON serialize failed: {e}")))?;
            (bytes, "json".into())
        }
        other => {
            let bytes = serde_json::to_vec_pretty(&other)
                .map_err(|e| AppError::Invalid(format!("JSON serialize failed: {e}")))?;
            (bytes, "json".into())
        }
    };

    let size = bytes.len().min(u32::MAX as usize) as u32;
    let temp_dir = std::env::temp_dir()
        .join("dataset-inspector")
        .join("huggingface");
    fs::create_dir_all(&temp_dir)?;
    let base_name = format!(
        "{}-{}-{}-r{}-{}",
        sanitize(&dataset),
        sanitize(&config),
        sanitize(&split),
        row_index,
        sanitize(&field_name)
    );
    let out: PathBuf = temp_dir.join(format!("{base_name}.{ext}"));
    fs::write(&out, &bytes)?;

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
