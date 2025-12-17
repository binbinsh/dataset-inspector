use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkSummary {
    pub filename: String,
    pub path: String,
    pub chunk_size: u32,
    pub chunk_bytes: u64,
    pub dim: Option<u32>,
    pub exists: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexSummary {
    pub index_path: String,
    pub root_dir: String,
    pub data_format: Vec<String>,
    pub compression: Option<String>,
    pub chunk_size: Option<u32>,
    pub chunk_bytes: Option<u64>,
    pub config_raw: serde_json::Value,
    pub chunks: Vec<ChunkSummary>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldMeta {
    pub field_index: usize,
    pub size: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemMeta {
    pub item_index: u32,
    pub total_bytes: u64,
    pub fields: Vec<FieldMeta>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldPreview {
    pub preview_text: Option<String>,
    pub hex_snippet: String,
    pub guessed_ext: Option<String>,
    pub is_binary: bool,
    pub size: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLeafResponse {
    pub path: String,
    pub size: u32,
    pub ext: String,
    pub opened: bool,
    pub needs_opener: bool,
    pub message: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedFileResponse {
    pub path: String,
    pub size: u32,
    pub ext: String,
}

