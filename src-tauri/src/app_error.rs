use serde::Serialize;
use thiserror::Error;

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
    #[error("remote error: {0}")]
    Remote(String),
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
