use std::{
    path::{Path, PathBuf},
    process::Command,
};

use tauri::async_runtime::spawn_blocking;

use crate::app_error::{AppError, AppResult};

#[tauri::command]
pub async fn open_path_with_app(path: String, app_path: String) -> AppResult<String> {
    spawn_blocking(move || {
        let target = PathBuf::from(path.trim());
        if !target.exists() {
            return Err(AppError::Missing("file does not exist".into()));
        }
        if !target.is_file() {
            return Err(AppError::Invalid("path is not a file".into()));
        }
        let app_path = app_path.trim();
        if app_path.is_empty() {
            return Err(AppError::Invalid("app path is empty".into()));
        }
        open_with_app_detached(&target, app_path).map_err(AppError::Open)?;
        Ok(format!("Opened {} with {}", target.display(), app_path))
    })
    .await
    .map_err(|e| AppError::Task(e.to_string()))?
}

pub fn open_with_app_detached(target: &Path, app_path: &str) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        let mut cmd = Command::new("open");
        cmd.arg("-a").arg(app_path).arg(target);
        if cmd.spawn().is_ok() {
            return Ok(());
        }
    }
    Command::new(app_path)
        .arg(target)
        .spawn()
        .map(|_| ())
        .map_err(|e| e.to_string())
}
