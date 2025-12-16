#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app_error;
mod audio;
mod litdata;
mod open_with;

use litdata::{list_chunk_items, load_chunk_list, load_index, open_leaf, peek_field, ChunkCache};
use open_with::open_path_with_app;

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .manage(ChunkCache::default())
        .invoke_handler(tauri::generate_handler![
            load_index,
            load_chunk_list,
            list_chunk_items,
            peek_field,
            open_leaf,
            open_path_with_app
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
