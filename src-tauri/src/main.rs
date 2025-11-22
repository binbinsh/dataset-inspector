#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod litdata;

use litdata::{list_chunk_items, load_chunk_list, load_index, open_leaf, peek_field, ChunkCache};

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
            open_leaf
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
