#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app_error;
mod audio;
mod huggingface;
mod litdata;
mod open_with;

#[cfg(desktop)]
use tauri::Emitter;
#[cfg(all(desktop, target_os = "macos"))]
use tauri::menu::{MenuBuilder, SubmenuBuilder};

use huggingface::{hf_dataset_preview, HfClient};
use huggingface::hf_open_field;
use litdata::{
    list_chunk_items, load_chunk_list, load_index, open_leaf, peek_field, prepare_audio_preview, ChunkCache,
};
use open_with::open_path_with_app;

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            #[cfg(desktop)]
            app.handle().plugin(tauri_plugin_updater::Builder::new().build())?;

            #[cfg(all(desktop, target_os = "macos"))]
            {
                let handle = app.handle();
                let app_menu = SubmenuBuilder::new(handle, handle.package_info().name.clone())
                    .about(None)
                    .separator()
                    .text("check_updates", "Check for Updates…")
                    .separator()
                    .services()
                    .separator()
                    .hide()
                    .hide_others()
                    .show_all()
                    .separator()
                    .quit()
                    .build()?;

                let edit_menu = SubmenuBuilder::new(handle, "Edit")
                    .undo()
                    .redo()
                    .separator()
                    .cut()
                    .copy()
                    .paste()
                    .select_all()
                    .build()?;

                let menu = MenuBuilder::new(handle).item(&app_menu).item(&edit_menu).build()?;
                app.set_menu(menu)?;
            }

            Ok(())
        })
        .on_menu_event(|app, event| {
            if event.id() == "check_updates" {
                let _ = app.emit_to("main", "app://check-updates", ());
            }
        })
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .manage(ChunkCache::default())
        .manage(HfClient::default())
        .invoke_handler(tauri::generate_handler![
            load_index,
            load_chunk_list,
            list_chunk_items,
            peek_field,
            open_leaf,
            prepare_audio_preview,
            open_path_with_app,
            hf_dataset_preview,
            hf_open_field
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
