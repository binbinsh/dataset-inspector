#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app_error;
mod audio;
mod huggingface;
mod ipc_types;
mod litdata;
mod mosaicml;
mod open_with;
mod webdataset;
mod zenodo;

#[cfg(all(desktop, target_os = "macos"))]
use tauri::menu::{MenuBuilder, SubmenuBuilder};
#[cfg(desktop)]
use tauri::Emitter;

use huggingface::hf_open_field;
use huggingface::{hf_dataset_preview, HfClient};
use litdata::{
    list_chunk_items, load_chunk_list, load_index, open_leaf, peek_field, prepare_audio_preview,
    ChunkCache,
};
use mosaicml::{
    mosaicml_list_samples, mosaicml_load_index, mosaicml_open_leaf, mosaicml_peek_field,
    mosaicml_prepare_audio_preview,
};
use open_with::open_path_with_app;
use webdataset::{
    detect_local_dataset, wds_list_samples, wds_load_dir, wds_open_member, wds_peek_member,
    wds_prepare_audio_preview, WdsScanCache,
};
use zenodo::{
    zenodo_open_file, zenodo_peek_file, zenodo_record_summary, zenodo_tar_inline_entry_media,
    zenodo_tar_list_entries_paged, zenodo_tar_open_entry, zenodo_tar_peek_entry,
    zenodo_zip_inline_entry_media, zenodo_zip_list_entries, zenodo_zip_open_entry,
    zenodo_zip_peek_entry, ZenodoClient, ZenodoTarScanCache, ZenodoZipIndexCache,
};

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            #[cfg(desktop)]
            app.handle()
                .plugin(tauri_plugin_updater::Builder::new().build())?;

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

                let menu = MenuBuilder::new(handle)
                    .item(&app_menu)
                    .item(&edit_menu)
                    .build()?;
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
        .manage(WdsScanCache::default())
        .manage(HfClient::default())
        .manage(ZenodoClient::default())
        .manage(ZenodoZipIndexCache::default())
        .manage(ZenodoTarScanCache::default())
        .invoke_handler(tauri::generate_handler![
            detect_local_dataset,
            load_index,
            load_chunk_list,
            list_chunk_items,
            peek_field,
            open_leaf,
            prepare_audio_preview,
            mosaicml_load_index,
            mosaicml_list_samples,
            mosaicml_peek_field,
            mosaicml_open_leaf,
            mosaicml_prepare_audio_preview,
            wds_load_dir,
            wds_list_samples,
            wds_peek_member,
            wds_open_member,
            wds_prepare_audio_preview,
            open_path_with_app,
            hf_dataset_preview,
            hf_open_field,
            zenodo_record_summary,
            zenodo_peek_file,
            zenodo_open_file,
            zenodo_zip_list_entries,
            zenodo_zip_peek_entry,
            zenodo_zip_open_entry,
            zenodo_zip_inline_entry_media,
            zenodo_tar_list_entries_paged,
            zenodo_tar_peek_entry,
            zenodo_tar_open_entry,
            zenodo_tar_inline_entry_media
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
