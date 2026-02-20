# Development

## Prerequisites
- Flutter SDK (stable, Dart >= 3.4)
- Platform toolchains for your target OS
  - macOS: Xcode
  - Windows: Visual Studio Build Tools
  - Linux: gcc + gtk3/gtk4 dev packages
- Supported targets: Windows, macOS, Linux (web planned).

## Commands
- Install deps: `flutter pub get`
- Generate desktop runners (first time only): `flutter create --platforms=windows,macos,linux .`
- Run desktop app: `flutter run -d macos|windows|linux`
- Build desktop app: `flutter build macos|windows|linux`
- Run all tests: `flutter test`

## Layout
- `lib/main.dart`: app entry point
- `lib/app.dart`: app theme + root wiring
- `lib/state/viewer_state.dart`: GUI state + orchestration (UI-facing path)
- `lib/services/dataset_kernel.dart`: unified dataset capability kernel
- `lib/services/dataset_workspace_store.dart`: mutable workspace persistence and snapshot/applications helpers
- `lib/services/*`: dataset parsers + remote clients
- `lib/services/huggingface_service.dart`: Hugging Face dataset viewer client
- `lib/services/zenodo_service.dart`: Zenodo record + entry preview
- `lib/services/update_service.dart`: update checks + downloads
- `lib/widgets/inspector_screen.dart`: primary UI
- `lib/widgets/audio_preview.dart`: audio playback
- `lib/widgets/video_preview.dart`: video playback

## Notes
- Heavy dataset parsing runs in Dart services; keep UI responsive by avoiding synchronous loops on the main isolate.
- Zstd/tar decompression uses temp caches under the system temp directory.
- See `docs/audio.md` for audio preview details.
- DuckDB bundling and custom httpfs builds are documented in `docs/duckdb.md`.
- Parquet and DuckDB behavior for large datasets is documented in `docs/duckdb.md`.
