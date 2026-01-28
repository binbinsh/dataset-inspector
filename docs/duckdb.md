# DuckDB Bundling

This project bundles custom DuckDB binaries with `httpfs` built in to avoid extension autoload/install at runtime. Build the library on the target OS (or CI for that OS), then copy it into the `dart_duckdb` plugin path so Flutter will package it into the app bundle.

## macOS
- Build: `scripts/build_duckdb_with_httpfs.sh`
- Output: `macos/Frameworks/libduckdb.dylib`
- Plugin copy (if available): `macos/Flutter/ephemeral/.symlinks/plugins/dart_duckdb/macos/Libraries/release/libduckdb.dylib`
- Rebuild: `flutter build macos`
Notes: The macOS build script compiles a static OpenSSL to avoid runtime linkage to Homebrew `libssl.3.dylib`.

## Linux
Prereqs: `cmake`, `ninja` (optional), `git`, OpenSSL dev headers (`libssl-dev`), and libcurl dev headers (`libcurl4-openssl-dev` on Debian/Ubuntu).

- Build: `scripts/build_duckdb_with_httpfs_linux.sh`
- Output: `build/duckdb-linux/output/libduckdb.so`
- Plugin copy (if available): `linux/flutter/ephemeral/.plugin_symlinks/dart_duckdb/linux/Libraries/release/libduckdb.so`
- Rebuild: `flutter build linux`

## Windows
Prereqs: Visual Studio Build Tools (MSVC), `cmake`, `git`, and libcurl/OpenSSL via vcpkg. If `httpfs` fails to build, set `CMAKE_TOOLCHAIN_FILE` and `OPENSSL_ROOT_DIR` (see DuckDB Windows build docs).

- Build: `scripts/build_duckdb_with_httpfs_windows.ps1`
- Output: `build\duckdb-windows\output\duckdb.dll`
- Plugin copy (if available): `windows\flutter\ephemeral\.plugin_symlinks\dart_duckdb\windows\Libraries\release\duckdb.dll`
- Rebuild: `flutter build windows`

## Environment overrides
- `DUCKDB_VERSION`: DuckDB tag to build (default `v1.4.4`).
- `DUCKDB_HTTPFS_REF`: Optional ref for the standalone `duckdb-httpfs` repo. If the in-tree extension is missing, the build scripts will clone `duckdb-httpfs` (default ref: `main`).
- `HTTPFS_SRC`: Path to a local `duckdb-httpfs` checkout (optional).
- `DART_DUCKDB_LINUX_DIR`: Override the Linux plugin path.
- `DART_DUCKDB_WINDOWS_DIR`: Override the Windows plugin path.
- `OPENSSL_ROOT_DIR`: CMake OpenSSL root (Linux/Windows).
- `OPENSSL_USE_STATIC_LIBS`: Set to `TRUE` for static OpenSSL (if available).
- `CMAKE_TOOLCHAIN_FILE`: CMake toolchain file (Windows).
- `DUCKDB_GENERATOR`: CMake generator (Windows).
- `DUCKDB_ARCH`: CMake architecture (Windows).
