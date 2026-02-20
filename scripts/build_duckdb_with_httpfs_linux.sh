#!/bin/bash
set -euo pipefail

# Build DuckDB with httpfs built-in on Linux.
# Produces libduckdb.so and installs it into the dart_duckdb plugin path if present.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build/duckdb-linux"

DEFAULT_DUCKDB_VERSION="v1.4.4"
DUCKDB_VERSION="${DUCKDB_VERSION:-$DEFAULT_DUCKDB_VERSION}"
if [[ "$DUCKDB_VERSION" != v* ]]; then
    DUCKDB_VERSION="v$DUCKDB_VERSION"
fi

ARCH="${DUCKDB_ARCH:-$(uname -m)}"

echo "=== Building DuckDB $DUCKDB_VERSION with httpfs built-in ==="
echo "Build directory: $BUILD_DIR"
echo "Architecture: $ARCH"

if ! command -v cmake >/dev/null 2>&1; then
    echo "ERROR: cmake not found."
    exit 1
fi

if ! command -v git >/dev/null 2>&1; then
    echo "ERROR: git not found."
    exit 1
fi

if command -v pkg-config >/dev/null 2>&1; then
    if ! pkg-config --exists openssl; then
        echo "WARN: OpenSSL dev package not found via pkg-config. httpfs may fail to build."
    fi
else
    echo "WARN: pkg-config not found. Ensure OpenSSL dev headers are installed."
fi

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

if [ ! -d "duckdb" ]; then
    echo "Cloning DuckDB..."
    git clone --depth 1 --branch "$DUCKDB_VERSION" https://github.com/duckdb/duckdb.git
fi

if [ ! -d "duckdb/.git" ]; then
    echo "ERROR: $BUILD_DIR/duckdb exists but is not a git repository."
    exit 1
fi

cd duckdb
CURRENT_TAG="$(git describe --tags --always || true)"
if [ "$CURRENT_TAG" != "$DUCKDB_VERSION" ]; then
    echo "Switching DuckDB repo to $DUCKDB_VERSION..."
    if ! git rev-parse "$DUCKDB_VERSION" >/dev/null 2>&1; then
        git fetch --depth 1 origin "refs/tags/$DUCKDB_VERSION:refs/tags/$DUCKDB_VERSION"
    fi
    git checkout "$DUCKDB_VERSION"
fi
cd ..

# Prepare httpfs extension sources
HTTPFS_DIR="$BUILD_DIR/duckdb/extension/httpfs"
HTTPFS_SRC="${HTTPFS_SRC:-$PROJECT_DIR/third_party/duckdb-httpfs}"

if [ -d "$HTTPFS_SRC" ]; then
    echo "Using httpfs source from: $HTTPFS_SRC"
    rm -rf "$HTTPFS_DIR"
    mkdir -p "$HTTPFS_DIR"
    cp -R "$HTTPFS_SRC/." "$HTTPFS_DIR/"
    rm -rf "$HTTPFS_DIR/.git" 2>/dev/null || true
elif [ -d "$HTTPFS_DIR" ]; then
    echo "Using DuckDB in-tree httpfs extension."
else
    HTTPFS_REF="${DUCKDB_HTTPFS_REF:-main}"
    echo "Cloning httpfs extension (ref: $HTTPFS_REF)..."
    rm -rf "$HTTPFS_DIR"
    git clone --depth 1 --branch "$HTTPFS_REF" https://github.com/duckdb/duckdb-httpfs.git "$HTTPFS_DIR"
fi

BUILD_OUT="$BUILD_DIR/duckdb/build/release"
rm -rf "$BUILD_OUT"
mkdir -p "$BUILD_OUT"

CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DBUILD_EXTENSIONS="httpfs;parquet;json"
    -DENABLE_EXTENSION_AUTOLOADING=OFF
    -DENABLE_EXTENSION_AUTOINSTALL=OFF
    -DBUILD_SHELL=OFF
    -DBUILD_UNITTESTS=OFF
)

if [ -n "${OPENSSL_ROOT_DIR:-}" ]; then
    CMAKE_ARGS+=("-DOPENSSL_ROOT_DIR=$OPENSSL_ROOT_DIR")
fi
if [ -n "${OPENSSL_USE_STATIC_LIBS:-}" ]; then
    CMAKE_ARGS+=("-DOPENSSL_USE_STATIC_LIBS=$OPENSSL_USE_STATIC_LIBS")
fi

cmake -S "$BUILD_DIR/duckdb" -B "$BUILD_OUT" "${CMAKE_ARGS[@]}"

CPU_COUNT="$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 4)"
cmake --build "$BUILD_OUT" --config Release --parallel "$CPU_COUNT"

SO_PATH="$BUILD_OUT/src/libduckdb.so"
if [ ! -f "$SO_PATH" ]; then
    echo "ERROR: libduckdb.so not found at $SO_PATH"
    echo "Checking for alternative locations..."
    find "$BUILD_OUT" -name "libduckdb.so" -o -name "libduckdb*.so" 2>/dev/null
    exit 1
fi

OUTPUT_DIR="$BUILD_DIR/output"
mkdir -p "$OUTPUT_DIR"

OUTPUT_FILE="$OUTPUT_DIR/libduckdb.so"
cp "$SO_PATH" "$OUTPUT_FILE"

PLUGIN_LINUX_DIR="${DART_DUCKDB_LINUX_DIR:-$PROJECT_DIR/linux/flutter/ephemeral/.plugin_symlinks/dart_duckdb/linux}"
PLUGIN_LIB_DIR="$PLUGIN_LINUX_DIR/Libraries/release"
if [ -d "$PLUGIN_LINUX_DIR" ]; then
    mkdir -p "$PLUGIN_LIB_DIR"
    cp "$OUTPUT_FILE" "$PLUGIN_LIB_DIR/libduckdb.so"
    echo "Installed into dart_duckdb plugin: $PLUGIN_LIB_DIR/libduckdb.so"
else
    echo "Note: dart_duckdb plugin path not found."
    echo "Run 'flutter pub get' first or set DART_DUCKDB_LINUX_DIR."
fi

echo "=== Build complete ==="
echo "Output: $OUTPUT_FILE"
echo ""
echo "Library info:"
file "$OUTPUT_FILE" || true
echo ""
echo "Verify httpfs is included:"
strings "$OUTPUT_FILE" | grep -i "httpfs" | head -3 || echo "(httpfs strings not found)"
