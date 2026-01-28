#!/bin/bash
set -euo pipefail

# Build DuckDB with httpfs statically linked.
# Produces libduckdb.dylib with httpfs built-in to avoid Gatekeeper extension issues.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build/duckdb"
OPENSSL_VERSION="${OPENSSL_VERSION:-3.0.13}"

DEFAULT_DUCKDB_VERSION="v1.4.4"
PODSPEC_JSON="$PROJECT_DIR/macos/Pods/Local Podspecs/dart_duckdb.podspec.json"

DETECTED_DUCKDB_VERSION=""
if [ -z "${DUCKDB_VERSION:-}" ] && [ -f "$PODSPEC_JSON" ]; then
    DETECTED_DUCKDB_VERSION="$(grep -oE "duckdb/releases/download/v[0-9.]+" "$PODSPEC_JSON" | head -n1 | sed 's#.*/##')"
fi

DUCKDB_VERSION="${DUCKDB_VERSION:-$DETECTED_DUCKDB_VERSION}"
DUCKDB_VERSION="${DUCKDB_VERSION:-$DEFAULT_DUCKDB_VERSION}"
if [[ "$DUCKDB_VERSION" != v* ]]; then
    DUCKDB_VERSION="v$DUCKDB_VERSION"
fi

ARCHS="${DUCKDB_ARCHS:-$(uname -m)}"
if [ "$ARCHS" = "universal" ]; then
    ARCHS="arm64;x86_64"
fi

echo "=== Building DuckDB $DUCKDB_VERSION with httpfs statically linked ==="
echo "Build directory: $BUILD_DIR"
echo "Architecture(s): $ARCHS"

build_universal_openssl() {
    local openssl_root="$BUILD_DIR/openssl"
    local tarball="openssl-${OPENSSL_VERSION}.tar.gz"
    local url="https://www.openssl.org/source/${tarball}"

    mkdir -p "$openssl_root"
    if [ ! -f "$openssl_root/$tarball" ]; then
        echo "Downloading OpenSSL ${OPENSSL_VERSION}..." >&2
        curl -L -o "$openssl_root/$tarball" "$url"
    fi

    for arch in arm64 x86_64; do
        local arch_dir="$openssl_root/$arch"
        local install_dir="$arch_dir/install"
        if [ -f "$install_dir/lib/libssl.a" ] && [ -f "$install_dir/lib/libcrypto.a" ]; then
            echo "OpenSSL (${arch}) already built." >&2
            continue
        fi
        echo "Building OpenSSL for ${arch}..." >&2
        rm -rf "$arch_dir"
        mkdir -p "$arch_dir"
        tar -xzf "$openssl_root/$tarball" -C "$arch_dir" --strip-components=1
        pushd "$arch_dir" >/dev/null
        ./Configure "darwin64-${arch}-cc" no-shared no-tests --prefix="$install_dir"
        make -j"$(sysctl -n hw.ncpu)"
        make install_sw
        popd >/dev/null
    done

    local universal_dir="$openssl_root/universal"
    mkdir -p "$universal_dir/lib" "$universal_dir/include"
    rm -rf "$universal_dir/include/openssl"
    cp -R "$openssl_root/arm64/install/include/openssl" "$universal_dir/include/"
    lipo -create \
        "$openssl_root/arm64/install/lib/libssl.a" \
        "$openssl_root/x86_64/install/lib/libssl.a" \
        -output "$universal_dir/lib/libssl.a"
    lipo -create \
        "$openssl_root/arm64/install/lib/libcrypto.a" \
        "$openssl_root/x86_64/install/lib/libcrypto.a" \
        -output "$universal_dir/lib/libcrypto.a"
    echo "$universal_dir"
}

build_openssl_for_arch() {
    local arch="$1"
    local openssl_root="$BUILD_DIR/openssl"
    local tarball="openssl-${OPENSSL_VERSION}.tar.gz"
    local url="https://www.openssl.org/source/${tarball}"
    local arch_dir="$openssl_root/$arch"
    local install_dir="$arch_dir/install"

    mkdir -p "$openssl_root"
    if [ ! -f "$openssl_root/$tarball" ]; then
        echo "Downloading OpenSSL ${OPENSSL_VERSION}..." >&2
        curl -L -o "$openssl_root/$tarball" "$url" >/dev/null
    fi

    if [ -f "$install_dir/lib/libssl.a" ] && [ -f "$install_dir/lib/libcrypto.a" ]; then
        echo "OpenSSL (${arch}) already built." >&2
        echo "$install_dir"
        return
    fi

    echo "Building OpenSSL for ${arch}..." >&2
    rm -rf "$arch_dir"
    mkdir -p "$arch_dir"
    tar -xzf "$openssl_root/$tarball" -C "$arch_dir" --strip-components=1 >/dev/null
    pushd "$arch_dir" >/dev/null
    ./Configure "darwin64-${arch}-cc" no-shared no-tests --prefix="$install_dir" >/dev/null
    make -j"$(sysctl -n hw.ncpu)" >/dev/null
    make install_sw >/dev/null
    popd >/dev/null

    echo "$install_dir"
}

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Clone DuckDB if not exists
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
HTTPFS_SRC="${HTTPFS_SRC:-$PROJECT_DIR/vendor/duckdb-httpfs}"

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

cd duckdb

# Clean previous build if exists
rm -rf build/release

# Configure and build
echo "Configuring CMake..."
mkdir -p build/release
cd build/release

CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_OSX_ARCHITECTURES="$ARCHS"
    -DBUILD_EXTENSIONS="httpfs;parquet;json"
    -DENABLE_EXTENSION_AUTOLOADING=OFF
    -DENABLE_EXTENSION_AUTOINSTALL=OFF
    -DBUILD_SHELL=OFF
    -DBUILD_UNITTESTS=OFF
)

if [[ "$ARCHS" != *";"* ]]; then
    CMAKE_ARGS+=("-DOSX_BUILD_ARCH=$ARCHS")
    OPENSSL_ROOT_DIR="$(build_openssl_for_arch "$ARCHS")"
    CMAKE_ARGS+=("-DOPENSSL_ROOT_DIR=$OPENSSL_ROOT_DIR" "-DOPENSSL_USE_STATIC_LIBS=TRUE")
else
    OPENSSL_ROOT_DIR="$(build_universal_openssl)"
    CMAKE_ARGS+=("-DOPENSSL_ROOT_DIR=$OPENSSL_ROOT_DIR" "-DOPENSSL_USE_STATIC_LIBS=TRUE")
fi

cmake ../.. "${CMAKE_ARGS[@]}"

echo "Building DuckDB (this may take a while)..."
cmake --build . --config Release --parallel "$(sysctl -n hw.ncpu)"

# Check if library was built
DYLIB_PATH="src/libduckdb.dylib"
if [ ! -f "$DYLIB_PATH" ]; then
    echo "ERROR: libduckdb.dylib not found at $DYLIB_PATH"
    echo "Checking for alternative locations..."
    find . -name "libduckdb.dylib" -o -name "libduckdb*.dylib" 2>/dev/null
    exit 1
fi

# Copy to output directory
OUTPUT_DIR="$PROJECT_DIR/macos/Frameworks"
mkdir -p "$OUTPUT_DIR"

OUTPUT_FILE="$OUTPUT_DIR/libduckdb.dylib"
cp "$DYLIB_PATH" "$OUTPUT_FILE"

# Install into dart_duckdb plugin path if available
PLUGIN_MACOS_DIR="${DART_DUCKDB_MACOS_DIR:-$PROJECT_DIR/macos/Flutter/ephemeral/.symlinks/plugins/dart_duckdb/macos}"
PLUGIN_LIB_DIR="$PLUGIN_MACOS_DIR/Libraries/release"
if [ -d "$PLUGIN_MACOS_DIR" ]; then
    mkdir -p "$PLUGIN_LIB_DIR"
    cp "$OUTPUT_FILE" "$PLUGIN_LIB_DIR/libduckdb.dylib"
    echo "Installed into dart_duckdb plugin: $PLUGIN_LIB_DIR/libduckdb.dylib"
else
    echo "Note: dart_duckdb plugin path not found."
    echo "Run 'flutter pub get' first or set DART_DUCKDB_MACOS_DIR."
fi

echo "=== Build complete ==="
echo "Output: $OUTPUT_FILE"
echo ""
echo "Library info:"
file "$OUTPUT_FILE"
echo ""
echo "Size: $(du -h "$OUTPUT_FILE" | cut -f1)"
echo ""
echo "Verify httpfs is included:"
nm "$OUTPUT_FILE" 2>/dev/null | grep -i "httpfs\\|HttpFile" | head -5 || echo "(checking with strings...)"
strings "$OUTPUT_FILE" | grep -i "httpfs" | head -3

echo ""
echo "=== Next steps ==="
echo "1. The library is ready at: $OUTPUT_FILE"
echo "2. Run 'flutter build macos' or 'flutter run -d macos'"
