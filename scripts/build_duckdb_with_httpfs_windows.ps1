$ErrorActionPreference = "Stop"

# Build DuckDB with httpfs built-in on Windows (MSVC or Ninja).
# Produces duckdb.dll and installs it into the dart_duckdb plugin path if present.

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Split-Path -Parent $scriptDir
$buildDir = Join-Path $projectDir "build\duckdb-windows"

$duckdbVersion = $env:DUCKDB_VERSION
if (-not $duckdbVersion) {
    $duckdbVersion = "v1.4.4"
}
if ($duckdbVersion -notmatch "^v") {
    $duckdbVersion = "v$duckdbVersion"
}

Write-Host "=== Building DuckDB $duckdbVersion with httpfs built-in ==="
Write-Host "Build directory: $buildDir"

foreach ($tool in @("git", "cmake")) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "Missing required tool: $tool"
    }
}

New-Item -ItemType Directory -Force $buildDir | Out-Null
$duckdbDir = Join-Path $buildDir "duckdb"

if (-not (Test-Path $duckdbDir)) {
    Write-Host "Cloning DuckDB..."
    git clone --depth 1 --branch $duckdbVersion https://github.com/duckdb/duckdb $duckdbDir
}

Push-Location $duckdbDir
$currentTag = git describe --tags --always
if ($currentTag -ne $duckdbVersion) {
    Write-Host "Switching DuckDB repo to $duckdbVersion..."
    git fetch --depth 1 origin "refs/tags/$duckdbVersion:refs/tags/$duckdbVersion"
    git checkout $duckdbVersion
}
Pop-Location

# Prepare httpfs extension sources
$httpfsRef = $env:DUCKDB_HTTPFS_REF
$httpfsSrc = $env:HTTPFS_SRC
if (-not $httpfsSrc) {
    $httpfsSrc = Join-Path $projectDir "third_party\duckdb-httpfs"
}
$httpfsDir = Join-Path $duckdbDir "extension\httpfs"
if (Test-Path $httpfsSrc) {
    Write-Host "Using httpfs source from: $httpfsSrc"
    if (Test-Path $httpfsDir) {
        Remove-Item -Recurse -Force $httpfsDir
    }
    New-Item -ItemType Directory -Force $httpfsDir | Out-Null
    Copy-Item -Path (Join-Path $httpfsSrc "*") -Destination $httpfsDir -Recurse -Force
} elseif (Test-Path $httpfsDir) {
    Write-Host "Using DuckDB in-tree httpfs extension."
} else {
    if (-not $httpfsRef) { $httpfsRef = "main" }
    Write-Host "Cloning httpfs extension (ref: $httpfsRef)..."
    if (Test-Path $httpfsDir) {
        Remove-Item -Recurse -Force $httpfsDir
    }
    git clone --depth 1 --branch $httpfsRef https://github.com/duckdb/duckdb-httpfs $httpfsDir
}

$buildOut = Join-Path $duckdbDir "build\release"
if (Test-Path $buildOut) {
    Remove-Item -Recurse -Force $buildOut
}
New-Item -ItemType Directory -Force $buildOut | Out-Null

$generator = $env:DUCKDB_GENERATOR
if (-not $generator) {
    $generator = "Visual Studio 17 2022"
}
$arch = $env:DUCKDB_ARCH
if (-not $arch) {
    $arch = "x64"
}

$vcpkgRoot = $env:VCPKG_INSTALLATION_ROOT
if (-not $vcpkgRoot) {
    $vcpkgRoot = "C:\vcpkg"
}
$vcpkgTriplet = $env:VCPKG_TARGET_TRIPLET
if (-not $vcpkgTriplet) {
    $vcpkgTriplet = "x64-windows-static"
}
$vcpkgInstalled = Join-Path $vcpkgRoot "installed\$vcpkgTriplet"

$cmakeArgs = @(
    "-S", $duckdbDir,
    "-B", $buildOut,
    "-DCMAKE_BUILD_TYPE=Release",
    "-DBUILD_EXTENSIONS=httpfs;parquet;json",
    "-DENABLE_EXTENSION_AUTOLOADING=OFF",
    "-DENABLE_EXTENSION_AUTOINSTALL=OFF",
    "-DBUILD_SHELL=OFF",
    "-DBUILD_UNITTESTS=OFF"
)

if ($env:CMAKE_TOOLCHAIN_FILE) {
    $cmakeArgs += "-DCMAKE_TOOLCHAIN_FILE=$env:CMAKE_TOOLCHAIN_FILE"
}
if ($env:OPENSSL_ROOT_DIR) {
    $cmakeArgs += "-DOPENSSL_ROOT_DIR=$env:OPENSSL_ROOT_DIR"
}
if ($env:OPENSSL_USE_STATIC_LIBS) {
    $cmakeArgs += "-DOPENSSL_USE_STATIC_LIBS=$env:OPENSSL_USE_STATIC_LIBS"
}
if (Test-Path $vcpkgInstalled) {
    $cmakeArgs += "-DCMAKE_PREFIX_PATH=$vcpkgInstalled"
    $curlLib = Join-Path $vcpkgInstalled "lib\libcurl.lib"
    $curlInclude = Join-Path $vcpkgInstalled "include"
    if (Test-Path $curlLib) {
        $cmakeArgs += "-DCURL_LIBRARY=$curlLib"
    }
    if (Test-Path $curlInclude) {
        $cmakeArgs += "-DCURL_INCLUDE_DIR=$curlInclude"
    }
    if ($vcpkgTriplet -match "-static" -or $env:OPENSSL_USE_STATIC_LIBS) {
        $cmakeArgs += "-DCURL_USE_STATIC_LIBS=ON"
    }
}

if ($generator) {
    $cmakeArgs += @("-G", $generator)
    if ($generator -notmatch "Ninja") {
        $cmakeArgs += @("-A", $arch)
    }
}

cmake @cmakeArgs

$parallel = $env:NUMBER_OF_PROCESSORS
if (-not $parallel) {
    $parallel = 4
}
cmake --build $buildOut --config Release --parallel $parallel

$dllPath = Join-Path $buildOut "src\Release\duckdb.dll"
if (-not (Test-Path $dllPath)) {
    $dllPath = Join-Path $buildOut "src\duckdb.dll"
}
if (-not (Test-Path $dllPath)) {
    $candidate = Get-ChildItem -Path $buildOut -Filter "duckdb.dll" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($candidate) {
        $dllPath = $candidate.FullName
    }
}

if (-not (Test-Path $dllPath)) {
    throw "duckdb.dll not found. Check the build output."
}

$outputDir = Join-Path $buildDir "output"
New-Item -ItemType Directory -Force $outputDir | Out-Null
$outputFile = Join-Path $outputDir "duckdb.dll"
Copy-Item -Path $dllPath -Destination $outputFile -Force

$pluginDir = $env:DART_DUCKDB_WINDOWS_DIR
if (-not $pluginDir) {
    $pluginDir = Join-Path $projectDir "windows\flutter\ephemeral\.plugin_symlinks\dart_duckdb\windows"
}
$pluginLibDir = Join-Path $pluginDir "Libraries\release"
if (Test-Path $pluginDir) {
    New-Item -ItemType Directory -Force $pluginLibDir | Out-Null
    Copy-Item -Path $outputFile -Destination (Join-Path $pluginLibDir "duckdb.dll") -Force
    Write-Host "Installed into dart_duckdb plugin: $pluginLibDir\duckdb.dll"
} else {
    Write-Host "Note: dart_duckdb plugin path not found."
    Write-Host "Run 'flutter pub get' first or set DART_DUCKDB_WINDOWS_DIR."
}

Write-Host "=== Build complete ==="
Write-Host "Output: $outputFile"
