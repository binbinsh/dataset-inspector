#!/bin/bash
# Sync version across package.json, tauri.conf.json, and Cargo.toml
# Usage: ./version.sh 0.5.2

set -e

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Usage: ./version.sh <version>"
  exit 1
fi

cd "$(dirname "$0")"

# Update Cargo.toml using cargo-edit
(cd src-tauri && cargo set-version "$VERSION")
echo "> Updated src-tauri/Cargo.toml"

# Update package.json and tauri.conf.json using node
node -e "
const fs = require('fs');
const version = '$VERSION';

['package.json', 'src-tauri/tauri.conf.json'].forEach(file => {
  const json = JSON.parse(fs.readFileSync(file, 'utf-8'));
  json.version = version;
  fs.writeFileSync(file, JSON.stringify(json, null, 2) + '\n');
  console.log('> Updated ' + file);
});
"

echo ""
echo "✅ Version set to $VERSION"
