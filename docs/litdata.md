# LitData support

Dataset Inspector supports Lightning-AI/litData shard layouts.

Dataset Inspector treats a LitData dataset as:
- **Index**: `index.json` (or `0.index.json` / `*.index.json`), optionally compressed with `.zst`/`.zstd`.
- **Chunks**: `.bin` files, optionally compressed (`.zst`/`.zstd`).
- **Items**: offset-indexed records inside each chunk.
- **Fields**: derived from the index `data_format` and used for extension hints.

## Usage notes
- You can load a dataset root directory, an `index.json` file, or a single chunk file.
- If no `index.json` is found, the app falls back to chunk-only mode and treats fields as bytes.
- Zstd-compressed chunks are decompressed into an in-memory cache (up to 128 MB per chunk).
- Previews show UTF-8 text or hex snippets; media types are inferred from `data_format` or magic bytes.
