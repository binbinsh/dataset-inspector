# Hugging Face Streaming Preview

Dataset Inspector can preview a subset of rows from Hugging Face datasets without downloading the full dataset locally.

In the app, paste a dataset URL into the main input field and click **Load**. When a Hugging Face source is detected, the Hugging Face token button appears.

## Key Features

### Direct Parquet Streaming with DuckDB

Dataset Inspector uses [DuckDB](https://duckdb.org/) to stream Parquet files directly from Hugging Face, providing several advantages over the huggingface.co website preview:

| Feature | Dataset Inspector | huggingface.co |
|---------|-------------------|----------------|
| **Unsupported datasets** | ✅ Works via direct Parquet access | ❌ Shows "Not supported" error |
| **Nested structures** | ✅ Full support (structs, lists, maps) | ⚠️ Limited |
| **Large datasets** | ✅ Streams only needed rows | ⚠️ May timeout |
| **Private/gated datasets** | ✅ With token | ✅ With token |

**Example datasets that work in Dataset Inspector but not on huggingface.co:**
- Datasets requiring custom Python code
- Datasets with complex nested schemas
- Datasets where the viewer service returns 501 errors

### How It Works

1. **Parquet API**: Fetches the list of Parquet files for the dataset split
2. **DuckDB + HTTP Range Requests**: Streams only the required row groups without downloading full files
3. **Prefetch Cache**: Background prefetching of the next page for faster navigation
4. **Smart Pagination**: Calculates which Parquet file contains the requested offset

## Supported inputs

- Dataset page URLs: `https://huggingface.co/datasets/<namespace>/<dataset-name>`
- Short URLs: `https://hf.co/datasets/<namespace>/<dataset-name>`
- `hf://` dataset URIs: `hf://datasets/<namespace>/<dataset-name>@<rev>/<path>` (revision/path are ignored)

## Token support

- Optional: add a Hugging Face token for private or gated datasets.
- Tokens are stored locally via shared_preferences.

## Technical Details

### Backend Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  HuggingFace    │────▶│  Parquet API     │────▶│  DuckDB         │
│  Service        │     │  (file list)     │     │  (streaming)    │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                                         │
                                                         ▼
                                                 ┌─────────────────┐
                                                 │  HTTP Range     │
                                                 │  Requests       │
                                                 └─────────────────┘
```

### API Endpoints Used

- **Splits**: `GET https://datasets-server.huggingface.co/splits?dataset=<org>/<name>`
- **Size**: `GET https://datasets-server.huggingface.co/size?dataset=<org>/<name>`
- **Parquet files**: `GET https://datasets-server.huggingface.co/parquet?dataset=<org>/<name>`
- **Direct Parquet streaming**: DuckDB reads `*.parquet` files via HTTP range requests

### Performance Optimizations

- **DuckDB warmup**: Initializes on app start for faster first load
- **Token caching**: Avoids re-authenticating on every request
- **HTTP metadata caching**: Reduces redundant metadata fetches
- **Prefetch cache**: Pre-loads next page in background
- **HTTP keep-alive**: Reuses connections for faster sequential requests

## Opening fields

- If a field contains a media asset with a `src` URL, the app downloads it and opens it.
- Allowed asset hosts: datasets-server.huggingface.co, huggingface.co, hf.co, cdn-lfs.huggingface.co.
- Otherwise the field value is serialized to JSON and opened as a temp file.

## Known limitations

- Only the dataset id is used from the URL; revisions and extra path segments are ignored.
- Datasets without Parquet exports cannot be previewed.
