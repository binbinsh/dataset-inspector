<p align="center">
  <img src="assets/icon.png" alt="Dataset Inspector Icon" width="128">
</p>

<h1 align="center">Dataset Inspector</h1>

Dataset Inspector is a desktop UI for inspecting local [Lightning-AI/litData](https://github.com/Lightning-AI/litData) shards, [MosaicML Streaming](https://github.com/mosaicml/streaming) (MDS) shards, and [WebDataset](https://github.com/webdataset/webdataset) tar shards, with support for previewing [Hugging Face](https://huggingface.co/blog/streaming-datasets) and [Zenodo](https://www.zenodo.org) datasets directly over HTTP without full downloads.
Supported platforms: Windows, macOS, and Linux (web planned).

## Design Goals

1. **Near-native access speed** — Access dataset records as close to the raw protocol/filesystem speed as possible. Minimize overhead from format parsing, decompression, and serialization so that the bottleneck is always I/O, never the tool.
2. **Unified access interface** — Provide a single API surface across all dataset formats (MDS, LitData, WebDataset, Parquet, etc.) and storage backends (local, NAS/SMB, SSH, HTTP). Callers should not need to know what the underlying format or transport is.
3. **Opportunistic acceleration** — Use every available technique (in-memory caching, pre-decompressed shard indices, database-backed metadata, parallel I/O) to make repeated and bulk access as fast as possible.

## Scope
- `dataset-inspector` (this repo): dataset loading/inspection UI and core data preview workflow.

## Features
- Auto-detect local LitData indexes/chunks, MosaicML MDS, and WebDataset shards.
- **Enhanced Hugging Face support**: Direct Parquet streaming via DuckDB — preview datasets that huggingface.co cannot display.
- Preview Zenodo records and browse ZIP/TAR entries with HTTP range requests.
- Remote datasets are accessed in direct streaming mode: in-memory acceleration is used, but no remote on-disk cache is maintained.
- Rich previews for JSON/text, images, audio, and video.
- Open fields with the system default app.

<table align="center">
  <tr>
    <td align="center" width="50%">
      <img src="images/litdata.png" width="100%">
      <br />
      <sub>Local LitData shards</sub>
    </td>
    <td align="center" width="50%">
      <img src="images/webdataset.png" width="100%">
      <br />
      <sub>Local WebDataset tar shards</sub>
    </td>
  </tr>
</table>
<table align="center">
  <tr>
    <td align="center" width="50%">
      <img src="images/huggingface.png" width="100%">
      <br />
      <sub>Hugging Face dataset preview</sub>
    </td>
    <td align="center" width="50%">
      <img src="images/zenodo.png" width="100%">
      <br />
      <sub>Zenodo record preview</sub>
    </td>
  </tr>
</table>

## Usage
1. Download Dataset Inspector installers from [Releases](https://github.com/binbinsh/dataset-inspector/releases).
2. Paste a local dataset path or Hugging Face/Zenodo URL, then press **Load**.
3. Local shards: pick a shard/chunk -> item/sample -> field, then preview fields.
4. Hugging Face: pick a config/split -> row -> field (add a token if needed).
5. Zenodo: pick a record -> file -> entry (ZIP/TAR), then preview/open files.
6. Report issues/feature requests: https://github.com/binbinsh/dataset-inspector/issues

## API

The running UI process exposes an HTTP API for dataset inspection, using the same
in-memory state used by the desktop interface. The API is read-only and provides
uniform concurrent access across all supported source protocols and dataset formats.

API response contract:

```json
{
  "ok": true,
  "requestId": "175136....",
  "apiVersion": "26.0228.1031",
  "timestamp": "2026-02-28T00:00:00.000Z",
  "meta": {
    "path": "/api/v1/opened",
    "method": "GET",
    "durationMs": 12,
    "requested": 1,
    "returned": 1,
    "count": 1,
    "concurrency": 8
  },
  "data": {
    "...": "..."
  }
}
```
响应头会回传 `X-Request-ID`，可用于日志链路追踪。

Error responses use:

```json
{
  "ok": false,
  "error": {
    "code": "INVALID_BOOLEAN",
    "message": "Invalid boolean for `details`.",
    "details": {
      "field": "details",
      "value": "maybe"
    }
  }
}
```

Dataset payloads keep a unified shape:
`id`, `identity`, `label`, `mode`, `sourceInput`, `isActive`, `selection`,
`details`, and `uniform` (a format-agnostic summary).

1. Start app with API enabled:
   - `flutter run -- --api`
   - optional: `--api-host 127.0.0.1 --api-port 8080`
   - optional headless mode: `--api-only` (no UI window)
   - optional startup source(s):
     - `--source remote://host/path/to/mds_shards` (repeatable)
     - `--sources remote://host/path/to/train,remote://host/path/to/dev`
   - env var: `DATASET_INSPECTOR_SOURCES` with comma/newline/semicolon separated source list
2. Query current opened datasets:
   - `GET /api/v1/opened`
   - `POST /api/v1/opened` (same semantics, supports body)
   - query/body params:
     - `details` (`true`/`false`, default `true`)
     - `fields` (projection fields, comma-separated, default all; supports `*`)
     - `projection` (alias for `fields`)
     - `concurrency` (integer, default `8`, max `64`)
     - `ids` (query only; for body use POST payload)
       - `ids=id1,id2,id3` or repeated `ids=id1&ids=id2`
     - `idList` (query alias)
     - `datasetIds` (query alias)
     - `includeMissing` (`true`/`false`, default `false` for GET list path)
   - `POST /api/v1/opened` with body:
     - `ids`: array of dataset IDs
     - `details`: (`true`/`false`)
     - `fields` / `projection`
     - `concurrency`
     - `includeMissing`
   - POST endpoints require JSON body (`Content-Type: application/json`).
   - response data for list:
     - `activeDatasetId`
     - `sourceInput` (if set)
     - `datasets` (array of dataset snapshots)
   - uniform field in each item:
     - `uniform.mode`: dataset mode
     - `uniform.kind`: dataset kind
     - `uniform.status`: loaded status of that dataset
     - `uniform.stats.recordCount`: rough record/item count if available
     - `uniform.stats.sizeBytes`: rough total bytes if available
     - `uniform.stats.hasMore`: whether more pages/items exist
3. Query one dataset by id:
   - `GET /api/v1/opened/{datasetId}`
   - query params:
     - `details` (`true`/`false`, default `true`)
     - `fields` / `projection`
4. Stream a bounded probe set from one opened dataset:
   - `POST /api/v1/opened/{datasetId}/extract`
   - body:
      - `shardName`
      - `offset` / `limit`
      - `audioFieldIndex`
      - `textFieldIndex`
      - optional `idFieldIndex`
      - optional `responseMode` (`stream` or `materialize`, default `stream`)
      - optional `audioEncoding` (`base64` or `none`, default `base64`)
    - default stream response:
      - content type: `application/x-ndjson`
      - line 1: `type=meta`
      - next lines: `type=record` and inline audio payloads
      - final line: `type=summary`
      - no local manifest/audio files are created
    - optional materialized response:
      - optional `outputDir`
      - optional `manifestName`
      - optional `audioDirName`
      - optional `overwrite`
    - materialized response data:
      - `manifestPath`
      - `audioDir`
      - `recordCount`
      - `records`
4. Batch inspect multiple opened datasets (high-concurrency):
   - `POST /api/v1/opened/batch`
   - `POST /api/v1/opened/_batch` (legacy兼容)
   - body:
     - `ids`: array of dataset IDs or comma-separated string
     - `details` (`true`/`false`, default `true`)
     - `fields` / `projection`
     - `includeMissing` (`true`/`false`, default `true` for batch)
     - `concurrency` (integer, default `8`, max `64`)
   - optional query params: `concurrency`, `includeMissing`, `fields` / `projection`
5. Health check:
   - `GET /health`
6. OpenAPI spec:
   - `docs/openapi.yaml`

Probe helper:

- `dart run tool/stream_mds_probe.dart --shard shard.00000.mds.zstd --limit 8`

## Docs
- LitData: [docs/litdata.md](docs/litdata.md)
- MosaicML MDS: [docs/mosaicml.md](docs/mosaicml.md)
- WebDataset: [docs/webdataset.md](docs/webdataset.md)
- Hugging Face: [docs/huggingface.md](docs/huggingface.md)
- Zenodo: [docs/zenodo.md](docs/zenodo.md)
- Audio preview: [docs/audio.md](docs/audio.md)
- Development: [docs/development.md](docs/development.md)
