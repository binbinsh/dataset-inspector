# WebDataset support

Dataset Inspector treats a WebDataset as:

- **Shards**: files ending with `.tar`, `.tar.gz`/`.tgz`, `.tar.zst`/`.tar.zstd` inside a directory (or a single shard path).
- **Samples**: adjacent files in a shard that share the same **prefix**.
  - Prefix = all directory components + the file name up to the first `.` in the base name (per WebDataset spec).
  - Example member: `images17/image194.left.jpg`
    - sample key: `images17/image194`
    - field name: `left.jpg`
- **Fields**: the remainder of the base file name after the first `.`, including all following extensions (e.g. `left.jpg`, `right.jpg`, `json`). Field names are lowercased; files without a suffix are labeled `bin`.

## Notes and limitations

- WebDataset is a streaming format, so the app builds sample pages by **scanning the shard stream**.
- The UI supports **Prev/Next paging** (no random jump). Without an index, jumping to an arbitrary sample requires scanning.
- Sequential paging is fast because the backend keeps a per-shard scan cache and continues from the last read position.
- Member previews are cached while scanning the current page, so previews are fastest when browsing forward.
- `.tar.zst`/`.tar.zstd` shards are decoded in streaming mode.
- WebDataset acceleration caches are in-memory only (no shard cache files are written).
- Opening a member reads the full entry and is capped at 256 MB.
- When `numSamplesTotal` is missing, the total is not known without a full scan.
- Preview/open operations extract the selected member to a temp file before opening.
