# WebDataset support

Dataset Inspector treats a WebDataset as:

- **Shards**: files ending with `.tar`, `.tar.gz`/`.tgz`, `.tar.zst`/`.tar.zstd` inside a directory.
- **Samples**: adjacent files in a shard that share the same **prefix**.
  - Prefix = all directory components + the file name up to the first `.` in the base name (per WebDataset spec).
  - Example member: `images17/image194.left.jpg`
    - sample key: `images17/image194`
    - field name: `left.jpg`
- **Fields**: the remainder of the base file name after the first `.`, including all following extensions (e.g. `left.jpg`, `right.jpg`, `json`).

## Notes and limitations

- WebDataset is a streaming format, so the app builds sample pages by **scanning the shard stream**.
- The UI supports **Prev/Next paging** (no random jump). Without an index, jumping to an arbitrary sample requires scanning.
- Sequential paging is fast because the backend keeps a per-shard scan cache and continues from the last read position.
- When `numSamplesTotal` is missing, the total is not known without a full scan.
- Preview/open operations extract the selected member to a temp file before opening.
