# MosaicML Streaming (MDS) support

Dataset Inspector supports the MosaicML Streaming dataset format (MDS).

Dataset Inspector treats an MDS dataset as:

- **Index**: an `index.json` file with a top-level `shards` array where each shard has `format: "mds"`.
- **Shards**: one file per shard (typically `shard.00000.mds`), optionally compressed (`*.mds.zst` / `*.mds.zstd`).
- **Samples**: random-access records addressed by `sample_index` inside a shard.
- **Fields**: columns inside a sample, indexed by `field_index` (shown as the column name when available).

## Notes and limitations

- Only MDS is supported.
- The app reads raw field bytes and does not deserialize unsafe Python types (for example, pickled objects).
- Scalar encodings are decoded to text for preview when possible.
- Compressed shards are decoded from stream inputs without creating shard cache files.
- To keep the UI responsive, the app lists only the first 5000 samples per shard.
- Opening a field reads the full field and is capped at 256 MB.
