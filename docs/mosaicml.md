# MosaicML Data Shards (MDS) support

Dataset Inspector supports the MosaicML Streaming dataset format (MDS) as documented in MosaicML Streaming.

Dataset Inspector treats an MDS dataset as:

- **Index**: an `index.json` file with a top-level `shards` array where each shard has `format: "mds"`.
- **Shards**: one file per shard (typically `shard.00000.mds`), optionally compressed (commonly `*.mds.zst` / `*.mds.zstd`).
- **Samples**: random-access records addressed by `sample_index` inside a shard.
- **Fields**: columns inside a sample, indexed by `field_index` (shown as the column name when available).

## Notes and limitations

- The app reads raw field bytes; it does not deserialize unsafe Python types (e.g. pickled objects).
- For compressed shards, the app may decompress the shard to a temporary cache directory before reading samples.
- To keep the UI responsive, the app lists only the first `MAX_LISTED_SAMPLES` samples per shard.

