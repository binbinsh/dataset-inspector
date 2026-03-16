# Dataset Inspector

- Split source code files into best modules if the files are longer than 1000 lines

## Scope
- This repository only contains dataset inspector UI and data preview functionality.

## Cache Policy
- Remote dataset access must stay streaming-first and direct.
- In-memory caches are allowed for performance.
- Do not add any on-disk cache/sync layer for remote datasets (no `remote_cache`, no cache quota/manager, no per-format cache directories).

## Versioning
- Use `YY.MMDD.HHMM` for app version numbers (for example: `26.0228.1031`).
