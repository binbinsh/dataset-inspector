# Zenodo preview

Dataset Inspector can preview Zenodo records directly over HTTP with range requests.

## Supported inputs
- `https://zenodo.org/records/<id>`
- `https://zenodo.org/record/<id>`
- Subdomains like `https://sandbox.zenodo.org/records/<id>`

## How it works
- Record metadata is fetched from `/api/records/<id>` to list files.
- File previews use HTTP range requests; large files open in your browser.
- ZIP files: entries are listed via the ZIP central directory; entries are fetched on demand.
- TAR files (`.tar`, `.tar.gz`/`.tgz`, `.tar.zst`/`.tar.zstd`): entries are streamed and paged (default 50, max 200).

## Limits
- Direct file preview is limited to 50 MB; larger files open in the browser.
- ZIP/TAR entry media is limited to 128 MB.
- TAR listing stops at 250000 entries.
- TAR media cache keeps up to 32 MB per entry and 256 MB total.

## Security
- Only `zenodo.org` and `*.zenodo.org` content URLs are allowed.
