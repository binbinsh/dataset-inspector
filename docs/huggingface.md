# Hugging Face Streaming Preview

Dataset Inspector can preview a subset of rows from Hugging Face datasets without downloading the full dataset locally.

In the app, paste a dataset URL into the main input field and click **Load**. When a Hugging Face source is detected, the Hugging Face token button appears.

## Supported inputs

- Dataset page URLs: `https://huggingface.co/datasets/<namespace>/<dataset-name>`
- Short URLs: `https://hf.co/datasets/<namespace>/<dataset-name>`
- `hf://` dataset URIs: `hf://datasets/<namespace>/<dataset-name>@<rev>/<path>` (revision/path are ignored)

## Token support

- Optional: add a Hugging Face token for private or gated datasets.
- Tokens are stored locally via shared_preferences.

## How it works (backend)

The Dart backend calls the public Hugging Face dataset viewer service:

- `GET https://datasets-server.huggingface.co/splits?dataset=<org>/<name>`
- `GET https://datasets-server.huggingface.co/rows?dataset=<org>/<name>&config=<config>&split=<split>&offset=<offset>&length=<length>`

The UI uses the returned `features` to render a table and paginates rows with `offset`. The app clamps `length` to `<= 100` (UI default: 50).

## Opening fields

- If a field contains a media asset with a `src` URL, the app downloads it and opens it.
- Allowed asset hosts: datasets-server.huggingface.co, huggingface.co, hf.co, cdn-lfs.huggingface.co.
- Otherwise the field value is serialized to JSON and opened as a temp file.

## Known limitations

- Some datasets are unsupported by the dataset viewer service (for example, datasets that require executing arbitrary Python code).
- Only the dataset id is used from the URL; revisions and extra path segments are ignored.
