# Hugging Face Streaming Preview

Dataset Inspector can preview a subset of rows from Hugging Face datasets without downloading the full dataset locally.

In the app, paste a dataset URL (or repo id) into the main input field and click **Preview** (auto-detected).

## What inputs are supported?

- Dataset page URLs: `https://huggingface.co/datasets/<namespace>/<dataset-name>`
- Short URLs: `https://hf.co/datasets/<namespace>/<dataset-name>`
- `hf://` dataset URIs (extracts `<namespace>/<dataset-name>`): `hf://datasets/<namespace>/<dataset-name>@<rev>/<path>`

## How it works (backend)

The Tauri backend calls the public Hugging Face dataset viewer service:

- `GET https://datasets-server.huggingface.co/splits?dataset=<org>/<name>`
- `GET https://datasets-server.huggingface.co/rows?dataset=<org>/<name>&config=<config>&split=<split>&offset=<offset>&length=<length>`

The UI uses the returned `features` to render a table and supports simple pagination by changing `offset`.

## Known limitations

- Some datasets are unsupported by the dataset viewer service (e.g. datasets that require executing arbitrary Python code). The backend surfaces this as an error message.
- The viewer returns sampled pages (typically up to 100 rows per request). The app clamps `length` to `<= 100`.
