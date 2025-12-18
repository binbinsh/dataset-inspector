<p align="center">
  <img src="src-tauri/icons/icon.png" alt="Dataset Inspector Icon" width="128">
</p>

<h1 align="center">Dataset Inspector</h1>

<p align="center">
  <a href="https://github.com/binbinsh/dataset-inspector/actions/workflows/build-tauri.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/binbinsh/dataset-inspector/build-tauri.yml?branch=main&label=macOS&logo=apple&logoColor=white" alt="macOS build">
  </a>
  <a href="https://github.com/binbinsh/dataset-inspector/actions/workflows/build-tauri.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/binbinsh/dataset-inspector/build-tauri.yml?branch=main&label=Ubuntu&logo=ubuntu&logoColor=white" alt="Ubuntu build">
  </a>
  <a href="https://github.com/binbinsh/dataset-inspector/actions/workflows/build-tauri.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/binbinsh/dataset-inspector/build-tauri.yml?branch=main&label=Windows&logo=windows&logoColor=white" alt="Windows build">
  </a>
</p>

## About
Dataset Inspector is a desktop UI for inspecting [Lightning-AI/litData](https://github.com/Lightning-AI/litData) shards, [MosaicML Streaming](https://github.com/mosaicml/streaming) (MDS) shards, [WebDataset](https://github.com/webdataset/webdataset) tar shards, and [Hugging Face streaming datasets](https://huggingface.co/blog/streaming-datasets).

## Features
- Inspect local LitData shards (`index.json` + `.bin/.zst` chunks).
- Inspect local MosaicML Streaming (MDS) shards (`index.json` + `.mds/.mds.zst`).
- Inspect local WebDataset shards (`.tar`, `.tar.gz`, `.tar.zst`).
- Inspect Hugging Face datasets via streaming API (no full local download).
- Inspect Zenodo records (list files, browse archive entries, and preview/open fields).
- Preview json/audio/image, copy values, and open extracted fields with your default app.

<table align="center">
  <tr>
    <td align="center" width="33%">
      <img src="images/litdata.png" width="100%">
      <br />
      <sub>Local LitData shards (index.json + .bin files)</sub>
    </td>
    <td align="center" width="33%">
      <img src="images/mosaicml-mds.png" width="100%">
      <br />
      <sub>Local MosaicML Streaming (MDS) shards (index.json + .mds files)</sub>
    </td>
    <td align="center" width="33%">
      <img src="images/webdataset.png" width="100%">
      <br />
      <sub>Local WebDataset shards (.tar files)</sub>
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <img src="images/huggingface.png" width="100%">
      <br />
      <sub>Hugging Face dataset preview</sub>
    </td>
    <td align="center" width="50%">
      <img src="images/zenodo.png" width="100%">
      <br />
      <sub>Zenodo record preview (files + archive entries)</sub>
    </td>
  </tr>
</table>

## Usage
1. Download Dataset Inspector installers from [Releases](https://github.com/binbinsh/dataset-inspector/releases).
2. Browse a local LitData/MosaicML/WebDataset folder, or a HF dataset URL, then press **Load**.
3. Paste a Zenodo record URL to browse record files and archive entries.
3. LitData / MosaicML shards: pick a shard → item → field, then preview fields.
4. WebDataset shards: pick a shard → sample → field, then preview/open files.
5. Hugging Face datasets: pick a split → row → field to preview values.
6. Report issues/ feature requests: https://github.com/binbinsh/dataset-inspector/issues
