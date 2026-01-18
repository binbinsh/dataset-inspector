# Audio preview

Dataset Inspector previews audio fields for LitData, MDS, WebDataset, and Zenodo sources. Playback is handled by the audioplayers plugin.

## Supported formats
- Common PCM and compressed formats (wav, mp3, flac, ogg/opus, aac, m4a).
- Availability depends on platform codecs.

## Behavior
- Audio previews load bytes into memory; large remote entries may be blocked by size limits (see `docs/zenodo.md`).
- The Open action writes the field to a temp file and launches the default app. If no default app exists, you can pick one and it is remembered per extension.
