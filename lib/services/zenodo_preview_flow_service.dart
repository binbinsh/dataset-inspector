enum ZenodoContainerKind {
  zip,
  tar,
  plain,
}

class ZenodoPreviewFlowService {
  const ZenodoPreviewFlowService();

  ZenodoContainerKind detectContainer(String filename) {
    final name = filename.toLowerCase();
    if (name.endsWith('.zip')) {
      return ZenodoContainerKind.zip;
    }
    if (name.endsWith('.tar') ||
        name.endsWith('.tar.gz') ||
        name.endsWith('.tgz') ||
        name.endsWith('.tar.zst') ||
        name.endsWith('.tar.zstd')) {
      return ZenodoContainerKind.tar;
    }
    return ZenodoContainerKind.plain;
  }

  bool isInlineMediaExt(String name) {
    final lower = name.toLowerCase();
    final ext = lower.contains('.') ? lower.split('.').last : '';
    return const <String>{
      'png',
      'jpg',
      'jpeg',
      'gif',
      'webp',
      'bmp',
      'svg',
      'wav',
      'mp3',
      'flac',
      'm4a',
      'ogg',
      'opus',
      'aac',
      'sph',
    }.contains(ext);
  }
}
