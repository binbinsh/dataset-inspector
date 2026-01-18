import 'dart:typed_data';

class ChunkSummary {
  const ChunkSummary({
    required this.filename,
    required this.path,
    required this.chunkSize,
    required this.chunkBytes,
    required this.dim,
    required this.exists,
  });

  final String filename;
  final String path;
  final int chunkSize;
  final int chunkBytes;
  final int? dim;
  final bool exists;
}

class IndexSummary {
  const IndexSummary({
    required this.indexPath,
    required this.rootDir,
    required this.dataFormat,
    required this.compression,
    required this.chunkSize,
    required this.chunkBytes,
    required this.configRaw,
    required this.chunks,
  });

  final String indexPath;
  final String rootDir;
  final List<String> dataFormat;
  final String? compression;
  final int? chunkSize;
  final int? chunkBytes;
  final Map<String, dynamic> configRaw;
  final List<ChunkSummary> chunks;
}

class FieldMeta {
  const FieldMeta({
    required this.fieldIndex,
    required this.size,
  });

  final int fieldIndex;
  final int size;
}

class ItemMeta {
  const ItemMeta({
    required this.itemIndex,
    required this.totalBytes,
    required this.fields,
  });

  final int itemIndex;
  final int totalBytes;
  final List<FieldMeta> fields;
}

class FieldPreview {
  const FieldPreview({
    required this.previewText,
    required this.hexSnippet,
    required this.guessedExt,
    required this.isBinary,
    required this.size,
  });

  final String? previewText;
  final String hexSnippet;
  final String? guessedExt;
  final bool isBinary;
  final int size;
}

class OpenLeafResponse {
  const OpenLeafResponse({
    required this.path,
    required this.size,
    required this.ext,
    required this.opened,
    required this.needsOpener,
    required this.message,
  });

  final String path;
  final int size;
  final String ext;
  final bool opened;
  final bool needsOpener;
  final String message;
}

class PreparedFileResponse {
  const PreparedFileResponse({
    required this.path,
    required this.size,
    required this.ext,
  });

  final String path;
  final int size;
  final String ext;
}

class PreparedMediaResponse {
  const PreparedMediaResponse({
    required this.bytes,
    required this.size,
    required this.ext,
  });

  final Uint8List bytes;
  final int size;
  final String ext;
}

class InlineMediaResponse {
  const InlineMediaResponse({
    required this.base64,
    required this.mime,
    required this.size,
    required this.ext,
  });

  final String base64;
  final String mime;
  final int size;
  final String ext;
}
