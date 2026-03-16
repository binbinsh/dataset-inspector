import 'dart:convert';
import 'dart:io';

import 'package:path/path.dart' as p;

import '../models/common.dart';

class LocalFilePreviewFlowService {
  const LocalFilePreviewFlowService();

  static const int defaultPreviewBytes = 64 * 1024;
  static const int defaultHexSnippetBytes = 2048;

  String normalizeExt(String path) {
    final name = p.basename(path);
    final dot = name.lastIndexOf('.');
    if (dot <= 0 || dot >= name.length - 1) return 'bin';
    return name.substring(dot + 1).toLowerCase();
  }

  bool isBinary(String ext, List<int> bytes) {
    const knownText = <String>{
      'txt',
      'json',
      'yaml',
      'yml',
      'csv',
      'tsv',
      'md',
      'toml',
      'ini',
      'cfg',
      'log',
      'rtf',
      'xml',
      'html',
      'css',
      'js',
      'ts',
      'dart',
      'py',
      'java',
      'cpp',
      'c',
      'h',
      'go',
      'rs',
      'sh',
      'bat',
      'ps1',
    };
    if (knownText.contains(ext)) return false;

    const knownBinary = <String>{
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
      'ogg',
      'm4a',
      'opus',
      'aac',
      'sph',
      'mp4',
      'webm',
      'mov',
      'zip',
      'tar',
      'gz',
      'bz2',
      'xz',
      'bin',
      'dat',
    };
    if (knownBinary.contains(ext)) return true;

    var control = 0;
    for (var i = 0; i < bytes.length; i += 1) {
      final value = bytes[i];
      if (value == 0) return true;
      if ((value < 0x07 && value != 0x09 && value != 0x0a && value != 0x0d) ||
          value == 0x0b) {
        control += 1;
      }
    }
    return control > 0 && control >= (bytes.length * 0.1).round();
  }

  String toHexSnippet(List<int> bytes,
      {int maxBytes = defaultHexSnippetBytes}) {
    final limit = bytes.length < maxBytes ? bytes.length : maxBytes;
    final buffer = StringBuffer();
    for (var i = 0; i < limit; i += 1) {
      final value = bytes[i];
      buffer.write(value.toRadixString(16).padLeft(2, '0'));
      if (i + 1 < limit) buffer.write(' ');
    }
    return buffer.toString();
  }

  FieldPreview buildRemotePreview({
    required String path,
    required List<int> bytes,
    int hexSnippetBytes = defaultHexSnippetBytes,
  }) {
    final ext = normalizeExt(path);
    if (isBinary(ext, bytes)) {
      final hex = toHexSnippet(bytes, maxBytes: hexSnippetBytes);
      return FieldPreview(
        previewText: '',
        hexSnippet: 'Read ${bytes.length} bytes\n$hex',
        guessedExt: ext,
        isBinary: true,
        size: bytes.length,
      );
    }
    final previewText = utf8.decode(bytes, allowMalformed: true);
    return FieldPreview(
      previewText: previewText,
      hexSnippet: '',
      guessedExt: ext,
      isBinary: false,
      size: bytes.length,
    );
  }

  Future<FieldPreview> readLocalFilePreview({
    required String path,
    required FieldPreview Function() emptyPreview,
    int previewBytes = defaultPreviewBytes,
    int hexSnippetBytes = defaultHexSnippetBytes,
  }) async {
    final file = File(path);
    FileStat? stat;
    try {
      stat = await file.stat();
    } catch (_) {
      stat = null;
    }
    if (stat == null || stat.type != FileSystemEntityType.file) {
      return emptyPreview();
    }

    List<int> rawBytes;
    try {
      rawBytes = await file.readAsBytes();
    } catch (_) {
      throw const FormatException('Unable to read file.');
    }

    final bytes = rawBytes.length > previewBytes
        ? rawBytes.sublist(0, previewBytes)
        : rawBytes;
    final ext = normalizeExt(path);
    if (isBinary(ext, rawBytes)) {
      final hex = toHexSnippet(rawBytes, maxBytes: hexSnippetBytes);
      final fileSizeLabel = rawBytes.length.toString();
      return FieldPreview(
        previewText: '',
        hexSnippet:
            'Size: $fileSizeLabel bytes${_hexChunkSuffix(rawBytes, hexSnippetBytes)}\n$hex',
        guessedExt: ext,
        isBinary: true,
        size: rawBytes.length,
      );
    }

    final previewText = utf8.decode(bytes, allowMalformed: true);
    return FieldPreview(
      previewText: rawBytes.length > previewBytes
          ? '$previewText\n\n(first $previewBytes bytes)'
          : previewText,
      hexSnippet: '',
      guessedExt: ext,
      isBinary: false,
      size: rawBytes.length,
    );
  }

  String _hexChunkSuffix(List<int> bytes, int hexSnippetBytes) {
    if (bytes.length <= hexSnippetBytes) return '';
    return '\n\n(truncated ${bytes.length - hexSnippetBytes} bytes)';
  }
}
