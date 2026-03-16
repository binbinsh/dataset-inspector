import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import '../models/common.dart';
import '../utils/audio.dart';
import '../utils/preview.dart';
import '../utils/zstd.dart';
import 'open_with_service.dart';

const _previewBytes = 16 * 1024;
const _maxCacheBytes = 128 * 1024 * 1024;

class _ChunkCache {
  // In-memory only. Never persist decoded chunks as cache files.
  final _cache = <String, Uint8List>{};

  Uint8List? fetch(String key) => _cache[key];

  void store(String key, Uint8List data) {
    if (data.length > _maxCacheBytes) return;
    _cache[key] = data;
  }
}

class _ParsedIndex {
  _ParsedIndex({
    required this.rootDir,
    required this.source,
    required this.config,
    required this.configRaw,
    required this.chunks,
  });

  final Directory rootDir;
  final File source;
  final _IndexConfig config;
  final Map<String, dynamic> configRaw;
  final List<_RawChunk> chunks;
}

class _IndexFile {
  _IndexFile({required this.chunks, required this.config});

  final List<_RawChunk> chunks;
  final _IndexConfig config;

  factory _IndexFile.fromJson(Map<String, dynamic> json) {
    final chunks = (json['chunks'] as List<dynamic>? ?? [])
        .map((e) => _RawChunk.fromJson(e as Map<String, dynamic>))
        .toList();
    final config =
        _IndexConfig.fromJson(json['config'] as Map<String, dynamic>? ?? {});
    return _IndexFile(chunks: chunks, config: config);
  }
}

class _IndexConfig {
  _IndexConfig({
    required this.compression,
    required this.chunkSize,
    required this.chunkBytes,
    required this.dataFormat,
    required this.dataSpec,
    required this.itemLoader,
  });

  final String? compression;
  final int? chunkSize;
  final int? chunkBytes;
  final List<String>? dataFormat;
  final String? dataSpec;
  final String? itemLoader;

  factory _IndexConfig.fromJson(Map<String, dynamic> json) {
    return _IndexConfig(
      compression: json['compression'] as String?,
      chunkSize: (json['chunk_size'] as num?)?.toInt(),
      chunkBytes: (json['chunk_bytes'] as num?)?.toInt(),
      dataFormat: (json['data_format'] as List<dynamic>?)
          ?.map((e) => e.toString())
          .toList(),
      dataSpec: json['data_spec'] as String?,
      itemLoader: json['item_loader'] as String?,
    );
  }
}

class _RawChunk {
  _RawChunk({
    required this.filename,
    required this.chunkBytes,
    required this.chunkSize,
    required this.dim,
  });

  final String filename;
  final int chunkBytes;
  final int chunkSize;
  final int? dim;

  factory _RawChunk.fromJson(Map<String, dynamic> json) {
    return _RawChunk(
      filename: json['filename'] as String,
      chunkBytes: (json['chunk_bytes'] as num).toInt(),
      chunkSize: (json['chunk_size'] as num).toInt(),
      dim: (json['dim'] as num?)?.toInt(),
    );
  }
}

abstract class _ChunkAccess {
  Future<Uint8List> readExactAt(int offset, int len);
  Future<void> close();
}

class _FileChunkAccess extends _ChunkAccess {
  _FileChunkAccess(this._file);

  final RandomAccessFile _file;

  @override
  Future<Uint8List> readExactAt(int offset, int len) async {
    await _file.setPosition(offset);
    final data = await _file.read(len);
    if (data.length != len) {
      throw const FormatException('Malformed chunk');
    }
    return data;
  }

  @override
  Future<void> close() => _file.close();
}

class _MemoryChunkAccess extends _ChunkAccess {
  _MemoryChunkAccess(this._buffer);

  final Uint8List _buffer;

  @override
  Future<Uint8List> readExactAt(int offset, int len) async {
    final end = offset + len;
    if (end > _buffer.length) {
      throw const FormatException('Malformed chunk');
    }
    return Uint8List.fromList(_buffer.sublist(offset, end));
  }

  @override
  Future<void> close() async {}
}

class LitDataService {
  LitDataService({OpenWithService? openWith})
      : _openWith = openWith ?? OpenWithService();

  final _ChunkCache _cache = _ChunkCache();
  final OpenWithService _openWith;

  Future<IndexSummary> loadIndex(String indexPath) async {
    final parsed = await _parseIndex(File(indexPath));
    final chunks = parsed.chunks.map((chunk) {
      final full = File('${parsed.rootDir.path}/${chunk.filename}');
      return ChunkSummary(
        filename: chunk.filename,
        path: full.path,
        chunkSize: chunk.chunkSize,
        chunkBytes: chunk.chunkBytes,
        dim: chunk.dim,
        exists: full.existsSync(),
      );
    }).toList();

    return IndexSummary(
      indexPath: parsed.source.path,
      rootDir: parsed.rootDir.path,
      dataFormat: parsed.config.dataFormat ?? <String>[],
      compression: parsed.config.compression,
      chunkSize: parsed.config.chunkSize,
      chunkBytes: parsed.config.chunkBytes,
      configRaw: parsed.configRaw,
      chunks: chunks,
    );
  }

  Future<IndexSummary> loadIndexFromBytes(
    Uint8List indexBytes, {
    String indexName = 'index.json',
  }) async {
    final parsed = _parseIndexBytes(indexBytes, indexName: indexName);
    final chunks = parsed.chunks.map((chunk) {
      return ChunkSummary(
        filename: chunk.filename,
        path: chunk.filename,
        chunkSize: chunk.chunkSize,
        chunkBytes: chunk.chunkBytes,
        dim: chunk.dim,
        exists: true,
      );
    }).toList(growable: false);
    return IndexSummary(
      indexPath: indexName,
      rootDir: '',
      dataFormat: parsed.config.dataFormat ?? <String>[],
      compression: parsed.config.compression,
      chunkSize: parsed.config.chunkSize,
      chunkBytes: parsed.config.chunkBytes,
      configRaw: parsed.configRaw,
      chunks: chunks,
    );
  }

  Future<IndexSummary> loadChunkList(List<String> paths) async {
    if (paths.isEmpty) {
      throw const FormatException('No chunk paths provided.');
    }

    final nameToPath = <String, File>{};
    Directory? rootDir;
    for (final p in paths) {
      final file = File(p);
      rootDir ??= file.parent;
      final name = file.uri.pathSegments.last;
      nameToPath[name] = file;
    }

    List<_RawChunk> rawChunks = [];
    List<String> dataFormat = ['bytes'];
    String? compression;
    int? chunkSize;
    int? chunkBytes;
    Map<String, dynamic>? configRaw;
    File? indexPath;

    final neighborIndex = _findNeighborIndex(File(paths.first));
    if (neighborIndex != null) {
      final parsed = await _parseIndexFile(neighborIndex);
      dataFormat = parsed.config.dataFormat ?? dataFormat;
      compression = parsed.config.compression;
      chunkSize = parsed.config.chunkSize;
      chunkBytes = parsed.config.chunkBytes;
      configRaw = parsed.configRaw;
      indexPath = parsed.source;
      rootDir = parsed.rootDir;
      final selected = nameToPath.keys.toSet();
      rawChunks = parsed.chunks
          .where((chunk) => selected.contains(chunk.filename))
          .toList();
    }

    final resolvedRootDir = rootDir ?? Directory.current;

    final covered = rawChunks.map((c) => c.filename).toSet();
    for (final entry in nameToPath.entries) {
      if (covered.contains(entry.key)) continue;
      final file = entry.value;
      final info = await file.stat();
      final raf = await file.open();
      final numBuf = await raf.read(4);
      if (numBuf.length != 4) {
        await raf.close();
        throw const FormatException('Malformed chunk');
      }
      final numItems = _readLeU32(numBuf).clamp(1, 0x7fffffff).toInt();
      final offsetsLen = (numItems + 1) * 4;
      final offsets = await raf.read(offsetsLen);
      await raf.close();
      if (offsets.length != offsetsLen) {
        throw const FormatException('Malformed chunk');
      }
      rawChunks.add(_RawChunk(
        filename: entry.key,
        chunkBytes: info.size,
        chunkSize: numItems,
        dim: null,
      ));
    }

    final resolvedIndexPath = indexPath ?? File(paths.first);
    final rawConfig = configRaw ??
        <String, dynamic>{
          'source': 'multi-bin',
          'data_format': dataFormat,
        };

    final chunks = rawChunks.map((chunk) {
      final path = nameToPath[chunk.filename] ??
          File('${resolvedRootDir.path}/${chunk.filename}');
      return ChunkSummary(
        filename: chunk.filename,
        path: path.path,
        chunkSize: chunk.chunkSize,
        chunkBytes: chunk.chunkBytes,
        dim: chunk.dim,
        exists: true,
      );
    }).toList();

    return IndexSummary(
      indexPath: resolvedIndexPath.path,
      rootDir: resolvedRootDir.path,
      dataFormat: dataFormat,
      compression: compression,
      chunkSize: chunkSize,
      chunkBytes: chunkBytes,
      configRaw: rawConfig,
      chunks: chunks,
    );
  }

  Future<List<String>> listChunkFiles(String dirPath) async {
    final dir = Directory(dirPath);
    if (!await dir.exists()) {
      throw FormatException('Missing directory $dirPath');
    }
    final files = <File>[];
    await for (final entry in dir.list(followLinks: false)) {
      if (entry is! File) continue;
      if (_looksLikeChunkFile(entry)) {
        files.add(entry);
      }
    }
    files.sort((a, b) => a.path.compareTo(b.path));
    return files.map((file) => file.path).toList();
  }

  Future<List<ItemMeta>> listChunkItems(
      String indexPath, String chunkFilename) async {
    final parsed = await _parseIndex(File(indexPath));
    final usesTokensLayout = _usesTokensItemLayout(parsed.config);
    final access = await _loadChunkAccess(parsed, chunkFilename);
    try {
      final formatLen = parsed.config.dataFormat?.length ?? 0;
      final (numItems, offsets) = await _parseOffsets(access);
      final items = <ItemMeta>[];
      for (var itemIdx = 0; itemIdx < numItems; itemIdx += 1) {
        final start = offsets[itemIdx];
        final end = offsets[itemIdx + 1];
        if (end < start) {
          throw const FormatException('Malformed chunk');
        }
        final sizes = await _readItemFieldSizes(
          access,
          start: start,
          end: end,
          formatLen: formatLen,
          usesTokensLayout: usesTokensLayout,
        );
        items.add(ItemMeta(
          itemIndex: itemIdx,
          totalBytes: end - start,
          fields: List.generate(sizes.length, (idx) {
            return FieldMeta(fieldIndex: idx, size: sizes[idx]);
          }),
        ));
      }
      return items;
    } finally {
      await access.close();
    }
  }

  Future<ItemPage> listChunkItemsPaged(
    String indexPath,
    String chunkFilename, {
    int offset = 0,
    int length = 200,
  }) async {
    final parsed = await _parseIndex(File(indexPath));
    final usesTokensLayout = _usesTokensItemLayout(parsed.config);
    final access = await _loadChunkAccess(parsed, chunkFilename);
    try {
      final formatLen = parsed.config.dataFormat?.length ?? 0;
      final numBuf = await access.readExactAt(0, 4);
      final numItems = _readLeU32(numBuf);

      final safeLength = length < 1 ? 1 : length;
      final start = offset.clamp(0, numItems).toInt();
      final end = (start + safeLength).clamp(0, numItems).toInt();

      if (start >= end) {
        return ItemPage(
          offset: start,
          length: safeLength,
          items: const <ItemMeta>[],
          partial: start < numItems,
          numItemsTotal: numItems,
        );
      }

      final offsets = await _readOffsetsRange(access, start, end);
      final items = <ItemMeta>[];
      for (var itemIdx = start; itemIdx < end; itemIdx += 1) {
        final localIndex = itemIdx - start;
        final startOffset = offsets[localIndex];
        final endOffset = offsets[localIndex + 1];
        if (endOffset < startOffset) {
          throw const FormatException('Malformed chunk');
        }
        final sizes = await _readItemFieldSizes(
          access,
          start: startOffset,
          end: endOffset,
          formatLen: formatLen,
          usesTokensLayout: usesTokensLayout,
        );
        items.add(ItemMeta(
          itemIndex: itemIdx,
          totalBytes: endOffset - startOffset,
          fields: List.generate(sizes.length, (idx) {
            return FieldMeta(fieldIndex: idx, size: sizes[idx]);
          }),
        ));
      }

      return ItemPage(
        offset: start,
        length: safeLength,
        items: items,
        partial: end < numItems,
        numItemsTotal: numItems,
      );
    } finally {
      await access.close();
    }
  }

  Future<List<ItemMeta>> listChunkItemsFromStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String chunkFilename,
    required Stream<List<int>> chunkStream,
  }) async {
    final parsed = await _loadParsedIndexForStream(
      indexPath: indexPath,
      indexBytes: indexBytes,
      indexName: indexName,
    );
    _chunkForFilename(parsed, chunkFilename);
    final usesTokensLayout = _usesTokensItemLayout(parsed.config);
    final formatLen = parsed.config.dataFormat?.length ?? 0;
    final access = await _loadChunkAccessFromStream(
      parsed: parsed,
      chunkFilename: chunkFilename,
      chunkStream: chunkStream,
    );
    try {
      final (numItems, offsets) = await _parseOffsets(access);
      final items = <ItemMeta>[];
      for (var itemIdx = 0; itemIdx < numItems; itemIdx += 1) {
        final start = offsets[itemIdx];
        final end = offsets[itemIdx + 1];
        if (end < start) {
          throw const FormatException('Malformed chunk');
        }
        final sizes = await _readItemFieldSizes(
          access,
          start: start,
          end: end,
          formatLen: formatLen,
          usesTokensLayout: usesTokensLayout,
        );
        items.add(ItemMeta(
          itemIndex: itemIdx,
          totalBytes: end - start,
          fields: List.generate(sizes.length, (idx) {
            return FieldMeta(fieldIndex: idx, size: sizes[idx]);
          }),
        ));
      }
      return items;
    } finally {
      await access.close();
    }
  }

  Future<FieldPreview> peekField({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final parsed = await _parseIndex(File(indexPath));
    final format = parsed.config.dataFormat ?? <String>[];
    final usesTokensLayout = _usesTokensItemLayout(parsed.config);
    final access = await _loadChunkAccess(parsed, chunkFilename);
    try {
      final (data, size) = await _readFieldBytes(
        access,
        itemIndex,
        fieldIndex,
        format.length,
        usesTokensLayout,
        _previewBytes,
      );
      final previewText = previewUtf8Text(data);
      final isBinary = previewText == null;
      final guessedExt = _guessExt(
          format.length > fieldIndex ? format[fieldIndex] : null, data);
      return FieldPreview(
        previewText: previewText,
        hexSnippet: hexSnippet(data),
        guessedExt: guessedExt,
        isBinary: isBinary,
        size: size,
      );
    } finally {
      await access.close();
    }
  }

  Future<FieldPreview> peekFieldFromStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> chunkStream,
  }) async {
    final parsed = await _loadParsedIndexForStream(
      indexPath: indexPath,
      indexBytes: indexBytes,
      indexName: indexName,
    );
    _chunkForFilename(parsed, chunkFilename);
    final format = parsed.config.dataFormat ?? <String>[];
    final usesTokensLayout = _usesTokensItemLayout(parsed.config);
    final access = await _loadChunkAccessFromStream(
      parsed: parsed,
      chunkFilename: chunkFilename,
      chunkStream: chunkStream,
    );
    try {
      final (data, size) = await _readFieldBytes(
        access,
        itemIndex,
        fieldIndex,
        format.length,
        usesTokensLayout,
        _previewBytes,
      );
      final previewText = previewUtf8Text(data);
      final isBinary = previewText == null;
      final guessedExt = _guessExt(
          format.length > fieldIndex ? format[fieldIndex] : null, data);
      return FieldPreview(
        previewText: previewText,
        hexSnippet: hexSnippet(data),
        guessedExt: guessedExt,
        isBinary: isBinary,
        size: size,
      );
    } finally {
      await access.close();
    }
  }

  Future<PreparedMediaResponse> prepareAudioPreview({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    return _prepareFieldBytes(
      indexPath: indexPath,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      convertSphereToWav: true,
    );
  }

  Future<PreparedMediaResponse> prepareAudioPreviewFromStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> chunkStream,
  }) async {
    final parsed = await _loadParsedIndexForStream(
      indexPath: indexPath,
      indexBytes: indexBytes,
      indexName: indexName,
    );
    _chunkForFilename(parsed, chunkFilename);
    final format = parsed.config.dataFormat ?? <String>[];
    final usesTokensLayout = _usesTokensItemLayout(parsed.config);
    final access = await _loadChunkAccessFromStream(
      parsed: parsed,
      chunkFilename: chunkFilename,
      chunkStream: chunkStream,
    );
    try {
      final (data, _) = await _readFieldBytes(
        access,
        itemIndex,
        fieldIndex,
        format.length,
        usesTokensLayout,
        null,
      );
      var ext = _guessExt(
              format.length > fieldIndex ? format[fieldIndex] : null, data) ??
          'bin';
      var bytes = data;
      if (ext == 'sph') {
        bytes = await decodeSphereToWavWithFallback(data);
        ext = 'wav';
      }
      return PreparedMediaResponse(bytes: bytes, size: bytes.length, ext: ext);
    } finally {
      await access.close();
    }
  }

  Future<PreparedFileResponse> prepareFieldFile({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    return _prepareFieldFile(
      indexPath: indexPath,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      convertSphereToWav: false,
    );
  }

  Future<PreparedFileResponse> prepareFieldFileFromStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> chunkStream,
  }) async {
    final parsed = await _loadParsedIndexForStream(
      indexPath: indexPath,
      indexBytes: indexBytes,
      indexName: indexName,
    );
    _chunkForFilename(parsed, chunkFilename);
    final format = parsed.config.dataFormat ?? <String>[];
    final usesTokensLayout = _usesTokensItemLayout(parsed.config);
    final access = await _loadChunkAccessFromStream(
      parsed: parsed,
      chunkFilename: chunkFilename,
      chunkStream: chunkStream,
    );
    try {
      final (data, size) = await _readFieldBytes(
        access,
        itemIndex,
        fieldIndex,
        format.length,
        usesTokensLayout,
        null,
      );
      var ext = _guessExt(
              format.length > fieldIndex ? format[fieldIndex] : null, data) ??
          'bin';
      final tempDir =
          Directory('${Directory.systemTemp.path}/dataset-inspector');
      await tempDir.create(recursive: true);
      final baseName = _sanitize('$chunkFilename-i$itemIndex-f$fieldIndex');
      final out = File('${tempDir.path}/$baseName.$ext');
      await out.writeAsBytes(data, flush: true);
      return PreparedFileResponse(path: out.path, size: size, ext: ext);
    } finally {
      await access.close();
    }
  }

  Future<PreparedFileResponse> _prepareFieldFile({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    required bool convertSphereToWav,
  }) async {
    final parsed = await _parseIndex(File(indexPath));
    final format = parsed.config.dataFormat ?? <String>[];
    final usesTokensLayout = _usesTokensItemLayout(parsed.config);
    final access = await _loadChunkAccess(parsed, chunkFilename);
    try {
      final (data, size) = await _readFieldBytes(
        access,
        itemIndex,
        fieldIndex,
        format.length,
        usesTokensLayout,
        null,
      );
      var ext = _guessExt(
              format.length > fieldIndex ? format[fieldIndex] : null, data) ??
          'bin';
      final tempDir =
          Directory('${Directory.systemTemp.path}/dataset-inspector');
      await tempDir.create(recursive: true);
      final baseName = _sanitize('$chunkFilename-i$itemIndex-f$fieldIndex');
      var out = File('${tempDir.path}/$baseName.$ext');
      await out.writeAsBytes(data, flush: true);

      if (convertSphereToWav && ext == 'sph') {
        final wavOut = File('${tempDir.path}/$baseName.wav');
        await writeSphereAsWav(data, wavOut);
        out = wavOut;
        ext = 'wav';
      }

      return PreparedFileResponse(path: out.path, size: size, ext: ext);
    } finally {
      await access.close();
    }
  }

  Future<PreparedMediaResponse> _prepareFieldBytes({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    required bool convertSphereToWav,
  }) async {
    final parsed = await _parseIndex(File(indexPath));
    final format = parsed.config.dataFormat ?? <String>[];
    final usesTokensLayout = _usesTokensItemLayout(parsed.config);
    final access = await _loadChunkAccess(parsed, chunkFilename);
    try {
      final (data, _) = await _readFieldBytes(
        access,
        itemIndex,
        fieldIndex,
        format.length,
        usesTokensLayout,
        null,
      );
      var ext = _guessExt(
              format.length > fieldIndex ? format[fieldIndex] : null, data) ??
          'bin';
      var bytes = data;
      if (convertSphereToWav && ext == 'sph') {
        bytes = await decodeSphereToWavWithFallback(data);
        ext = 'wav';
      }
      return PreparedMediaResponse(bytes: bytes, size: bytes.length, ext: ext);
    } finally {
      await access.close();
    }
  }

  Future<OpenLeafResponse> openLeaf({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    String? openerAppPath,
  }) async {
    final parsed = await _parseIndex(File(indexPath));
    final format = parsed.config.dataFormat ?? <String>[];
    final usesTokensLayout = _usesTokensItemLayout(parsed.config);
    final access = await _loadChunkAccess(parsed, chunkFilename);
    try {
      final (data, size) = await _readFieldBytes(
        access,
        itemIndex,
        fieldIndex,
        format.length,
        usesTokensLayout,
        null,
      );
      var ext = _guessExt(
              format.length > fieldIndex ? format[fieldIndex] : null, data) ??
          'bin';
      final tempDir =
          Directory('${Directory.systemTemp.path}/dataset-inspector');
      await tempDir.create(recursive: true);
      final baseName = _sanitize('$chunkFilename-i$itemIndex-f$fieldIndex');
      var out = File('${tempDir.path}/$baseName.$ext');
      await out.writeAsBytes(data, flush: true);

      if (ext == 'sph') {
        final wavOut = File('${tempDir.path}/$baseName.wav');
        try {
          await writeSphereAsWav(data, wavOut);
          out = wavOut;
          ext = 'wav';
        } on Exception catch (err) {
          final base = '${out.path} ($size bytes)';
          return OpenLeafResponse(
            path: out.path,
            size: size,
            ext: ext,
            opened: false,
            needsOpener: true,
            message:
                '$base · sph decode failed: $err · choose an app to open it',
          );
        }
      }

      final result = await _openWith.openFile(out.path, appPath: openerAppPath);
      final base = '${out.path} ($size bytes)';
      final needsOpener = !result.opened && result.error != null;
      var message = base;
      if (needsOpener) {
        message = '$base · no default app found, choose an app to open it';
      }
      return OpenLeafResponse(
        path: out.path,
        size: size,
        ext: ext,
        opened: result.opened,
        needsOpener: needsOpener,
        message: message,
      );
    } finally {
      await access.close();
    }
  }

  Future<_ParsedIndex> _loadParsedIndexForStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
  }) async {
    if (indexBytes != null) {
      return _parseIndexBytes(indexBytes, indexName: indexName);
    }
    final normalizedPath = indexPath?.trim() ?? '';
    if (normalizedPath.isEmpty) {
      throw const FormatException(
          'missing LitData index source (indexPath or indexBytes)');
    }
    return _parseIndex(File(normalizedPath));
  }

  _RawChunk _chunkForFilename(_ParsedIndex parsed, String chunkFilename) {
    final trimmed = chunkFilename.trim();
    if (trimmed.isEmpty) {
      throw const FormatException('chunk filename is empty');
    }
    for (final chunk in parsed.chunks) {
      if (chunk.filename == trimmed) return chunk;
    }
    throw FormatException('Unknown chunk filename: $chunkFilename');
  }

  Future<_ChunkAccess> _loadChunkAccessFromStream({
    required _ParsedIndex parsed,
    required String chunkFilename,
    required Stream<List<int>> chunkStream,
  }) async {
    final chunk = _chunkForFilename(parsed, chunkFilename);
    final compression = parsed.config.compression?.toLowerCase();
    final bytes = await _readStreamBytes(chunkStream);
    if (compression == 'zstd') {
      final decoded = decodeZstd(bytes);
      _validateChunkLength(chunk, decoded.length);
      return _MemoryChunkAccess(decoded);
    }
    if (compression == null) {
      if (_isLikelyZstdChunk(chunkFilename: chunkFilename, bytes: bytes)) {
        try {
          final decoded = decodeZstd(bytes);
          _validateChunkLength(chunk, decoded.length);
          return _MemoryChunkAccess(decoded);
        } catch (_) {
          // Fall through to raw bytes when payload is not valid zstd.
        }
      }
      _validateChunkLength(chunk, bytes.length);
      return _MemoryChunkAccess(bytes);
    }
    throw FormatException('Unsupported compression: $compression');
  }

  bool _usesTokensItemLayout(_IndexConfig config) {
    final itemLoader = config.itemLoader?.trim();
    if (itemLoader == null || itemLoader.isEmpty) {
      return false;
    }
    if (itemLoader == 'PyTreeLoader') {
      return false;
    }
    if (itemLoader == 'TokensLoader') {
      return true;
    }
    throw FormatException('Unsupported LitData item loader: $itemLoader');
  }

  void _validateChunkLength(_RawChunk chunk, int actualLength) {
    if (chunk.chunkBytes <= 0 || actualLength == chunk.chunkBytes) {
      return;
    }
    throw FormatException(
      'Chunk length mismatch for ${chunk.filename}: expected '
      '${chunk.chunkBytes} bytes, got $actualLength',
    );
  }

  Future<Uint8List> _readStreamBytes(
    Stream<List<int>> stream, {
    int? maxBytes,
  }) async {
    final limit = maxBytes != null && maxBytes > 0 ? maxBytes : null;
    final builder = BytesBuilder(copy: false);
    var read = 0;
    await for (final chunk in stream) {
      if (chunk.isEmpty) continue;
      if (limit == null) {
        builder.add(chunk);
        read += chunk.length;
        continue;
      }
      final remaining = limit - read;
      if (remaining <= 0) break;
      if (chunk.length <= remaining) {
        builder.add(chunk);
        read += chunk.length;
      } else {
        builder.add(chunk.sublist(0, remaining));
        read += remaining;
      }
      if (read >= limit) break;
    }
    return builder.takeBytes();
  }

  _ParsedIndex _parseIndexBytes(
    Uint8List bytes, {
    required String indexName,
  }) {
    final decodedBytes = _decodeIndexBytes(bytes, fileName: indexName);
    final decoded =
        jsonDecode(utf8.decode(decodedBytes)) as Map<String, dynamic>;
    final parsed = _IndexFile.fromJson(decoded);
    final config = parsed.config;
    final configRaw = decoded['config'] is Map<String, dynamic>
        ? decoded['config'] as Map<String, dynamic>
        : <String, dynamic>{};
    return _ParsedIndex(
      rootDir: Directory.current,
      source: File(indexName),
      config: config,
      configRaw: configRaw,
      chunks: parsed.chunks,
    );
  }

  Uint8List _decodeIndexBytes(Uint8List bytes, {String? fileName}) {
    final lower = (fileName ?? '').toLowerCase();
    if (lower.endsWith('.zst') || lower.endsWith('.zstd')) {
      return decodeZstd(bytes);
    }
    if (_looksLikeZstdFrame(bytes)) {
      try {
        return decodeZstd(bytes);
      } catch (_) {
        // Keep original bytes if the payload is not a valid zstd stream.
      }
    }
    return bytes;
  }

  bool _looksLikeZstdFrame(Uint8List bytes) {
    return bytes.length >= 4 &&
        bytes[0] == 0x28 &&
        bytes[1] == 0xB5 &&
        bytes[2] == 0x2F &&
        bytes[3] == 0xFD;
  }

  bool _isLikelyZstdChunk({
    required String chunkFilename,
    required Uint8List bytes,
  }) {
    return _isLikelyZstdChunkName(chunkFilename) || _looksLikeZstdFrame(bytes);
  }

  bool _isLikelyZstdChunkName(String chunkFilename) {
    final lower = chunkFilename.toLowerCase();
    return lower.endsWith('.zst') || lower.endsWith('.zstd');
  }

  Future<_ParsedIndex> _parseIndex(File indexPath) async {
    if (_isChunkPath(indexPath)) {
      final neighbor = _findNeighborIndex(indexPath);
      if (neighbor != null) {
        return _parseIndex(neighbor);
      }
      return _parseChunkOnly(indexPath);
    }

    final resolved = await _resolveIndexPath(indexPath);
    return _parseIndexFile(resolved);
  }

  Future<_ParsedIndex> _parseIndexFile(File path) async {
    final content = await _readIndexFile(path);
    final decoded = jsonDecode(content) as Map<String, dynamic>;
    final parsed = _IndexFile.fromJson(decoded);
    final config = parsed.config;
    final configRaw = decoded['config'] is Map<String, dynamic>
        ? decoded['config'] as Map<String, dynamic>
        : <String, dynamic>{};
    final rootDir = path.parent;
    return _ParsedIndex(
      rootDir: rootDir,
      source: path,
      config: config,
      configRaw: configRaw,
      chunks: parsed.chunks,
    );
  }

  Future<_ParsedIndex> _parseChunkOnly(File path) async {
    final rootDir = path.parent;
    final raf = await path.open();
    final size = (await path.stat()).size;
    final numBuf = await raf.read(4);
    if (numBuf.length != 4) {
      await raf.close();
      throw const FormatException('Malformed chunk');
    }
    final numItems = _readLeU32(numBuf);
    final offsetsLen = (numItems + 1) * 4;
    final offsets = await raf.read(offsetsLen);
    await raf.close();
    if (offsets.length != offsetsLen) {
      throw const FormatException('Malformed chunk');
    }

    final chunk = _RawChunk(
      filename: path.uri.pathSegments.last,
      chunkBytes: size,
      chunkSize: numItems == 0 ? 1 : numItems,
      dim: null,
    );
    final fallbackConfig = _IndexConfig(
      compression: null,
      chunkSize: numItems == 0 ? 1 : numItems,
      chunkBytes: size,
      dataFormat: const ['bytes'],
      dataSpec: null,
      itemLoader: null,
    );
    return _ParsedIndex(
      rootDir: rootDir,
      source: path,
      config: fallbackConfig,
      configRaw: <String, dynamic>{
        'source': 'single-chunk',
        'data_format': const ['bytes'],
      },
      chunks: [chunk],
    );
  }

  Future<File> _resolveIndexPath(File path) async {
    if (await path.exists()) {
      return path;
    }
    if (await Directory(path.path).exists()) {
      final dir = Directory(path.path);
      final candidates = [
        'index.json',
        'index.json.zstd',
        'index.json.zst',
        '0.index.json',
        '0.index.json.zstd',
        '0.index.json.zst',
      ];
      for (final name in candidates) {
        final candidate = File('${dir.path}/$name');
        if (await candidate.exists()) return candidate;
      }
      final entries = dir
          .listSync()
          .whereType<File>()
          .where((file) =>
              file.path.endsWith('.index.json') ||
              file.path.contains('.index.json.'))
          .toList();
      entries.sort((a, b) => a.path.compareTo(b.path));
      if (entries.isNotEmpty) return entries.first;
    } else if (path.parent.existsSync()) {
      final base = path.uri.pathSegments.last.split('.').first;
      final parent = path.parent;
      final candidates = [
        path,
        File('${path.path}.json'),
        File('${path.path}.json.zstd'),
        File('${path.path}.json.zst'),
        File('${parent.path}/$base.json'),
        File('${parent.path}/$base.json.zstd'),
        File('${parent.path}/$base.json.zst'),
      ];
      for (final candidate in candidates) {
        if (await candidate.exists()) return candidate;
      }
    }
    throw FormatException('Missing ${path.path}');
  }

  Future<String> _readIndexFile(File path) async {
    final lower = path.path.toLowerCase();
    if (lower.endsWith('.zst') || lower.endsWith('.zstd')) {
      final bytes = await path.readAsBytes();
      final decoded = decodeZstd(bytes);
      return utf8.decode(decoded);
    }
    return path.readAsString();
  }

  Future<_ChunkAccess> _loadChunkAccess(
      _ParsedIndex parsed, String chunkFilename) async {
    final chunk = _chunkForFilename(parsed, chunkFilename);
    final chunkPath = File('${parsed.rootDir.path}/$chunkFilename');
    if (!chunkPath.existsSync()) {
      throw FormatException('Missing ${chunkPath.path}');
    }
    final compression = parsed.config.compression?.toLowerCase();
    if (compression == 'zstd') {
      final key = chunkPath.path;
      final cached = _cache.fetch(key);
      if (cached != null) {
        _validateChunkLength(chunk, cached.length);
        return _MemoryChunkAccess(cached);
      }
      final bytes = await chunkPath.readAsBytes();
      final decoded = decodeZstd(bytes);
      _validateChunkLength(chunk, decoded.length);
      _cache.store(key, decoded);
      return _MemoryChunkAccess(decoded);
    }
    if (compression != null) {
      throw FormatException('Unsupported compression: $compression');
    }
    if (_isLikelyZstdChunkName(chunkFilename)) {
      final key = chunkPath.path;
      final cached = _cache.fetch(key);
      if (cached != null) {
        _validateChunkLength(chunk, cached.length);
        return _MemoryChunkAccess(cached);
      }
      final bytes = await chunkPath.readAsBytes();
      final decoded = decodeZstd(bytes);
      _validateChunkLength(chunk, decoded.length);
      _cache.store(key, decoded);
      return _MemoryChunkAccess(decoded);
    }
    final fileSize = (await chunkPath.stat()).size;
    _validateChunkLength(chunk, fileSize);
    return _FileChunkAccess(await chunkPath.open());
  }

  Future<(int, List<int>)> _parseOffsets(_ChunkAccess access) async {
    final numBuf = await access.readExactAt(0, 4);
    final numItems = _readLeU32(numBuf);
    final offsetsLen = (numItems + 1) * 4;
    final offsetsBuf = await access.readExactAt(4, offsetsLen);
    final offsets = <int>[];
    for (var i = 0; i < offsetsBuf.length; i += 4) {
      offsets.add(_readLeU32(offsetsBuf.sublist(i, i + 4)));
    }
    return (numItems, offsets);
  }

  Future<List<int>> _readOffsetsRange(
    _ChunkAccess access,
    int start,
    int endExclusive,
  ) async {
    if (endExclusive < start) return const <int>[];
    final count = endExclusive - start + 1;
    final offsetStart = 4 + (start * 4);
    final bytes = await access.readExactAt(offsetStart, count * 4);
    if (bytes.length != count * 4) {
      throw const FormatException('Malformed chunk');
    }
    final offsets = <int>[];
    for (var i = 0; i < bytes.length; i += 4) {
      offsets.add(_readLeU32(bytes.sublist(i, i + 4)));
    }
    return offsets;
  }

  Future<(Uint8List, int)> _readFieldBytes(
    _ChunkAccess access,
    int itemIndex,
    int fieldIndex,
    int formatLen,
    bool usesTokensLayout,
    int? limit,
  ) async {
    final headerLen = usesTokensLayout ? 0 : formatLen * 4;
    final (numItems, offsets) = await _parseOffsets(access);
    if (itemIndex >= numItems) {
      throw const FormatException('Item index out of range');
    }
    final start = offsets[itemIndex];
    final end = offsets[itemIndex + 1];
    if (end < start) {
      throw const FormatException('Malformed chunk');
    }
    final sizes = await _readItemFieldSizes(
      access,
      start: start,
      end: end,
      formatLen: formatLen,
      usesTokensLayout: usesTokensLayout,
    );
    if (fieldIndex >= sizes.length) {
      throw const FormatException('Field index out of range');
    }
    var cursor = start + headerLen;
    for (var idx = 0; idx < sizes.length; idx += 1) {
      final size = sizes[idx];
      if (idx == fieldIndex) {
        final desired = limit == null ? size : limit.clamp(0, size).toInt();
        final data = await access.readExactAt(cursor, desired);
        return (data, size);
      }
      cursor += size;
    }
    throw const FormatException('Malformed chunk');
  }

  Future<List<int>> _readItemFieldSizes(
    _ChunkAccess access, {
    required int start,
    required int end,
    required int formatLen,
    required bool usesTokensLayout,
  }) async {
    if (usesTokensLayout) {
      return <int>[end - start];
    }
    final headerLen = formatLen * 4;
    if (headerLen <= 0) {
      return const <int>[];
    }
    final head = await access.readExactAt(start, headerLen);
    return List<int>.generate(formatLen, (idx) {
      final pos = idx * 4;
      return _readLeU32(head.sublist(pos, pos + 4));
    }, growable: false);
  }

  int _readLeU32(List<int> bytes) {
    if (bytes.length < 4) throw const FormatException('Malformed chunk');
    final data = ByteData.sublistView(Uint8List.fromList(bytes));
    return data.getUint32(0, Endian.little);
  }

  bool _isChunkPath(File path) {
    final name = path.path.toLowerCase();
    if (name.contains('index.json')) return false;
    return name.endsWith('.bin') ||
        name.contains('.bin.') ||
        name.endsWith('.zst') ||
        name.endsWith('.zstd');
  }

  bool _looksLikeChunkFile(File path) {
    final name = path.path.toLowerCase();
    if (name.contains('index.json')) return false;
    if (name.endsWith('.tar') ||
        name.endsWith('.tar.gz') ||
        name.endsWith('.tgz') ||
        name.endsWith('.tar.zst') ||
        name.endsWith('.tar.zstd')) {
      return false;
    }
    if (name.endsWith('.mds') ||
        name.endsWith('.mds.zst') ||
        name.endsWith('.mds.zstd')) {
      return false;
    }
    return _isChunkPath(path);
  }

  File? _findNeighborIndex(File chunkPath) {
    final parent = chunkPath.parent;
    final candidates = [
      'index.json',
      'index.json.zstd',
      'index.json.zst',
      '0.index.json',
      '0.index.json.zstd',
      '0.index.json.zst',
    ];
    for (final name in candidates) {
      final candidate = File('${parent.path}/$name');
      if (candidate.existsSync()) return candidate;
    }
    final globbed = parent
        .listSync()
        .whereType<File>()
        .where((file) =>
            file.path.endsWith('.index.json') ||
            file.path.contains('.index.json.'))
        .toList();
    globbed.sort((a, b) => a.path.compareTo(b.path));
    return globbed.isNotEmpty ? globbed.first : null;
  }

  String _sanitize(String input) {
    final buffer = StringBuffer();
    for (final rune in input.runes) {
      final char = String.fromCharCode(rune);
      if (RegExp(r'[A-Za-z0-9]').hasMatch(char)) {
        buffer.write(char);
      } else {
        buffer.write('-');
      }
    }
    return buffer.toString();
  }

  String? _guessExt(String? format, Uint8List data) {
    if (format != null) {
      final fmtLower = format.toLowerCase();
      if (fmtLower == 'bytes' || fmtLower == 'bin') {
        return _detectMagicExt(data) ?? 'bin';
      }
      final colon = format.split(':');
      if (colon.length == 2) {
        final subtype = colon[1].trim().replaceAll('.', '');
        if (subtype.isNotEmpty) return subtype;
      }
      final extSplit = format.split('.');
      if (extSplit.length > 1) {
        final ext = extSplit.last.trim();
        if (ext.isNotEmpty) return ext;
      }
      const map = {
        'jpeg': 'jpg',
        'jpg': 'jpg',
        'pil': 'png',
        'png': 'png',
        'tiff': 'tiff',
        'str': 'txt',
        'string': 'txt',
        'int': 'txt',
        'float': 'txt',
        'bool': 'txt',
        'bytes': 'bin',
      };
      if (map.containsKey(fmtLower)) {
        final ext = map[fmtLower]!;
        if (ext == 'bin') {
          return _detectMagicExt(data) ?? 'bin';
        }
        return ext;
      }
      if (fmtLower == 'audio') {
        return _detectMagicExt(data) ?? 'wav';
      }
      if (fmtLower.contains('wav')) return 'wav';
      if (fmtLower.contains('mp3')) return 'mp3';
      if (fmtLower.contains('flac')) return 'flac';
    }
    final magic = _detectMagicExt(data);
    if (magic != null) return magic;
    final text = previewUtf8Text(data);
    if (text != null && text.trim().isNotEmpty) return 'txt';
    return null;
  }

  String? _detectMagicExt(Uint8List data) {
    if (isSphereFile(data)) return 'sph';
    if (data.length >= 12 &&
        data[0] == 0x52 &&
        data[1] == 0x49 &&
        data[2] == 0x46 &&
        data[3] == 0x46 &&
        data[8] == 0x57 &&
        data[9] == 0x41 &&
        data[10] == 0x56 &&
        data[11] == 0x45) {
      return 'wav';
    }
    if (data.length >= 3 &&
        data[0] == 0x49 &&
        data[1] == 0x44 &&
        data[2] == 0x33) {
      return 'mp3';
    }
    if (data.length >= 2 && data[0] == 0xff && (data[1] & 0xe0) == 0xe0) {
      return 'mp3';
    }
    if (data.length >= 4 &&
        data[0] == 0x66 &&
        data[1] == 0x4c &&
        data[2] == 0x61 &&
        data[3] == 0x43) {
      return 'flac';
    }
    if (data.length >= 8 &&
        data[0] == 0x89 &&
        data[1] == 0x50 &&
        data[2] == 0x4e &&
        data[3] == 0x47) {
      return 'png';
    }
    if (data.length >= 3 &&
        data[0] == 0xff &&
        data[1] == 0xd8 &&
        data[2] == 0xff) {
      return 'jpg';
    }
    if (data.length >= 6 &&
        data[0] == 0x47 &&
        data[1] == 0x49 &&
        data[2] == 0x46 &&
        data[3] == 0x38) {
      return 'gif';
    }
    if (data.length >= 12 &&
        data[0] == 0x52 &&
        data[1] == 0x49 &&
        data[2] == 0x46 &&
        data[3] == 0x46 &&
        data[8] == 0x57 &&
        data[9] == 0x45 &&
        data[10] == 0x42 &&
        data[11] == 0x50) {
      return 'webp';
    }
    return null;
  }
}
