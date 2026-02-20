import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:crypto/crypto.dart';

import '../models/common.dart';
import '../utils/audio.dart';
import '../utils/preview.dart';
import '../utils/zstd.dart';
import 'open_with_service.dart';

const _previewBytes = 16 * 1024;
const _maxListedSamples = 200;
const _maxOpenBytes = 256 * 1024 * 1024;

class MosaicmlService {
  MosaicmlService({OpenWithService? openWith})
      : _openWith = openWith ?? OpenWithService();

  final OpenWithService _openWith;

  Future<IndexSummary> loadIndex(String indexPath) async {
    final (rootDir, resolved, index) = await _parseIndex(File(indexPath));
    final first = index.shards.isNotEmpty ? index.shards.first : null;
    if (first == null) {
      throw const FormatException('index.json contains no shards');
    }
    if (first.version != 2) {
      throw FormatException(
          'unsupported MDS version: ${first.version} (expected 2)');
    }
    if (first.format.toLowerCase() != 'mds') {
      throw FormatException(
          'unsupported dataset format: ${first.format} (expected mds)');
    }

    final dataFormat = first.columnNames;
    final compression = first.compression;
    final configRaw = <String, dynamic>{
      'format': 'mds',
      'version': first.version,
      'columnNames': first.columnNames,
      'columnEncodings': first.columnEncodings,
      'columnSizes': first.columnSizes,
      'compression': first.compression,
    };

    final chunks = index.shards.map((shard) {
      final rawPath = File('${rootDir.path}/${shard.rawData.basename}');
      var exists = rawPath.existsSync();
      var bytes = shard.rawData.bytes;
      if (!exists) {
        if (shard.zipData != null) {
          final zipPath = File('${rootDir.path}/${shard.zipData!.basename}');
          if (zipPath.existsSync()) {
            exists = true;
            bytes = shard.zipData!.bytes;
          }
        }
        if (!exists) {
          final zst = File('${rootDir.path}/${shard.rawData.basename}.zst');
          final zstd = File('${rootDir.path}/${shard.rawData.basename}.zstd');
          if (zst.existsSync() || zstd.existsSync()) {
            exists = true;
          }
        }
      }
      return ChunkSummary(
        filename: shard.rawData.basename,
        path: rawPath.path,
        chunkSize: shard.samples,
        chunkBytes: bytes,
        dim: null,
        exists: exists,
      );
    }).toList();

    return IndexSummary(
      indexPath: resolved.path,
      rootDir: rootDir.path,
      dataFormat: dataFormat,
      compression: compression,
      chunkSize: null,
      chunkBytes: null,
      configRaw: configRaw,
      chunks: chunks,
    );
  }

  Future<List<ItemMeta>> listSamples({
    required String indexPath,
    required String shardFilename,
  }) async {
    final page = await listSamplesPaged(
      indexPath: indexPath,
      shardFilename: shardFilename,
      offset: 0,
      length: _maxListedSamples,
    );
    return page.items;
  }

  Future<ItemPage> listSamplesPaged({
    required String indexPath,
    required String shardFilename,
    int offset = 0,
    int length = 200,
  }) async {
    final (rootDir, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final rawPath = File('${rootDir.path}/${shard.rawData.basename}');
    if (rawPath.existsSync()) {
      return _listSamplesPagedFromRaw(
        rawPath: rawPath,
        shard: shard,
        offset: offset,
        length: length,
      );
    }

    final compressedPath = _resolveCompressedShardPath(rootDir, shard);
    if (compressedPath != null) {
      final kind = _compressionKind(
        shard.compression,
        compressedPath.uri.pathSegments.isNotEmpty
            ? compressedPath.uri.pathSegments.last
            : compressedPath.path,
      );
      if (kind == 'zstd') {
        final streamed = await _listSamplesPagedFromZstdStream(
          zipPath: compressedPath,
          shard: shard,
          offset: offset,
          length: length,
        );
        if (streamed != null) {
          return streamed;
        }
      }

      // Fall back to one-time local decompression for stable random access.
      final resolvedRaw = await _resolveRawShardPath(rootDir, shard);
      return _listSamplesPagedFromRaw(
        rawPath: resolvedRaw,
        shard: shard,
        offset: offset,
        length: length,
      );
    }

    final resolvedRaw = await _resolveRawShardPath(rootDir, shard);
    return _listSamplesPagedFromRaw(
      rawPath: resolvedRaw,
      shard: shard,
      offset: offset,
      length: length,
    );
  }

  Future<List<ItemMeta>> listSamplesFromZstdCompressedStream({
    required String indexPath,
    required String shardFilename,
    required Stream<List<int>> compressedStream,
  }) async {
    final page = await listSamplesPagedFromZstdCompressedStream(
      indexPath: indexPath,
      shardFilename: shardFilename,
      compressedStream: compressedStream,
      offset: 0,
      length: _maxListedSamples,
    );
    return page.items;
  }

  Future<ItemPage> listSamplesPagedFromZstdCompressedStream({
    required String indexPath,
    required String shardFilename,
    required Stream<List<int>> compressedStream,
    int offset = 0,
    int length = 200,
  }) async {
    final (_, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final kind = _compressionKind(shard.compression, shardFilename);
    if (kind != 'zstd') {
      throw FormatException(
        'Unsupported compressed stream for shard $shardFilename (expected zstd).',
      );
    }
    final streamed = await _listSamplesPagedFromZstdInputStream(
      compressedStream: compressedStream,
      shard: shard,
      offset: offset,
      length: length,
    );
    if (streamed == null) {
      throw const FormatException(
          'System zstd executable is required for stream decoding.');
    }
    return streamed;
  }

  Future<FieldPreview> peekField({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final (rootDir, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;
    final shouldReadFull = _scalarEncoding(encoding);
    final field = await _readFieldBytes(
      rootDir: rootDir,
      shard: shard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxBytes: shouldReadFull ? null : _previewBytes,
    );
    final data = field.bytes;
    final fieldSize = field.size;

    String? previewText;
    if (encoding != null && shouldReadFull) {
      final decoded = _decodeScalarToText(encoding, data);
      previewText = decoded == null ? null : _takePreviewChars(decoded);
    } else {
      previewText = previewUtf8Text(data);
    }

    final guessedExt = _mdsGuessExt(encoding, data);
    final isBinary = previewText == null;
    return FieldPreview(
      previewText: previewText,
      hexSnippet: hexSnippet(data),
      guessedExt: guessedExt,
      isBinary: isBinary,
      size: fieldSize,
    );
  }

  Future<FieldPreview> peekFieldFromZstdCompressedStream({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> compressedStream,
  }) async {
    final (_, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final kind = _compressionKind(shard.compression, shardFilename);
    if (kind != 'zstd') {
      throw FormatException(
        'Unsupported compressed stream for shard $shardFilename (expected zstd).',
      );
    }
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;
    final shouldReadFull = _scalarEncoding(encoding);
    final field = await _readFieldBytesFromZstdInputStream(
      compressedStream: compressedStream,
      shard: shard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxBytes: shouldReadFull ? null : _previewBytes,
    );
    if (field == null) {
      throw const FormatException(
          'System zstd executable is required for stream decoding.');
    }
    final data = field.bytes;
    final fieldSize = field.size;

    String? previewText;
    if (encoding != null && shouldReadFull) {
      final decoded = _decodeScalarToText(encoding, data);
      previewText = decoded == null ? null : _takePreviewChars(decoded);
    } else {
      previewText = previewUtf8Text(data);
    }

    final guessedExt = _mdsGuessExt(encoding, data);
    final isBinary = previewText == null;
    return FieldPreview(
      previewText: previewText,
      hexSnippet: hexSnippet(data),
      guessedExt: guessedExt,
      isBinary: isBinary,
      size: fieldSize,
    );
  }

  Future<OpenLeafResponse> openLeaf({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    String? openerAppPath,
  }) async {
    final (rootDir, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;
    final field = await _readFieldBytes(
      rootDir: rootDir,
      shard: shard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxBytes: null,
    );
    final data = field.bytes;
    final size = field.size;
    var ext = _mdsGuessExt(encoding, data) ?? 'bin';
    var content = data;
    if (encoding != null) {
      final decoded = _decodeScalarToText(encoding, data);
      if (decoded != null && (ext == 'txt' || ext == 'json')) {
        content = Uint8List.fromList(utf8.encode(decoded));
      }
    }

    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector');
    await tempDir.create(recursive: true);
    final baseName = _sanitize('$shardFilename-i$itemIndex-f$fieldIndex');
    var out = File('${tempDir.path}/$baseName.$ext');
    await out.writeAsBytes(content, flush: true);

    if (ext == 'sph') {
      final wavOut = File('${tempDir.path}/$baseName.wav');
      await writeSphereAsWav(content, wavOut);
      out = wavOut;
      ext = 'wav';
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
  }

  Future<PreparedMediaResponse> prepareAudioPreview({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    return _prepareFieldBytes(
      indexPath: indexPath,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      convertSphereToWav: true,
    );
  }

  Future<PreparedMediaResponse> prepareAudioPreviewFromZstdCompressedStream({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> compressedStream,
  }) async {
    final (_, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final kind = _compressionKind(shard.compression, shardFilename);
    if (kind != 'zstd') {
      throw FormatException(
        'Unsupported compressed stream for shard $shardFilename (expected zstd).',
      );
    }
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;
    final field = await _readFieldBytesFromZstdInputStream(
      compressedStream: compressedStream,
      shard: shard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxBytes: null,
    );
    if (field == null) {
      throw const FormatException(
          'System zstd executable is required for stream decoding.');
    }
    final data = field.bytes;
    var ext = _mdsGuessExt(encoding, data) ?? 'bin';
    var bytes = data;
    if (ext == 'sph') {
      bytes = await decodeSphereToWavWithFallback(data);
      ext = 'wav';
    }
    return PreparedMediaResponse(bytes: bytes, size: bytes.length, ext: ext);
  }

  Future<PreparedFileResponse> prepareFieldFile({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    return _prepareFieldFile(
      indexPath: indexPath,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      convertSphereToWav: false,
    );
  }

  Future<PreparedFileResponse> prepareFieldFileFromZstdCompressedStream({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> compressedStream,
    bool convertSphereToWav = false,
  }) async {
    final (_, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final kind = _compressionKind(shard.compression, shardFilename);
    if (kind != 'zstd') {
      throw FormatException(
        'Unsupported compressed stream for shard $shardFilename (expected zstd).',
      );
    }
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;
    final field = await _readFieldBytesFromZstdInputStream(
      compressedStream: compressedStream,
      shard: shard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxBytes: null,
    );
    if (field == null) {
      throw const FormatException(
          'System zstd executable is required for stream decoding.');
    }
    final data = field.bytes;
    final size = field.size;
    var ext = _mdsGuessExt(encoding, data) ?? 'bin';

    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector');
    await tempDir.create(recursive: true);
    final baseName = _sanitize('$shardFilename-i$itemIndex-f$fieldIndex');
    var out = File('${tempDir.path}/$baseName.$ext');
    await out.writeAsBytes(data, flush: true);

    if (convertSphereToWav && ext == 'sph') {
      final wavOut = File('${tempDir.path}/$baseName.wav');
      await writeSphereAsWav(data, wavOut);
      out = wavOut;
      ext = 'wav';
    }

    return PreparedFileResponse(path: out.path, size: size, ext: ext);
  }

  Future<PreparedFileResponse> _prepareFieldFile({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required bool convertSphereToWav,
  }) async {
    final (rootDir, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;
    final field = await _readFieldBytes(
      rootDir: rootDir,
      shard: shard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxBytes: null,
    );
    final data = field.bytes;
    final size = field.size;
    var ext = _mdsGuessExt(encoding, data) ?? 'bin';

    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector');
    await tempDir.create(recursive: true);
    final baseName = _sanitize('$shardFilename-i$itemIndex-f$fieldIndex');
    var out = File('${tempDir.path}/$baseName.$ext');
    await out.writeAsBytes(data, flush: true);

    if (convertSphereToWav && ext == 'sph') {
      final wavOut = File('${tempDir.path}/$baseName.wav');
      await writeSphereAsWav(data, wavOut);
      out = wavOut;
      ext = 'wav';
    }

    return PreparedFileResponse(path: out.path, size: size, ext: ext);
  }

  Future<PreparedMediaResponse> _prepareFieldBytes({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required bool convertSphereToWav,
  }) async {
    final (rootDir, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;
    final field = await _readFieldBytes(
      rootDir: rootDir,
      shard: shard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxBytes: null,
    );
    final data = field.bytes;
    var ext = _mdsGuessExt(encoding, data) ?? 'bin';
    var bytes = data;
    if (convertSphereToWav && ext == 'sph') {
      bytes = await decodeSphereToWavWithFallback(data);
      ext = 'wav';
    }
    return PreparedMediaResponse(bytes: bytes, size: bytes.length, ext: ext);
  }

  Future<(Directory, File, _MdsIndexFile)> _parseIndex(File indexPath) async {
    final resolved = await _resolveIndexPath(indexPath);
    final bytes = await _readIndexBytes(resolved);
    final decoded = jsonDecode(utf8.decode(bytes)) as Map<String, dynamic>;
    final index = _MdsIndexFile.fromJson(decoded);
    return (resolved.parent, resolved, index);
  }

  Future<File> _resolveIndexPath(File path) async {
    if (await path.exists()) return path;
    final dir = Directory(path.path);
    if (await dir.exists()) {
      final candidates = ['index.json', 'index.json.zstd', 'index.json.zst'];
      for (final name in candidates) {
        final candidate = File('${dir.path}/$name');
        if (await candidate.exists()) return candidate;
      }
      throw FormatException('no index.json found in ${dir.path}');
    }
    throw FormatException('Missing ${path.path}');
  }

  Future<Uint8List> _readIndexBytes(File path) async {
    final lower = path.path.toLowerCase();
    final bytes = await path.readAsBytes();
    if (lower.endsWith('.zst') || lower.endsWith('.zstd')) {
      return decodeZstd(bytes);
    }
    return Uint8List.fromList(bytes);
  }

  _MdsShard _shardForFilename(_MdsIndexFile index, String shardFilename) {
    final trimmed = shardFilename.trim();
    if (trimmed.isEmpty) {
      throw const FormatException('missing shard filename');
    }
    for (final shard in index.shards) {
      if (shard.rawData.basename == trimmed) return shard;
      if (shard.zipData?.basename == trimmed) return shard;
    }
    throw FormatException('unknown shard: $trimmed');
  }

  String? _compressionKind(String? value, String filename) {
    final normalized = (value ?? '').trim().toLowerCase();
    if (normalized.startsWith('zstd')) return 'zstd';
    final lower = filename.toLowerCase();
    if (lower.endsWith('.zst') || lower.endsWith('.zstd')) return 'zstd';
    return null;
  }

  Future<File> _resolveRawShardPath(Directory rootDir, _MdsShard shard) async {
    final rawPath = File('${rootDir.path}/${shard.rawData.basename}');
    if (rawPath.existsSync()) return rawPath;

    if (shard.zipData != null) {
      final zipPath = File('${rootDir.path}/${shard.zipData!.basename}');
      if (zipPath.existsSync()) {
        final kind =
            _compressionKind(shard.compression, shard.zipData!.basename);
        if (kind == 'zstd') {
          return _decompressZstdToTemp(zipPath);
        }
        if (kind != null) {
          throw FormatException('Unsupported compression: $kind');
        }
        throw const FormatException('missing compression metadata');
      }
    }

    final zstdCandidates = [
      File('${rootDir.path}/${shard.rawData.basename}.zstd'),
      File('${rootDir.path}/${shard.rawData.basename}.zst'),
    ];
    for (final candidate in zstdCandidates) {
      if (candidate.existsSync()) {
        return _decompressZstdToTemp(candidate);
      }
    }

    throw FormatException(
        'shard data file not found for ${shard.rawData.basename}');
  }

  File? _resolveCompressedShardPath(Directory rootDir, _MdsShard shard) {
    if (shard.zipData != null) {
      final zipPath = File('${rootDir.path}/${shard.zipData!.basename}');
      if (zipPath.existsSync()) return zipPath;
    }
    final zstdCandidates = [
      File('${rootDir.path}/${shard.rawData.basename}.zstd'),
      File('${rootDir.path}/${shard.rawData.basename}.zst'),
    ];
    for (final candidate in zstdCandidates) {
      if (candidate.existsSync()) return candidate;
    }
    return null;
  }

  Future<_FieldBytes> _readFieldBytes({
    required Directory rootDir,
    required _MdsShard shard,
    required int itemIndex,
    required int fieldIndex,
    required int? maxBytes,
  }) async {
    final rawPath = File('${rootDir.path}/${shard.rawData.basename}');
    if (rawPath.existsSync()) {
      return _readFieldBytesFromRaw(
        rawPath: rawPath,
        shard: shard,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxBytes: maxBytes,
      );
    }

    final compressedPath = _resolveCompressedShardPath(rootDir, shard);
    if (compressedPath != null) {
      final kind = _compressionKind(
        shard.compression,
        compressedPath.uri.pathSegments.isNotEmpty
            ? compressedPath.uri.pathSegments.last
            : compressedPath.path,
      );
      if (kind == 'zstd') {
        final streamed = await _readFieldBytesFromZstdStream(
          zipPath: compressedPath,
          shard: shard,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          maxBytes: maxBytes,
        );
        if (streamed != null) return streamed;
      }
    }

    final resolvedRaw = await _resolveRawShardPath(rootDir, shard);
    return _readFieldBytesFromRaw(
      rawPath: resolvedRaw,
      shard: shard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxBytes: maxBytes,
    );
  }

  Future<_FieldBytes> _readFieldBytesFromRaw({
    required File rawPath,
    required _MdsShard shard,
    required int itemIndex,
    required int fieldIndex,
    required int? maxBytes,
  }) async {
    final fp = await rawPath.open();
    try {
      final (begin, end) = await _readSampleOffsets(fp, itemIndex);
      final sizes = await _readVariableSizes(fp, begin, shard);
      final (fieldStart, fieldSize) =
          _fieldStartOffset(begin, shard, fieldIndex, sizes);
      final available = end - fieldStart;
      if (available < fieldSize) {
        throw const FormatException('Malformed shard');
      }
      if (maxBytes == null && fieldSize > _maxOpenBytes) {
        throw FormatException(
            'field is too large to open ($fieldSize bytes, max $_maxOpenBytes)');
      }
      final desired = maxBytes == null
          ? fieldSize
          : math.min(fieldSize, math.max(0, maxBytes));
      await fp.setPosition(fieldStart);
      final data = await fp.read(desired);
      if (data.length != desired) {
        throw const FormatException('Malformed shard');
      }
      return _FieldBytes(bytes: data, size: fieldSize);
    } finally {
      await fp.close();
    }
  }

  Future<ItemPage> _listSamplesPagedFromRaw({
    required File rawPath,
    required _MdsShard shard,
    required int offset,
    required int length,
  }) async {
    final fp = await rawPath.open();
    try {
      await fp.setPosition(0);
      final numBuf = await fp.read(4);
      if (numBuf.length != 4) {
        throw const FormatException('Malformed shard');
      }
      final numInFile = _readLeU32(numBuf);
      final expected = shard.samples;
      final total = numInFile < expected ? numInFile : expected;

      final safeLength = length < 1 ? 1 : length;
      final start = offset.clamp(0, total).toInt();
      final end = (start + safeLength).clamp(0, total).toInt();

      if (start >= end) {
        return ItemPage(
          offset: start,
          length: safeLength,
          items: const <ItemMeta>[],
          partial: start < total,
          numItemsTotal: total,
        );
      }

      final items = <ItemMeta>[];
      for (var idx = start; idx < end; idx += 1) {
        final (begin, endOffset) = await _readSampleOffsets(fp, idx);
        final sizes = await _readVariableSizes(fp, begin, shard);
        final fields = List.generate(sizes.length, (fieldIndex) {
          return FieldMeta(fieldIndex: fieldIndex, size: sizes[fieldIndex]);
        });
        items.add(ItemMeta(
          itemIndex: idx,
          totalBytes: endOffset - begin,
          fields: fields,
        ));
      }

      return ItemPage(
        offset: start,
        length: safeLength,
        items: items,
        partial: end < total,
        numItemsTotal: total,
      );
    } finally {
      await fp.close();
    }
  }

  Future<ItemPage?> _listSamplesPagedFromZstdStream({
    required File zipPath,
    required _MdsShard shard,
    required int offset,
    required int length,
  }) async {
    final executable = _resolveSystemZstdExecutable();
    if (executable == null) return null;

    final process = await Process.start(
      executable,
      ['-d', '-q', '-c', zipPath.path],
    );
    final stderrFuture = process.stderr.transform(utf8.decoder).join();

    final safeLength = length < 1 ? 1 : length;
    final varCols = shard.columnSizes.where((s) => s == null).length;
    final varHeaderLen = varCols * 4;

    final countCapture = _ByteRangeCapture(0, 4);
    _ByteRangeCapture? offsetsCapture;
    List<({int begin, int end})>? sampleOffsets;
    List<_ByteRangeCapture>? headerCaptures;

    var streamOffset = 0;
    var initialized = false;
    var done = false;
    var total = 0;
    var start = 0;
    var end = 0;

    try {
      await for (final chunkData in process.stdout) {
        final chunk =
            chunkData is Uint8List ? chunkData : Uint8List.fromList(chunkData);

        countCapture.capture(chunk, streamOffset);
        if (!initialized && countCapture.complete) {
          final numInFile = _readLeU32(countCapture.bytes);
          final expected = shard.samples;
          total = numInFile < expected ? numInFile : expected;
          start = offset.clamp(0, total).toInt();
          end = (start + safeLength).clamp(0, total).toInt();
          initialized = true;
          if (start >= end) {
            done = true;
            process.kill();
            break;
          }
          final offsetStart = (1 + start) * 4;
          final offsetEnd = offsetStart + (end - start + 1) * 4;
          offsetsCapture = _ByteRangeCapture(offsetStart, offsetEnd);
        }

        offsetsCapture?.capture(chunk, streamOffset);
        if (initialized &&
            sampleOffsets == null &&
            offsetsCapture != null &&
            offsetsCapture.complete) {
          final offsets = <({int begin, int end})>[];
          final bytes = offsetsCapture.bytes;
          for (var i = 0; i < end - start; i += 1) {
            final beginPos = i * 4;
            final endPos = beginPos + 4;
            final nextEndPos = endPos + 4;
            final begin = _readLeU32(bytes.sublist(beginPos, endPos));
            final endOffset = _readLeU32(bytes.sublist(endPos, nextEndPos));
            if (endOffset < begin) {
              throw const FormatException('Malformed shard');
            }
            offsets.add((begin: begin, end: endOffset));
          }
          sampleOffsets = offsets;
          if (varHeaderLen > 0) {
            final captures = <_ByteRangeCapture>[];
            for (final sample in sampleOffsets) {
              final headerEnd = sample.begin + varHeaderLen;
              if (headerEnd > sample.end) {
                throw const FormatException('Malformed shard');
              }
              captures.add(_ByteRangeCapture(sample.begin, headerEnd));
            }
            headerCaptures = captures;
          } else {
            headerCaptures = const <_ByteRangeCapture>[];
          }
        }

        final captures = headerCaptures;
        if (captures != null && captures.isNotEmpty) {
          for (final capture in captures) {
            capture.capture(chunk, streamOffset);
          }
        }

        streamOffset += chunk.length;
        if (initialized &&
            sampleOffsets != null &&
            headerCaptures != null &&
            headerCaptures.every((capture) => capture.complete)) {
          done = true;
          process.kill();
          break;
        }
      }
    } catch (_) {
      process.kill();
      rethrow;
    } finally {
      if (!done) {
        process.kill();
      }
    }

    final stderrText = await stderrFuture;
    await process.exitCode;

    if (!initialized) {
      final detail = stderrText.trim();
      if (detail.isNotEmpty) {
        throw FormatException(
            'failed to stream decode ${zipPath.path}: $detail');
      }
      throw const FormatException('Malformed shard');
    }

    if (start >= end) {
      return ItemPage(
        offset: start,
        length: safeLength,
        items: const <ItemMeta>[],
        partial: start < total,
        numItemsTotal: total,
      );
    }

    if (!done || sampleOffsets == null || headerCaptures == null) {
      final detail = stderrText.trim();
      if (detail.isNotEmpty) {
        throw FormatException(
            'failed to stream decode ${zipPath.path}: $detail');
      }
      throw const FormatException('Malformed shard');
    }

    final fixedSizes =
        varHeaderLen == 0 ? _buildVariableSizes(shard, Uint8List(0)) : null;
    final items = <ItemMeta>[];
    for (var i = 0; i < sampleOffsets.length; i += 1) {
      final sample = sampleOffsets[i];
      final sizes =
          fixedSizes ?? _buildVariableSizes(shard, headerCaptures[i].bytes);
      final fields = List.generate(sizes.length, (fieldIndex) {
        return FieldMeta(fieldIndex: fieldIndex, size: sizes[fieldIndex]);
      });
      items.add(
        ItemMeta(
          itemIndex: start + i,
          totalBytes: sample.end - sample.begin,
          fields: fields,
        ),
      );
    }

    return ItemPage(
      offset: start,
      length: safeLength,
      items: items,
      partial: end < total,
      numItemsTotal: total,
    );
  }

  Future<_FieldBytes?> _readFieldBytesFromZstdStream({
    required File zipPath,
    required _MdsShard shard,
    required int itemIndex,
    required int fieldIndex,
    required int? maxBytes,
  }) async {
    final executable = _resolveSystemZstdExecutable();
    if (executable == null) return null;

    final process = await Process.start(
      executable,
      ['-d', '-q', '-c', zipPath.path],
    );
    final stderrFuture = process.stderr.transform(utf8.decoder).join();

    final offsetPos = (1 + itemIndex) * 4;
    final offsetsCapture = _ByteRangeCapture(offsetPos, offsetPos + 8);
    final varCols = shard.columnSizes.where((s) => s == null).length;

    int streamOffset = 0;
    int? sampleBegin;
    int? sampleEnd;
    int? fieldSize;
    _ByteRangeCapture? varHeaderCapture;
    _ByteRangeCapture? fieldCapture;
    var done = false;

    try {
      await for (final chunkData in process.stdout) {
        final chunk =
            chunkData is Uint8List ? chunkData : Uint8List.fromList(chunkData);

        offsetsCapture.capture(chunk, streamOffset);
        if (sampleBegin == null && offsetsCapture.complete) {
          sampleBegin = _readLeU32(offsetsCapture.bytes.sublist(0, 4));
          sampleEnd = _readLeU32(offsetsCapture.bytes.sublist(4, 8));
          if (sampleEnd < sampleBegin) {
            throw const FormatException('Malformed shard');
          }
          final varHeaderLen = varCols * 4;
          if (varHeaderLen == 0) {
            final sizes = _buildVariableSizes(shard, Uint8List(0));
            final (start, size) =
                _fieldStartOffset(sampleBegin, shard, fieldIndex, sizes);
            if (sampleEnd - start < size) {
              throw const FormatException('Malformed shard');
            }
            if (maxBytes == null && size > _maxOpenBytes) {
              throw FormatException(
                  'field is too large to open ($size bytes, max $_maxOpenBytes)');
            }
            final desired =
                maxBytes == null ? size : math.min(size, math.max(0, maxBytes));
            fieldSize = size;
            fieldCapture = _ByteRangeCapture(start, start + desired);
          } else {
            varHeaderCapture =
                _ByteRangeCapture(sampleBegin, sampleBegin + varHeaderLen);
          }
        }

        varHeaderCapture?.capture(chunk, streamOffset);
        if (fieldCapture == null &&
            sampleBegin != null &&
            sampleEnd != null &&
            varHeaderCapture != null &&
            varHeaderCapture.complete) {
          final sizes = _buildVariableSizes(shard, varHeaderCapture.bytes);
          final (start, size) =
              _fieldStartOffset(sampleBegin, shard, fieldIndex, sizes);
          if (sampleEnd - start < size) {
            throw const FormatException('Malformed shard');
          }
          if (maxBytes == null && size > _maxOpenBytes) {
            throw FormatException(
                'field is too large to open ($size bytes, max $_maxOpenBytes)');
          }
          final desired =
              maxBytes == null ? size : math.min(size, math.max(0, maxBytes));
          fieldSize = size;
          fieldCapture = _ByteRangeCapture(start, start + desired);
        }

        fieldCapture?.capture(chunk, streamOffset);
        streamOffset += chunk.length;
        if (fieldCapture != null &&
            fieldCapture.complete &&
            fieldSize != null) {
          done = true;
          process.kill();
          break;
        }
      }
    } catch (_) {
      process.kill();
      rethrow;
    } finally {
      if (!done) {
        process.kill();
      }
    }

    final stderrText = await stderrFuture;
    await process.exitCode;
    if (!done || fieldCapture == null || fieldSize == null) {
      final detail = stderrText.trim();
      if (detail.isNotEmpty) {
        throw FormatException(
            'failed to stream decode ${zipPath.path}: $detail');
      }
      throw const FormatException('Malformed shard');
    }

    return _FieldBytes(bytes: fieldCapture.bytes, size: fieldSize);
  }

  Future<_FieldBytes?> _readFieldBytesFromZstdInputStream({
    required Stream<List<int>> compressedStream,
    required _MdsShard shard,
    required int itemIndex,
    required int fieldIndex,
    required int? maxBytes,
  }) async {
    final executable = _resolveSystemZstdExecutable();
    if (executable == null) return null;

    final process = await Process.start(
      executable,
      ['-d', '-q', '-c'],
    );
    final inputPipe = _startCompressedStreamPipe(
      compressedStream: compressedStream,
      process: process,
    );
    final stderrFuture = process.stderr.transform(utf8.decoder).join();

    final offsetPos = (1 + itemIndex) * 4;
    final offsetsCapture = _ByteRangeCapture(offsetPos, offsetPos + 8);
    final varCols = shard.columnSizes.where((s) => s == null).length;

    int streamOffset = 0;
    int? sampleBegin;
    int? sampleEnd;
    int? fieldSize;
    _ByteRangeCapture? varHeaderCapture;
    _ByteRangeCapture? fieldCapture;
    var done = false;

    try {
      await for (final chunkData in process.stdout) {
        final chunk =
            chunkData is Uint8List ? chunkData : Uint8List.fromList(chunkData);

        offsetsCapture.capture(chunk, streamOffset);
        if (sampleBegin == null && offsetsCapture.complete) {
          sampleBegin = _readLeU32(offsetsCapture.bytes.sublist(0, 4));
          sampleEnd = _readLeU32(offsetsCapture.bytes.sublist(4, 8));
          if (sampleEnd < sampleBegin) {
            throw const FormatException('Malformed shard');
          }
          final varHeaderLen = varCols * 4;
          if (varHeaderLen == 0) {
            final sizes = _buildVariableSizes(shard, Uint8List(0));
            final (start, size) =
                _fieldStartOffset(sampleBegin, shard, fieldIndex, sizes);
            if (sampleEnd - start < size) {
              throw const FormatException('Malformed shard');
            }
            if (maxBytes == null && size > _maxOpenBytes) {
              throw FormatException(
                  'field is too large to open ($size bytes, max $_maxOpenBytes)');
            }
            final desired =
                maxBytes == null ? size : math.min(size, math.max(0, maxBytes));
            fieldSize = size;
            fieldCapture = _ByteRangeCapture(start, start + desired);
          } else {
            varHeaderCapture =
                _ByteRangeCapture(sampleBegin, sampleBegin + varHeaderLen);
          }
        }

        varHeaderCapture?.capture(chunk, streamOffset);
        if (fieldCapture == null &&
            sampleBegin != null &&
            sampleEnd != null &&
            varHeaderCapture != null &&
            varHeaderCapture.complete) {
          final sizes = _buildVariableSizes(shard, varHeaderCapture.bytes);
          final (start, size) =
              _fieldStartOffset(sampleBegin, shard, fieldIndex, sizes);
          if (sampleEnd - start < size) {
            throw const FormatException('Malformed shard');
          }
          if (maxBytes == null && size > _maxOpenBytes) {
            throw FormatException(
                'field is too large to open ($size bytes, max $_maxOpenBytes)');
          }
          final desired =
              maxBytes == null ? size : math.min(size, math.max(0, maxBytes));
          fieldSize = size;
          fieldCapture = _ByteRangeCapture(start, start + desired);
        }

        fieldCapture?.capture(chunk, streamOffset);
        streamOffset += chunk.length;
        if (fieldCapture != null &&
            fieldCapture.complete &&
            fieldSize != null) {
          done = true;
          await inputPipe.stop();
          process.kill();
          break;
        }
      }
    } catch (_) {
      await inputPipe.stop();
      process.kill();
      rethrow;
    } finally {
      if (!done) {
        await inputPipe.stop();
        process.kill();
      }
    }

    final stderrText = await stderrFuture;
    await inputPipe.done;
    await process.exitCode;
    if (!done || fieldCapture == null || fieldSize == null) {
      final detail = stderrText.trim();
      if (detail.isNotEmpty) {
        throw FormatException('failed to stream decode zstd input: $detail');
      }
      throw const FormatException('Malformed shard');
    }

    return _FieldBytes(bytes: fieldCapture.bytes, size: fieldSize);
  }

  Future<ItemPage?> _listSamplesPagedFromZstdInputStream({
    required Stream<List<int>> compressedStream,
    required _MdsShard shard,
    required int offset,
    required int length,
  }) async {
    final executable = _resolveSystemZstdExecutable();
    if (executable == null) return null;

    final process = await Process.start(
      executable,
      ['-d', '-q', '-c'],
    );
    final inputPipe = _startCompressedStreamPipe(
      compressedStream: compressedStream,
      process: process,
    );
    final stderrFuture = process.stderr.transform(utf8.decoder).join();

    final safeLength = length < 1 ? 1 : length;

    final countCapture = _ByteRangeCapture(0, 4);
    _ByteRangeCapture? offsetsCapture;
    List<({int begin, int end})>? sampleOffsets;

    var streamOffset = 0;
    var initialized = false;
    var done = false;
    var total = 0;
    var start = 0;
    var end = 0;

    try {
      await for (final chunkData in process.stdout) {
        final chunk =
            chunkData is Uint8List ? chunkData : Uint8List.fromList(chunkData);

        countCapture.capture(chunk, streamOffset);
        if (!initialized && countCapture.complete) {
          final numInFile = _readLeU32(countCapture.bytes);
          final expected = shard.samples;
          total = numInFile < expected ? numInFile : expected;
          start = offset.clamp(0, total).toInt();
          end = (start + safeLength).clamp(0, total).toInt();
          initialized = true;
          if (start >= end) {
            done = true;
            await inputPipe.stop();
            process.kill();
            break;
          }
          final offsetStart = (1 + start) * 4;
          final offsetEnd = offsetStart + (end - start + 1) * 4;
          offsetsCapture = _ByteRangeCapture(offsetStart, offsetEnd);
        }

        offsetsCapture?.capture(chunk, streamOffset);
        if (initialized &&
            sampleOffsets == null &&
            offsetsCapture != null &&
            offsetsCapture.complete) {
          final offsets = <({int begin, int end})>[];
          final bytes = offsetsCapture.bytes;
          for (var i = 0; i < end - start; i += 1) {
            final beginPos = i * 4;
            final endPos = beginPos + 4;
            final nextEndPos = endPos + 4;
            final begin = _readLeU32(bytes.sublist(beginPos, endPos));
            final endOffset = _readLeU32(bytes.sublist(endPos, nextEndPos));
            if (endOffset < begin) {
              throw const FormatException('Malformed shard');
            }
            offsets.add((begin: begin, end: endOffset));
          }
          sampleOffsets = offsets;
          done = true;
          await inputPipe.stop();
          process.kill();
          break;
        }

        streamOffset += chunk.length;
      }
    } catch (_) {
      await inputPipe.stop();
      process.kill();
      rethrow;
    } finally {
      if (!done) {
        await inputPipe.stop();
        process.kill();
      }
    }

    final stderrText = await stderrFuture;
    await inputPipe.done;
    await process.exitCode;

    if (!initialized) {
      final detail = stderrText.trim();
      if (detail.isNotEmpty) {
        throw FormatException('failed to stream decode zstd input: $detail');
      }
      throw const FormatException('Malformed shard');
    }

    if (start >= end) {
      return ItemPage(
        offset: start,
        length: safeLength,
        items: const <ItemMeta>[],
        partial: start < total,
        numItemsTotal: total,
      );
    }

    if (!done || sampleOffsets == null) {
      final detail = stderrText.trim();
      if (detail.isNotEmpty) {
        throw FormatException('failed to stream decode zstd input: $detail');
      }
      throw const FormatException('Malformed shard');
    }

    final fieldSizesTemplate =
        shard.columnSizes.map((fixed) => fixed ?? 0).toList(growable: false);
    final items = <ItemMeta>[];
    for (var i = 0; i < sampleOffsets.length; i += 1) {
      final sample = sampleOffsets[i];
      final fields = List.generate(fieldSizesTemplate.length, (fieldIndex) {
        return FieldMeta(
            fieldIndex: fieldIndex, size: fieldSizesTemplate[fieldIndex]);
      });
      items.add(
        ItemMeta(
          itemIndex: start + i,
          totalBytes: sample.end - sample.begin,
          fields: fields,
        ),
      );
    }

    return ItemPage(
      offset: start,
      length: safeLength,
      items: items,
      partial: end < total,
      numItemsTotal: total,
    );
  }

  _ProcessInputPipe _startCompressedStreamPipe({
    required Stream<List<int>> compressedStream,
    required Process process,
  }) {
    final completer = Completer<void>();
    final inputController = StreamController<List<int>>(sync: true);
    var stdinClosed = false;
    var stopping = false;
    var sourceDone = false;
    late final StreamSubscription<List<int>> subscription;
    void completeDone([Object? error, StackTrace? stackTrace]) {
      if (completer.isCompleted) return;
      if (error == null) {
        completer.complete();
      } else {
        completer.completeError(error, stackTrace ?? StackTrace.current);
      }
    }

    Future<void> closeStdin() async {
      if (stdinClosed) return;
      stdinClosed = true;
      try {
        await process.stdin.close();
      } catch (_) {}
    }

    final stdinWriteFuture = () async {
      try {
        await process.stdin.addStream(inputController.stream);
      } catch (error, stackTrace) {
        if (!_isBenignPipeWriteError(error)) {
          completeDone(error, stackTrace);
        }
      } finally {
        await closeStdin();
        if (sourceDone) {
          completeDone();
        }
      }
    }();

    Future<void> stop() async {
      if (stopping) {
        await completer.future;
        return;
      }
      stopping = true;
      try {
        await subscription.cancel();
      } catch (_) {}
      sourceDone = true;
      if (!inputController.isClosed) {
        try {
          await inputController.close();
        } catch (_) {}
      }
      try {
        await stdinWriteFuture;
      } catch (error, stackTrace) {
        if (!_isBenignPipeWriteError(error)) {
          completeDone(error, stackTrace);
        }
      }
      await closeStdin();
      completeDone();
      await completer.future;
    }

    subscription = compressedStream.listen(
      (chunk) {
        if (chunk.isEmpty || stopping || inputController.isClosed) return;
        try {
          inputController.add(chunk);
        } catch (error, stackTrace) {
          if (_isBenignPipeWriteError(error)) {
            completeDone();
            unawaited(stop().catchError((_) {}));
            return;
          }
          completeDone(error, stackTrace);
          unawaited(stop().catchError((_) {}));
        }
      },
      onError: (Object error, StackTrace stackTrace) {
        sourceDone = true;
        if (!inputController.isClosed) {
          try {
            inputController.addError(error, stackTrace);
          } catch (_) {}
          unawaited(inputController.close().catchError((_) {}));
        }
        if (_isBenignPipeWriteError(error)) {
          completeDone();
          return;
        }
        completeDone(error, stackTrace);
      },
      onDone: () {
        sourceDone = true;
        if (!inputController.isClosed) {
          unawaited(inputController.close().catchError((_) {}));
        }
      },
      cancelOnError: false,
    );

    unawaited(process.exitCode.then((_) {
      return stop();
    }).catchError((_) {}));

    return _ProcessInputPipe(
      done: completer.future,
      stop: stop,
    );
  }

  bool _isBenignPipeWriteError(Object error) {
    final message = error.toString().toLowerCase();
    return message.contains('broken pipe') ||
        message.contains('errno = 32') ||
        message.contains('write failed') ||
        message.contains('streamsink is closed') ||
        message.contains('write on closed');
  }

  Future<File> _decompressZstdToTemp(File zipPath) async {
    final key = _hashKeyForPath(zipPath);
    final outDir =
        Directory('${Directory.systemTemp.path}/dataset-inspector/mds-cache');
    await outDir.create(recursive: true);
    final outPath = File('${outDir.path}/$key.mds');
    if (outPath.existsSync()) return outPath;

    final usedSystemZstd = await _tryDecompressWithSystemZstd(zipPath, outPath);
    if (!usedSystemZstd) {
      final bytes = await zipPath.readAsBytes();
      final decoded = decodeZstd(bytes);
      await outPath.writeAsBytes(decoded, flush: true);
    }

    return outPath;
  }

  Future<bool> _tryDecompressWithSystemZstd(File zipPath, File outPath) async {
    final executable = _resolveSystemZstdExecutable();
    if (executable == null) return false;
    final result = await Process.run(
      executable,
      ['-d', '-q', '-f', zipPath.path, '-o', outPath.path],
    );
    return result.exitCode == 0 && outPath.existsSync();
  }

  String? _resolveSystemZstdExecutable() {
    const candidates = [
      '/opt/homebrew/bin/zstd',
      '/usr/local/bin/zstd',
      '/usr/bin/zstd',
      'zstd',
    ];
    for (final candidate in candidates) {
      try {
        if (candidate.contains('/')) {
          if (File(candidate).existsSync()) return candidate;
          continue;
        }
        final check = Process.runSync(candidate, ['--version']);
        if (check.exitCode == 0) return candidate;
      } catch (_) {
        // Ignore and continue trying the next candidate.
      }
    }
    return null;
  }

  String _hashKeyForPath(File path) {
    final stat = path.statSync();
    final payload =
        '${path.path}:${stat.size}:${stat.modified.millisecondsSinceEpoch}';
    return sha1.convert(utf8.encode(payload)).toString();
  }

  Future<(int, int)> _readSampleOffsets(RandomAccessFile fp, int idx) async {
    final offset = (1 + idx) * 4;
    await fp.setPosition(offset);
    final buf = await fp.read(8);
    if (buf.length != 8) throw const FormatException('Malformed shard');
    final begin = _readLeU32(buf.sublist(0, 4));
    final end = _readLeU32(buf.sublist(4, 8));
    if (end < begin) throw const FormatException('Malformed shard');
    return (begin, end);
  }

  Future<List<int>> _readVariableSizes(
      RandomAccessFile fp, int begin, _MdsShard shard) async {
    final varCols = shard.columnSizes.where((s) => s == null).length;
    final headerLen = varCols * 4;
    Uint8List header = Uint8List(0);
    if (headerLen > 0) {
      await fp.setPosition(begin);
      header = await fp.read(headerLen);
      if (header.length != headerLen) {
        throw const FormatException('Malformed shard');
      }
    }
    return _buildVariableSizes(shard, header);
  }

  List<int> _buildVariableSizes(_MdsShard shard, Uint8List header) {
    final sizes = <int>[];
    var varIdx = 0;
    for (final fixed in shard.columnSizes) {
      if (fixed != null) {
        sizes.add(fixed);
      } else {
        final start = varIdx * 4;
        final end = start + 4;
        if (end > header.length) {
          throw const FormatException('Malformed shard');
        }
        final size = _readLeU32(header.sublist(start, end));
        sizes.add(size);
        varIdx += 1;
      }
    }
    return sizes;
  }

  (int, int) _fieldStartOffset(
      int begin, _MdsShard shard, int fieldIndex, List<int> sizes) {
    if (fieldIndex >= sizes.length) {
      throw const FormatException('field index out of range');
    }
    final varCols = shard.columnSizes.where((s) => s == null).length;
    final headerLen = varCols * 4;
    var cursor = begin + headerLen;
    for (var idx = 0; idx < sizes.length; idx += 1) {
      if (idx == fieldIndex) {
        return (cursor, sizes[idx]);
      }
      cursor += sizes[idx];
    }
    throw const FormatException('Malformed shard');
  }

  bool _scalarEncoding(String? encoding) {
    final lower = encoding?.trim().toLowerCase();
    const scalar = {
      'int',
      'int8',
      'int16',
      'int32',
      'int64',
      'uint8',
      'uint16',
      'uint32',
      'uint64',
      'float32',
      'float64',
    };
    return lower != null && scalar.contains(lower);
  }

  String? _decodeScalarToText(String encoding, Uint8List data) {
    final enc = encoding.trim().toLowerCase();
    ByteData? view;
    if (data.isNotEmpty) {
      view = ByteData.sublistView(data);
    }
    switch (enc) {
      case 'str':
      case 'str_int':
      case 'str_float':
      case 'str_decimal':
      case 'json':
        return utf8.decode(data, allowMalformed: true);
      case 'int':
      case 'int64':
        if (data.length != 8) return null;
        return view!.getInt64(0, Endian.little).toString();
      case 'int32':
        if (data.length != 4) return null;
        return view!.getInt32(0, Endian.little).toString();
      case 'int16':
        if (data.length != 2) return null;
        return view!.getInt16(0, Endian.little).toString();
      case 'int8':
        if (data.length != 1) return null;
        return data[0].toSigned(8).toString();
      case 'uint64':
        if (data.length != 8) return null;
        return view!.getUint64(0, Endian.little).toString();
      case 'uint32':
        if (data.length != 4) return null;
        return view!.getUint32(0, Endian.little).toString();
      case 'uint16':
        if (data.length != 2) return null;
        return view!.getUint16(0, Endian.little).toString();
      case 'uint8':
        if (data.length != 1) return null;
        return data[0].toString();
      case 'float64':
        if (data.length != 8) return null;
        return view!.getFloat64(0, Endian.little).toString();
      case 'float32':
        if (data.length != 4) return null;
        return view!.getFloat32(0, Endian.little).toString();
      default:
        return null;
    }
  }

  String _takePreviewChars(String text) {
    var count = 0;
    final buffer = StringBuffer();
    for (final rune in text.runes) {
      if (count >= previewTextChars) break;
      buffer.writeCharCode(rune);
      count += 1;
    }
    return buffer.toString();
  }

  String? _mdsGuessExt(String? encoding, Uint8List data) {
    final enc = (encoding ?? '').trim();
    if (enc.isEmpty) {
      return _detectMagicExt(data);
    }
    final encLower = enc.toLowerCase();
    const map = {
      'jpeg': 'jpg',
      'jpg': 'jpg',
      'pil': 'png',
      'png': 'png',
      'tiff': 'tiff',
      'str': 'txt',
      'str_int': 'txt',
      'str_float': 'txt',
      'str_decimal': 'txt',
      'int': 'txt',
      'int8': 'txt',
      'int16': 'txt',
      'int32': 'txt',
      'int64': 'txt',
      'uint8': 'txt',
      'uint16': 'txt',
      'uint32': 'txt',
      'uint64': 'txt',
      'float16': 'txt',
      'float32': 'txt',
      'float64': 'txt',
      'json': 'json',
      'bytes': 'bin',
      'pkl': 'pkl',
    };
    if (map.containsKey(encLower)) {
      final ext = map[encLower]!;
      if (ext == 'bin') return _detectMagicExt(data) ?? 'bin';
      return ext;
    }
    if (encLower == 'audio') {
      return _detectMagicExt(data) ?? 'wav';
    }
    if (encLower.contains(':')) {
      final subtype = encLower.split(':')[1].trim().replaceAll('.', '');
      if (subtype.isNotEmpty) return subtype;
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
    return null;
  }

  int _readLeU32(List<int> bytes) {
    if (bytes.length < 4) throw const FormatException('Malformed shard');
    final data = ByteData.sublistView(Uint8List.fromList(bytes));
    return data.getUint32(0, Endian.little);
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
}

class _FieldBytes {
  _FieldBytes({
    required this.bytes,
    required this.size,
  });

  final Uint8List bytes;
  final int size;
}

class _ProcessInputPipe {
  _ProcessInputPipe({
    required this.done,
    required this.stop,
  });

  final Future<void> done;
  final Future<void> Function() stop;
}

class _ByteRangeCapture {
  _ByteRangeCapture(this.start, this.end)
      : bytes = Uint8List(math.max(0, end - start));

  final int start;
  final int end;
  final Uint8List bytes;
  int _written = 0;

  bool get complete => _written >= bytes.length;

  void capture(Uint8List chunk, int chunkStart) {
    if (complete || bytes.isEmpty || chunk.isEmpty) return;
    final chunkEnd = chunkStart + chunk.length;
    final overlapStart = math.max(start, chunkStart);
    final overlapEnd = math.min(end, chunkEnd);
    if (overlapEnd <= overlapStart) return;
    final srcStart = overlapStart - chunkStart;
    final dstStart = overlapStart - start;
    final length = overlapEnd - overlapStart;
    bytes.setRange(dstStart, dstStart + length, chunk, srcStart);
    final newWritten = dstStart + length;
    if (newWritten > _written) _written = newWritten;
  }
}

class _MdsIndexFile {
  _MdsIndexFile({required this.shards});

  final List<_MdsShard> shards;

  factory _MdsIndexFile.fromJson(Map<String, dynamic> json) {
    final shardsJson = json['shards'] as List<dynamic>? ?? [];
    final shards = shardsJson
        .map((e) => _MdsShard.fromJson(e as Map<String, dynamic>))
        .toList();
    return _MdsIndexFile(shards: shards);
  }
}

class _MdsShard {
  _MdsShard({
    required this.columnEncodings,
    required this.columnNames,
    required this.columnSizes,
    required this.compression,
    required this.format,
    required this.hashes,
    required this.rawData,
    required this.samples,
    required this.sizeLimit,
    required this.version,
    required this.zipData,
  });

  final List<String> columnEncodings;
  final List<String> columnNames;
  final List<int?> columnSizes;
  final String? compression;
  final String format;
  final Map<String, String> hashes;
  final _FileInfo rawData;
  final int samples;
  final int? sizeLimit;
  final int version;
  final _FileInfo? zipData;

  factory _MdsShard.fromJson(Map<String, dynamic> json) {
    final rawData =
        _FileInfo.fromJson(json['raw_data'] as Map<String, dynamic>);
    final zipData = json['zip_data'] == null
        ? null
        : _FileInfo.fromJson(json['zip_data'] as Map<String, dynamic>);
    return _MdsShard(
      columnEncodings: (json['column_encodings'] as List<dynamic>? ?? [])
          .map((e) => e.toString())
          .toList(),
      columnNames: (json['column_names'] as List<dynamic>? ?? [])
          .map((e) => e.toString())
          .toList(),
      columnSizes: (json['column_sizes'] as List<dynamic>? ?? [])
          .map((e) => e == null ? null : (e as num).toInt())
          .toList(),
      compression: json['compression'] as String?,
      format: json['format'] as String? ?? '',
      hashes: _parseHashes(json['hashes'], rawData, zipData),
      rawData: rawData,
      samples: (json['samples'] as num).toInt(),
      sizeLimit: (json['size_limit'] as num?)?.toInt(),
      version: (json['version'] as num).toInt(),
      zipData: zipData,
    );
  }

  static Map<String, String> _parseHashes(
    dynamic value,
    _FileInfo rawData,
    _FileInfo? zipData,
  ) {
    if (value is Map<String, dynamic>) {
      return value.map((k, v) => MapEntry(k, v.toString()));
    }
    if (value is List<dynamic>) {
      final out = <String, String>{};
      for (final algo in value) {
        final key = algo.toString();
        if (key.isEmpty) continue;
        out[key] = rawData.hashes[key] ?? zipData?.hashes[key] ?? '';
      }
      return out;
    }
    return <String, String>{};
  }
}

class _FileInfo {
  _FileInfo({
    required this.basename,
    required this.bytes,
    required this.hashes,
  });

  final String basename;
  final int bytes;
  final Map<String, String> hashes;

  factory _FileInfo.fromJson(Map<String, dynamic> json) {
    return _FileInfo(
      basename: json['basename'] as String? ?? '',
      bytes: (json['bytes'] as num?)?.toInt() ?? 0,
      hashes: (json['hashes'] as Map<String, dynamic>? ?? {})
          .map((k, v) => MapEntry(k, v.toString())),
    );
  }
}
