import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';

import '../models/common.dart';
import '../utils/audio.dart';
import '../utils/preview.dart';
import '../utils/zstd.dart';
import 'open_with_service.dart';

const _previewBytes = 16 * 1024;
const _maxListedSamples = 5000;
const _maxOpenBytes = 256 * 1024 * 1024;

class MosaicmlService {
  MosaicmlService({OpenWithService? openWith}) : _openWith = openWith ?? OpenWithService();

  final OpenWithService _openWith;

  Future<IndexSummary> loadIndex(String indexPath) async {
    final (rootDir, resolved, index) = await _parseIndex(File(indexPath));
    final first = index.shards.isNotEmpty ? index.shards.first : null;
    if (first == null) {
      throw const FormatException('index.json contains no shards');
    }
    if (first.version != 2) {
      throw FormatException('unsupported MDS version: ${first.version} (expected 2)');
    }
    if (first.format.toLowerCase() != 'mds') {
      throw FormatException('unsupported dataset format: ${first.format} (expected mds)');
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
    final (rootDir, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final rawPath = await _resolveRawShardPath(rootDir, shard);

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
      final limit = total < _maxListedSamples ? total : _maxListedSamples;

      final items = <ItemMeta>[];
      for (var idx = 0; idx < limit; idx += 1) {
        final (begin, end) = await _readSampleOffsets(fp, idx);
        final sizes = await _readVariableSizes(fp, begin, shard);
        final fields = List.generate(sizes.length, (fieldIndex) {
          return FieldMeta(fieldIndex: fieldIndex, size: sizes[fieldIndex]);
        });
        items.add(ItemMeta(
          itemIndex: idx,
          totalBytes: end - begin,
          fields: fields,
        ));
      }
      return items;
    } finally {
      await fp.close();
    }
  }

  Future<FieldPreview> peekField({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final (rootDir, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final rawPath = await _resolveRawShardPath(rootDir, shard);
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;

    final fp = await rawPath.open();
    try {
      final (begin, end) = await _readSampleOffsets(fp, itemIndex);
      final sizes = await _readVariableSizes(fp, begin, shard);
      final (fieldStart, fieldSize) = _fieldStartOffset(begin, shard, fieldIndex, sizes);
      final available = end - fieldStart;
      if (available < fieldSize) {
        throw const FormatException('Malformed shard');
      }

      final shouldReadFull = _scalarEncoding(encoding);
      final desired = shouldReadFull ? fieldSize : fieldSize.clamp(0, _previewBytes).toInt();
      await fp.setPosition(fieldStart);
      final data = await fp.read(desired);

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
    } finally {
      await fp.close();
    }
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
    final rawPath = await _resolveRawShardPath(rootDir, shard);
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;

    final fp = await rawPath.open();
    try {
      final (data, size) = await _readFieldFull(fp, shard, itemIndex, fieldIndex);
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
    } finally {
      await fp.close();
    }
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

  Future<PreparedFileResponse> _prepareFieldFile({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required bool convertSphereToWav,
  }) async {
    final (rootDir, _, index) = await _parseIndex(File(indexPath));
    final shard = _shardForFilename(index, shardFilename);
    final rawPath = await _resolveRawShardPath(rootDir, shard);
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;

    final fp = await rawPath.open();
    try {
      final (data, size) = await _readFieldFull(fp, shard, itemIndex, fieldIndex);
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
    } finally {
      await fp.close();
    }
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
    final rawPath = await _resolveRawShardPath(rootDir, shard);
    final encoding = shard.columnEncodings.length > fieldIndex
        ? shard.columnEncodings[fieldIndex]
        : null;

    final fp = await rawPath.open();
    try {
      final (data, _) = await _readFieldFull(fp, shard, itemIndex, fieldIndex);
      var ext = _mdsGuessExt(encoding, data) ?? 'bin';
      var bytes = data;
      if (convertSphereToWav && ext == 'sph') {
        bytes = await decodeSphereToWavWithFallback(data);
        ext = 'wav';
      }
      return PreparedMediaResponse(bytes: bytes, size: bytes.length, ext: ext);
    } finally {
      await fp.close();
    }
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
        final kind = _compressionKind(shard.compression, shard.zipData!.basename);
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

    throw FormatException('shard data file not found for ${shard.rawData.basename}');
  }

  Future<File> _decompressZstdToTemp(File zipPath) async {
    final key = _hashKeyForPath(zipPath);
    final outDir = Directory('${Directory.systemTemp.path}/dataset-inspector/mds-cache');
    await outDir.create(recursive: true);
    final outPath = File('${outDir.path}/$key.mds');
    if (outPath.existsSync()) return outPath;
    final bytes = await zipPath.readAsBytes();
    final decoded = decodeZstd(bytes);
    await outPath.writeAsBytes(decoded, flush: true);
    return outPath;
  }

  String _hashKeyForPath(File path) {
    final stat = path.statSync();
    final payload = '${path.path}:${stat.size}:${stat.modified.millisecondsSinceEpoch}';
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

  Future<List<int>> _readVariableSizes(RandomAccessFile fp, int begin, _MdsShard shard) async {
    final sizes = <int>[];
    final varCols = shard.columnSizes.where((s) => s == null).length;
    final headerLen = varCols * 4;
    Uint8List header = Uint8List(0);
    if (headerLen > 0) {
      await fp.setPosition(begin);
      header = await fp.read(headerLen);
      if (header.length != headerLen) throw const FormatException('Malformed shard');
    }
    var varIdx = 0;
    for (final fixed in shard.columnSizes) {
      if (fixed != null) {
        sizes.add(fixed);
      } else {
        final start = varIdx * 4;
        final size = _readLeU32(header.sublist(start, start + 4));
        sizes.add(size);
        varIdx += 1;
      }
    }
    return sizes;
  }

  (int, int) _fieldStartOffset(int begin, _MdsShard shard, int fieldIndex, List<int> sizes) {
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
    if (data.length >= 3 && data[0] == 0x49 && data[1] == 0x44 && data[2] == 0x33) {
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

  Future<(Uint8List, int)> _readFieldFull(
    RandomAccessFile fp,
    _MdsShard shard,
    int itemIndex,
    int fieldIndex,
  ) async {
    final (begin, end) = await _readSampleOffsets(fp, itemIndex);
    final sizes = await _readVariableSizes(fp, begin, shard);
    final (fieldStart, fieldSize) = _fieldStartOffset(begin, shard, fieldIndex, sizes);
    final available = end - fieldStart;
    if (available < fieldSize) {
      throw const FormatException('Malformed shard');
    }
    if (fieldSize > _maxOpenBytes) {
      throw FormatException('field is too large to open ($fieldSize bytes, max $_maxOpenBytes)');
    }
    await fp.setPosition(fieldStart);
    final data = await fp.read(fieldSize);
    if (data.length != fieldSize) throw const FormatException('Malformed shard');
    return (data, fieldSize);
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
      hashes: (json['hashes'] as Map<String, dynamic>? ?? {})
          .map((k, v) => MapEntry(k, v.toString())),
      rawData: _FileInfo.fromJson(json['raw_data'] as Map<String, dynamic>),
      samples: (json['samples'] as num).toInt(),
      sizeLimit: (json['size_limit'] as num?)?.toInt(),
      version: (json['version'] as num).toInt(),
      zipData: json['zip_data'] == null
          ? null
          : _FileInfo.fromJson(json['zip_data'] as Map<String, dynamic>),
    );
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
