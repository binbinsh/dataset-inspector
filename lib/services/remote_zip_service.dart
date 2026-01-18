import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:http/http.dart' as http;

import '../models/common.dart';
import '../utils/preview.dart';

const remoteZipPreviewMaxCompressedBytes = 8 * 1024 * 1024;
const remoteZipInlineMaxBytes = 128 * 1024 * 1024;

const _zipTailInitialBytes = 1024 * 1024;
const _zipTailMaxBytes = 8 * 1024 * 1024;
const _zipMaxCentralDirBytes = 64 * 1024 * 1024;

class RemoteZipEntrySummary {
  const RemoteZipEntrySummary({
    required this.name,
    required this.method,
    required this.compressedSize,
    required this.uncompressedSize,
    required this.isDir,
  });

  final String name;
  final int method;
  final int compressedSize;
  final int uncompressedSize;
  final bool isDir;
}

class RemoteZipService {
  RemoteZipService({
    http.Client? client,
    required bool Function(Uri url) allowUrl,
    String? userAgent,
    int previewMaxCompressedBytes = remoteZipPreviewMaxCompressedBytes,
    int inlineMaxCompressedBytes = remoteZipInlineMaxBytes,
  })  : _client = client ?? http.Client(),
        _allowUrl = allowUrl,
        _userAgent = userAgent ?? 'dataset-inspector/2.3.1 (flutter)',
        _previewMaxCompressedBytes = previewMaxCompressedBytes,
        _inlineMaxCompressedBytes = inlineMaxCompressedBytes;

  final http.Client _client;
  final bool Function(Uri url) _allowUrl;
  final String _userAgent;
  final int _previewMaxCompressedBytes;
  final int _inlineMaxCompressedBytes;
  final Map<String, _ZipIndex> _cache = {};

  Future<List<RemoteZipEntrySummary>> listEntries({
    required String contentUrl,
    required String filename,
  }) async {
    final name = filename.trim();
    if (name.isEmpty) throw const FormatException('Missing filename.');
    if (!_looksLikeZip(name)) throw const FormatException('Selected file is not a ZIP archive.');

    final index = await _getIndex(contentUrl);
    return index.entries
        .map((entry) => RemoteZipEntrySummary(
              name: entry.name,
              method: entry.method,
              compressedSize: entry.compressedSize,
              uncompressedSize: entry.uncompressedSize,
              isDir: entry.isDir,
            ))
        .toList();
  }

  Future<FieldPreview> peekEntry({
    required String contentUrl,
    required String entryName,
    required int previewBytes,
  }) async {
    final index = await _getIndex(contentUrl);
    final entry = index.findEntry(entryName);
    final url = _parseContentUrl(contentUrl);
    final data = await _readZipEntryPreviewBytes(url, entry, previewBytes);

    final previewText = previewUtf8Text(data);
    final guessedExt = _extFromFilename(entry.name) ?? _inferBasicExt(data);

    return FieldPreview(
      previewText: previewText,
      hexSnippet: hexSnippet(data),
      guessedExt: guessedExt,
      isBinary: previewText == null,
      size: entry.uncompressedSize.clamp(0, 0xFFFFFFFF).toInt(),
    );
  }

  Future<Uint8List> readEntryBytes({
    required String contentUrl,
    required String entryName,
    required int limitBytes,
  }) async {
    final index = await _getIndex(contentUrl);
    final entry = index.findEntry(entryName);
    final url = _parseContentUrl(contentUrl);
    return _readZipEntryBytes(url, entry, limitBytes);
  }

  Future<_ZipIndex> _getIndex(String contentUrl) async {
    final trimmed = contentUrl.trim();
    if (trimmed.isEmpty) throw const FormatException('Missing content URL.');
    final existing = _cache[trimmed];
    if (existing != null) return existing;
    final url = _parseContentUrl(trimmed);
    final index = await _buildZipIndex(url);
    _cache[trimmed] = index;
    return index;
  }

  Uri _parseContentUrl(String contentUrl) {
    final trimmed = contentUrl.trim();
    if (trimmed.isEmpty) throw const FormatException('Missing content URL.');
    final url = Uri.tryParse(trimmed);
    if (url == null) throw const FormatException('Invalid content URL.');
    if (!_allowUrl(url)) {
      throw const FormatException('Blocked content URL.');
    }
    return url;
  }

  String? _extFromFilename(String name) {
    final trimmed = name.trim();
    final base = trimmed.split('/').last;
    if (!base.contains('.')) return null;
    final ext = base.split('.').last.trim().toLowerCase();
    return ext.isEmpty ? null : ext;
  }

  bool _looksLikeZip(String filename) {
    return _extFromFilename(filename) == 'zip';
  }

  String? _inferBasicExt(Uint8List data) {
    if (data.length >= 8 &&
        data[0] == 0x89 &&
        data[1] == 0x50 &&
        data[2] == 0x4e &&
        data[3] == 0x47) {
      return 'png';
    }
    if (data.length >= 3 && data[0] == 0xff && data[1] == 0xd8 && data[2] == 0xff) {
      return 'jpg';
    }
    if (data.length >= 6 && data[0] == 0x47 && data[1] == 0x49 && data[2] == 0x46) {
      return 'gif';
    }
    return null;
  }

  Future<Uint8List> _readZipEntryPreviewBytes(
    Uri url,
    _ZipEntryIndex entry,
    int previewBytes,
  ) async {
    if (previewBytes <= 0) return Uint8List(0);
    if (entry.isDir) throw const FormatException('ZIP entry is a directory.');
    if ((entry.flags & 1) == 1) {
      throw const FormatException('Encrypted ZIP entries are not supported.');
    }
    final localHeader = await _rangeRequest(url, entry.localHeaderOffset, entry.localHeaderOffset + 64);
    final dataOffset = _localHeaderDataOffset(localHeader.$1);
    final dataStart = entry.localHeaderOffset + dataOffset;

    if (entry.compressedSize == 0) return Uint8List(0);

    if (entry.method == 0) {
      final end = dataStart + entry.compressedSize - 1;
      final wantEnd = (dataStart + previewBytes - 1).clamp(dataStart, end).toInt();
      final data = await _rangeRequest(url, dataStart, wantEnd);
      return data.$1;
    }

    if (entry.method != 8) {
      throw FormatException('Unsupported ZIP compression method: ${entry.method}');
    }

    if (entry.compressedSize > _previewMaxCompressedBytes) {
      return Uint8List(0);
    }

    try {
      final data = await _rangeRequest(
        url,
        dataStart,
        dataStart + entry.compressedSize - 1,
      );
      return _inflateDeflatePreview(data.$1, previewBytes);
    } on FormatException {
      return Uint8List(0);
    }
  }

  Future<Uint8List> _readZipEntryBytes(
    Uri url,
    _ZipEntryIndex entry,
    int limit,
  ) async {
    if (entry.isDir) throw const FormatException('ZIP entry is a directory.');
    if ((entry.flags & 1) == 1) {
      throw const FormatException('Encrypted ZIP entries are not supported.');
    }
    if (entry.uncompressedSize > limit) {
      throw FormatException('ZIP entry expanded beyond the limit (${entry.uncompressedSize} bytes).');
    }
    final localHeader = await _rangeRequest(url, entry.localHeaderOffset, entry.localHeaderOffset + 64);
    final dataOffset = _localHeaderDataOffset(localHeader.$1);
    final dataStart = entry.localHeaderOffset + dataOffset;

    if (entry.method == 0) {
      final data = await _rangeRequest(url, dataStart, dataStart + entry.compressedSize - 1);
      return data.$1;
    }
    if (entry.method != 8) {
      throw FormatException('Unsupported ZIP compression method: ${entry.method}');
    }

    if (entry.compressedSize > _inlineMaxCompressedBytes) {
      throw const FormatException('ZIP entry is too large to open.');
    }

    final data = await _rangeRequest(url, dataStart, dataStart + entry.compressedSize - 1);
    return _inflateDeflateWithLimit(data.$1, limit);
  }

  int _localHeaderDataOffset(Uint8List localHeader) {
    if (localHeader.length < 30 || _readU32Le(localHeader, 0) != 0x04034b50) {
      throw const FormatException('Invalid ZIP local header.');
    }
    final nameLen = _readU16Le(localHeader, 26);
    final extraLen = _readU16Le(localHeader, 28);
    return 30 + nameLen + extraLen;
  }

  Uint8List _inflateDeflateWithLimit(Uint8List compressed, int limit) {
    final decoded = ZLibCodec(raw: true).decode(compressed);
    if (decoded.length > limit) {
      throw const FormatException('ZIP entry expanded beyond the limit.');
    }
    return Uint8List.fromList(decoded);
  }

  Future<Uint8List> _inflateDeflatePreview(Uint8List compressed, int limit) async {
    if (limit <= 0) return Uint8List(0);
    final output = BytesBuilder();
    final completer = Completer<Uint8List>();
    late final StreamSubscription<List<int>> sub;
    void finish([Object? error, StackTrace? stack]) {
      if (completer.isCompleted) return;
      if (error != null) {
        completer.completeError(error, stack);
        return;
      }
      completer.complete(output.toBytes());
    }

    final controller = StreamController<List<int>>();
    sub = ZLibDecoder(raw: true).bind(controller.stream).listen(
      (chunk) {
        if (output.length >= limit) return;
        final remaining = limit - output.length;
        if (chunk.length <= remaining) {
          output.add(chunk);
        } else {
          output.add(chunk.sublist(0, remaining));
        }
        if (output.length >= limit) {
          finish();
          sub.cancel();
        }
      },
      onError: (err, stack) => finish(err, stack),
      onDone: () => finish(),
    );

    controller.add(compressed);
    await controller.close();
    return completer.future;
  }

  Future<_ZipIndex> _buildZipIndex(Uri url) async {
    final cd = await _readZipCentralDirectoryInfo(url);
    if (cd.centralDirSize == 0 || cd.centralDirSize > _zipMaxCentralDirBytes) {
      throw const FormatException('ZIP central directory is too large to parse.');
    }
    final end = cd.centralDirOffset + cd.centralDirSize - 1;
    final response = await _rangeRequest(url, cd.centralDirOffset, end);
    final entries = _parseCentralDirectoryEntries(response.$1, cd.totalEntries);
    return _ZipIndex(entries);
  }

  Future<_ZipCentralDirectory> _readZipCentralDirectoryInfo(Uri url) async {
    var tailLen = _zipTailInitialBytes;
    Uint8List tail = Uint8List(0);
    int tailStart = 0;
    int eocdRel = 0;
    while (true) {
      final result = await _suffixRangeRequest(url, tailLen);
      tail = result.$1;
      tailStart = result.$2;
      final found = _findZipEocd(tail);
      if (found != null) {
        eocdRel = found;
        break;
      }
      if (tailLen >= _zipTailMaxBytes) {
        throw const FormatException('Unable to locate ZIP EOCD (tail too small).');
      }
      tailLen = (tailLen * 2).clamp(_zipTailInitialBytes, _zipTailMaxBytes).toInt();
    }

    final eocdAbs = tailStart + eocdRel;
    final sig = _readU32Le(tail, eocdRel);
    if (sig != 0x06054b50) {
      throw const FormatException('Invalid ZIP EOCD signature.');
    }
    final entriesU16 = _readU16Le(tail, eocdRel + 10);
    final cdSizeU32 = _readU32Le(tail, eocdRel + 12);
    final cdOffsetU32 = _readU32Le(tail, eocdRel + 16);

    final needsZip64 = entriesU16 == 0xFFFF || cdSizeU32 == 0xFFFFFFFF || cdOffsetU32 == 0xFFFFFFFF;
    if (!needsZip64) {
      return _ZipCentralDirectory(
        totalEntries: entriesU16,
        centralDirSize: cdSizeU32,
        centralDirOffset: cdOffsetU32,
      );
    }

    if (eocdAbs < 20) {
      throw const FormatException('ZIP64 locator is out of bounds.');
    }
    final locatorStart = eocdAbs - 20;
    final locator = await _rangeRequest(url, locatorStart, eocdAbs - 1);
    if (locator.$1.length < 20 || _readU32Le(locator.$1, 0) != 0x07064b50) {
      throw const FormatException('Missing ZIP64 locator.');
    }
    final zip64Offset = _readU64Le(locator.$1, 8);
    final zip64 = await _rangeRequest(url, zip64Offset, zip64Offset + 55);
    if (zip64.$1.length < 56 || _readU32Le(zip64.$1, 0) != 0x06064b50) {
      throw const FormatException('Missing ZIP64 EOCD record.');
    }
    final totalEntries = _readU64Le(zip64.$1, 32);
    final centralDirSize = _readU64Le(zip64.$1, 40);
    final centralDirOffset = _readU64Le(zip64.$1, 48);

    return _ZipCentralDirectory(
      totalEntries: totalEntries,
      centralDirSize: centralDirSize,
      centralDirOffset: centralDirOffset,
    );
  }

  int? _findZipEocd(Uint8List buf) {
    if (buf.length < 22) return null;
    final start = buf.length - 22 - 65535;
    final begin = start < 0 ? 0 : start;
    for (var i = buf.length - 22; i >= begin; i -= 1) {
      if (_readU32Le(buf, i) != 0x06054b50) continue;
      final commentLen = _readU16Le(buf, i + 20);
      if (i + 22 + commentLen == buf.length) {
        return i;
      }
    }
    return null;
  }

  List<_ZipEntryIndex> _parseCentralDirectoryEntries(Uint8List buf, int maxEntriesHint) {
    final entries = <_ZipEntryIndex>[];
    var pos = 0;
    while (pos + 46 <= buf.length) {
      final sig = _readU32Le(buf, pos);
      if (sig != 0x02014b50) break;
      final flags = _readU16Le(buf, pos + 8);
      final method = _readU16Le(buf, pos + 10);
      final compressedSizeU32 = _readU32Le(buf, pos + 20);
      final uncompressedSizeU32 = _readU32Le(buf, pos + 24);
      final nameLen = _readU16Le(buf, pos + 28);
      final extraLen = _readU16Le(buf, pos + 30);
      final commentLen = _readU16Le(buf, pos + 32);
      final localHeaderOffsetU32 = _readU32Le(buf, pos + 42);
      final headerEnd = pos + 46;
      final nameStart = headerEnd;
      final nameEnd = nameStart + nameLen;
      final extraStart = nameEnd;
      final extraEnd = extraStart + extraLen;
      final commentEnd = extraEnd + commentLen;
      final nameBytes = buf.sublist(nameStart, nameEnd);
      final extraBytes = extraStart < buf.length ? buf.sublist(extraStart, extraEnd) : Uint8List(0);
      final name = utf8.decode(nameBytes, allowMalformed: true);
      final isDir = name.endsWith('/');

      final needZip64Uncompressed = uncompressedSizeU32 == 0xFFFFFFFF;
      final needZip64Compressed = compressedSizeU32 == 0xFFFFFFFF;
      final needZip64LocalOffset = localHeaderOffsetU32 == 0xFFFFFFFF;
      final zip64 = _parseZip64Extra(extraBytes, needZip64Uncompressed, needZip64Compressed, needZip64LocalOffset);

      final compressedSize = zip64.$2 ?? compressedSizeU32;
      final uncompressedSize = zip64.$1 ?? uncompressedSizeU32;
      final localHeaderOffset = zip64.$3 ?? localHeaderOffsetU32;

      entries.add(_ZipEntryIndex(
        name: name,
        method: method,
        flags: flags,
        compressedSize: compressedSize,
        uncompressedSize: uncompressedSize,
        localHeaderOffset: localHeaderOffset,
        isDir: isDir,
      ));

      if (maxEntriesHint > 0 && entries.length >= maxEntriesHint) {
        // Keep parsing until buffer end or invalid signature.
      }
      pos = commentEnd;
    }
    return entries;
  }

  (int?, int?, int?) _parseZip64Extra(
    Uint8List extra,
    bool needUncompressed,
    bool needCompressed,
    bool needLocalOffset,
  ) {
    var pos = 0;
    while (pos + 4 <= extra.length) {
      final headerId = _readU16Le(extra, pos);
      final dataSize = _readU16Le(extra, pos + 2);
      pos += 4;
      if (pos + dataSize > extra.length) break;
      if (headerId == 0x0001) {
        var cursor = pos;
        int? uncompressed;
        int? compressed;
        int? localOffset;
        if (needUncompressed) {
          uncompressed = _readU64Le(extra, cursor);
          cursor += 8;
        }
        if (needCompressed) {
          compressed = _readU64Le(extra, cursor);
          cursor += 8;
        }
        if (needLocalOffset) {
          localOffset = _readU64Le(extra, cursor);
        }
        return (uncompressed, compressed, localOffset);
      }
      pos += dataSize;
    }
    return (null, null, null);
  }

  int _readU16Le(Uint8List input, int offset) {
    return input[offset] | (input[offset + 1] << 8);
  }

  int _readU32Le(Uint8List input, int offset) {
    return input[offset] |
        (input[offset + 1] << 8) |
        (input[offset + 2] << 16) |
        (input[offset + 3] << 24);
  }

  int _readU64Le(Uint8List input, int offset) {
    final low = _readU32Le(input, offset);
    final high = _readU32Le(input, offset + 4);
    return (high << 32) | low;
  }

  Future<(Uint8List, int?)> _rangeRequest(
    Uri url,
    int start,
    int endInclusive,
  ) async {
    final response = await _client.get(
      url,
      headers: {HttpHeaders.rangeHeader: 'bytes=$start-$endInclusive', HttpHeaders.userAgentHeader: _userAgent},
    );
    if (response.statusCode != 200 && response.statusCode != 206) {
      throw Exception('HTTP ${response.statusCode} from $url');
    }
    final total = _parseContentRangeTotal(response.headers['content-range']);
    return (response.bodyBytes, total);
  }

  Future<(Uint8List, int, int)> _suffixRangeRequest(
    Uri url,
    int suffixLen,
  ) async {
    final response = await _client.get(
      url,
      headers: {HttpHeaders.rangeHeader: 'bytes=-$suffixLen', HttpHeaders.userAgentHeader: _userAgent},
    );
    if (response.statusCode != 200 && response.statusCode != 206) {
      throw Exception('HTTP ${response.statusCode} from $url');
    }
    final contentRange = response.headers['content-range'];
    if (contentRange == null) {
      throw Exception('Missing Content-Range from $url');
    }
    final range = _parseContentRange(contentRange);
    if (range == null) throw Exception('Missing Content-Range from $url');
    return (response.bodyBytes, range.$1, range.$3);
  }

  int? _parseContentRangeTotal(String? value) {
    if (value == null) return null;
    final parts = value.split('/');
    if (parts.length != 2) return null;
    if (parts[1] == '*') return null;
    return int.tryParse(parts[1]);
  }

  (int, int, int)? _parseContentRange(String value) {
    final trimmed = value.trim();
    if (!trimmed.startsWith('bytes ')) return null;
    final parts = trimmed.substring(6).split('/');
    if (parts.length != 2) return null;
    final total = int.tryParse(parts[1]);
    if (total == null) return null;
    final range = parts[0].split('-');
    if (range.length != 2) return null;
    final start = int.tryParse(range[0]);
    final end = int.tryParse(range[1]);
    if (start == null || end == null) return null;
    return (start, end, total);
  }
}

class _ZipIndex {
  _ZipIndex(this.entries);

  final List<_ZipEntryIndex> entries;

  _ZipEntryIndex findEntry(String entryName) {
    final trimmed = entryName.trim();
    if (trimmed.isEmpty) throw const FormatException('Missing ZIP entry name.');
    return entries.firstWhere(
      (entry) => entry.name == trimmed,
      orElse: () => throw FormatException("Entry '$trimmed' not found in ZIP."),
    );
  }
}

class _ZipEntryIndex {
  _ZipEntryIndex({
    required this.name,
    required this.method,
    required this.flags,
    required this.compressedSize,
    required this.uncompressedSize,
    required this.localHeaderOffset,
    required this.isDir,
  });

  final String name;
  final int method;
  final int flags;
  final int compressedSize;
  final int uncompressedSize;
  final int localHeaderOffset;
  final bool isDir;
}

class _ZipCentralDirectory {
  _ZipCentralDirectory({
    required this.totalEntries,
    required this.centralDirSize,
    required this.centralDirOffset,
  });

  final int totalEntries;
  final int centralDirSize;
  final int centralDirOffset;
}
