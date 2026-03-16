import 'dart:async';
import 'dart:typed_data';

const _tarBlockSize = 512;
const _maxTarMetaBytes = 1024 * 1024;

class TarEntryMeta {
  TarEntryMeta({
    required this.path,
    required this.size,
    required this.isDir,
    required this.dataOffset,
  });

  final String path;
  final int size;
  final bool isDir;
  final int dataOffset;
}

class TarEntry {
  TarEntry({
    required this.meta,
    required this.bytes,
  });

  final TarEntryMeta meta;
  final Uint8List? bytes;
}

class TarStreamReader {
  TarStreamReader(this._reader);

  final ByteReader _reader;
  String? _pendingLongName;
  String? _pendingPaxPath;

  Future<TarEntry?> next({int? readBytes}) async {
    return nextWithBytes((_) => readBytes);
  }

  Future<TarEntry?> nextWithBytes(int? Function(TarEntryMeta meta) chooser) async {
    while (true) {
      final header = await _reader.readExact(_tarBlockSize);
      if (header == null) return null;
      if (_isZeroBlock(header)) {
        final next = await _reader.readExact(_tarBlockSize);
        if (next == null || _isZeroBlock(next)) {
          return null;
        }
        final entry = await _processHeader(next, chooser);
        if (entry != null) return entry;
        continue;
      }
      final entry = await _processHeader(header, chooser);
      if (entry != null) return entry;
    }
  }

  Future<TarEntry?> _processHeader(
    Uint8List header,
    int? Function(TarEntryMeta meta) chooser,
  ) async {
    final size = _parseTarSize(header) ?? 0;
    final typeflag = header[156];

    if (typeflag == 0x4c) {
      if (size > _maxTarMetaBytes) {
        throw const FormatException('tar longname entry is too large');
      }
      final data = await _reader.readExact(size);
      if (data == null) return null;
      _pendingLongName = _parseTarString(data);
      await _reader.skip(_tarPadding(size));
      return null;
    }

    if (typeflag == 0x78 || typeflag == 0x67) {
      if (size > _maxTarMetaBytes) {
        throw const FormatException('tar pax entry is too large');
      }
      final data = await _reader.readExact(size);
      if (data == null) return null;
      final path = _parsePaxPath(data);
      if (path != null) {
        _pendingPaxPath = path;
      }
      await _reader.skip(_tarPadding(size));
      return null;
    }

    var path = _pendingLongName ?? _parseUstarPath(header);
    _pendingLongName = null;
    if (_pendingPaxPath != null) {
      path = _pendingPaxPath!;
      _pendingPaxPath = null;
    }

    final normalized = normalizeTarPath(path);
    final isDir = typeflag == 0x35;
    if (normalized.isEmpty) {
      await _reader.skip(size + _tarPadding(size));
      return null;
    }

    final dataOffset = _reader.position;
    final meta = TarEntryMeta(path: normalized, size: size, isDir: isDir, dataOffset: dataOffset);
    final readBytes = chooser(meta);

    Uint8List? data;
    if (readBytes != null && readBytes > 0 && !isDir && size > 0) {
      final want = size < readBytes ? size : readBytes;
      data = await _reader.readExact(want);
      if (data == null) return null;
      final remaining = size - data.length;
      if (remaining > 0) {
        await _reader.skip(remaining);
      }
    } else {
      await _reader.skip(size);
    }
    await _reader.skip(_tarPadding(size));

    return TarEntry(meta: meta, bytes: data);
  }

  int _tarPadding(int size) {
    final pad = (_tarBlockSize - (size % _tarBlockSize)) % _tarBlockSize;
    return pad;
  }

  bool _isZeroBlock(Uint8List block) {
    for (final b in block) {
      if (b != 0) return false;
    }
    return true;
  }
}

class ByteReader {
  ByteReader(Stream<List<int>> stream) : _iterator = StreamIterator(stream);

  final StreamIterator<List<int>> _iterator;
  final List<int> _buffer = [];
  int _position = 0;

  int get position => _position;

  Future<Uint8List?> readExact(int size) async {
    if (size == 0) return Uint8List(0);
    final ok = await _fill(size);
    if (!ok) return null;
    final out = Uint8List.fromList(_buffer.sublist(0, size));
    _buffer.removeRange(0, size);
    _position += size;
    return out;
  }

  Future<void> skip(int size) async {
    var remaining = size;
    while (remaining > 0) {
      if (_buffer.isNotEmpty) {
        final take = remaining < _buffer.length ? remaining : _buffer.length;
        _buffer.removeRange(0, take);
        _position += take;
        remaining -= take;
        continue;
      }
      if (!await _iterator.moveNext()) return;
      final chunk = _iterator.current;
      if (chunk.length <= remaining) {
        remaining -= chunk.length;
        _position += chunk.length;
      } else {
        _buffer.addAll(chunk.sublist(remaining));
        _position += remaining;
        remaining = 0;
      }
    }
  }

  Future<bool> _fill(int size) async {
    while (_buffer.length < size) {
      if (!await _iterator.moveNext()) return false;
      _buffer.addAll(_iterator.current);
    }
    return true;
  }
}

int? _parseTarSize(Uint8List header) {
  return _parseTarOctal(header.sublist(124, 136));
}

int? _parseTarOctal(Uint8List slice) {
  final cleaned = <int>[];
  for (final b in slice) {
    if (b == 0 || b == 0x20 || b == 0x0a || b == 0x0d || b == 0x09) {
      continue;
    }
    cleaned.add(b);
  }
  if (cleaned.isEmpty) return 0;
  final s = String.fromCharCodes(cleaned).trim();
  if (s.isEmpty) return 0;
  return int.tryParse(s, radix: 8);
}

String _parseTarString(Uint8List data) {
  final trimmed = <int>[];
  for (final b in data) {
    if (b == 0) break;
    trimmed.add(b);
  }
  return String.fromCharCodes(trimmed).trim().replaceAll(RegExp(r'[\r\n]+$'), '');
}

String? _parsePaxPath(Uint8List data) {
  final s = String.fromCharCodes(data);
  for (final line in s.split(RegExp(r'[\r\n]+'))) {
    final spaceIdx = line.indexOf(' ');
    if (spaceIdx <= 0) continue;
    final rest = line.substring(spaceIdx + 1);
    final eqIdx = rest.indexOf('=');
    if (eqIdx <= 0) continue;
    final key = rest.substring(0, eqIdx);
    final value = rest
        .substring(eqIdx + 1)
        .trim()
        .replaceAll(String.fromCharCode(0), '');
    if (key == 'path' && value.isNotEmpty) return value;
  }
  return null;
}

String _parseUstarPath(Uint8List header) {
  final name = _parseTarString(header.sublist(0, 100));
  final prefix = _parseTarString(header.sublist(345, 500));
  if (prefix.isEmpty) return name;
  if (name.isEmpty) return prefix;
  return '$prefix/$name';
}

String normalizeTarPath(String path) {
  var normalized = path.trim();
  if (normalized.startsWith('./')) {
    normalized = normalized.substring(2);
  }
  if (normalized.startsWith('/')) {
    normalized = normalized.substring(1);
  }
  normalized = normalized.replaceAll('\\', '/');
  return normalized;
}

ByteReaderHandle openTarStreamReader(Stream<List<int>> stream) {
  final reader = ByteReader(stream);
  return ByteReaderHandle(reader);
}

class ByteReaderHandle {
  ByteReaderHandle(this.reader);

  final ByteReader reader;
}

TarStreamReader createTarStream(ByteReaderHandle handle) {
  return TarStreamReader(handle.reader);
}
