import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:path/path.dart' as p;

enum LhotseManifestType {
  recordings,
  supervisions,
  cuts,
}

extension LhotseManifestTypeX on LhotseManifestType {
  String get name => switch (this) {
        LhotseManifestType.recordings => 'recordings',
        LhotseManifestType.supervisions => 'supervisions',
        LhotseManifestType.cuts => 'cuts',
      };
}

class LhotseService {
  static const int defaultPageLength = 128;
  static const int maxPageLength = 2048;

  static const Set<String> _supportedManifestNames = <String>{
    'recordings',
    'supervisions',
    'cuts',
  };

  Future<bool> detectLocalSource(String input) async {
    final trimmed = input.trim();
    if (trimmed.isEmpty) return false;

    final file = File(trimmed);
    if (await file.exists()) {
      final kind = _manifestNameFromPath(file.path);
      if (kind != null) return true;
      return false;
    }

    final dir = Directory(trimmed);
    if (!await dir.exists()) return false;

    for (final name in _supportedManifestNames) {
      final plain = File(p.join(dir.path, '$name.jsonl'));
      if (await plain.exists()) return true;
      final compressed = File(p.join(dir.path, '$name.jsonl.gz'));
      if (await compressed.exists()) return true;
    }
    return false;
  }

  Future<Map<String, dynamic>> loadSource(
    String input, {
    int sampleRowsPerManifest = 16,
  }) async {
    final rootDir = await _resolveRootDirectory(input);
    final manifests = <Map<String, dynamic>>[];
    for (final type in LhotseManifestType.values) {
      final file = await _resolveManifestFile(rootDir, type.name);
      if (file == null) continue;
      final lines = await _readJsonLines(file);
      final sampleRows = lines.take(sampleRowsPerManifest).toList();
      manifests.add({
        'name': type.name,
        'path': file.path,
        'compressed': _isGzipPath(file.path),
        'rows': lines.length,
        'sample_rows': sampleRows,
      });
    }

    return {
      'format': 'lhotse-jsonl',
      'root_dir': rootDir.path,
      'manifests': manifests,
      'manifest_count': manifests.length,
    };
  }

  Future<Map<String, dynamic>> listEntries({
    required String input,
    required String manifest,
    int offset = 0,
    int length = defaultPageLength,
  }) async {
    _validatePage(offset: offset, length: length);
    final manifestName = _normalizeManifestName(manifest);
    final rootDir = await _resolveRootDirectory(input);
    final file = await _resolveManifestFile(rootDir, manifestName);
    if (file == null) {
      throw FormatException(
        'Lhotse manifest "$manifestName" not found under ${rootDir.path}.',
      );
    }

    final rows = await _readJsonLines(file);
    final total = rows.length;
    final start = offset.clamp(0, total);
    final end = min(start + length, total);
    final pageRows = rows.sublist(start, end);

    return {
      'format': 'lhotse-jsonl',
      'root_dir': rootDir.path,
      'manifest': manifestName,
      'path': file.path,
      'offset': start,
      'length': length,
      'total': total,
      'partial': start > 0 || end < total,
      'entries': pageRows,
    };
  }

  Future<Map<String, dynamic>> writeEntries({
    required String input,
    required String manifest,
    required List<dynamic> entries,
    bool overwrite = false,
    bool compressed = false,
  }) async {
    final manifestName = _normalizeManifestName(manifest);
    final rootDir = await _resolveRootDirectory(input, createIfMissing: true);
    final targetPath = p.join(
      rootDir.path,
      '$manifestName.jsonl${compressed ? '.gz' : ''}',
    );
    final target = File(targetPath);
    if (!overwrite && await target.exists()) {
      throw FormatException(
        'Manifest already exists: ${target.path}. Set overwrite=true to replace.',
      );
    }

    final normalized = entries
        .map((entry) => _normalizeManifestEntry(entry, manifestName))
        .toList(growable: false);
    await _writeJsonLines(
      target,
      normalized,
      compressed: compressed,
    );
    final stat = await target.stat();

    return {
      'format': 'lhotse-jsonl',
      'root_dir': rootDir.path,
      'manifest': manifestName,
      'path': target.path,
      'rows': normalized.length,
      'size': stat.size,
      'updated_at': stat.modified.toUtc().toIso8601String(),
      'compressed': compressed,
      'operation': 'write',
    };
  }

  Future<Map<String, dynamic>> appendEntries({
    required String input,
    required String manifest,
    required List<dynamic> entries,
    bool createIfMissing = true,
  }) async {
    final manifestName = _normalizeManifestName(manifest);
    final rootDir = await _resolveRootDirectory(input, createIfMissing: true);
    var target = await _resolveManifestFile(rootDir, manifestName);
    if (target == null) {
      if (!createIfMissing) {
        throw FormatException(
          'Manifest not found: $manifestName under ${rootDir.path}.',
        );
      }
      target = File(p.join(rootDir.path, '$manifestName.jsonl'));
      await _writeJsonLines(target, const <Map<String, dynamic>>[]);
    }

    final normalized = entries
        .map((entry) => _normalizeManifestEntry(entry, manifestName))
        .toList(growable: false);
    final all = await _readJsonLines(target);
    all.addAll(normalized);
    await _writeJsonLines(
      target,
      all,
      compressed: _isGzipPath(target.path),
    );
    final stat = await target.stat();

    return {
      'format': 'lhotse-jsonl',
      'root_dir': rootDir.path,
      'manifest': manifestName,
      'path': target.path,
      'appended_rows': normalized.length,
      'rows': all.length,
      'size': stat.size,
      'updated_at': stat.modified.toUtc().toIso8601String(),
      'compressed': _isGzipPath(target.path),
      'operation': 'append',
    };
  }

  List<Map<String, dynamic>> rowsToCuts(
    List<dynamic> rows, {
    String recordKeyField = 'record_key',
    String? cutPrefix,
  }) {
    final cuts = <Map<String, dynamic>>[];
    for (var index = 0; index < rows.length; index++) {
      final row = rows[index];
      final normalizedRow = row is Map<String, dynamic>
          ? Map<String, dynamic>.of(row)
          : <String, dynamic>{'value': row};
      final rowId = _stringOrNull(normalizedRow[recordKeyField]) ??
          _stringOrNull(normalizedRow['id']) ??
          '${cutPrefix ?? 'cut'}_$index';
      final start = _toNonNegativeDouble(normalizedRow['start']) ?? 0.0;
      final duration = _toNonNegativeDouble(normalizedRow['duration']) ?? 0.0;
      final channel = _toInt(normalizedRow['channel']) ?? 0;
      final recordingId = _stringOrNull(normalizedRow['recording_id']) ?? rowId;

      final cut = <String, dynamic>{
        'id': rowId,
        'start': start,
        'duration': duration,
        'channel': channel,
      };

      final recording = normalizedRow['recording'];
      if (recording is Map<String, dynamic>) {
        cut['recording'] = recording;
      } else {
        cut['recording_id'] = recordingId;
      }

      final supervisions = normalizedRow['supervisions'];
      if (supervisions is List) {
        cut['supervisions'] = supervisions
            .map((item) => item is Map<String, dynamic>
                ? Map<String, dynamic>.of(item)
                : <String, dynamic>{'value': item})
            .toList(growable: false);
      } else {
        final text = _stringOrNull(normalizedRow['text']);
        if (text != null && text.isNotEmpty) {
          cut['supervisions'] = [
            {
              'id': '${rowId}_sup0',
              'recording_id': recordingId,
              'start': start,
              'duration': duration,
              'channel': channel,
              'text': text,
            }
          ];
        }
      }

      final custom = <String, dynamic>{};
      for (final entry in normalizedRow.entries) {
        switch (entry.key) {
          case 'id':
          case 'record_key':
          case 'recording_id':
          case 'recording':
          case 'start':
          case 'duration':
          case 'channel':
          case 'supervisions':
          case 'text':
            continue;
          default:
            custom[entry.key] = entry.value;
        }
      }
      if (custom.isNotEmpty) {
        cut['custom'] = custom;
      }

      cuts.add(cut);
    }
    return cuts;
  }

  Future<Directory> _resolveRootDirectory(
    String input, {
    bool createIfMissing = false,
  }) async {
    final trimmed = input.trim();
    if (trimmed.isEmpty) {
      throw const FormatException('Lhotse input path is empty.');
    }

    final file = File(trimmed);
    if (await file.exists()) {
      return file.parent;
    }

    final dir = Directory(trimmed);
    if (await dir.exists()) {
      return dir;
    }

    if (!createIfMissing) {
      throw FormatException('Lhotse path does not exist: $trimmed');
    }
    await dir.create(recursive: true);
    return dir;
  }

  Future<File?> _resolveManifestFile(
    Directory rootDir,
    String manifestName,
  ) async {
    final normalized = _normalizeManifestName(manifestName);
    final plain = File(p.join(rootDir.path, '$normalized.jsonl'));
    if (await plain.exists()) return plain;
    final gz = File(p.join(rootDir.path, '$normalized.jsonl.gz'));
    if (await gz.exists()) return gz;
    return null;
  }

  List<Map<String, dynamic>> _normalizeEntries(
    List<dynamic> entries,
    String manifest,
  ) {
    return entries
        .map((entry) => _normalizeManifestEntry(entry, manifest))
        .toList(growable: false);
  }

  Map<String, dynamic> _normalizeManifestEntry(dynamic entry, String manifest) {
    if (entry is! Map) {
      throw const FormatException(
        'Each Lhotse manifest entry must be an object/map.',
      );
    }
    final normalized = <String, dynamic>{};
    for (final field in entry.entries) {
      final key = field.key;
      if (key is! String) {
        throw const FormatException(
            'Lhotse manifest field names must be strings.');
      }
      normalized[key] = field.value;
    }

    final id = _stringOrNull(normalized['id']);
    if (id == null || id.isEmpty) {
      throw const FormatException(
          'Lhotse manifest entry must contain non-empty "id".');
    }

    switch (manifest) {
      case 'recordings':
        _normalizeRecordingEntry(normalized);
        break;
      case 'supervisions':
        _normalizeSupervisionEntry(normalized);
        break;
      case 'cuts':
        _normalizeCutEntry(normalized);
        break;
    }
    return normalized;
  }

  void _normalizeRecordingEntry(Map<String, dynamic> entry) {
    entry['id'] = _requiredString(entry, 'id');
    final sources = entry['sources'];
    if (sources is! List || sources.isEmpty) {
      throw const FormatException(
        'Lhotse recording entry must contain non-empty "sources".',
      );
    }
    final samplingRate = _toInt(entry['sampling_rate']);
    if (samplingRate == null || samplingRate <= 0) {
      throw const FormatException(
        'Lhotse recording entry must contain positive integer "sampling_rate".',
      );
    }
    entry['sampling_rate'] = samplingRate;

    final numSamples = _toInt(entry['num_samples']);
    if (numSamples == null || numSamples < 0) {
      throw const FormatException(
        'Lhotse recording entry must contain non-negative integer "num_samples".',
      );
    }
    entry['num_samples'] = numSamples;

    final duration = _toNonNegativeDouble(entry['duration']);
    if (duration == null) {
      throw const FormatException(
        'Lhotse recording entry must contain non-negative "duration".',
      );
    }
    entry['duration'] = duration;
  }

  void _normalizeSupervisionEntry(Map<String, dynamic> entry) {
    entry['id'] = _requiredString(entry, 'id');
    entry['recording_id'] = _requiredString(entry, 'recording_id');
    final start = _toNonNegativeDouble(entry['start']);
    if (start == null) {
      throw const FormatException(
        'Lhotse supervision entry must contain non-negative "start".',
      );
    }
    entry['start'] = start;
    final duration = _toNonNegativeDouble(entry['duration']);
    if (duration == null) {
      throw const FormatException(
        'Lhotse supervision entry must contain non-negative "duration".',
      );
    }
    entry['duration'] = duration;
    if (!entry.containsKey('channel')) {
      entry['channel'] = 0;
    }
  }

  void _normalizeCutEntry(Map<String, dynamic> entry) {
    entry['id'] = _requiredString(entry, 'id');
    final start = _toNonNegativeDouble(entry['start']);
    if (start == null) {
      throw const FormatException(
        'Lhotse cut entry must contain non-negative "start".',
      );
    }
    entry['start'] = start;
    final duration = _toNonNegativeDouble(entry['duration']);
    if (duration == null) {
      throw const FormatException(
        'Lhotse cut entry must contain non-negative "duration".',
      );
    }
    entry['duration'] = duration;

    final hasRecording = entry['recording'] is Map<String, dynamic>;
    final recordingId = _stringOrNull(entry['recording_id']);
    if (!hasRecording && (recordingId == null || recordingId.isEmpty)) {
      throw const FormatException(
        'Lhotse cut entry must contain either "recording" or "recording_id".',
      );
    }
    if (!entry.containsKey('channel')) {
      entry['channel'] = 0;
    }
    if (!entry.containsKey('supervisions')) {
      entry['supervisions'] = const <dynamic>[];
    }
  }

  Future<List<Map<String, dynamic>>> _readJsonLines(File file) async {
    if (!await file.exists()) {
      return <Map<String, dynamic>>[];
    }

    final text = await _readText(file);
    if (text.trim().isEmpty) {
      return <Map<String, dynamic>>[];
    }
    final lines = const LineSplitter().convert(text);
    final rows = <Map<String, dynamic>>[];
    for (final line in lines) {
      final trimmed = line.trim();
      if (trimmed.isEmpty) continue;
      final decoded = jsonDecode(trimmed);
      if (decoded is! Map) {
        throw FormatException('Invalid JSONL object in ${file.path}.');
      }
      final row = <String, dynamic>{};
      for (final entry in decoded.entries) {
        final key = entry.key;
        if (key is! String) {
          throw FormatException('Invalid JSONL key in ${file.path}.');
        }
        row[key] = entry.value;
      }
      rows.add(row);
    }
    return rows;
  }

  Future<void> _writeJsonLines(
    File file,
    List<Map<String, dynamic>> rows, {
    bool? compressed,
  }) async {
    final shouldCompress = compressed ?? _isGzipPath(file.path);
    final parent = file.parent;
    if (!await parent.exists()) {
      await parent.create(recursive: true);
    }

    final buffer = StringBuffer();
    for (final row in rows) {
      buffer.writeln(jsonEncode(row));
    }
    final payload = utf8.encode(buffer.toString());
    if (shouldCompress) {
      await file.writeAsBytes(gzip.encode(payload), flush: true);
      return;
    }
    await file.writeAsString(buffer.toString(), flush: true);
  }

  Future<String> _readText(File file) async {
    if (_isGzipPath(file.path)) {
      final bytes = await file.readAsBytes();
      if (bytes.isEmpty) return '';
      final decompressed = gzip.decode(bytes);
      return utf8.decode(decompressed, allowMalformed: true);
    }
    return file.readAsString();
  }

  String _normalizeManifestName(String input) {
    final normalized = input.trim().toLowerCase();
    if (_supportedManifestNames.contains(normalized)) {
      return normalized;
    }
    throw FormatException(
      'Unsupported lhotse manifest "$input". Supported: recordings, supervisions, cuts.',
    );
  }

  String? _manifestNameFromPath(String path) {
    final lower = p.basename(path).toLowerCase();
    if (lower.endsWith('.jsonl.gz')) {
      final name = lower.substring(0, lower.length - '.jsonl.gz'.length);
      return _supportedManifestNames.contains(name) ? name : null;
    }
    if (lower.endsWith('.jsonl')) {
      final name = lower.substring(0, lower.length - '.jsonl'.length);
      return _supportedManifestNames.contains(name) ? name : null;
    }
    return null;
  }

  bool _isGzipPath(String path) => path.toLowerCase().endsWith('.gz');

  void _validatePage({
    required int offset,
    required int length,
  }) {
    if (offset < 0) {
      throw const FormatException('Argument "offset" must be >= 0.');
    }
    if (length <= 0) {
      throw const FormatException('Argument "length" must be > 0.');
    }
    if (length > maxPageLength) {
      throw FormatException('Argument "length" must be <= $maxPageLength.');
    }
  }

  String _requiredString(Map<String, dynamic> entry, String key) {
    final value = _stringOrNull(entry[key]);
    if (value == null || value.isEmpty) {
      throw FormatException('Missing required string field "$key".');
    }
    return value;
  }

  String? _stringOrNull(dynamic value) {
    if (value == null) return null;
    final text = value.toString().trim();
    return text.isEmpty ? null : text;
  }

  int? _toInt(dynamic value) {
    if (value is int) return value;
    if (value is num) return value.toInt();
    if (value is String) return int.tryParse(value.trim());
    return null;
  }

  double? _toNonNegativeDouble(dynamic value) {
    if (value == null) return null;
    final parsed = switch (value) {
      double d => d,
      int i => i.toDouble(),
      num n => n.toDouble(),
      String s => double.tryParse(s.trim()),
      _ => null,
    };
    if (parsed == null || parsed.isNaN || parsed.isInfinite || parsed < 0) {
      return null;
    }
    return parsed;
  }
}
