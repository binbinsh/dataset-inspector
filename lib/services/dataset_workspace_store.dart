import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:path/path.dart' as p;

import 'lhotse_service.dart';

/// Persistent mutable workspace layer on top of immutable dataset sources.
///
/// Storage format (workspace root):
/// - manifest.json
/// - operations.jsonl
/// - artifacts/*.json
/// - snapshots/*.jsonl + snapshots/*.meta.json
class DatasetWorkspaceStore {
  DatasetWorkspaceStore({
    String? rootDirectoryPath,
    LhotseService? lhotse,
  })  : _rootDirectoryPath = rootDirectoryPath ??
            p.join(
              Directory.current.path,
              '.dataset-inspector',
              'workspaces',
            ),
        _lhotse = lhotse ?? LhotseService();

  static const String formatVersion = 'dataset-workspace-v1';
  static const int defaultPageLength = 128;
  static const int maxPageLength = 1024;

  static const Set<String> supportedOperationTypes = <String>{
    'set_field',
    'delete_field',
    'delete_record',
    'restore_record',
    'tag_record',
    'note',
  };

  final String _rootDirectoryPath;
  final Random _random = Random.secure();
  final LhotseService _lhotse;

  Directory get _rootDir => Directory(_rootDirectoryPath);

  Future<Map<String, dynamic>> createWorkspace({
    String? label,
    Map<String, dynamic>? source,
    List<String>? tags,
  }) async {
    await _ensureRootDir();
    final workspaceId = _newWorkspaceId();
    final workspaceDir = _workspaceDir(workspaceId);
    await workspaceDir.create(recursive: true);
    await _artifactsDir(workspaceId).create(recursive: true);
    await _snapshotsDir(workspaceId).create(recursive: true);
    await _lhotseDir(workspaceId).create(recursive: true);
    await _operationsFile(workspaceId).writeAsString('');

    final now = _nowIsoUtc();
    final manifest = <String, dynamic>{
      'format_version': formatVersion,
      'workspace_id': workspaceId,
      'label': _normalizeNullableString(label),
      'source': source ?? const <String, dynamic>{},
      'tags': tags?.map((it) => it.trim()).where((it) => it.isNotEmpty).toList(
                growable: false,
              ) ??
          const <String>[],
      'created_at': now,
      'updated_at': now,
      'operation_count': 0,
      'artifact_count': 0,
      'snapshot_count': 0,
      'lhotse_manifest_count': 0,
      'path': workspaceDir.path,
    };
    await _writeManifest(workspaceId, manifest);
    return manifest;
  }

  Future<Map<String, dynamic>> listWorkspaces({
    int offset = 0,
    int length = defaultPageLength,
  }) async {
    _validatePage(offset: offset, length: length);
    await _ensureRootDir();

    final manifests = <Map<String, dynamic>>[];
    await for (final entity in _rootDir.list(followLinks: false)) {
      if (entity is! Directory) continue;
      final workspaceId = p.basename(entity.path);
      final manifest = await _safeReadManifest(workspaceId);
      if (manifest != null) manifests.add(manifest);
    }

    manifests.sort(
      (a, b) =>
          (_asString(b['updated_at'])).compareTo(_asString(a['updated_at'])),
    );

    final total = manifests.length;
    final start = offset.clamp(0, total);
    final end = min(start + length, total);
    final page = manifests.sublist(start, end);

    return {
      'offset': start,
      'length': length,
      'total': total,
      'partial': start > 0 || end < total,
      'items': page,
    };
  }

  Future<Map<String, dynamic>> getWorkspace({
    required String workspaceId,
  }) async {
    final manifest = await _readManifest(workspaceId);
    return Map<String, dynamic>.of(manifest);
  }

  Future<Map<String, dynamic>> appendOperations({
    required String workspaceId,
    required List<dynamic> operations,
  }) async {
    await _requireWorkspaceExists(workspaceId);
    if (operations.isEmpty) {
      throw const FormatException('Argument "operations" cannot be empty.');
    }

    final normalized =
        operations.map(_normalizeOperation).toList(growable: false);
    final sink = _operationsFile(workspaceId).openWrite(mode: FileMode.append);
    for (final op in normalized) {
      sink.writeln(jsonEncode(op));
    }
    await sink.close();

    final manifest = await _refreshManifestCounts(workspaceId);
    return {
      'workspace_id': workspaceId,
      'appended_count': normalized.length,
      'operation_count': manifest['operation_count'],
      'updated_at': manifest['updated_at'],
      'operations': normalized,
    };
  }

  Future<Map<String, dynamic>> listOperations({
    required String workspaceId,
    int offset = 0,
    int length = defaultPageLength,
  }) async {
    _validatePage(offset: offset, length: length);
    await _requireWorkspaceExists(workspaceId);

    final allOperations = await _readAllOperations(workspaceId);
    final total = allOperations.length;
    final start = offset.clamp(0, total);
    final end = min(start + length, total);

    return {
      'workspace_id': workspaceId,
      'offset': start,
      'length': length,
      'total': total,
      'partial': start > 0 || end < total,
      'operations': allOperations.sublist(start, end),
    };
  }

  Future<Map<String, dynamic>> saveArtifact({
    required String workspaceId,
    required String name,
    required dynamic data,
    bool overwrite = false,
  }) async {
    await _requireWorkspaceExists(workspaceId);
    final sanitized = _sanitizeArtifactName(name);
    final file =
        File(p.join(_artifactsDir(workspaceId).path, '$sanitized.json'));
    if (!overwrite && await file.exists()) {
      throw FormatException('Artifact already exists: $sanitized');
    }

    final jsonText = const JsonEncoder.withIndent('  ').convert(data);
    await file.writeAsString(jsonText, flush: true);

    final manifest = await _refreshManifestCounts(workspaceId);
    final stat = await file.stat();
    return {
      'workspace_id': workspaceId,
      'name': sanitized,
      'path': file.path,
      'size': stat.size,
      'updated_at': stat.modified.toUtc().toIso8601String(),
      'artifact_count': manifest['artifact_count'],
    };
  }

  Future<Map<String, dynamic>> listArtifacts({
    required String workspaceId,
  }) async {
    await _requireWorkspaceExists(workspaceId);
    final artifacts = <Map<String, dynamic>>[];
    final dir = _artifactsDir(workspaceId);
    if (await dir.exists()) {
      await for (final entity in dir.list(followLinks: false)) {
        if (entity is! File) continue;
        if (!entity.path.endsWith('.json')) continue;
        final stat = await entity.stat();
        artifacts.add({
          'name': p.basenameWithoutExtension(entity.path),
          'path': entity.path,
          'size': stat.size,
          'updated_at': stat.modified.toUtc().toIso8601String(),
        });
      }
    }
    artifacts
        .sort((a, b) => _asString(a['name']).compareTo(_asString(b['name'])));
    return {
      'workspace_id': workspaceId,
      'total': artifacts.length,
      'artifacts': artifacts,
    };
  }

  Future<Map<String, dynamic>> saveSnapshot({
    required String workspaceId,
    required String name,
    required List<dynamic> rows,
    bool overwrite = false,
    bool syncLhotseCuts = true,
    String recordKeyField = 'record_key',
  }) async {
    await _requireWorkspaceExists(workspaceId);
    final sanitized = _sanitizeArtifactName(name);
    final snapshotDir = _snapshotsDir(workspaceId);
    await snapshotDir.create(recursive: true);
    final rowsPath = p.join(snapshotDir.path, '$sanitized.jsonl');
    final metaPath = p.join(snapshotDir.path, '$sanitized.meta.json');
    final rowsFile = File(rowsPath);
    final metaFile = File(metaPath);
    if (!overwrite && (await rowsFile.exists() || await metaFile.exists())) {
      throw FormatException('Snapshot already exists: $sanitized');
    }

    final columns = <String>{};
    final sink = rowsFile.openWrite(mode: FileMode.writeOnly);
    for (final row in rows) {
      if (row is Map<String, dynamic>) {
        columns.addAll(row.keys);
      }
      sink.writeln(jsonEncode(row));
    }
    await sink.close();

    final rowStat = await rowsFile.stat();
    final metadata = <String, dynamic>{
      'workspace_id': workspaceId,
      'name': sanitized,
      'path': rowsPath,
      'row_count': rows.length,
      'size': rowStat.size,
      'columns': columns.toList()..sort(),
      'created_at': _nowIsoUtc(),
    };

    if (syncLhotseCuts) {
      final lhotseResult = await _lhotse.writeEntries(
        input: _lhotseDir(workspaceId).path,
        manifest: 'cuts',
        entries: _lhotse.rowsToCuts(
          rows,
          recordKeyField: recordKeyField,
          cutPrefix: sanitized,
        ),
        overwrite: true,
      );
      metadata['lhotse_cuts_path'] = lhotseResult['path'];
      metadata['lhotse_cuts_rows'] = lhotseResult['rows'];
    }

    await metaFile.writeAsString(
      const JsonEncoder.withIndent('  ').convert(metadata),
      flush: true,
    );

    final manifest = await _refreshManifestCounts(workspaceId);
    return {
      ...metadata,
      'snapshot_count': manifest['snapshot_count'],
    };
  }

  Future<Map<String, dynamic>> listSnapshots({
    required String workspaceId,
  }) async {
    await _requireWorkspaceExists(workspaceId);
    final snapshots = <Map<String, dynamic>>[];
    final dir = _snapshotsDir(workspaceId);
    if (await dir.exists()) {
      await for (final entity in dir.list(followLinks: false)) {
        if (entity is! File) continue;
        if (!entity.path.endsWith('.meta.json')) continue;
        final parsed = await _safeReadJsonFile(entity);
        if (parsed is Map<String, dynamic>) {
          snapshots.add(parsed);
        }
      }
    }
    snapshots
        .sort((a, b) => _asString(a['name']).compareTo(_asString(b['name'])));
    return {
      'workspace_id': workspaceId,
      'total': snapshots.length,
      'snapshots': snapshots,
    };
  }

  Future<Map<String, dynamic>> describeLhotseManifests({
    required String workspaceId,
  }) async {
    await _requireWorkspaceExists(workspaceId);
    final source = await _lhotse.loadSource(_lhotseDir(workspaceId).path);
    final manifest = await _refreshManifestCounts(workspaceId);
    return {
      'workspace_id': workspaceId,
      ...source,
      'lhotse_manifest_count': manifest['lhotse_manifest_count'],
    };
  }

  Future<Map<String, dynamic>> listLhotseManifestEntries({
    required String workspaceId,
    required String manifest,
    int offset = 0,
    int length = defaultPageLength,
  }) async {
    _validatePage(offset: offset, length: length);
    await _requireWorkspaceExists(workspaceId);
    final listed = await _lhotse.listEntries(
      input: _lhotseDir(workspaceId).path,
      manifest: manifest,
      offset: offset,
      length: length,
    );
    return {
      'workspace_id': workspaceId,
      ...listed,
    };
  }

  Future<Map<String, dynamic>> saveLhotseManifest({
    required String workspaceId,
    required String manifest,
    required List<dynamic> entries,
    bool overwrite = false,
    bool compressed = false,
  }) async {
    await _requireWorkspaceExists(workspaceId);
    final saved = await _lhotse.writeEntries(
      input: _lhotseDir(workspaceId).path,
      manifest: manifest,
      entries: entries,
      overwrite: overwrite,
      compressed: compressed,
    );
    final refreshed = await _refreshManifestCounts(workspaceId);
    return {
      'workspace_id': workspaceId,
      ...saved,
      'lhotse_manifest_count': refreshed['lhotse_manifest_count'],
    };
  }

  Future<Map<String, dynamic>> appendLhotseManifestEntries({
    required String workspaceId,
    required String manifest,
    required List<dynamic> entries,
    bool createIfMissing = true,
  }) async {
    await _requireWorkspaceExists(workspaceId);
    final appended = await _lhotse.appendEntries(
      input: _lhotseDir(workspaceId).path,
      manifest: manifest,
      entries: entries,
      createIfMissing: createIfMissing,
    );
    final refreshed = await _refreshManifestCounts(workspaceId);
    return {
      'workspace_id': workspaceId,
      ...appended,
      'lhotse_manifest_count': refreshed['lhotse_manifest_count'],
    };
  }

  Future<Map<String, dynamic>> applyOperations({
    required String workspaceId,
    required List<dynamic> records,
    String recordKeyField = 'record_key',
  }) async {
    await _requireWorkspaceExists(workspaceId);
    final operations = await _readAllOperations(workspaceId);
    final normalizedRecords = <Map<String, dynamic>>[];
    final indexByKey = <String, int>{};
    final deletedKeys = <String>{};
    int generatedKeys = 0;

    for (var index = 0; index < records.length; index++) {
      final raw = records[index];
      final row = raw is Map<String, dynamic>
          ? Map<String, dynamic>.of(raw)
          : <String, dynamic>{'value': raw};
      final providedKey = row[recordKeyField];
      final resolvedKey = providedKey == null
          ? (row['record_index']?.toString() ?? 'row:$index')
          : providedKey.toString();
      if (providedKey == null) {
        row[recordKeyField] = resolvedKey;
        generatedKeys++;
      }
      normalizedRecords.add(row);
      indexByKey[resolvedKey] = normalizedRecords.length - 1;
    }

    var appliedCount = 0;
    var skippedCount = 0;
    for (final op in operations) {
      final type = _asString(op['type']);
      switch (type) {
        case 'set_field':
          {
            final key = _asString(op['record_key']);
            final rowIndex = indexByKey[key];
            if (rowIndex == null) {
              skippedCount++;
              continue;
            }
            final field = _asString(op['field']);
            normalizedRecords[rowIndex][field] = op['value'];
            appliedCount++;
            break;
          }
        case 'delete_field':
          {
            final key = _asString(op['record_key']);
            final rowIndex = indexByKey[key];
            if (rowIndex == null) {
              skippedCount++;
              continue;
            }
            final field = _asString(op['field']);
            normalizedRecords[rowIndex].remove(field);
            appliedCount++;
            break;
          }
        case 'delete_record':
          {
            final key = _asString(op['record_key']);
            if (!indexByKey.containsKey(key)) {
              skippedCount++;
              continue;
            }
            deletedKeys.add(key);
            appliedCount++;
            break;
          }
        case 'restore_record':
          {
            final key = _asString(op['record_key']);
            if (!indexByKey.containsKey(key)) {
              skippedCount++;
              continue;
            }
            deletedKeys.remove(key);
            appliedCount++;
            break;
          }
        case 'tag_record':
          {
            final key = _asString(op['record_key']);
            final rowIndex = indexByKey[key];
            if (rowIndex == null) {
              skippedCount++;
              continue;
            }
            final tag = _asString(op['tag']);
            final row = normalizedRecords[rowIndex];
            final existing = row['_tags'];
            final tags = existing is List
                ? existing.map((it) => it.toString()).toSet()
                : <String>{};
            tags.add(tag);
            row['_tags'] = tags.toList()..sort();
            appliedCount++;
            break;
          }
        case 'note':
          {
            appliedCount++;
            break;
          }
        default:
          {
            skippedCount++;
            break;
          }
      }
    }

    final materialized = normalizedRecords.where((row) {
      final key = row[recordKeyField]?.toString();
      return key == null || !deletedKeys.contains(key);
    }).toList(growable: false);

    return {
      'workspace_id': workspaceId,
      'record_key_field': recordKeyField,
      'input_count': records.length,
      'output_count': materialized.length,
      'dropped_count': records.length - materialized.length,
      'generated_keys': generatedKeys,
      'operation_count': operations.length,
      'applied_count': appliedCount,
      'skipped_count': skippedCount,
      'records': materialized,
    };
  }

  Future<void> _ensureRootDir() async {
    if (!await _rootDir.exists()) {
      await _rootDir.create(recursive: true);
    }
  }

  Future<void> _requireWorkspaceExists(String workspaceId) async {
    if (!_isValidWorkspaceId(workspaceId)) {
      throw FormatException('Invalid workspace_id: $workspaceId');
    }
    final dir = _workspaceDir(workspaceId);
    if (!await dir.exists()) {
      throw FormatException('Unknown workspace_id: $workspaceId');
    }
    final manifest = _manifestFile(workspaceId);
    if (!await manifest.exists()) {
      throw FormatException(
          'Corrupt workspace (manifest missing): $workspaceId');
    }
  }

  Future<Map<String, dynamic>?> _safeReadManifest(String workspaceId) async {
    try {
      return await _readManifest(workspaceId);
    } catch (_) {
      return null;
    }
  }

  Future<Map<String, dynamic>> _readManifest(String workspaceId) async {
    final file = _manifestFile(workspaceId);
    final decoded = await _safeReadJsonFile(file);
    if (decoded is! Map<String, dynamic>) {
      throw FormatException(
          'Invalid manifest format for workspace: $workspaceId');
    }
    return decoded;
  }

  Future<void> _writeManifest(
      String workspaceId, Map<String, dynamic> manifest) {
    final file = _manifestFile(workspaceId);
    return file.writeAsString(
      const JsonEncoder.withIndent('  ').convert(manifest),
      flush: true,
    );
  }

  Future<Map<String, dynamic>> _refreshManifestCounts(
      String workspaceId) async {
    final manifest = await _readManifest(workspaceId);
    manifest['operation_count'] =
        await _countJsonLines(_operationsFile(workspaceId));
    manifest['artifact_count'] = await _countFilesByExtension(
      _artifactsDir(workspaceId),
      extension: '.json',
    );
    manifest['snapshot_count'] = await _countFilesByExtension(
      _snapshotsDir(workspaceId),
      extension: '.meta.json',
    );
    manifest['lhotse_manifest_count'] = await _countLhotseManifestFiles(
      _lhotseDir(workspaceId),
    );
    manifest['updated_at'] = _nowIsoUtc();
    await _writeManifest(workspaceId, manifest);
    return manifest;
  }

  Future<List<Map<String, dynamic>>> _readAllOperations(
      String workspaceId) async {
    final file = _operationsFile(workspaceId);
    if (!await file.exists()) {
      return <Map<String, dynamic>>[];
    }

    final lines = await file.readAsLines();
    final operations = <Map<String, dynamic>>[];
    for (final line in lines) {
      final trimmed = line.trim();
      if (trimmed.isEmpty) continue;
      final parsed = jsonDecode(trimmed);
      if (parsed is Map<String, dynamic>) {
        operations.add(parsed);
      }
    }
    return operations;
  }

  Map<String, dynamic> _normalizeOperation(dynamic rawOperation) {
    if (rawOperation is! Map) {
      throw const FormatException('Each operation must be an object.');
    }
    final op = Map<String, dynamic>.of(rawOperation.cast<String, dynamic>());
    final type = _requiredStringFromMap(op, 'type');
    if (!supportedOperationTypes.contains(type)) {
      throw FormatException('Unsupported operation type: $type');
    }

    final normalized = <String, dynamic>{
      'op_id': _normalizeNullableString(op['op_id']?.toString()) ??
          _newOperationId(),
      'type': type,
      'timestamp':
          _normalizeNullableString(op['timestamp']?.toString()) ?? _nowIsoUtc(),
    };

    switch (type) {
      case 'set_field':
        normalized['record_key'] = _requiredStringFromMap(op, 'record_key');
        normalized['field'] = _requiredStringFromMap(op, 'field');
        normalized['value'] = op['value'];
        break;
      case 'delete_field':
        normalized['record_key'] = _requiredStringFromMap(op, 'record_key');
        normalized['field'] = _requiredStringFromMap(op, 'field');
        break;
      case 'delete_record':
      case 'restore_record':
        normalized['record_key'] = _requiredStringFromMap(op, 'record_key');
        break;
      case 'tag_record':
        normalized['record_key'] = _requiredStringFromMap(op, 'record_key');
        normalized['tag'] = _requiredStringFromMap(op, 'tag');
        break;
      case 'note':
        normalized['note'] = _requiredStringFromMap(op, 'note');
        final recordKey =
            _normalizeNullableString(op['record_key']?.toString());
        if (recordKey != null) normalized['record_key'] = recordKey;
        break;
    }

    final metadata = op['metadata'];
    if (metadata is Map) {
      normalized['metadata'] =
          Map<String, dynamic>.of(metadata.cast<String, dynamic>());
    }
    return normalized;
  }

  Future<dynamic> _safeReadJsonFile(File file) async {
    final text = await file.readAsString();
    return jsonDecode(text);
  }

  Future<int> _countJsonLines(File file) async {
    if (!await file.exists()) return 0;
    final lines = await file.readAsLines();
    return lines.where((line) => line.trim().isNotEmpty).length;
  }

  Future<int> _countFilesByExtension(
    Directory dir, {
    required String extension,
  }) async {
    if (!await dir.exists()) return 0;
    var count = 0;
    await for (final entity in dir.list(followLinks: false)) {
      if (entity is File && entity.path.endsWith(extension)) {
        count++;
      }
    }
    return count;
  }

  Future<int> _countLhotseManifestFiles(Directory dir) async {
    if (!await dir.exists()) return 0;
    var count = 0;
    await for (final entity in dir.list(followLinks: false)) {
      if (entity is! File) continue;
      final name = p.basename(entity.path).toLowerCase();
      if (name == 'recordings.jsonl' ||
          name == 'recordings.jsonl.gz' ||
          name == 'supervisions.jsonl' ||
          name == 'supervisions.jsonl.gz' ||
          name == 'cuts.jsonl' ||
          name == 'cuts.jsonl.gz') {
        count++;
      }
    }
    return count;
  }

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

  String _newWorkspaceId() {
    final micros = DateTime.now().toUtc().microsecondsSinceEpoch;
    return 'ws_${micros}_${_randomHex(6)}';
  }

  String _newOperationId() {
    final micros = DateTime.now().toUtc().microsecondsSinceEpoch;
    return 'op_${micros}_${_randomHex(8)}';
  }

  String _randomHex(int bytes) {
    final values = List<int>.generate(bytes, (_) => _random.nextInt(256));
    final buf = StringBuffer();
    for (final value in values) {
      buf.write(value.toRadixString(16).padLeft(2, '0'));
    }
    return buf.toString();
  }

  String _nowIsoUtc() => DateTime.now().toUtc().toIso8601String();

  String _requiredStringFromMap(Map<String, dynamic> value, String key) {
    final raw = value[key];
    if (raw is! String || raw.trim().isEmpty) {
      throw FormatException('Missing required string field: $key');
    }
    return raw.trim();
  }

  String? _normalizeNullableString(String? value) {
    if (value == null) return null;
    final trimmed = value.trim();
    return trimmed.isEmpty ? null : trimmed;
  }

  String _sanitizeArtifactName(String value) {
    final trimmed = value.trim();
    if (trimmed.isEmpty) {
      throw const FormatException('Artifact/snapshot name cannot be empty.');
    }
    final sanitized = trimmed
        .replaceAll(RegExp(r'[^a-zA-Z0-9._-]+'), '_')
        .replaceAll(RegExp(r'_+'), '_')
        .replaceAll(RegExp(r'^[_\.]+|[_\.]+$'), '');
    if (sanitized.isEmpty) {
      throw FormatException('Invalid artifact/snapshot name: $value');
    }
    return sanitized;
  }

  bool _isValidWorkspaceId(String value) {
    return RegExp(r'^[a-zA-Z0-9._-]+$').hasMatch(value);
  }

  String _asString(dynamic value) => value?.toString() ?? '';

  Directory _workspaceDir(String workspaceId) =>
      Directory(p.join(_rootDirectoryPath, workspaceId));

  File _manifestFile(String workspaceId) =>
      File(p.join(_workspaceDir(workspaceId).path, 'manifest.json'));

  File _operationsFile(String workspaceId) =>
      File(p.join(_workspaceDir(workspaceId).path, 'operations.jsonl'));

  Directory _artifactsDir(String workspaceId) =>
      Directory(p.join(_workspaceDir(workspaceId).path, 'artifacts'));

  Directory _snapshotsDir(String workspaceId) =>
      Directory(p.join(_workspaceDir(workspaceId).path, 'snapshots'));

  Directory _lhotseDir(String workspaceId) =>
      Directory(p.join(_workspaceDir(workspaceId).path, 'lhotse'));
}
