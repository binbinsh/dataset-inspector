import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dartssh2/dartssh2.dart';
import 'package:minio/minio.dart' as s3;
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';
import 'package:smb_connect/smb_connect.dart';

import '../models/remote_host.dart';

part 'remote_ds_samba.dart';
part 'remote_ds_ssh.dart';
part 'remote_ds_r2.dart';

typedef RemoteStatusCallback = void Function(String message);

class RemoteHostConnectionResult {
  const RemoteHostConnectionResult({
    required this.ok,
    required this.message,
    required this.writable,
  });

  final bool ok;
  final String message;
  final bool writable;
}

class RemotePathEntry {
  const RemotePathEntry({
    required this.path,
    required this.name,
    required this.isDirectory,
    this.sizeBytes,
    this.modifiedAt,
  });

  final String path;
  final String name;
  final bool isDirectory;
  final int? sizeBytes;
  final DateTime? modifiedAt;
}

class RemoteCacheStats {
  const RemoteCacheStats({
    required this.rootPath,
    required this.totalBytes,
    required this.fileCount,
  });

  final String rootPath;
  final int totalBytes;
  final int fileCount;
}

class RemoteDatasetService {
  Future<String> resolveDatasetPath({
    required RemoteHostConfig host,
    required String datasetPath,
    RemoteStatusCallback? onStatus,
  }) async {
    final trimmedPath = datasetPath.trim();
    switch (host.type) {
      case RemoteHostType.samba:
        return _resolveSambaPath(
          host,
          trimmedPath,
          onStatus: onStatus,
        );
      case RemoteHostType.ssh:
        return _resolveSshPath(host, trimmedPath, onStatus: onStatus);
      case RemoteHostType.r2:
        return _syncR2PrefixToCache(host, trimmedPath, onStatus: onStatus);
    }
  }

  Future<RemoteHostConnectionResult> testConnection({
    required RemoteHostConfig host,
    bool verifyWrite = true,
    RemoteStatusCallback? onStatus,
  }) async {
    try {
      switch (host.type) {
        case RemoteHostType.samba:
          return _testSambaConnection(
            host: host,
            verifyWrite: verifyWrite,
            onStatus: onStatus,
          );
        case RemoteHostType.ssh:
          return _testSshConnection(
            host: host,
            verifyWrite: verifyWrite,
            onStatus: onStatus,
          );
        case RemoteHostType.r2:
          return _testR2Connection(
            host: host,
            verifyWrite: verifyWrite,
            onStatus: onStatus,
          );
      }
    } catch (error) {
      return RemoteHostConnectionResult(
        ok: false,
        message: error.toString(),
        writable: false,
      );
    }
  }

  Future<List<RemotePathEntry>> listEntries({
    required RemoteHostConfig host,
    required String directoryPath,
    RemoteStatusCallback? onStatus,
  }) async {
    switch (host.type) {
      case RemoteHostType.samba:
        return _listSambaEntries(
          host: host,
          directoryPath: directoryPath,
          onStatus: onStatus,
        );
      case RemoteHostType.ssh:
        return _listSshEntries(
          host: host,
          directoryPath: directoryPath,
          onStatus: onStatus,
        );
      case RemoteHostType.r2:
        return _listR2Entries(
          host: host,
          directoryPath: directoryPath,
          onStatus: onStatus,
        );
    }
  }

  Future<void> writeTextFile({
    required RemoteHostConfig host,
    required String remotePath,
    required String content,
    bool overwrite = true,
    RemoteStatusCallback? onStatus,
  }) async {
    await writeBytesFile(
      host: host,
      remotePath: remotePath,
      bytes: utf8.encode(content),
      overwrite: overwrite,
      onStatus: onStatus,
    );
  }

  Future<void> writeBytesFile({
    required RemoteHostConfig host,
    required String remotePath,
    required List<int> bytes,
    bool overwrite = true,
    RemoteStatusCallback? onStatus,
  }) async {
    switch (host.type) {
      case RemoteHostType.samba:
        await _writeSambaBytes(
          host: host,
          remotePath: remotePath,
          bytes: bytes,
          overwrite: overwrite,
          onStatus: onStatus,
        );
        return;
      case RemoteHostType.ssh:
        await _writeSshBytes(
          host: host,
          remotePath: remotePath,
          bytes: bytes,
          overwrite: overwrite,
          onStatus: onStatus,
        );
        return;
      case RemoteHostType.r2:
        await _writeR2Bytes(
          host: host,
          remotePath: remotePath,
          bytes: bytes,
          overwrite: overwrite,
          onStatus: onStatus,
        );
        return;
    }
  }

  Future<Uint8List> readBytesFile({
    required RemoteHostConfig host,
    required String remotePath,
    int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) async {
    switch (host.type) {
      case RemoteHostType.samba:
        return _readSambaBytes(
          host: host,
          remotePath: remotePath,
          maxBytes: maxBytes,
          onStatus: onStatus,
        );
      case RemoteHostType.ssh:
        return _readSshBytes(
          host: host,
          remotePath: remotePath,
          maxBytes: maxBytes,
          onStatus: onStatus,
        );
      case RemoteHostType.r2:
        return _readR2Bytes(
          host: host,
          remotePath: remotePath,
          maxBytes: maxBytes,
          onStatus: onStatus,
        );
    }
  }

  Stream<List<int>> openReadFile({
    required RemoteHostConfig host,
    required String remotePath,
    RemoteStatusCallback? onStatus,
  }) {
    switch (host.type) {
      case RemoteHostType.samba:
        return _openReadSamba(
          host: host,
          remotePath: remotePath,
          onStatus: onStatus,
        );
      case RemoteHostType.ssh:
        return _openReadSsh(
          host: host,
          remotePath: remotePath,
          onStatus: onStatus,
        );
      case RemoteHostType.r2:
        return _openReadR2(
          host: host,
          remotePath: remotePath,
          onStatus: onStatus,
        );
    }
  }

  Future<RemoteCacheStats> loadCacheStats({
    RemoteHostConfig? host,
    Iterable<RemoteHostConfig>? allHosts,
  }) async {
    final roots = await _resolveCacheRoots(host: host, allHosts: allHosts);
    var bytes = 0;
    var files = 0;
    for (final root in roots) {
      final stats = await _scanDirectory(root);
      bytes += stats.totalBytes;
      files += stats.fileCount;
    }
    if (host != null) {
      final root = await _resolveHostCacheRoot(host);
      return RemoteCacheStats(
        rootPath: root.path,
        totalBytes: bytes,
        fileCount: files,
      );
    }
    final support = await getApplicationSupportDirectory();
    return RemoteCacheStats(
      rootPath: p.join(support.path, 'remote_cache'),
      totalBytes: bytes,
      fileCount: files,
    );
  }

  Future<void> clearCache({
    RemoteHostConfig? host,
    Iterable<RemoteHostConfig>? allHosts,
  }) async {
    final roots = await _resolveCacheRoots(host: host, allHosts: allHosts);
    for (final root in roots) {
      if (await root.exists()) {
        await root.delete(recursive: true);
      }
    }
  }

  Future<void> enforceCacheQuota({
    required int maxBytes,
    Iterable<RemoteHostConfig>? allHosts,
    RemoteStatusCallback? onStatus,
  }) async {
    if (maxBytes <= 0) return;
    final roots = await _resolveCacheRoots(allHosts: allHosts);
    final files = <_CacheFileMeta>[];
    var totalBytes = 0;

    for (final root in roots) {
      final listed = await _collectCacheFiles(root);
      for (final item in listed) {
        files.add(item);
        totalBytes += item.sizeBytes;
      }
    }

    if (totalBytes <= maxBytes) return;

    files.sort((left, right) => left.modifiedAt.compareTo(right.modifiedAt));
    var removed = 0;
    for (final file in files) {
      if (totalBytes <= maxBytes) break;
      try {
        await file.file.delete();
        totalBytes -= file.sizeBytes;
        removed += 1;
      } catch (_) {}
    }

    if (removed > 0) {
      onStatus?.call(
        'Remote cache quota applied: removed $removed files, current size ${_formatBytes(totalBytes)}.',
      );
    }
  }

  Future<List<Directory>> _resolveCacheRoots({
    RemoteHostConfig? host,
    Iterable<RemoteHostConfig>? allHosts,
  }) async {
    final roots = <Directory>[];

    void addRoot(Directory dir) {
      final normalized = p.normalize(p.absolute(dir.path));
      for (final existing in roots) {
        final existingPath = p.normalize(p.absolute(existing.path));
        if (existingPath == normalized ||
            p.isWithin(existingPath, normalized)) {
          return;
        }
      }
      roots.removeWhere((existing) {
        final existingPath = p.normalize(p.absolute(existing.path));
        return p.isWithin(normalized, existingPath);
      });
      roots.add(Directory(normalized));
    }

    if (host != null) {
      addRoot(await _resolveHostCacheRoot(host));
      return roots;
    }

    final support = await getApplicationSupportDirectory();
    final sharedRoot = Directory(p.join(support.path, 'remote_cache'));
    addRoot(sharedRoot);

    for (final candidate in allHosts ?? const <RemoteHostConfig>[]) {
      final hostRoot = await _resolveHostCacheRoot(candidate);
      final normalizedHostRoot = p.normalize(p.absolute(hostRoot.path));
      final normalizedSharedRoot = p.normalize(p.absolute(sharedRoot.path));
      if (p.isWithin(normalizedSharedRoot, normalizedHostRoot) ||
          normalizedHostRoot == normalizedSharedRoot) {
        continue;
      }
      addRoot(hostRoot);
    }

    return roots;
  }

  Future<Directory> _resolveHostCacheRoot(RemoteHostConfig host) async {
    switch (host.type) {
      case RemoteHostType.samba:
        final config = host.samba;
        if (config == null) {
          throw const FormatException('Invalid Samba host configuration.');
        }
        final sambaRoot = await _resolveSambaCacheRoot();
        return Directory(
          p.join(
            sambaRoot.path,
            _slugifyHost(host.id),
            _slugifyHost(config.share),
          ),
        );
      case RemoteHostType.ssh:
        final support = await getApplicationSupportDirectory();
        return Directory(
          p.join(support.path, 'remote_cache', 'ssh', _slugifyHost(host.id)),
        );
      case RemoteHostType.r2:
        final config = host.r2;
        if (config == null) {
          throw const FormatException('Invalid R2 host configuration.');
        }
        final r2Root = await _resolveR2CacheRoot(config);
        return Directory(
          p.join(
            r2Root.path,
            _slugifyHost(host.id),
            _slugifyHost(config.bucket),
          ),
        );
    }
  }

  Future<RemoteCacheStats> _scanDirectory(Directory directory) async {
    if (!await directory.exists()) {
      return RemoteCacheStats(
        rootPath: directory.path,
        totalBytes: 0,
        fileCount: 0,
      );
    }

    var bytes = 0;
    var files = 0;
    await for (final entity in directory.list(recursive: true)) {
      if (entity is! File) continue;
      files += 1;
      try {
        bytes += await entity.length();
      } catch (_) {}
    }

    return RemoteCacheStats(
      rootPath: directory.path,
      totalBytes: bytes,
      fileCount: files,
    );
  }

  Future<List<_CacheFileMeta>> _collectCacheFiles(Directory directory) async {
    if (!await directory.exists()) return const <_CacheFileMeta>[];
    final files = <_CacheFileMeta>[];
    await for (final entity in directory.list(recursive: true)) {
      if (entity is! File) continue;
      try {
        final stat = await entity.stat();
        files.add(
          _CacheFileMeta(
            file: entity,
            sizeBytes: stat.size,
            modifiedAt: stat.modified,
          ),
        );
      } catch (_) {}
    }
    return files;
  }

  int _compareEntries(RemotePathEntry left, RemotePathEntry right) {
    final typeCmp = (left.isDirectory ? 0 : 1).compareTo(
      right.isDirectory ? 0 : 1,
    );
    if (typeCmp != 0) return typeCmp;
    return left.name.toLowerCase().compareTo(right.name.toLowerCase());
  }

  _ParsedEndpoint _parseEndpoint({
    required String rawEndpoint,
    required bool fallbackHttps,
  }) {
    final trimmed = rawEndpoint.trim();
    if (trimmed.isEmpty) {
      throw const FormatException('Endpoint is required.');
    }

    Uri uri;
    if (trimmed.contains('://')) {
      uri = Uri.parse(trimmed);
    } else {
      uri = Uri.parse('${fallbackHttps ? 'https' : 'http'}://$trimmed');
    }

    final host = uri.host.trim().isEmpty ? uri.path.trim() : uri.host.trim();
    if (host.isEmpty) {
      throw FormatException('Invalid endpoint: $rawEndpoint');
    }

    final useHttps = uri.scheme.isEmpty ? fallbackHttps : uri.scheme == 'https';
    final port = uri.hasPort ? uri.port : null;

    return _ParsedEndpoint(host: host, port: port, useHttps: useHttps);
  }

  DateTime? _timestampToDateTime(int value) {
    if (value <= 0) return null;
    final millis = value > 1000000000000 ? value : value * 1000;
    return DateTime.fromMillisecondsSinceEpoch(millis, isUtc: true);
  }

  String _buildSambaAbsolutePath({
    required SambaRemoteHostConfig config,
    required String relativePath,
  }) {
    final share = _normalizeShare(config.share);
    if (share.isEmpty) {
      throw const FormatException('Samba share is required.');
    }
    final withinShare = _normalizeUnixPrefix(relativePath);
    if (withinShare.isEmpty) return '/$share';
    return '/$share/$withinShare';
  }

  String _ensureSambaDirectoryPath(String path) {
    final normalized = _normalizeAbsoluteSmbPath(path);
    return normalized.endsWith('/') ? normalized : '$normalized/';
  }

  String _normalizeAbsoluteSmbPath(String path) {
    final normalized = path.replaceAll('\\', '/').trim();
    if (normalized.isEmpty) return '/';

    var value = normalized;
    if (!value.startsWith('/')) {
      value = '/$value';
    }
    value = value.replaceAll(RegExp(r'/+'), '/');

    if (value.length > 1 && value.endsWith('/')) {
      value = value.substring(0, value.length - 1);
    }

    return value;
  }

  String _relativeSambaPath(
    String baseAbsolutePath,
    String targetAbsolutePath,
  ) {
    final base = _normalizeAbsoluteSmbPath(baseAbsolutePath);
    final target = _normalizeAbsoluteSmbPath(targetAbsolutePath);
    if (target == base) return '';

    final prefix = '$base/';
    if (target.startsWith(prefix)) {
      return target.substring(prefix.length);
    }

    final withoutLeading =
        target.startsWith('/') ? target.substring(1) : target;
    final parts = withoutLeading.split('/');
    if (parts.length <= 1) return '';
    return parts.sublist(1).join('/');
  }

  String _parentUnixPath(String path) {
    final normalized = _normalizeUnixPrefix(path);
    if (normalized.isEmpty) return '';
    final parts = normalized.split('/');
    if (parts.length <= 1) return '';
    return parts.sublist(0, parts.length - 1).join('/');
  }

  String _lastUnixSegment(String path) {
    final normalized = _normalizeUnixPrefix(path);
    if (normalized.isEmpty) return '';
    final parts = normalized.split('/');
    return parts.last;
  }

  String _normalizeShare(String share) {
    var normalized = share.trim().replaceAll('\\', '/');
    while (normalized.startsWith('/')) {
      normalized = normalized.substring(1);
    }
    while (normalized.endsWith('/')) {
      normalized = normalized.substring(0, normalized.length - 1);
    }
    return normalized;
  }

  String _normalizeRegion(String value) {
    final trimmed = value.trim();
    return trimmed.isEmpty ? 'auto' : trimmed;
  }

  String _normalizeUnixPrefix(String value) {
    final trimmed = value.trim().replaceAll('\\', '/');
    if (trimmed.isEmpty) return '';

    var normalized = trimmed;
    while (normalized.startsWith('/')) {
      normalized = normalized.substring(1);
    }
    while (normalized.endsWith('/')) {
      normalized = normalized.substring(0, normalized.length - 1);
    }
    return normalized;
  }

  String _normalizeUnixAbsolutePath(String value) {
    final normalized = value.trim().replaceAll('\\', '/');
    if (normalized.isEmpty) return '/';
    var path = normalized.replaceAll(RegExp(r'/+'), '/');
    if (!path.startsWith('/')) {
      path = '/$path';
    }
    if (path.length > 1 && path.endsWith('/')) {
      path = path.substring(0, path.length - 1);
    }
    return path;
  }

  String _joinUnixAbsolutePath(String leftAbsolute, String rightRelative) {
    final left = _normalizeUnixAbsolutePath(leftAbsolute);
    final right = _normalizeUnixPrefix(rightRelative);
    if (right.isEmpty) return left;
    if (left == '/') return '/$right';
    return '$left/$right';
  }

  String _parentUnixAbsolutePath(String value) {
    final normalized = _normalizeUnixAbsolutePath(value);
    if (normalized == '/') return '/';
    final index = normalized.lastIndexOf('/');
    if (index <= 0) return '/';
    return normalized.substring(0, index);
  }

  String _joinPrefix(String left, String right) {
    final leftNorm = _normalizeUnixPrefix(left);
    final rightNorm = _normalizeUnixPrefix(right);
    if (leftNorm.isEmpty) return rightNorm;
    if (rightNorm.isEmpty) return leftNorm;
    return '$leftNorm/$rightNorm';
  }

  String _slugifyHost(String value) {
    final lower = value.trim().toLowerCase();
    if (lower.isEmpty) return 'unknown';
    final slug = lower.replaceAll(RegExp(r'[^a-z0-9._-]+'), '-');
    return slug.replaceAll(RegExp(r'-{2,}'), '-');
  }

  String _slugifyPath(String value) {
    final normalized = _normalizeUnixPrefix(value);
    if (normalized.isEmpty) return '_root';
    final slug = normalized.replaceAll('/', '__');
    return _slugifyHost(slug);
  }

  String? _relativeObjectPath({
    required String key,
    required String listedPrefix,
  }) {
    final normalizedKey = key.replaceAll('\\', '/');
    if (listedPrefix.isNotEmpty) {
      if (!normalizedKey.startsWith(listedPrefix)) return null;
      final remainder = normalizedKey.substring(listedPrefix.length);
      if (remainder.isEmpty) return null;
      return _normalizeUnixPrefix(remainder);
    }
    return _normalizeUnixPrefix(normalizedKey);
  }

  String _formatBytes(int bytes) {
    if (bytes < 1024) return '$bytes B';
    const units = <String>['KB', 'MB', 'GB', 'TB'];
    var value = bytes.toDouble();
    var unit = -1;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit += 1;
    }
    final fixed =
        value >= 100 ? value.toStringAsFixed(0) : value.toStringAsFixed(1);
    return '$fixed ${units[unit]}';
  }
}

class _ParsedEndpoint {
  const _ParsedEndpoint({
    required this.host,
    required this.port,
    required this.useHttps,
  });

  final String host;
  final int? port;
  final bool useHttps;
}

class _R2Context {
  const _R2Context({required this.client, required this.bucket});

  final s3.Minio client;
  final String bucket;
}

class _R2ListPage {
  const _R2ListPage({required this.objects, required this.prefixes});

  final List<_R2ObjectMeta> objects;
  final List<String> prefixes;
}

class _R2ObjectMeta {
  const _R2ObjectMeta({
    required this.key,
    required this.sizeBytes,
    required this.lastModified,
  });

  final String key;
  final int sizeBytes;
  final DateTime? lastModified;
}

class _CacheFileMeta {
  const _CacheFileMeta({
    required this.file,
    required this.sizeBytes,
    required this.modifiedAt,
  });

  final File file;
  final int sizeBytes;
  final DateTime modifiedAt;
}
