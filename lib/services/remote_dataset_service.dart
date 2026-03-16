import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dartssh2/dartssh2.dart';
import 'package:libsmbclient_ffi/libsmbclient_ffi.dart' as native_smb;
import 'package:minio/minio.dart' as s3;

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

class RemoteDatasetService {
  // Remote dataset paths must stay direct and streaming-first.
  // Performance acceleration should use in-memory structures only.
  // Do not reintroduce on-disk remote cache/sync directories here.
  Future<String> resolveDatasetPath({
    required RemoteHostConfig host,
    required String datasetPath,
    RemoteStatusCallback? onStatus,
  }) async {
    final hostId = host.id.trim();
    if (hostId.isEmpty) {
      throw const FormatException('Remote host ID is required.');
    }
    final normalizedPath = _normalizeUnixPrefix(datasetPath);
    onStatus?.call(
      'Using direct remote mode: files are accessed remotely without local cache.',
    );
    return Uri(
      scheme: 'remote',
      host: hostId,
      pathSegments:
          normalizedPath.isEmpty ? const <String>[] : normalizedPath.split('/'),
    ).toString();
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
    int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) {
    switch (host.type) {
      case RemoteHostType.samba:
        return _openReadSamba(
          host: host,
          remotePath: remotePath,
          maxBytes: maxBytes,
          onStatus: onStatus,
        );
      case RemoteHostType.ssh:
        return _openReadSsh(
          host: host,
          remotePath: remotePath,
          maxBytes: maxBytes,
          onStatus: onStatus,
        );
      case RemoteHostType.r2:
        return _openReadR2(
          host: host,
          remotePath: remotePath,
          maxBytes: maxBytes,
          onStatus: onStatus,
        );
    }
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
