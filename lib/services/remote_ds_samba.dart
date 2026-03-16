part of 'remote_dataset_service.dart';

extension _RemoteDatasetServiceSamba on RemoteDatasetService {
  /// Create a native SMB3 client from host config.
  native_smb.SmbClient _nativeSmbClient(SambaRemoteHostConfig config) {
    return native_smb.SmbClient(
      host: config.host,
      share: config.share,
      basePath: config.basePath,
      username: config.username,
      password: config.password,
    );
  }

  Future<RemoteHostConnectionResult> _testSambaConnection({
    required RemoteHostConfig host,
    required bool verifyWrite,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.samba;
    if (config == null) {
      return const RemoteHostConnectionResult(
        ok: false,
        message: 'Invalid Samba host configuration.',
        writable: false,
      );
    }

    try {
      onStatus?.call('Testing Samba connection (native SMB3)...');
      final client = _nativeSmbClient(config);
      try {
        final ok = await client.testConnection();
        if (!ok) {
          return const RemoteHostConnectionResult(
            ok: false,
            message: 'Samba connection test failed.',
            writable: false,
          );
        }
        if (!verifyWrite) {
          return const RemoteHostConnectionResult(
            ok: true,
            message: 'Samba connection is reachable (native SMB3).',
            writable: false,
          );
        }
        // Write probe
        final probePath =
            '.pi_write_probe_${DateTime.now().millisecondsSinceEpoch}.txt';
        await client.writeFile(probePath, utf8.encode('probe') as Uint8List);
        await client.deleteFile(probePath);
        return const RemoteHostConnectionResult(
          ok: true,
          message: 'Samba connection is reachable and writable (native SMB3).',
          writable: true,
        );
      } finally {
        client.dispose();
      }
    } catch (error) {
      return RemoteHostConnectionResult(
        ok: false,
        message: error.toString(),
        writable: false,
      );
    }
  }

  Future<List<RemotePathEntry>> _listSambaEntries({
    required RemoteHostConfig host,
    required String directoryPath,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.samba;
    if (config == null) {
      throw const FormatException('Invalid Samba host configuration.');
    }

    final normalizedPath = _normalizeUnixPrefix(directoryPath);
    onStatus?.call('Listing (native SMB3): $normalizedPath');

    final client = _nativeSmbClient(config);
    try {
      final smbEntries = await client.listDir(normalizedPath);
      final entries = <RemotePathEntry>[];
      for (final entry in smbEntries) {
        final name = entry.name.trim();
        if (name.isEmpty) continue;
        entries.add(
          RemotePathEntry(
            path: _joinPrefix(normalizedPath, name),
            name: name,
            isDirectory: entry.isDirectory,
            sizeBytes: entry.sizeBytes,
            modifiedAt: null, // libsmbclient readdir doesn't provide mtime
          ),
        );
      }
      entries.sort(_compareEntries);
      return entries;
    } finally {
      client.dispose();
    }
  }

  Future<void> _writeSambaBytes({
    required RemoteHostConfig host,
    required String remotePath,
    required List<int> bytes,
    required bool overwrite,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.samba;
    if (config == null) {
      throw const FormatException('Invalid Samba host configuration.');
    }

    final normalizedPath = _normalizeUnixPrefix(remotePath);
    if (normalizedPath.isEmpty) {
      throw const FormatException('remotePath is required for Samba writes.');
    }

    onStatus?.call('Writing (native SMB3): $normalizedPath');
    final client = _nativeSmbClient(config);
    try {
      // Ensure parent directory exists
      final parent = _parentUnixPath(normalizedPath);
      if (parent.isNotEmpty) {
        await _ensureNativeSambaDirectory(client, parent);
      }

      if (!overwrite) {
        final info = await client.stat(normalizedPath);
        if (info.exists) {
          throw FormatException('Target already exists: $normalizedPath');
        }
      }

      await client.writeFile(
        normalizedPath,
        bytes is Uint8List ? bytes : Uint8List.fromList(bytes),
      );
      onStatus?.call('Uploaded ${bytes.length} bytes via native SMB3.');
    } finally {
      client.dispose();
    }
  }

  Future<Uint8List> _readSambaBytes({
    required RemoteHostConfig host,
    required String remotePath,
    required int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.samba;
    if (config == null) {
      throw const FormatException('Invalid Samba host configuration.');
    }

    final normalizedPath = _normalizeUnixPrefix(remotePath);
    if (normalizedPath.isEmpty) {
      throw const FormatException('remotePath is required for Samba reads.');
    }

    onStatus?.call('Reading (native SMB3): $normalizedPath');
    final client = _nativeSmbClient(config);
    try {
      final data = await client.readFile(normalizedPath);
      if (maxBytes != null && data.length > maxBytes) {
        return Uint8List.sublistView(data, 0, maxBytes);
      }
      return data;
    } finally {
      client.dispose();
    }
  }

  Stream<List<int>> _openReadSamba({
    required RemoteHostConfig host,
    required String remotePath,
    int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) async* {
    final config = host.samba;
    if (config == null) {
      throw const FormatException('Invalid Samba host configuration.');
    }

    final normalizedPath = _normalizeUnixPrefix(remotePath);
    if (normalizedPath.isEmpty) {
      throw const FormatException('remotePath is required for Samba reads.');
    }

    onStatus?.call('Streaming (native SMB3): $normalizedPath');
    final client = _nativeSmbClient(config);
    try {
      await for (final chunk in client.openRead(normalizedPath, maxBytes: maxBytes)) {
        yield chunk;
      }
    } finally {
      client.dispose();
    }
  }

  /// Recursively ensure a remote directory path exists.
  Future<void> _ensureNativeSambaDirectory(
    native_smb.SmbClient client,
    String directoryPath,
  ) async {
    final normalized = _normalizeUnixPrefix(directoryPath);
    if (normalized.isEmpty) return;

    final segments = normalized.split('/');
    var current = '';
    for (final segment in segments) {
      final name = segment.trim();
      if (name.isEmpty) continue;
      current = current.isEmpty ? name : '$current/$name';
      await client.mkDir(current);
    }
  }
}
