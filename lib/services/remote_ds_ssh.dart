part of 'remote_dataset_service.dart';

extension _RemoteDatasetServiceSsh on RemoteDatasetService {
  Future<String> _resolveSshPath(
    RemoteHostConfig host,
    String datasetPath, {
    RemoteStatusCallback? onStatus,
  }) async {
    if (host.ssh == null) {
      throw const FormatException('Invalid SSH host configuration.');
    }
    final normalizedPath = _normalizeUnixPrefix(datasetPath);
    onStatus?.call(
      'Using SSH direct mode: files are accessed remotely without local cache.',
    );
    return Uri(
      scheme: 'remote',
      host: host.id.trim(),
      pathSegments:
          normalizedPath.isEmpty ? const <String>[] : normalizedPath.split('/'),
    ).toString();
  }

  Future<RemoteHostConnectionResult> _testSshConnection({
    required RemoteHostConfig host,
    required bool verifyWrite,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.ssh;
    if (config == null) {
      return const RemoteHostConnectionResult(
        ok: false,
        message: 'Invalid SSH host configuration.',
        writable: false,
      );
    }

    try {
      onStatus?.call('Testing SSH/SFTP connection...');
      await _withSftpConnection(config, (sftp, _) async {
        final rootPath = _buildSshAbsolutePath(
          config: config,
          relativePath: '',
        );
        final root = await sftp.stat(rootPath);
        if (!root.isDirectory) {
          throw FormatException('SSH path is not a directory: $rootPath');
        }

        if (!verifyWrite) return;
        final probeName =
            '.pi_write_probe_${DateTime.now().millisecondsSinceEpoch}.txt';
        final probePath = _joinUnixAbsolutePath(rootPath, probeName);
        final probeFile = await sftp.open(
          probePath,
          mode: SftpFileOpenMode.write |
              SftpFileOpenMode.create |
              SftpFileOpenMode.truncate,
        );
        try {
          await probeFile.writeBytes(Uint8List.fromList(utf8.encode('probe')));
        } finally {
          await probeFile.close();
        }
        await sftp.remove(probePath);
      });

      return RemoteHostConnectionResult(
        ok: true,
        message: verifyWrite
            ? 'SSH/SFTP connection is reachable and writable.'
            : 'SSH/SFTP connection is reachable.',
        writable: verifyWrite,
      );
    } catch (error) {
      return RemoteHostConnectionResult(
        ok: false,
        message: error.toString(),
        writable: false,
      );
    }
  }

  Future<List<RemotePathEntry>> _listSshEntries({
    required RemoteHostConfig host,
    required String directoryPath,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.ssh;
    if (config == null) {
      throw const FormatException('Invalid SSH host configuration.');
    }

    final normalizedPath = _normalizeUnixPrefix(directoryPath);
    final absolutePath = _buildSshAbsolutePath(
      config: config,
      relativePath: normalizedPath,
    );
    onStatus?.call('Listing SSH path: $absolutePath');

    return _withSftpConnection(config, (sftp, _) async {
      try {
        final attrs = await sftp.stat(absolutePath);
        if (!attrs.isDirectory) return const <RemotePathEntry>[];
      } catch (error) {
        if (_isSftpNoSuchFileError(error)) {
          return const <RemotePathEntry>[];
        }
        rethrow;
      }

      final listed = await sftp.listdir(absolutePath);
      final entries = <RemotePathEntry>[];
      for (final item in listed) {
        final name = item.filename.trim();
        if (name.isEmpty || name == '.' || name == '..') continue;
        final attrs = item.attr;
        final isDirectory = attrs.isDirectory;
        entries.add(
          RemotePathEntry(
            path: _joinPrefix(normalizedPath, name),
            name: name,
            isDirectory: isDirectory,
            sizeBytes: isDirectory ? null : attrs.size,
            modifiedAt: _timestampToDateTime(attrs.modifyTime ?? 0),
          ),
        );
      }
      entries.sort(_compareEntries);
      return entries;
    });
  }

  Future<void> _writeSshBytes({
    required RemoteHostConfig host,
    required String remotePath,
    required List<int> bytes,
    required bool overwrite,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.ssh;
    if (config == null) {
      throw const FormatException('Invalid SSH host configuration.');
    }
    final normalizedPath = _normalizeUnixPrefix(remotePath);
    if (normalizedPath.isEmpty) {
      throw const FormatException('remotePath is required for SSH writes.');
    }

    final absolutePath = _buildSshAbsolutePath(
      config: config,
      relativePath: normalizedPath,
    );
    await _withSftpConnection(config, (sftp, _) async {
      await _ensureSshRemoteDirectory(
        sftp: sftp,
        absoluteDirectoryPath: _parentUnixAbsolutePath(absolutePath),
      );
      final existing = await _sshStatOrNull(sftp, absolutePath);
      if (existing != null) {
        if (existing.isDirectory) {
          throw FormatException('Target is a directory: $normalizedPath');
        }
        if (!overwrite) {
          throw FormatException('Target already exists: $normalizedPath');
        }
        await sftp.remove(absolutePath);
      }
      final file = await sftp.open(
        absolutePath,
        mode: SftpFileOpenMode.write |
            SftpFileOpenMode.create |
            SftpFileOpenMode.truncate,
      );
      try {
        await file.writeBytes(Uint8List.fromList(bytes));
      } finally {
        await file.close();
      }
    });
    onStatus?.call('Uploaded ${bytes.length} bytes to SSH/SFTP.');
  }

  Future<Uint8List> _readSshBytes({
    required RemoteHostConfig host,
    required String remotePath,
    required int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.ssh;
    if (config == null) {
      throw const FormatException('Invalid SSH host configuration.');
    }
    final normalizedPath = _normalizeUnixPrefix(remotePath);
    if (normalizedPath.isEmpty) {
      throw const FormatException('remotePath is required for SSH reads.');
    }

    final absolutePath = _buildSshAbsolutePath(
      config: config,
      relativePath: normalizedPath,
    );
    onStatus?.call('Reading SSH file: $absolutePath');

    return _withSftpConnection(config, (sftp, _) async {
      final attrs = await sftp.stat(absolutePath);
      if (attrs.isDirectory) {
        throw FormatException('SSH path is a directory: $normalizedPath');
      }
      final file = await sftp.open(
        absolutePath,
        mode: SftpFileOpenMode.read,
      );
      try {
        return _readStreamBytes(
          file.read(
            length: maxBytes != null && maxBytes > 0 ? maxBytes : null,
          ),
          maxBytes: maxBytes,
        );
      } finally {
        await file.close();
      }
    });
  }

  Stream<List<int>> _openReadSsh({
    required RemoteHostConfig host,
    required String remotePath,
    RemoteStatusCallback? onStatus,
  }) async* {
    final config = host.ssh;
    if (config == null) {
      throw const FormatException('Invalid SSH host configuration.');
    }
    final normalizedPath = _normalizeUnixPrefix(remotePath);
    if (normalizedPath.isEmpty) {
      throw const FormatException('remotePath is required for SSH reads.');
    }

    final absolutePath = _buildSshAbsolutePath(
      config: config,
      relativePath: normalizedPath,
    );
    onStatus?.call('Streaming SSH file: $absolutePath');

    final hostAddress = config.host.trim();
    final username = config.username.trim();
    if (hostAddress.isEmpty || username.isEmpty) {
      throw const FormatException('SSH host and username are required.');
    }
    final port = config.port <= 0 ? 22 : config.port;
    final identities = _resolveSshIdentities(config);
    final password = config.password?.trim();
    final hasPassword = password != null && password.isNotEmpty;
    if (identities.isEmpty && !hasPassword) {
      throw const FormatException(
        'SSH authentication requires password or private key.',
      );
    }

    SSHSocket? socket;
    SSHClient? client;
    SftpClient? sftp;
    SftpFile? remoteFile;
    try {
      socket = await SSHSocket.connect(
        hostAddress,
        port,
        timeout: const Duration(seconds: 10),
      );
      client = SSHClient(
        socket,
        username: username,
        identities: identities.isEmpty ? null : identities,
        onPasswordRequest: hasPassword ? () => password : null,
        onVerifyHostKey: (_, __) => true,
        keepAliveInterval: const Duration(seconds: 20),
      );
      await client.authenticated.timeout(const Duration(seconds: 15));
      sftp = await client.sftp();
      final attrs = await sftp.stat(absolutePath);
      if (attrs.isDirectory) {
        throw FormatException('SSH path is a directory: $normalizedPath');
      }
      remoteFile = await sftp.open(
        absolutePath,
        mode: SftpFileOpenMode.read,
      );
      await for (final chunk in remoteFile.read()) {
        if (chunk.isEmpty) continue;
        yield chunk;
      }
    } finally {
      if (remoteFile != null) {
        try {
          await remoteFile.close();
        } catch (_) {}
      }
      if (sftp != null) {
        try {
          sftp.close();
        } catch (_) {}
      }
      if (client != null) {
        client.close();
        try {
          await client.done;
        } catch (_) {}
      }
      if (socket != null) {
        try {
          await socket.close();
        } catch (_) {}
      }
    }
  }

  Future<T> _withSftpConnection<T>(
    SshRemoteHostConfig config,
    Future<T> Function(SftpClient sftp, SSHClient client) action,
  ) async {
    final host = config.host.trim();
    final username = config.username.trim();
    if (host.isEmpty || username.isEmpty) {
      throw const FormatException('SSH host and username are required.');
    }
    final port = config.port <= 0 ? 22 : config.port;
    final identities = _resolveSshIdentities(config);
    final password = config.password?.trim();
    final hasPassword = password != null && password.isNotEmpty;
    if (identities.isEmpty && !hasPassword) {
      throw const FormatException(
        'SSH authentication requires password or private key.',
      );
    }

    final socket = await SSHSocket.connect(
      host,
      port,
      timeout: const Duration(seconds: 10),
    );
    final client = SSHClient(
      socket,
      username: username,
      identities: identities.isEmpty ? null : identities,
      onPasswordRequest: hasPassword ? () => password : null,
      onVerifyHostKey: (_, __) => true,
      keepAliveInterval: const Duration(seconds: 20),
    );
    try {
      await client.authenticated.timeout(const Duration(seconds: 15));
      final sftp = await client.sftp();
      try {
        return await action(sftp, client);
      } finally {
        sftp.close();
      }
    } finally {
      client.close();
      try {
        await client.done;
      } catch (_) {}
      try {
        await socket.close();
      } catch (_) {}
    }
  }

  List<SSHKeyPair> _resolveSshIdentities(SshRemoteHostConfig config) {
    final privateKey = config.privateKey?.trim();
    if (privateKey == null || privateKey.isEmpty) {
      return const <SSHKeyPair>[];
    }
    final passphrase = config.privateKeyPassphrase?.trim();
    try {
      return SSHKeyPair.fromPem(
        privateKey,
        passphrase == null || passphrase.isEmpty ? null : passphrase,
      );
    } catch (error) {
      throw FormatException('Invalid SSH private key: $error');
    }
  }

  Future<SftpFileAttrs?> _sshStatOrNull(SftpClient sftp, String path) async {
    try {
      return await sftp.stat(path);
    } catch (error) {
      if (_isSftpNoSuchFileError(error)) return null;
      rethrow;
    }
  }

  Future<void> _ensureSshRemoteDirectory({
    required SftpClient sftp,
    required String absoluteDirectoryPath,
  }) async {
    final target = _normalizeUnixAbsolutePath(absoluteDirectoryPath);
    if (target == '/') return;

    var current = '/';
    final parts = target.split('/').where((segment) => segment.isNotEmpty);
    for (final part in parts) {
      current = current == '/' ? '/$part' : '$current/$part';
      final existing = await _sshStatOrNull(sftp, current);
      if (existing == null) {
        await sftp.mkdir(current);
        continue;
      }
      if (!existing.isDirectory) {
        throw FormatException('SSH path is not a directory: $current');
      }
    }
  }

  String _buildSshAbsolutePath({
    required SshRemoteHostConfig config,
    required String relativePath,
  }) {
    final base = _normalizeUnixAbsolutePath(config.basePath ?? '/');
    final relative = _normalizeUnixPrefix(relativePath);
    if (relative.isEmpty) return base;
    return _joinUnixAbsolutePath(base, relative);
  }

  bool _isSftpNoSuchFileError(Object error) {
    return error is SftpStatusError && error.code == SftpStatusCode.noSuchFile;
  }
}
