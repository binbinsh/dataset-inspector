part of 'remote_dataset_service.dart';

final Map<String, ({String host, String? sourceAddress})>
    _sambaPreferredTargets = <String, ({String host, String? sourceAddress})>{};

extension _RemoteDatasetServiceSamba on RemoteDatasetService {
  Future<String> _resolveSambaPath(
    RemoteHostConfig host,
    String datasetPath, {
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.samba;
    if (config == null) {
      throw const FormatException('Invalid Samba host configuration.');
    }

    final basePath = _normalizeUnixPrefix(config.basePath ?? '');
    final suffix = _normalizeUnixPrefix(datasetPath);

    return _syncSambaPathToCache(
      host: host,
      config: config,
      relativePath: _joinPrefix(basePath, suffix),
      onStatus: onStatus,
    );
  }

  Future<String> _syncSambaPathToCache({
    required RemoteHostConfig host,
    required SambaRemoteHostConfig config,
    required String relativePath,
    RemoteStatusCallback? onStatus,
  }) async {
    final cacheRoot = await _resolveSambaCacheRoot();
    final localPrefixSlug = _slugifyPath(relativePath);
    final destination = Directory(
      p.join(
        cacheRoot.path,
        _slugifyHost(host.id),
        _slugifyHost(config.share),
        localPrefixSlug,
      ),
    );
    await destination.create(recursive: true);

    final baseAbs = _buildSambaAbsolutePath(
      config: config,
      relativePath: relativePath,
    );

    var copied = 0;
    onStatus?.call('Syncing Samba path to local cache: $baseAbs');

    await _withSambaConnection(config, (connect) async {
      final target = await connect.file(baseAbs);
      if (!target.isExists) {
        throw FormatException('Samba path does not exist: $baseAbs');
      }
      if (!target.isDirectory()) {
        final filename = _lastUnixSegment(relativePath);
        if (filename.isEmpty) {
          throw FormatException('Samba file name is empty: $baseAbs');
        }
        final outFile = File(p.join(destination.path, filename));
        await outFile.parent.create(recursive: true);
        final existingSize =
            await outFile.stat().then((s) => s.size).catchError((_) => -1);
        final existingSatisfiesFull =
            existingSize == target.size && target.size >= 0;
        if (!existingSatisfiesFull) {
          final stream = await connect.openRead(target);
          final sink = outFile.openWrite();
          try {
            await stream.map<List<int>>((chunk) => chunk).pipe(sink);
          } finally {
            await sink.close();
          }
        }
        copied = 1;
        return;
      }
      await _downloadSambaDirectoryRecursive(
        connect: connect,
        absoluteFolderPath: baseAbs,
        rootFolderPath: baseAbs,
        destination: destination,
        onFileCopied: () {
          copied += 1;
          if (copied % 20 == 0) {
            onStatus?.call('Samba sync progress: $copied files');
          }
        },
      );
    });

    onStatus?.call('Samba sync complete: $copied files cached.');
    return destination.path;
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
      onStatus?.call('Testing Samba connection...');
      await _withSambaConnection(config, (connect) async {
        final root = _buildSambaAbsolutePath(config: config, relativePath: '');
        final folder = await connect.file(_ensureSambaDirectoryPath(root));
        if (!folder.isExists || !folder.isDirectory()) {
          throw FormatException('Samba path does not exist: $root');
        }
        if (!verifyWrite) return;
        final probeRelative = _joinPrefix(
          config.basePath ?? '',
          '.pi_write_probe_${DateTime.now().millisecondsSinceEpoch}.txt',
        );
        await _writeSambaBytesViaConnect(
          connect: connect,
          config: config,
          relativePath: probeRelative,
          bytes: utf8.encode('probe'),
          overwrite: true,
        );
        await _deleteSambaFileViaConnect(
          connect: connect,
          config: config,
          relativePath: probeRelative,
        );
      });
      return RemoteHostConnectionResult(
        ok: true,
        message: verifyWrite
            ? 'Samba connection is reachable and writable.'
            : 'Samba connection is reachable.',
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
    final folderAbs = _buildSambaAbsolutePath(
      config: config,
      relativePath: normalizedPath,
    );
    onStatus?.call('Listing Samba path: $folderAbs');

    return _withSambaConnection(config, (connect) async {
      final folder = await connect.file(_ensureSambaDirectoryPath(folderAbs));
      if (!folder.isExists || !folder.isDirectory()) {
        return const <RemotePathEntry>[];
      }
      final children = await connect.listFiles(folder);
      final entries = <RemotePathEntry>[];
      for (final child in children) {
        final name = child.name.trim();
        if (name.isEmpty || name == '.' || name == '..') continue;
        entries.add(
          RemotePathEntry(
            path: _joinPrefix(normalizedPath, name),
            name: name,
            isDirectory: child.isDirectory(),
            sizeBytes: child.isDirectory() ? null : child.size,
            modifiedAt: _timestampToDateTime(child.lastModified),
          ),
        );
      }
      entries.sort(_compareEntries);
      return entries;
    });
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

    final relativePath = _joinPrefix(config.basePath ?? '', normalizedPath);
    await _withSambaConnection(config, (connect) async {
      await _writeSambaBytesViaConnect(
        connect: connect,
        config: config,
        relativePath: relativePath,
        bytes: bytes,
        overwrite: overwrite,
      );
    });
    onStatus?.call('Uploaded ${bytes.length} bytes to Samba.');
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

    final relativePath = _joinPrefix(config.basePath ?? '', normalizedPath);
    final absolutePath = _buildSambaAbsolutePath(
      config: config,
      relativePath: relativePath,
    );
    onStatus?.call('Reading Samba file: $absolutePath');

    return _withSambaConnection(config, (connect) async {
      final file = await connect.file(absolutePath);
      if (!file.isExists || file.isDirectory()) {
        throw FormatException('Samba file not found: $normalizedPath');
      }
      final stream = await connect.openRead(file);
      return _readStreamBytes(stream, maxBytes: maxBytes);
    });
  }

  Stream<List<int>> _openReadSamba({
    required RemoteHostConfig host,
    required String remotePath,
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

    final relativePath = _joinPrefix(config.basePath ?? '', normalizedPath);
    final absolutePath = _buildSambaAbsolutePath(
      config: config,
      relativePath: relativePath,
    );
    onStatus?.call('Streaming Samba file: $absolutePath');

    final hostAddress = config.host.trim();
    if (hostAddress.isEmpty) {
      throw const FormatException('Samba host is required.');
    }
    if (config.port != 445) {
      throw FormatException(
        'Configured Samba port ${config.port} is not supported by the current '
        'SMB client. Use TCP 445, or connect via SSH/SFTP instead.',
      );
    }

    SmbConnect? connect;
    ({String host, String? sourceAddress})? target;
    try {
      final resolved = await _connectSambaWithFallback(config);
      connect = resolved.connect;
      target = resolved.target;
      final file = await connect.file(absolutePath);
      if (!file.isExists || file.isDirectory()) {
        throw FormatException('Samba file not found: $normalizedPath');
      }
      final stream = await connect.openRead(file);
      await for (final chunk in stream) {
        if (chunk.isEmpty) continue;
        yield chunk;
      }
    } on SocketException catch (error) {
      final targetHost = target?.host ?? hostAddress;
      throw FormatException(
        _describeSambaSocketError(targetHost, config.port, error),
      );
    } catch (error) {
      if (error is FormatException) rethrow;
      final detail = error.toString().trim();
      if (detail.contains('SmbAuthException')) {
        throw FormatException(
          'Samba authentication failed for $hostAddress:${config.port}. '
          'Check username/password and server auth policy.',
        );
      }
      if (detail.contains("Can't read 4 from Socket")) {
        throw FormatException(
          'Samba handshake failed for $hostAddress:${config.port}. '
          'TCP is reachable but SMB negotiation did not complete. '
          'This is typically caused by SMB dialect/security-policy mismatch '
          '(for example SMB3-only policy) or the server dropping the session.',
        );
      }
      throw FormatException(
        'Samba stream open failed for $hostAddress:${config.port}: $error',
      );
    } finally {
      if (connect != null) {
        try {
          await connect.close();
        } catch (_) {}
      }
    }
  }

  Future<void> _writeSambaBytesViaConnect({
    required SmbConnect connect,
    required SambaRemoteHostConfig config,
    required String relativePath,
    required List<int> bytes,
    required bool overwrite,
  }) async {
    final normalized = _normalizeUnixPrefix(relativePath);
    if (normalized.isEmpty) {
      throw const FormatException('Samba relative path is empty.');
    }

    final parentWithinShare = _parentUnixPath(normalized);
    await _ensureSambaRemoteDirectory(
      connect: connect,
      config: config,
      relativeDirectoryPath: parentWithinShare,
    );

    final absolutePath = _buildSambaAbsolutePath(
      config: config,
      relativePath: normalized,
    );

    final existing = await connect.file(absolutePath);
    if (existing.isExists) {
      if (!overwrite) {
        throw FormatException('Target already exists: $normalized');
      }
      if (existing.isDirectory()) {
        throw FormatException('Target is a directory: $normalized');
      }
      await connect.delete(existing);
    }

    final file = await connect.createFile(absolutePath);
    final sink = await connect.openWrite(file);
    try {
      sink.add(bytes);
      await sink.flush();
    } finally {
      await sink.close();
    }
  }

  Future<void> _deleteSambaFileViaConnect({
    required SmbConnect connect,
    required SambaRemoteHostConfig config,
    required String relativePath,
  }) async {
    final absolutePath = _buildSambaAbsolutePath(
      config: config,
      relativePath: relativePath,
    );
    final file = await connect.file(absolutePath);
    if (file.isExists && !file.isDirectory()) {
      await connect.delete(file);
    }
  }

  Future<void> _ensureSambaRemoteDirectory({
    required SmbConnect connect,
    required SambaRemoteHostConfig config,
    required String relativeDirectoryPath,
  }) async {
    final share = _normalizeShare(config.share);
    if (share.isEmpty) {
      throw const FormatException('Samba share is required.');
    }

    var current = '/$share';
    final normalized = _normalizeUnixPrefix(relativeDirectoryPath);
    if (normalized.isEmpty) return;

    for (final segment in normalized.split('/')) {
      final name = segment.trim();
      if (name.isEmpty) continue;
      current = '$current/$name';
      final folder = await connect.file(_ensureSambaDirectoryPath(current));
      if (folder.isExists) {
        if (!folder.isDirectory()) {
          throw FormatException('Path is not a directory: $current');
        }
        continue;
      }
      await connect.createFolder(_ensureSambaDirectoryPath(current));
    }
  }

  Future<T> _withSambaConnection<T>(
    SambaRemoteHostConfig config,
    Future<T> Function(SmbConnect connect) action,
  ) async {
    final host = config.host.trim();
    if (host.isEmpty) {
      throw const FormatException('Samba host is required.');
    }
    if (config.port != 445) {
      throw FormatException(
        'Configured Samba port ${config.port} is not supported by the current '
        'SMB client. Use TCP 445, or connect via SSH/SFTP instead.',
      );
    }
    final resolved = await _connectSambaWithFallback(config);
    final connect = resolved.connect;
    try {
      return await action(connect);
    } finally {
      await connect.close();
    }
  }

  Future<
      ({
        SmbConnect connect,
        ({String host, String? sourceAddress}) target,
      })> _connectSambaWithFallback(SambaRemoteHostConfig config) async {
    final host = config.host.trim();
    final targets = await _sambaConnectionTargets(
      host: host,
      port: config.port,
    );
    final failures = <String>[];
    for (final target in targets) {
      try {
        final connect = await SmbConnect.connectAuth(
          host: target.host,
          username: config.username?.trim() ?? '',
          password: config.password?.trim() ?? '',
          domain: '',
          sourceAddress: target.sourceAddress,
        );
        _sambaPreferredTargets['$host:${config.port}'] = target;
        return (connect: connect, target: target);
      } on SocketException catch (error) {
        failures
            .add(_describeSambaSocketError(target.host, config.port, error));
      } catch (error) {
        final detail = error.toString().trim();
        if (detail.contains('SmbAuthException')) {
          throw FormatException(
            'Samba authentication failed for $host:${config.port}. '
            'Check username/password and server auth policy.',
          );
        }
        if (detail.contains("Can't read 4 from Socket")) {
          failures.add(
            'Samba handshake failed for ${target.host}:${config.port}'
            '${target.sourceAddress == null ? '' : ' [source=${target.sourceAddress}]'}'
            ': $detail',
          );
          continue;
        }
        failures.add(
          'Samba session setup failed for ${target.host}:${config.port}'
          '${target.sourceAddress == null ? '' : ' [source=${target.sourceAddress}]'}'
          ': $detail',
        );
      }
    }
    final failureText = failures.isEmpty
        ? 'No successful SMB handshake.'
        : failures.join(' | ');
    throw FormatException(
      'Samba session setup failed for $host:${config.port} after '
      '${targets.length} target attempt(s). $failureText',
    );
  }

  Future<List<({String host, String? sourceAddress})>> _sambaConnectionTargets({
    required String host,
    required int port,
  }) async {
    final out = <({String host, String? sourceAddress})>[];
    final seen = <String>{};
    void add(String targetHost, {String? sourceAddress}) {
      final key = '$targetHost|${sourceAddress ?? ''}';
      if (seen.add(key)) {
        out.add((host: targetHost, sourceAddress: sourceAddress));
      }
    }

    final preferred = _sambaPreferredTargets['$host:$port'];
    if (preferred != null) {
      add(preferred.host, sourceAddress: preferred.sourceAddress);
    }
    add(host);
    final candidates = await _sambaHostCandidates(host);
    for (final candidate in candidates) {
      add(candidate);
      if (out.length >= 4) {
        break;
      }
    }
    return out;
  }

  Future<List<String>> _sambaHostCandidates(String host) async {
    final normalized = host.trim();
    if (normalized.isEmpty) return const <String>[];
    if (InternetAddress.tryParse(normalized) != null) {
      return <String>[normalized];
    }
    final out = <String>[];
    try {
      final lookup = await InternetAddress.lookup(normalized);
      final ipv4 = lookup
          .where((entry) => entry.type == InternetAddressType.IPv4)
          .map((entry) => entry.address);
      final ipv6 = lookup
          .where((entry) => entry.type == InternetAddressType.IPv6)
          .map((entry) => entry.address);
      for (final address in <String>[...ipv4, ...ipv6]) {
        if (!out.contains(address)) {
          out.add(address);
        }
      }
    } catch (_) {}
    if (!out.contains(normalized)) {
      out.add(normalized);
    }
    return out;
  }

  String _describeSambaSocketError(
    String host,
    int port,
    SocketException error,
  ) {
    final code = error.osError?.errorCode;
    final rawMessage = error.osError?.message.trim() ?? error.message.trim();
    final message = rawMessage.isEmpty ? 'Socket error.' : rawMessage;
    final lower = message.toLowerCase();

    if (code == 65 || lower.contains('no route to host')) {
      return 'Cannot reach Samba host $host:$port ($message). '
          'Check VPN/network route/firewall from the device running Flutter '
          '(simulators/emulators may not inherit host VPN routes).';
    }
    if (lower.contains('failed host lookup') ||
        lower.contains('name or service not known')) {
      return 'Cannot resolve Samba host $host:$port ($message). '
          'Check DNS/hostname.';
    }
    if (lower.contains('connection refused')) {
      return 'Samba host $host:$port refused the connection ($message). '
          'Check SMB service availability and firewall rules.';
    }
    return 'Samba connection failed for $host:$port ($message).';
  }

  Future<void> _downloadSambaDirectoryRecursive({
    required SmbConnect connect,
    required String absoluteFolderPath,
    required String rootFolderPath,
    required Directory destination,
    required void Function() onFileCopied,
  }) async {
    final folder = await connect.file(
      _ensureSambaDirectoryPath(absoluteFolderPath),
    );
    if (!folder.isExists || !folder.isDirectory()) {
      throw FormatException('Samba folder not found: $absoluteFolderPath');
    }

    final children = await connect.listFiles(folder);
    for (final child in children) {
      final childAbs = _normalizeAbsoluteSmbPath(child.path);
      final relative = _relativeSambaPath(
        _normalizeAbsoluteSmbPath(rootFolderPath),
        childAbs,
      );
      if (relative.isEmpty) continue;

      final localPath = p.normalize(p.join(destination.path, relative));
      if (!p.isWithin(destination.path, localPath) &&
          localPath != destination.path) {
        continue;
      }

      if (child.isDirectory()) {
        await Directory(localPath).create(recursive: true);
        await _downloadSambaDirectoryRecursive(
          connect: connect,
          absoluteFolderPath: childAbs,
          rootFolderPath: rootFolderPath,
          destination: destination,
          onFileCopied: onFileCopied,
        );
        continue;
      }

      final outFile = File(localPath);
      await outFile.parent.create(recursive: true);
      final existingSize =
          await outFile.stat().then((s) => s.size).catchError((_) => -1);
      if (existingSize == child.size && child.size >= 0) {
        continue;
      }

      final stream = await connect.openRead(child);
      final sink = outFile.openWrite();
      try {
        await stream.map<List<int>>((chunk) => chunk).pipe(sink);
      } finally {
        await sink.close();
      }
      onFileCopied();
    }
  }

  Future<Directory> _resolveSambaCacheRoot() async {
    final support = await getApplicationSupportDirectory();
    return Directory(p.join(support.path, 'remote_cache', 'samba'));
  }
}
