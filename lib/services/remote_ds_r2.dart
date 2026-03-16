part of 'remote_dataset_service.dart';

extension _RemoteDatasetServiceR2 on RemoteDatasetService {
  Future<RemoteHostConnectionResult> _testR2Connection({
    required RemoteHostConfig host,
    required bool verifyWrite,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.r2;
    if (config == null) {
      return const RemoteHostConnectionResult(
        ok: false,
        message: 'Invalid R2 host configuration.',
        writable: false,
      );
    }

    try {
      final context = _buildR2Context(config);
      onStatus?.call('Testing R2 connection...');
      final exists = await context.client.bucketExists(context.bucket);
      if (!exists) {
        return RemoteHostConnectionResult(
          ok: false,
          message: 'Bucket does not exist or access denied: ${context.bucket}',
          writable: false,
        );
      }

      if (!verifyWrite) {
        return const RemoteHostConnectionResult(
          ok: true,
          message: 'R2 connection is reachable.',
          writable: false,
        );
      }

      final probeKey = _joinPrefix(
        config.basePrefix ?? '',
        '.pi_write_probe_${DateTime.now().millisecondsSinceEpoch}.txt',
      );
      await _uploadR2Object(
        client: context.client,
        bucket: context.bucket,
        key: probeKey,
        bytes: Uint8List.fromList(utf8.encode('probe')),
      );
      await context.client.removeObject(context.bucket, probeKey);

      return const RemoteHostConnectionResult(
        ok: true,
        message: 'R2 connection is reachable and writable.',
        writable: true,
      );
    } catch (error) {
      return RemoteHostConnectionResult(
        ok: false,
        message: error.toString(),
        writable: false,
      );
    }
  }

  Future<List<RemotePathEntry>> _listR2Entries({
    required RemoteHostConfig host,
    required String directoryPath,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.r2;
    if (config == null) {
      throw const FormatException('Invalid Cloudflare R2 host configuration.');
    }

    final context = _buildR2Context(config);
    final normalizedPath = _normalizeUnixPrefix(directoryPath);
    final prefix = _joinPrefix(config.basePrefix ?? '', normalizedPath);
    final listedPrefix = prefix.isEmpty ? '' : '$prefix/';

    onStatus?.call(
      'Listing R2 path: ${context.bucket}/${listedPrefix.isEmpty ? '' : listedPrefix}',
    );

    final page = await _listR2Page(
      client: context.client,
      bucket: context.bucket,
      prefix: listedPrefix,
    );

    final entries = <RemotePathEntry>[];

    for (final dirPrefix in page.prefixes) {
      final relative = _relativeObjectPath(
        key: dirPrefix,
        listedPrefix: listedPrefix,
      );
      if (relative == null || relative.isEmpty) continue;
      final normalized = _normalizeUnixPrefix(relative);
      if (normalized.isEmpty) continue;
      final leaf = _lastUnixSegment(normalized);
      if (leaf.isEmpty) continue;
      entries.add(
        RemotePathEntry(
          path: _joinPrefix(normalizedPath, leaf),
          name: leaf,
          isDirectory: true,
        ),
      );
    }

    for (final object in page.objects) {
      final key = object.key.trim();
      if (key.isEmpty) continue;
      final relative = _relativeObjectPath(
        key: key,
        listedPrefix: listedPrefix,
      );
      if (relative == null || relative.isEmpty) continue;
      if (relative.contains('/')) continue;
      entries.add(
        RemotePathEntry(
          path: _joinPrefix(normalizedPath, relative),
          name: relative,
          isDirectory: false,
          sizeBytes: object.sizeBytes >= 0 ? object.sizeBytes : null,
          modifiedAt: object.lastModified,
        ),
      );
    }

    entries.sort(_compareEntries);
    return entries;
  }

  Future<void> _writeR2Bytes({
    required RemoteHostConfig host,
    required String remotePath,
    required List<int> bytes,
    required bool overwrite,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.r2;
    if (config == null) {
      throw const FormatException('Invalid Cloudflare R2 host configuration.');
    }

    final normalizedPath = _normalizeUnixPrefix(remotePath);
    if (normalizedPath.isEmpty) {
      throw const FormatException('remotePath is required for R2 writes.');
    }

    final context = _buildR2Context(config);
    final key = _joinPrefix(config.basePrefix ?? '', normalizedPath);

    if (!overwrite) {
      final exists = await _r2ObjectExists(
        client: context.client,
        bucket: context.bucket,
        key: key,
      );
      if (exists) {
        throw FormatException('Target already exists: $key');
      }
    }

    await _uploadR2Object(
      client: context.client,
      bucket: context.bucket,
      key: key,
      bytes: Uint8List.fromList(bytes),
    );

    onStatus?.call('Uploaded ${bytes.length} bytes to R2 object "$key".');
  }

  Future<Uint8List> _readR2Bytes({
    required RemoteHostConfig host,
    required String remotePath,
    required int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) async {
    final config = host.r2;
    if (config == null) {
      throw const FormatException('Invalid Cloudflare R2 host configuration.');
    }

    final normalizedPath = _normalizeUnixPrefix(remotePath);
    if (normalizedPath.isEmpty) {
      throw const FormatException('remotePath is required for R2 reads.');
    }

    final context = _buildR2Context(config);
    final key = _joinPrefix(config.basePrefix ?? '', normalizedPath);
    onStatus?.call('Reading R2 object: ${context.bucket}/$key');

    final stream = await context.client.getObject(context.bucket, key);
    return _readStreamBytes(
      stream,
      maxBytes: maxBytes,
    );
  }

  Stream<List<int>> _openReadR2({
    required RemoteHostConfig host,
    required String remotePath,
    int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) async* {
    final config = host.r2;
    if (config == null) {
      throw const FormatException('Invalid Cloudflare R2 host configuration.');
    }

    final normalizedPath = _normalizeUnixPrefix(remotePath);
    if (normalizedPath.isEmpty) {
      throw const FormatException('remotePath is required for R2 reads.');
    }

    final context = _buildR2Context(config);
    final key = _joinPrefix(config.basePrefix ?? '', normalizedPath);
    onStatus?.call('Streaming R2 object: ${context.bucket}/$key');

    final stream = maxBytes != null && maxBytes > 0
        ? await context.client.getPartialObject(context.bucket, key, 0, maxBytes)
        : await context.client.getObject(context.bucket, key);
    await for (final chunk in stream) {
      if (chunk.isEmpty) continue;
      yield chunk;
    }
  }

  Future<Uint8List> _readStreamBytes(
    Stream<List<int>> stream, {
    required int? maxBytes,
  }) async {
    final limit = maxBytes != null && maxBytes > 0 ? maxBytes : null;
    if (limit == 0) {
      return Uint8List(0);
    }
    final builder = BytesBuilder();
    var read = 0;
    await for (final chunk in stream) {
      if (chunk.isEmpty) continue;
      if (limit == null) {
        builder.add(chunk);
        read += chunk.length;
        continue;
      }
      final remaining = limit - read;
      if (remaining <= 0) {
        break;
      }
      if (chunk.length <= remaining) {
        builder.add(chunk);
        read += chunk.length;
      } else {
        builder.add(chunk.sublist(0, remaining));
        read += remaining;
      }
      if (read >= limit) {
        break;
      }
    }
    return builder.takeBytes();
  }

  Future<void> _uploadR2Object({
    required s3.Minio client,
    required String bucket,
    required String key,
    required Uint8List bytes,
  }) async {
    await client.putObject(
      bucket,
      key,
      Stream<Uint8List>.value(bytes),
      size: bytes.length,
    );
  }

  Future<bool> _r2ObjectExists({
    required s3.Minio client,
    required String bucket,
    required String key,
  }) async {
    try {
      await client.statObject(bucket, key, retrieveAcls: false);
      return true;
    } catch (error) {
      final message = error.toString().toLowerCase();
      if (message.contains('nosuchkey') ||
          message.contains('not found') ||
          message.contains('404')) {
        return false;
      }
      rethrow;
    }
  }

  Future<_R2ListPage> _listR2Page({
    required s3.Minio client,
    required String bucket,
    required String prefix,
  }) async {
    final objects = <_R2ObjectMeta>[];
    final prefixes = <String>{};

    await for (final chunk in client.listObjectsV2(
      bucket,
      prefix: prefix,
      recursive: false,
    )) {
      for (final object in chunk.objects) {
        final meta = _toR2ObjectMeta(object);
        if (meta != null) {
          objects.add(meta);
        }
      }
      prefixes.addAll(chunk.prefixes);
    }

    return _R2ListPage(
      objects: objects,
      prefixes: prefixes.toList(growable: false),
    );
  }

  // ignore: unused_element
  Future<List<_R2ObjectMeta>> _listR2Objects({
    required s3.Minio client,
    required String bucket,
    required String prefix,
    required bool recursive,
  }) async {
    final objects = <_R2ObjectMeta>[];
    await for (final chunk in client.listObjectsV2(
      bucket,
      prefix: prefix,
      recursive: recursive,
    )) {
      for (final object in chunk.objects) {
        final meta = _toR2ObjectMeta(object);
        if (meta != null) {
          objects.add(meta);
        }
      }
    }
    return objects;
  }

  _R2ObjectMeta? _toR2ObjectMeta(dynamic object) {
    final key = object.key?.toString().trim() ?? '';
    if (key.isEmpty) return null;
    final rawSize = object.size;
    var size = -1;
    if (rawSize is int) {
      size = rawSize;
    } else if (rawSize is num) {
      size = rawSize.toInt();
    } else if (rawSize != null) {
      size = int.tryParse(rawSize.toString()) ?? -1;
    }
    final modified = object.lastModified;
    return _R2ObjectMeta(
      key: key,
      sizeBytes: size,
      lastModified: modified is DateTime ? modified : null,
    );
  }

  _R2Context _buildR2Context(R2RemoteHostConfig config) {
    final endpoint = _parseEndpoint(
      rawEndpoint: config.endpoint,
      fallbackHttps: config.useHttps,
    );

    final client = s3.Minio(
      endPoint: endpoint.host,
      port: endpoint.port,
      useSSL: endpoint.useHttps,
      accessKey: config.accessKeyId.trim(),
      secretKey: config.secretAccessKey.trim(),
      region: _normalizeRegion(config.region),
      pathStyle: true,
    );

    return _R2Context(
      client: client,
      bucket: config.bucket.trim(),
    );
  }
}
