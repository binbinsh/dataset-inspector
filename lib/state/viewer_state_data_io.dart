part of 'viewer_state.dart';

extension ViewerStateDataIo on ViewerState {
  Future<Uint8List> readDirectoryFileBytes(
    String path, {
    int? maxBytes,
  }) async {
    final explicitRemote = _parseRemoteDirectorySource(path);
    final remote = explicitRemote ?? _activeRemoteDirectorySource;
    if (remote != null) {
      final hostId = explicitRemote?.hostId ?? remote.hostId;
      final host = _findRemoteHost(hostId);
      if (host == null) {
        throw FormatException('Remote host not found: $hostId');
      }
      final remotePath = explicitRemote != null
          ? _normalizeRemoteDirectoryPath(explicitRemote.path)
          : _normalizeRemoteDirectoryPath(path);
      return _remoteDatasets.readBytesFile(
        host: host,
        remotePath: remotePath,
        maxBytes: maxBytes,
        onStatus: (message) {
          statusMessage = message;
        },
      );
    }
    final httpUri = _parseHttpSourceUri(path);
    if (httpUri != null) {
      return _httpDatasets.readBytes(
        url: httpUri,
        maxBytes: maxBytes,
        onStatus: (message) {
          statusMessage = message;
        },
      );
    }
    final file = File(path);
    if (!await file.exists()) {
      throw const FormatException('Selected file does not exist.');
    }
    if (maxBytes != null && maxBytes > 0) {
      final builder = BytesBuilder();
      var read = 0;
      await for (final chunk in file.openRead()) {
        if (chunk.isEmpty) continue;
        final remaining = maxBytes - read;
        if (remaining <= 0) break;
        if (chunk.length <= remaining) {
          builder.add(chunk);
          read += chunk.length;
        } else {
          builder.add(chunk.sublist(0, remaining));
          read += remaining;
        }
        if (read >= maxBytes) {
          break;
        }
      }
      return builder.takeBytes();
    }
    return file.readAsBytes();
  }

  Future<String> readDirectoryFileText(
    String path, {
    int maxBytes = _remoteTextPreviewBytes,
  }) async {
    final bytes = await readDirectoryFileBytes(path, maxBytes: maxBytes);
    if (bytes.isEmpty) return '';
    return utf8.decode(bytes, allowMalformed: true);
  }

  Future<ParquetPreviewTable> readDirectoryParquetTable(
    String path, {
    int offset = 0,
    int length = 2000,
  }) async {
    final explicitRemote = _parseRemoteDirectorySource(path);
    final remote = explicitRemote ?? _activeRemoteDirectorySource;
    if (remote == null) {
      return _parquetPreview.previewLocal(
        parquetPath: path,
        offset: offset,
        length: length,
      );
    }
    final hostId = explicitRemote?.hostId ?? remote.hostId;
    final host = _findRemoteHost(hostId);
    if (host == null) {
      throw FormatException('Remote host not found: $hostId');
    }
    final remotePath = explicitRemote != null
        ? _normalizeRemoteDirectoryPath(explicitRemote.path)
        : _normalizeRemoteDirectoryPath(path);
    return _parquetPreview.previewRemote(
      remoteDatasets: _remoteDatasets,
      host: host,
      remotePath: remotePath,
      offset: offset,
      length: length,
      onStatus: (message) {
        statusMessage = message;
      },
    );
  }

  Future<List<ItemMeta>> listLocalDirectoryMdsItems(String shardPath) async {
    final explicitRemoteShard = _parseRemoteDirectorySource(shardPath);
    final remote = explicitRemoteShard ?? _activeRemoteDirectorySource;
    final httpUri = _parseHttpSourceUri(shardPath);
    final lowerPath = shardPath.trim().toLowerCase();
    final isRemoteExtCompressed = (remote != null || httpUri != null) &&
        (lowerPath.endsWith('.zst') || lowerPath.endsWith('.zstd'));
    final initialRemoteShardPath = explicitRemoteShard != null
        ? _normalizeRemoteDirectoryPath(explicitRemoteShard.path)
        : _normalizeRemoteDirectoryPath(shardPath);
    final source = await _resolveLocalDirectoryMdsSource(shardPath);
    final (:isCompressed, :remotePath) =
        (remote != null || httpUri != null)
            ? await _resolveRemoteMdsCompression(
                isCompressed: isRemoteExtCompressed,
                remotePath: initialRemoteShardPath,
                indexBytes: source.indexBytes,
                indexName: source.indexName,
                shardFilename: source.shardFilename,
              )
            : (
                isCompressed: isRemoteExtCompressed,
                remotePath: initialRemoteShardPath
              );
    final isRemoteCompressed = isCompressed;
    final resolvedRemoteShardPath = remotePath;
    try {
      if (remote != null) {
        final hostId = explicitRemoteShard?.hostId ?? remote.hostId;
        final host = _findRemoteHost(hostId);
        if (host == null) {
          throw FormatException('Remote host not found: $hostId');
        }
        if (isRemoteCompressed) {
          return _listMdsItemsFromRemoteCompressedStreamWithRetry(
            host: host,
            remotePath: resolvedRemoteShardPath,
            indexBytes: source.indexBytes,
            indexName: source.indexName,
            shardFilename: source.shardFilename,
            maxAttempts: 4,
          );
        }
        return _listMdsItemsFromRemoteRawStreamWithRetry(
          host: host,
          remotePath: resolvedRemoteShardPath,
          indexBytes: source.indexBytes,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          maxAttempts: 4,
        );
      }
      if (httpUri != null) {
        if (isRemoteCompressed) {
          final initialCompressedBytes =
              await _estimateRemoteCompressedMdsScanBytes(
            indexBytes: source.indexBytes,
            indexName: source.indexName,
            shardFilename: source.shardFilename,
          );
          return _remoteOps.listMdsItemsFromCompressedStreamWithRetry(
            mosaicml: _mosaicml,
            indexBytes: source.indexBytes,
            indexName: source.indexName,
            shardFilename: source.shardFilename,
            initialCompressedBytes: initialCompressedBytes,
            compressedShardCacheKey: _httpCompressedMdsCacheKey(httpUri),
            maxAttempts: 4,
            openCompressedStream: (maxBytes) => _httpDatasets.openRead(
              url: httpUri,
              maxBytes: maxBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            ),
          );
        }
        return _remoteOps.listMdsItemsFromRawStreamWithRetry(
          mosaicml: _mosaicml,
          indexBytes: source.indexBytes,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          maxAttempts: 4,
          openRawStream: () => _httpDatasets.openRead(
            url: httpUri,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }
      return await _mosaicml.listSamples(
        indexPath: source.indexPath!,
        shardFilename: source.shardFilename,
      );
    } catch (error) {
      if (!_remoteOps.looksLikeMdsCorruption(error.toString())) {
        rethrow;
      }
      final refreshed = await _resolveLocalDirectoryMdsSource(
        shardPath,
        forceRefreshRemoteShard: true,
      );
      if (remote != null) {
        final hostId = explicitRemoteShard?.hostId ?? remote.hostId;
        final host = _findRemoteHost(hostId);
        if (host == null) {
          throw FormatException('Remote host not found: $hostId');
        }
        if (isRemoteCompressed) {
          return _listMdsItemsFromRemoteCompressedStreamWithRetry(
            host: host,
            remotePath: resolvedRemoteShardPath,
            indexBytes: refreshed.indexBytes,
            indexName: refreshed.indexName,
            shardFilename: refreshed.shardFilename,
            maxAttempts: 5,
          );
        }
        return _listMdsItemsFromRemoteRawStreamWithRetry(
          host: host,
          remotePath: resolvedRemoteShardPath,
          indexBytes: refreshed.indexBytes,
          indexName: refreshed.indexName,
          shardFilename: refreshed.shardFilename,
          maxAttempts: 5,
        );
      }
      if (httpUri != null) {
        if (isRemoteCompressed) {
          final initialCompressedBytes =
              await _estimateRemoteCompressedMdsScanBytes(
            indexBytes: refreshed.indexBytes,
            indexName: refreshed.indexName,
            shardFilename: refreshed.shardFilename,
          );
          return _remoteOps.listMdsItemsFromCompressedStreamWithRetry(
            mosaicml: _mosaicml,
            indexBytes: refreshed.indexBytes,
            indexName: refreshed.indexName,
            shardFilename: refreshed.shardFilename,
            initialCompressedBytes: initialCompressedBytes,
            compressedShardCacheKey: _httpCompressedMdsCacheKey(httpUri),
            maxAttempts: 5,
            openCompressedStream: (maxBytes) => _httpDatasets.openRead(
              url: httpUri,
              maxBytes: maxBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            ),
          );
        }
        return _remoteOps.listMdsItemsFromRawStreamWithRetry(
          mosaicml: _mosaicml,
          indexBytes: refreshed.indexBytes,
          indexName: refreshed.indexName,
          shardFilename: refreshed.shardFilename,
          maxAttempts: 5,
          openRawStream: () => _httpDatasets.openRead(
            url: httpUri,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }
      return _mosaicml.listSamples(
        indexPath: refreshed.indexPath!,
        shardFilename: refreshed.shardFilename,
      );
    }
  }

  Future<ItemPage> listLocalDirectoryMdsItemsPage(
    String shardPath, {
    int offset = 0,
    int length = 200,
  }) async {
    final safeOffset = offset < 0 ? 0 : offset;
    final safeLength = length < 1 ? 1 : length;
    try {
      final items = await listLocalDirectoryMdsItems(shardPath);
      final start = safeOffset.clamp(0, items.length).toInt();
      final end = (start + safeLength).clamp(0, items.length).toInt();
      return ItemPage(
        offset: start,
        length: safeLength,
        items: items.sublist(start, end),
        partial: end < items.length,
        numItemsTotal: items.length,
      );
    } catch (error) {
      if (!_remoteOps.looksLikeMdsCorruption(error.toString())) {
        rethrow;
      }
      final source = await _resolveLocalDirectoryMdsSource(shardPath);
      final summary = source.indexBytes == null
          ? await _mosaicml.loadIndex(source.indexPath!)
          : await _mosaicml.loadIndexFromBytes(
              source.indexBytes!,
              indexName: source.indexName,
            );
      final shard = _findMdsChunkForShard(
            summary: summary,
            shardFilename: source.shardFilename,
          ) ??
          (summary.chunks.isEmpty ? null : summary.chunks.first);
      final total = shard?.chunkSize ?? 0;
      final fieldCount = summary.dataFormat.isEmpty ? 1 : summary.dataFormat.length;
      final start = safeOffset.clamp(0, total).toInt();
      final end = (start + safeLength).clamp(0, total).toInt();
      final fields = List<FieldMeta>.generate(
        fieldCount,
        (index) => FieldMeta(fieldIndex: index, size: 0),
        growable: false,
      );
      final items = <ItemMeta>[];
      for (var idx = start; idx < end; idx += 1) {
        items.add(
          ItemMeta(
            itemIndex: idx,
            totalBytes: 0,
            fields: fields,
          ),
        );
      }
      return ItemPage(
        offset: start,
        length: safeLength,
        items: items,
        partial: end < total,
        numItemsTotal: total,
      );
    }
  }

  Future<FieldPreview> peekLocalDirectoryMdsField({
    required String shardPath,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final explicitRemoteShard = _parseRemoteDirectorySource(shardPath);
    final remote = explicitRemoteShard ?? _activeRemoteDirectorySource;
    final httpUri = _parseHttpSourceUri(shardPath);
    final lowerPath = shardPath.trim().toLowerCase();
    final isRemoteExtCompressed = (remote != null || httpUri != null) &&
        (lowerPath.endsWith('.zst') || lowerPath.endsWith('.zstd'));
    final initialRemoteShardPath = explicitRemoteShard != null
        ? _normalizeRemoteDirectoryPath(explicitRemoteShard.path)
        : _normalizeRemoteDirectoryPath(shardPath);
    final source = await _resolveLocalDirectoryMdsSource(shardPath);
    final (:isCompressed, :remotePath) =
        (remote != null || httpUri != null)
            ? await _resolveRemoteMdsCompression(
                isCompressed: isRemoteExtCompressed,
                remotePath: initialRemoteShardPath,
                indexBytes: source.indexBytes,
                indexName: source.indexName,
                shardFilename: source.shardFilename,
              )
            : (
                isCompressed: isRemoteExtCompressed,
                remotePath: initialRemoteShardPath
              );
    final isRemoteCompressed = isCompressed;
    final resolvedRemoteShardPath = remotePath;
    Future<FieldPreview> peekRemoteField({
      required Uint8List indexBytes,
      required String indexName,
      required String shardFilename,
      required bool compressed,
    }) async {
      final remoteSource = remote;
      if (remoteSource == null) {
        throw const FormatException('Remote directory source is not active.');
      }
      final hostId = explicitRemoteShard?.hostId ?? remoteSource.hostId;
      final host = _findRemoteHost(hostId);
      if (host == null) {
        throw FormatException('Remote host not found: $hostId');
      }
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          targetItemIndex: itemIndex,
        );
        return _peekMdsFieldFromRemoteCompressedStreamWithRetry(
          host: host,
          remotePath: resolvedRemoteShardPath,
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          initialCompressedBytes: initialCompressedBytes,
        );
      }
      return _peekMdsFieldFromRemoteRawStreamWithRetry(
        host: host,
        remotePath: resolvedRemoteShardPath,
        indexBytes: indexBytes,
        indexName: indexName,
        shardFilename: shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }

    Future<FieldPreview> peekHttpField({
      required Uint8List indexBytes,
      required String indexName,
      required String shardFilename,
      required bool compressed,
    }) async {
      final uri = httpUri;
      if (uri == null) {
        throw const FormatException('HTTP source is not active.');
      }
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          targetItemIndex: itemIndex,
        );
        return _remoteOps.peekMdsFieldFromCompressedStreamWithRetry(
          mosaicml: _mosaicml,
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          initialCompressedBytes: initialCompressedBytes,
          compressedShardCacheKey: _httpCompressedMdsCacheKey(uri),
          maxAttempts: 2,
          openCompressedStream: (maxBytes) => _httpDatasets.openRead(
            url: uri,
            maxBytes: maxBytes,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }
      return _remoteOps.peekMdsFieldFromRawStreamWithRetry(
        mosaicml: _mosaicml,
        indexBytes: indexBytes,
        indexName: indexName,
        shardFilename: shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 2,
        openRawStream: () => _httpDatasets.openRead(
          url: uri,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    try {
      if (remote != null) {
        return await peekRemoteField(
          indexBytes: source.indexBytes!,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          compressed: isRemoteCompressed,
        );
      }
      if (httpUri != null) {
        return await peekHttpField(
          indexBytes: source.indexBytes!,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          compressed: isRemoteCompressed,
        );
      }
      return await _mosaicml.peekField(
        indexPath: source.indexPath!,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    } catch (error) {
      if (!_remoteOps.looksLikeMdsCorruption(error.toString())) {
        rethrow;
      }
      final refreshed = await _resolveLocalDirectoryMdsSource(
        shardPath,
        forceRefreshRemoteShard: true,
      );
      if (remote != null) {
        return await peekRemoteField(
          indexBytes: refreshed.indexBytes!,
          indexName: refreshed.indexName,
          shardFilename: refreshed.shardFilename,
          compressed: isRemoteCompressed,
        );
      }
      if (httpUri != null) {
        return await peekHttpField(
          indexBytes: refreshed.indexBytes!,
          indexName: refreshed.indexName,
          shardFilename: refreshed.shardFilename,
          compressed: isRemoteCompressed,
        );
      }
      return _mosaicml.peekField(
        indexPath: refreshed.indexPath!,
        shardFilename: refreshed.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }
  }

  bool isLocalDirectoryMdsShardPath(String path) {
    return _looksLikeMdsShardPath(path);
  }

  ChunkSummary? _findMdsChunkForShard({
    required IndexSummary summary,
    required String shardFilename,
  }) {
    final target = _normalizeChunkName(shardFilename);
    if (target.isEmpty) return null;
    for (final chunk in summary.chunks) {
      final filename = _normalizeChunkName(chunk.filename);
      final basename = _normalizeChunkName(p.basename(chunk.path));
      if (filename == target || basename == target) {
        return chunk;
      }
    }
    return null;
  }

  Future<List<String>> localDirectoryMdsFieldFormats(String shardPath) async {
    final source = await _resolveLocalDirectoryMdsSource(shardPath);
    final summary = source.indexBytes == null
        ? await _mosaicml.loadIndex(source.indexPath!)
        : await _mosaicml.loadIndexFromBytes(
            source.indexBytes!,
            indexName: source.indexName,
          );
    final raw = summary.configRaw['columnEncodings'];
    if (raw is List) {
      return raw.map((value) => value.toString()).toList(growable: false);
    }
    return List<String>.from(summary.dataFormat, growable: false);
  }

  bool _looksLikeMdsShardPath(String path) {
    return _sourceRouter.isMdsShardName(path);
  }

  bool _isMdsCompressedShard(String shardFilename) {
    final lower = shardFilename.trim().toLowerCase();
    if (lower.endsWith('.zst') || lower.endsWith('.zstd')) {
      return true;
    }
    final compression = indexSummary?.compression?.trim().toLowerCase();
    if (compression == null || compression.isEmpty) {
      return false;
    }
    return compression == 'zstd' ||
        compression == 'zst' ||
        compression.contains('zstd');
  }

  /// Resolves compressed remote shard path from the MDS index when the
  /// filename extension alone doesn't indicate compression.
  ///
  /// Returns an updated (isCompressed, remotePath) pair. When the index shows
  /// a `zip_data` entry for the shard, [remotePath] is rewritten to use the
  /// compressed basename (e.g. `shard.00109.mds.zstd`).
  Future<({bool isCompressed, String remotePath})>
      _resolveRemoteMdsCompression({
    required bool isCompressed,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
  }) async {
    if (isCompressed) {
      return (isCompressed: true, remotePath: remotePath);
    }
    final compressedBasename =
        await _mosaicml.resolveCompressedShardBasename(
      indexBytes: indexBytes,
      indexName: indexName,
      shardFilename: shardFilename,
    );
    if (compressedBasename != null) {
      final rawBase = p.basename(remotePath);
      final parent = remotePath.substring(
        0,
        remotePath.length - rawBase.length,
      );
      return (isCompressed: true, remotePath: '$parent$compressedBasename');
    }
    return (isCompressed: isCompressed, remotePath: remotePath);
  }

  Future<
      ({
        String? indexPath,
        Uint8List? indexBytes,
        String indexName,
        String shardFilename
      })> _resolveLocalDirectoryMdsSource(
    String shardPath, {
    bool forceRefreshRemoteShard = false,
  }) async {
    final trimmed = shardPath.trim();
    if (trimmed.isEmpty) {
      throw const FormatException('MDS shard path is empty.');
    }

    final httpUri = _parseHttpSourceUri(trimmed);
    if (httpUri != null) {
      final shardName = p.basename(httpUri.path).trim();
      if (shardName.isEmpty || !_looksLikeMdsShardPath(shardName)) {
        throw FormatException('Not an MDS shard file: $httpUri');
      }
      final directoryUri = _httpDatasets.parentDirectoryUri(httpUri);
      final indexCandidates = DatasetSourceRoutingService.mdsIndexCandidates;
      if (forceRefreshRemoteShard) {
        // HTTP streaming mode uses direct reads; no persistent state to refresh.
      }
      Uint8List? indexBytes;
      String? indexName;
      for (final candidate in indexCandidates) {
        final indexUri = _httpDatasets.resolveFromDirectory(
          directoryUri,
          candidate,
        );
        try {
          indexBytes = await _httpDatasets.readBytes(
            url: indexUri,
            onStatus: (message) {
              statusMessage = message;
            },
          );
          indexName = candidate;
          break;
        } catch (_) {}
      }
      if (indexBytes == null || indexName == null) {
        throw FormatException(
            'MDS index not found near HTTP shard: ${directoryUri.toString()}');
      }
      return (
        indexPath: null,
        indexBytes: indexBytes,
        indexName: indexName,
        shardFilename: shardName
      );
    }

    final explicitRemoteShard = _parseRemoteDirectorySource(trimmed);
    final remote = explicitRemoteShard ?? _activeRemoteDirectorySource;
    if (remote == null) {
      final normalizedPath = _normalizeDatasetDir(trimmed);
      final shardName = p.basename(normalizedPath).trim();
      if (shardName.isEmpty || !_looksLikeMdsShardPath(shardName)) {
        throw FormatException('Not an MDS shard file: $normalizedPath');
      }
      final parentDir = File(normalizedPath).parent.path;
      return (
        indexPath: parentDir,
        indexBytes: null,
        indexName: 'index.json',
        shardFilename: shardName
      );
    }

    final normalizedShardPath = explicitRemoteShard != null
        ? _normalizeRemoteDirectoryPath(explicitRemoteShard.path)
        : _normalizeRemoteDirectoryPath(trimmed);
    if (normalizedShardPath.isEmpty) {
      throw const FormatException('Invalid remote MDS shard path.');
    }
    final shardName = p.basename(normalizedShardPath).trim();
    if (shardName.isEmpty || !_looksLikeMdsShardPath(shardName)) {
      throw FormatException('Not an MDS shard file: $normalizedShardPath');
    }
    final parentRaw = _normalizeRemoteDirectoryPath(
      p.dirname(normalizedShardPath),
    );
    final parentDir = parentRaw == '.' ? '' : parentRaw;
    final hostId = explicitRemoteShard?.hostId ?? remote.hostId;
    final host = _findRemoteHost(hostId);
    if (host == null) {
      throw FormatException('Remote host not found: $hostId');
    }

    final indexCandidates = DatasetSourceRoutingService.mdsIndexCandidates;
    if (forceRefreshRemoteShard) {
      // Remote MDS runs with direct streaming; refresh does not require disk state.
    }

    Uint8List? remoteIndexBytes;
    String? remoteIndexName;
    for (final candidate in indexCandidates) {
      final remoteIndexPath = _joinRemoteDirectoryPath(parentDir, candidate);
      try {
        remoteIndexBytes = await _remoteDatasets.readBytesFile(
          host: host,
          remotePath: remoteIndexPath,
          onStatus: (message) {
            statusMessage = message;
          },
        );
        remoteIndexName = candidate;
        break;
      } catch (_) {}
    }
    if (remoteIndexBytes == null || remoteIndexName == null) {
      final display = parentDir.isEmpty ? '/' : '/$parentDir';
      throw FormatException(
          'MDS index not found in remote directory: $display');
    }

    return (
      indexPath: null,
      indexBytes: remoteIndexBytes,
      indexName: remoteIndexName,
      shardFilename: shardName
    );
  }

  Future<List<ItemMeta>> _listMdsItemsFromRemoteCompressedStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    int? initialCompressedBytes,
    int maxAttempts = 3,
  }) async {
    final resolvedInitialCompressedBytes = initialCompressedBytes ??
        await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
        );
    return _remoteOps.listMdsItemsFromCompressedStreamWithRetry(
      mosaicml: _mosaicml,
      indexBytes: indexBytes,
      indexName: indexName,
      shardFilename: shardFilename,
      initialCompressedBytes: resolvedInitialCompressedBytes,
      compressedShardCacheKey: _remoteCompressedMdsCacheKey(
        host: host,
        remotePath: remotePath,
      ),
      maxAttempts: maxAttempts,
      openCompressedStream: (maxBytes) => _remoteDatasets.openReadFile(
        host: host,
        remotePath: remotePath,
        maxBytes: maxBytes,
        onStatus: (message) {
          statusMessage = message;
        },
      ),
    );
  }

  Future<List<ItemMeta>> _listMdsItemsFromRemoteRawStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    int maxAttempts = 3,
  }) async {
    return _remoteOps.listMdsItemsFromRawStreamWithRetry(
      mosaicml: _mosaicml,
      indexBytes: indexBytes,
      indexName: indexName,
      shardFilename: shardFilename,
      maxAttempts: maxAttempts,
      openRawStream: () => _remoteDatasets.openReadFile(
        host: host,
        remotePath: remotePath,
        onStatus: (message) {
          statusMessage = message;
        },
      ),
    );
  }

  Future<FieldPreview> _peekMdsFieldFromRemoteCompressedStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    int? initialCompressedBytes,
    int maxAttempts = 2,
  }) async {
    final resolvedInitialCompressedBytes = initialCompressedBytes ??
        await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          targetItemIndex: itemIndex,
        );
    return _remoteOps.peekMdsFieldFromCompressedStreamWithRetry(
      mosaicml: _mosaicml,
      indexBytes: indexBytes,
      indexName: indexName,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      initialCompressedBytes: resolvedInitialCompressedBytes,
      compressedShardCacheKey: _remoteCompressedMdsCacheKey(
        host: host,
        remotePath: remotePath,
      ),
      maxAttempts: maxAttempts,
      openCompressedStream: (maxBytes) => _remoteDatasets.openReadFile(
        host: host,
        remotePath: remotePath,
        maxBytes: maxBytes,
        onStatus: (message) {
          statusMessage = message;
        },
      ),
    );
  }

  Future<FieldPreview> _peekMdsFieldFromRemoteRawStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    int maxAttempts = 2,
  }) async {
    return _remoteOps.peekMdsFieldFromRawStreamWithRetry(
      mosaicml: _mosaicml,
      indexBytes: indexBytes,
      indexName: indexName,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxAttempts: maxAttempts,
      openRawStream: () => _remoteDatasets.openReadFile(
        host: host,
        remotePath: remotePath,
        onStatus: (message) {
          statusMessage = message;
        },
      ),
    );
  }

  Future<List<ItemMeta>> _listLitdataItemsFromRemoteStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String chunkFilename,
    int maxAttempts = 3,
  }) async {
    return _remoteOps.listLitdataItemsFromStreamWithRetry(
      litdata: _litdata,
      indexBytes: indexBytes,
      indexName: indexName,
      chunkFilename: chunkFilename,
      maxAttempts: maxAttempts,
      openChunkStream: () => _remoteDatasets.openReadFile(
        host: host,
        remotePath: remotePath,
        onStatus: (message) {
          statusMessage = message;
        },
      ),
    );
  }

  Future<FieldPreview> _peekLitdataFieldFromRemoteStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    int maxAttempts = 2,
  }) async {
    return _remoteOps.peekLitdataFieldFromStreamWithRetry(
      litdata: _litdata,
      indexBytes: indexBytes,
      indexName: indexName,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxAttempts: maxAttempts,
      openChunkStream: () => _remoteDatasets.openReadFile(
        host: host,
        remotePath: remotePath,
        onStatus: (message) {
          statusMessage = message;
        },
      ),
    );
  }

  Future<PreparedMediaResponse> _prepareLitdataAudioFromRemoteStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    int maxAttempts = 2,
  }) async {
    return _remoteOps.prepareLitdataAudioFromStreamWithRetry(
      litdata: _litdata,
      indexBytes: indexBytes,
      indexName: indexName,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxAttempts: maxAttempts,
      openChunkStream: () => _remoteDatasets.openReadFile(
        host: host,
        remotePath: remotePath,
        onStatus: (message) {
          statusMessage = message;
        },
      ),
    );
  }

  Future<PreparedFileResponse> _prepareLitdataFileFromRemoteStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    int maxAttempts = 2,
  }) async {
    return _remoteOps.prepareLitdataFileFromStreamWithRetry(
      litdata: _litdata,
      indexBytes: indexBytes,
      indexName: indexName,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxAttempts: maxAttempts,
      openChunkStream: () => _remoteDatasets.openReadFile(
        host: host,
        remotePath: remotePath,
        onStatus: (message) {
          statusMessage = message;
        },
      ),
    );
  }

  void registerLocalDirectoryItems(Iterable<LocalDirectoryItem> items) {
    for (final item in items) {
      _localDirectoryItemCache[item.path] = item;
    }
  }

  Future<List<LocalDirectoryItem>> listLocalDirectoryItems(
    String directoryPath,
  ) {
    final explicitRemote = _parseRemoteDirectorySource(directoryPath);
    final remote = explicitRemote ?? _activeRemoteDirectorySource;
    if (remote != null) {
      final normalizedPath = explicitRemote != null
          ? _normalizeRemoteDirectoryPath(explicitRemote.path)
          : _normalizeRemoteDirectoryPath(directoryPath);
      return _loadRemoteDirectoryItems(
        hostId: explicitRemote?.hostId ?? remote.hostId,
        directoryPath: normalizedPath,
      );
    }
    final normalizedPath = _normalizeDatasetDir(directoryPath);
    return _loadLocalDirectoryItems(normalizedPath);
  }

  void selectField(int? idx) {
    if (mode == ViewerMode.localDirectory) {
      selectedFieldIndex = idx;
      final selected = selectedLocalDirectoryItem;
      final isMdsSelection = selected != null &&
          !selected.isDirectory &&
          _looksLikeMdsShardPath(selected.path);
      if (!isMdsSelection || idx == null || selectedItemIndex == null) {
        if (idx == null && isMdsSelection && selectedItemIndex != null) {
          localFilePreviewFuture = null;
        }
        _notifyStateChanged();
        return;
      }
      localFilePreviewFuture = _captureFutureError(
        peekLocalDirectoryMdsField(
          shardPath: selected.path,
          itemIndex: selectedItemIndex!,
          fieldIndex: idx,
        ),
        context: 'Local MDS preview failed',
        fallback: _emptyFieldPreview,
      );
      _notifyStateChanged();
      return;
    }
    selectedFieldIndex = idx;
    if (idx == null || selectedItemIndex == null || selectedChunkName == null) {
      fieldPreviewFuture = null;
      mdsFieldPreviewFuture = null;
      _notifyStateChanged();
      return;
    }
    if (mode == ViewerMode.litdataIndex || mode == ViewerMode.litdataChunks) {
      if (indexSummary == null) {
        fieldPreviewFuture = null;
      } else {
        fieldPreviewFuture = _captureFutureError(
          _safeLitdataPreview(
            indexSummary!.indexPath,
            selectedChunkName!,
            selectedItemIndex!,
            idx,
          ),
          context: 'LitData preview failed',
          fallback: _emptyFieldPreview,
        );
      }
    }
    if (mode == ViewerMode.mdsIndex) {
      if (indexSummary == null) {
        mdsFieldPreviewFuture = null;
      } else {
        mdsFieldPreviewFuture = _captureFutureError(
          _previewMdsField(
            indexPath: indexSummary!.indexPath,
            shardFilename: selectedChunkName!,
            itemIndex: selectedItemIndex!,
            fieldIndex: idx,
          ),
          context: 'MosaicML preview failed',
          fallback: _emptyFieldPreview,
        );
      }
    }
    _notifyStateChanged();
  }

  void selectWdsShard(String? filename) {
    selectedShardName = filename;
    wdsSelectedSampleKey = null;
    wdsSelectedMemberPath = null;
    wdsSelectedMemberName = null;
    wdsOffset = 0;
    _loadWdsSamples();
    _syncActiveDatasetSelection();
    _notifyStateChanged();
  }

  void selectWdsSample(String? key, {List<WdsFieldInfo>? fields}) {
    wdsSelectedSampleKey = key;
    if (key == null) {
      wdsSelectedMemberPath = null;
      wdsSelectedMemberName = null;
      wdsPreviewFuture = null;
      _notifyStateChanged();
      return;
    }

    String? retainedPath;
    String? retainedName;
    if (fields != null && fields.isNotEmpty) {
      if (wdsSelectedMemberPath != null) {
        final match = fields.firstWhere(
          (field) => field.memberPath == wdsSelectedMemberPath,
          orElse: () => fields.first,
        );
        if (match.memberPath == wdsSelectedMemberPath) {
          retainedPath = match.memberPath;
          retainedName = match.name;
        }
      }
      if (retainedPath == null && wdsSelectedMemberName != null) {
        final match = fields.firstWhere(
          (field) => field.name == wdsSelectedMemberName,
          orElse: () => fields.first,
        );
        if (match.name == wdsSelectedMemberName) {
          retainedPath = match.memberPath;
          retainedName = match.name;
        }
      }
    }

    if (retainedPath != null) {
      selectWdsMember(retainedPath, memberName: retainedName);
      return;
    }

    wdsSelectedMemberPath = null;
    wdsSelectedMemberName = null;
    wdsPreviewFuture = null;
    _notifyStateChanged();
  }

  void selectWdsMember(String? memberPath, {String? memberName}) {
    wdsSelectedMemberPath = memberPath;
    wdsSelectedMemberName = memberName ?? wdsSelectedMemberName;
    if (memberPath == null ||
        selectedShardName == null ||
        wdsDirSummary == null) {
      wdsPreviewFuture = null;
      _notifyStateChanged();
      return;
    }
    wdsPreviewFuture = _captureFutureError(
      () async {
        if (_activeRemoteWebdatasetSource() != null ||
            _activeHttpWebdatasetShardUri() != null) {
          return _webdataset.peekMemberFromStream(
            shardStream: _openWebdatasetShardStream(selectedShardName!),
            shardFilename: selectedShardName!,
            memberPath: memberPath,
          );
        }
        final dirPath =
            await _resolveWebdatasetDirPathForShard(selectedShardName!);
        return _webdataset.peekMember(
          dirPath: dirPath,
          shardFilename: selectedShardName!,
          memberPath: memberPath,
        );
      }(),
      context: 'WebDataset preview failed',
      fallback: _emptyFieldPreview,
    );
    _notifyStateChanged();
  }

  void setHfConfigSplit(String config, String split) {
    hfConfigOverride = config;
    hfSplitOverride = split;
    hfOffset = 0;
    _syncActiveDatasetSelection();
    _refreshHfPreview();
  }

  void setHfOffset(int offset) {
    hfOffset = offset < 0 ? 0 : offset;
    _refreshHfPreview();
  }

  void selectHfRow(int? rowIndex) {
    hfSelectedRowIndex = rowIndex;
    _notifyStateChanged();
  }

  void selectHfField(String? fieldName) {
    hfSelectedFieldName = fieldName;
    _notifyStateChanged();
  }

  void selectZenodoFile(String? key) {
    zenodoSelectedFileKey = key;
    zenodoSelectedEntryName = null;
    zenodoEntriesOffset = 0;
    _loadZenodoEntries();
    _syncActiveDatasetSelection();
    _notifyStateChanged();
  }

  void selectZenodoEntry(String? name) {
    zenodoSelectedEntryName = name;
    _loadZenodoEntryPreview();
    _notifyStateChanged();
  }

  void setZenodoEntriesOffset(int offset) {
    zenodoEntriesOffset = offset < 0 ? 0 : offset;
    zenodoSelectedEntryName = null;
    _loadZenodoEntries();
    _syncActiveDatasetSelection();
    _notifyStateChanged();
  }

  void setWdsOffset(int offset) {
    wdsOffset = offset < 0 ? 0 : offset;
    _loadWdsSamples();
    _notifyStateChanged();
  }

  void setStatusMessage(String? message) {
    statusMessage = message;
    _notifyStateChanged();
  }

  void _clearFieldPreviewCache() {
    _fieldPreviewRequestId += 1;
    litdataFieldPreviewByIndex = {};
    mdsFieldPreviewByIndex = {};
  }

  bool _isFieldPreviewActive(int requestId, int itemIndex, String chunkName) {
    return requestId == _fieldPreviewRequestId &&
        selectedItemIndex == itemIndex &&
        selectedChunkName == chunkName;
  }

  Future<void> _primeFieldPreviewsForItem(int fieldCount) async {
    if (fieldCount <= 0) return;
    if (selectedItemIndex == null || selectedChunkName == null) return;
    if (indexSummary == null || indexSummary!.indexPath.isEmpty) return;
    if (!(mode == ViewerMode.litdataIndex ||
        mode == ViewerMode.litdataChunks ||
        mode == ViewerMode.mdsIndex)) {
      return;
    }

    final itemIndex = selectedItemIndex!;
    final chunkName = selectedChunkName!;
    final requestId = ++_fieldPreviewRequestId;
    if (mode == ViewerMode.mdsIndex) {
      mdsFieldPreviewByIndex = {};
    } else {
      litdataFieldPreviewByIndex = {};
    }
    _notifyStateChanged();

    for (var fieldIndex = 0; fieldIndex < fieldCount; fieldIndex += 1) {
      if (!_isFieldPreviewActive(requestId, itemIndex, chunkName)) return;
      FieldPreview preview;
      if (mode == ViewerMode.mdsIndex) {
        preview = await _safeMdsPreview(
          indexSummary!.indexPath,
          chunkName,
          itemIndex,
          fieldIndex,
        );
      } else {
        preview = await _safeLitdataPreview(
          indexSummary!.indexPath,
          chunkName,
          itemIndex,
          fieldIndex,
        );
      }
      if (!_isFieldPreviewActive(requestId, itemIndex, chunkName)) return;
      if (mode == ViewerMode.mdsIndex) {
        mdsFieldPreviewByIndex[fieldIndex] = preview;
      } else {
        litdataFieldPreviewByIndex[fieldIndex] = preview;
      }
      _notifyStateChanged();
    }
  }

  Future<FieldPreview> _safeLitdataPreview(
    String indexPath,
    String chunkName,
    int itemIndex,
    int fieldIndex,
  ) async {
    try {
      final remoteSource = _activeRemoteLitdataRuntime();
      if (remoteSource != null) {
        final host = _findRemoteHost(remoteSource.hostId);
        if (host == null) {
          throw FormatException(
              'Remote host not found: ${remoteSource.hostId}');
        }
        final chunkPath =
            _joinRemoteDirectoryPath(remoteSource.directoryPath, chunkName);
        return await _peekLitdataFieldFromRemoteStreamWithRetry(
          host: host,
          remotePath: chunkPath,
          indexBytes: remoteSource.indexBytes,
          indexName: remoteSource.indexName,
          chunkFilename: chunkName,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
        );
      }
      final httpSource = _activeHttpLitdataRuntime();
      if (httpSource != null) {
        final chunkUri = _resolveHttpLitdataChunkUri(httpSource, chunkName);
        return await _remoteOps.peekLitdataFieldFromStreamWithRetry(
          litdata: _litdata,
          indexBytes: httpSource.indexBytes,
          indexName: httpSource.indexName,
          chunkFilename: chunkName,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          maxAttempts: 2,
          openChunkStream: () => _httpDatasets.openRead(
            url: chunkUri,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }
      return await _litdata.peekField(
        indexPath: indexPath,
        chunkFilename: chunkName,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    } catch (_) {
      return _emptyFieldPreview();
    }
  }

  Future<FieldPreview> _safeMdsPreview(
    String indexPath,
    String chunkName,
    int itemIndex,
    int fieldIndex,
  ) async {
    try {
      return await _previewMdsField(
        indexPath: indexPath,
        shardFilename: chunkName,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    } catch (_) {
      return _emptyFieldPreview();
    }
  }

  Future<FieldPreview> _previewMdsField({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final compressed = _isMdsCompressedShard(shardFilename);
    final remoteSource = _activeRemoteMdsRuntime();
    if (remoteSource != null) {
      final host = _findRemoteHost(remoteSource.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remoteSource.hostId}');
      }
      final remotePath =
          _joinRemoteDirectoryPath(remoteSource.directoryPath, shardFilename);
      if (compressed) {
        return _peekMdsFieldFromRemoteCompressedStreamWithRetry(
          host: host,
          remotePath: remotePath,
          indexBytes: remoteSource.indexBytes,
          indexName: remoteSource.indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
        );
      }
      return _peekMdsFieldFromRemoteRawStreamWithRetry(
        host: host,
        remotePath: remotePath,
        indexBytes: remoteSource.indexBytes,
        indexName: remoteSource.indexName,
        shardFilename: shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }

    final httpSource = _activeHttpMdsRuntime();
    if (httpSource != null) {
      final shardUri = _resolveHttpMdsShardUri(httpSource, shardFilename);
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: httpSource.indexBytes,
          indexName: httpSource.indexName,
          shardFilename: shardFilename,
          targetItemIndex: itemIndex,
        );
        return _remoteOps.peekMdsFieldFromCompressedStreamWithRetry(
          mosaicml: _mosaicml,
          indexBytes: httpSource.indexBytes,
          indexName: httpSource.indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          initialCompressedBytes: initialCompressedBytes,
          compressedShardCacheKey: _httpCompressedMdsCacheKey(shardUri),
          maxAttempts: 2,
          openCompressedStream: (maxBytes) => _httpDatasets.openRead(
            url: shardUri,
            maxBytes: maxBytes,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }
      return _remoteOps.peekMdsFieldFromRawStreamWithRetry(
        mosaicml: _mosaicml,
        indexBytes: httpSource.indexBytes,
        indexName: httpSource.indexName,
        shardFilename: shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 2,
        openRawStream: () => _httpDatasets.openRead(
          url: shardUri,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    return _mosaicml.peekField(
      indexPath: indexPath,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }
}
