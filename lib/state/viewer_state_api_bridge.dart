part of 'viewer_state.dart';

extension ViewerStateApiBridge on ViewerState {
  Future<Map<String, dynamic>> apiListOpenedDatasets({
    bool includeDetails = true,
    int? concurrency,
  }) async {
    final active = activeDatasetId;
    final selectedConcurrency = _normalizeApiConcurrency(concurrency);
    final datasets = List<LoadedDatasetSource>.from(openedDatasets);
    final detailPayloads = await _runConcurrentTasks(
      items: datasets,
      maxConcurrency: selectedConcurrency,
      mapper: (dataset) => _inspectOpenedDatasetSnapshot(dataset,
          includeDetails: includeDetails),
    );
    return <String, dynamic>{
      'ok': true,
      'count': detailPayloads.length,
      'activeDatasetId': active,
      'concurrency': selectedConcurrency,
      'datasets': detailPayloads,
      'sourceInput': sourceInput.trim().isEmpty ? null : sourceInput.trim(),
    };
  }

  Future<Map<String, dynamic>?> apiInspectDataset(
    String datasetId, {
    bool includeDetails = true,
  }) async {
    final id = datasetId.trim();
    if (id.isEmpty) return null;
    final dataset = _datasetById(id);
    if (dataset == null) return null;
    return _inspectOpenedDatasetSnapshot(
      dataset,
      includeDetails: includeDetails,
    );
  }

  int _normalizeApiConcurrency(int? concurrency) {
    if (concurrency == null) return 8;
    final raw = concurrency;
    if (raw <= 0) return 1;
    if (raw > 64) return 64;
    return raw;
  }

  Future<List<Map<String, dynamic>>> _runConcurrentTasks<T>({
    required List<T> items,
    required int maxConcurrency,
    required Future<Map<String, dynamic>> Function(T item) mapper,
  }) async {
    if (items.isEmpty) return const <Map<String, dynamic>>[];
    final results = List<Map<String, dynamic>?>.filled(
      items.length,
      null,
      growable: false,
    );
    var next = 0;
    final workerCount = maxConcurrency.clamp(1, items.length);

    Future<void> worker() async {
      while (true) {
        final index = next;
        if (index >= items.length) return;
        next += 1;
        final item = items[index];
        results[index] = await _safeApiInspectSnapshot(item, mapper);
      }
    }

    await Future.wait<void>(List<Future<void>>.generate(
      workerCount,
      (_) => worker(),
    ));
    return results.whereType<Map<String, dynamic>>().toList(growable: false);
  }

  Future<Map<String, dynamic>> _safeApiInspectSnapshot<T>(
    T item,
    Future<Map<String, dynamic>> Function(T) mapper,
  ) async {
    try {
      return await mapper(item);
    } catch (error, stack) {
      AppLogger.error(
        'API dataset inspection failed',
        tag: 'api',
        error: error,
        stackTrace: stack,
      );
      return <String, dynamic>{
        'ok': false,
        'status': 'error',
        'error': error.toString(),
      };
    }
  }

  String _apiDatasetSourceInput(LoadedDatasetSource dataset) {
    final explicit = dataset.payload?.trim();
    if (explicit != null && explicit.isNotEmpty) {
      return explicit;
    }
    return dataset.sourceInput;
  }

  Map<String, dynamic> _baseOpenedDatasetSnapshot(
    LoadedDatasetSource dataset,
  ) {
    return <String, dynamic>{
      'id': dataset.id,
      'identity': dataset.identity,
      'label': dataset.label,
      'mode': dataset.mode.name,
      'sourceInput': dataset.sourceInput,
      'payload': dataset.payload == null || dataset.payload!.trim().isEmpty
          ? null
          : dataset.payload!.trim(),
      'paths': dataset.paths == null || dataset.paths!.isEmpty
          ? null
          : List<String>.from(dataset.paths!),
      'expanded': dataset.expanded,
      'isActive': activeDatasetId == dataset.id,
      'selection': <String, dynamic>{
        'selectedChunkName': dataset.selectedChunkName,
        'selectedShardName': dataset.selectedShardName,
        'selectedHfConfig': dataset.selectedHfConfig,
        'selectedHfSplit': dataset.selectedHfSplit,
        'selectedZenodoFileKey': dataset.selectedZenodoFileKey,
      },
    };
  }

  Future<Map<String, dynamic>> _inspectOpenedDatasetSnapshot(
    LoadedDatasetSource dataset, {
    bool includeDetails = true,
  }) async {
    final snapshot = _baseOpenedDatasetSnapshot(dataset)
      ..['ok'] = true
      ..['status'] = 'ready';
    final details = <String, dynamic>{};
    if (!includeDetails) {
      details['kind'] = _resolveKindForMode(dataset.mode);
      details['status'] = _resolveReadStatus(dataset);
      snapshot['details'] = details;
      snapshot['uniform'] =
          _buildUniformReadView(dataset: dataset, details: details);
      return snapshot;
    }

    switch (dataset.mode) {
      case ViewerMode.litdataIndex:
      case ViewerMode.litdataChunks:
        details['kind'] = 'litdata';
        details.addAll(await _apiDescribeLitdataSummary(dataset));
        break;
      case ViewerMode.mdsIndex:
        details['kind'] = 'mosaicml';
        details.addAll(await _apiDescribeMdsSummary(dataset));
        break;
      case ViewerMode.webdatasetDir:
        details['kind'] = 'webdataset';
        details.addAll(await _apiDescribeWebdatasetSummary(dataset));
        break;
      case ViewerMode.localDirectory:
        details['kind'] = 'localDirectory';
        details.addAll(await _apiDescribeLocalDirectorySummary(dataset));
        break;
      case ViewerMode.huggingface:
        details['kind'] = 'huggingface';
        details.addAll(await _apiDescribeHuggingFaceSummary(dataset));
        break;
      case ViewerMode.zenodo:
        details['kind'] = 'zenodo';
        details.addAll(await _apiDescribeZenodoSummary(dataset));
        break;
    }

    snapshot['details'] = details;
    snapshot['uniform'] = _buildUniformReadView(
      dataset: dataset,
      details: details,
    );
    return snapshot;
  }

  String _resolveKindForMode(ViewerMode mode) {
    switch (mode) {
      case ViewerMode.litdataIndex:
      case ViewerMode.litdataChunks:
        return 'litdata';
      case ViewerMode.mdsIndex:
        return 'mosaicml';
      case ViewerMode.webdatasetDir:
        return 'webdataset';
      case ViewerMode.localDirectory:
        return 'localDirectory';
      case ViewerMode.huggingface:
        return 'huggingface';
      case ViewerMode.zenodo:
        return 'zenodo';
    }
  }

  String _resolveReadStatus(LoadedDatasetSource dataset) {
    return dataset.mode == ViewerMode.litdataIndex ||
            dataset.mode == ViewerMode.litdataChunks ||
            dataset.mode == ViewerMode.mdsIndex
        ? (dataset.indexSummary == null ? 'notLoaded' : 'ready')
        : dataset.mode == ViewerMode.webdatasetDir
            ? (dataset.wdsDirSummary == null ? 'notLoaded' : 'ready')
            : dataset.mode == ViewerMode.huggingface
                ? (dataset.hfPreview == null ? 'notLoaded' : 'ready')
                : dataset.mode == ViewerMode.zenodo
                    ? (dataset.zenodoRecord == null ? 'notLoaded' : 'ready')
                    : (dataset.id.isNotEmpty ? 'ready' : 'notLoaded');
  }

  Map<String, dynamic> _buildUniformReadView({
    required LoadedDatasetSource dataset,
    required Map<String, dynamic> details,
  }) {
    final kind =
        details['kind']?.toString() ?? _resolveKindForMode(dataset.mode);
    final status = details['status']?.toString() ?? 'unknown';
    return <String, dynamic>{
      'mode': dataset.mode.name,
      'kind': kind,
      'status': status,
      'stats': <String, dynamic>{
        'recordCount': _extractCount(details),
        'sizeBytes': _extractBytes(details),
        'hasMore': _extractHasMore(details),
      },
      'selection': <String, dynamic>{
        'selectedChunkName': dataset.selectedChunkName,
        'selectedShardName': dataset.selectedShardName,
        'selectedHfConfig': dataset.selectedHfConfig,
        'selectedHfSplit': dataset.selectedHfSplit,
        'selectedZenodoFileKey': dataset.selectedZenodoFileKey,
      },
    };
  }

  int? _extractCount(Map<String, dynamic> details) {
    return _readInt(details, const <String>[
      'itemCount',
      'fileCount',
      'shardCount',
      'approxChunkItems',
      'numRowsTotal',
      'length',
      'rowCount',
    ]);
  }

  int? _extractBytes(Map<String, dynamic> details) {
    return _readInt(details, const <String>[
      'sizeBytes',
      'totalFileBytes',
      'approxChunkBytes',
      'totalBytes',
      'chunkBytes',
    ]);
  }

  bool? _extractHasMore(Map<String, dynamic> details) {
    final keys = const <String>[
      'hasMoreChunks',
      'hasMoreShards',
      'hasMoreItems',
      'hasMoreRows',
      'hasMoreFiles',
    ];
    for (final key in keys) {
      final value = details[key];
      if (value is bool) return value;
    }
    return null;
  }

  int? _readInt(Map<String, dynamic> details, List<String> keys) {
    for (final key in keys) {
      final value = details[key];
      if (value is int) return value;
      if (value is num) return value.toInt();
      if (value is String) {
        final parsed = int.tryParse(value);
        if (parsed != null) return parsed;
      }
    }
    return null;
  }

  Future<Map<String, dynamic>> _apiDescribeLitdataSummary(
    LoadedDatasetSource dataset,
  ) async {
    final summary = await _apiResolveIndexSummary(dataset);
    var numRecords = 0;
    var totalBytes = 0;
    for (final chunk in summary.chunks) {
      numRecords += chunk.chunkSize;
      if (chunk.chunkBytes > 0) {
        totalBytes += chunk.chunkBytes;
      }
    }
    return <String, dynamic>{
      'kind': 'litdata',
      'status': 'ready',
      'indexPath': summary.indexPath,
      'rootDir': summary.rootDir,
      'dataFormat': summary.dataFormat,
      'compression': summary.compression,
      'chunkSize': summary.chunkSize,
      'chunkBytes': summary.chunkBytes,
      'chunks': summary.chunks
          .map(
            (chunk) => <String, dynamic>{
              'filename': chunk.filename,
              'path': chunk.path,
              'chunkSize': chunk.chunkSize,
              'chunkBytes': chunk.chunkBytes,
              'dim': chunk.dim,
              'exists': chunk.exists,
            },
          )
          .toList(growable: false),
      'chunkCount': summary.chunks.length,
      'approxChunkItems': numRecords,
      'approxChunkBytes': totalBytes,
    };
  }

  Future<Map<String, dynamic>> _apiDescribeMdsSummary(
    LoadedDatasetSource dataset,
  ) async {
    final summary = await _apiResolveIndexSummary(dataset);
    var numRecords = 0;
    var totalBytes = 0;
    for (final chunk in summary.chunks) {
      numRecords += chunk.chunkSize;
      if (chunk.chunkBytes > 0) {
        totalBytes += chunk.chunkBytes;
      }
    }
    return <String, dynamic>{
      'kind': 'mosaicml',
      'status': 'ready',
      'indexPath': summary.indexPath,
      'rootDir': summary.rootDir,
      'dataFormat': summary.dataFormat,
      'compression': summary.compression,
      'chunkSize': summary.chunkSize,
      'chunkBytes': summary.chunkBytes,
      'chunks': summary.chunks
          .map(
            (chunk) => <String, dynamic>{
              'filename': chunk.filename,
              'path': chunk.path,
              'chunkSize': chunk.chunkSize,
              'chunkBytes': chunk.chunkBytes,
              'dim': chunk.dim,
              'exists': chunk.exists,
            },
          )
          .toList(growable: false),
      'chunkCount': summary.chunks.length,
      'approxChunkItems': numRecords,
      'approxChunkBytes': totalBytes,
    };
  }

  Future<Map<String, dynamic>> _apiDescribeWebdatasetSummary(
    LoadedDatasetSource dataset,
  ) async {
    final summary = await _apiResolveWebdatasetSummary(dataset);
    return <String, dynamic>{
      'kind': 'webdataset',
      'status': 'ready',
      'dirPath': summary.dirPath,
      'shardCount': summary.shards.length,
      'shards': summary.shards
          .map(
            (shard) => <String, dynamic>{
              'filename': shard.filename,
              'path': shard.path,
              'bytes': shard.bytes,
              'exists': shard.exists,
            },
          )
          .toList(growable: false),
      'totalBytes': summary.shards.fold<int>(0, (value, shard) {
        final bytes = shard.bytes;
        return bytes < 0 ? value : value + bytes;
      }),
    };
  }

  Future<Map<String, dynamic>> _apiDescribeLocalDirectorySummary(
    LoadedDatasetSource dataset,
  ) async {
    final items = await _apiLoadLocalDirectoryItems(dataset);
    final files =
        items.where((item) => !item.isDirectory).toList(growable: false);
    final directories =
        items.where((item) => item.isDirectory).toList(growable: false);
    final totalSize = files.fold<int>(0, (value, item) {
      final itemSize = item.size;
      return itemSize == null ? value : value + itemSize;
    });
    return <String, dynamic>{
      'kind': 'localDirectory',
      'status': 'ready',
      'path': _apiDatasetSourceInput(dataset),
      'itemCount': items.length,
      'fileCount': files.length,
      'directoryCount': directories.length,
      'totalFileBytes': totalSize,
      'items': items
          .map(
            (item) => <String, dynamic>{
              'name': item.name,
              'path': item.path,
              'isDirectory': item.isDirectory,
              'size': item.size,
              'modifiedAt': item.modifiedAt?.toIso8601String(),
            },
          )
          .toList(growable: false),
    };
  }

  Future<Map<String, dynamic>> _apiDescribeHuggingFaceSummary(
    LoadedDatasetSource dataset,
  ) async {
    final preview = await _apiLoadHfPreview(dataset);
    final features = preview.features
        .map(
          (feature) => <String, dynamic>{
            'name': feature.name,
            'dtype': feature.dtype,
          },
        )
        .toList(growable: false);
    final rowCount = preview.rows.length;
    return <String, dynamic>{
      'kind': 'huggingface',
      'status': 'ready',
      'dataset': preview.dataset,
      'config': preview.config,
      'split': preview.split,
      'numRowsTotal': preview.numRowsTotal,
      'offset': preview.offset,
      'length': preview.length,
      'partial': preview.partial,
      'featureCount': preview.features.length,
      'features': features,
      'firstRows': rowCount == 0
          ? const <Map<String, dynamic>>[]
          : _takeSafeRows(preview.rows, count: 3),
      'hasMoreRows': rowCount > 3,
    };
  }

  Future<Map<String, dynamic>> _apiDescribeZenodoSummary(
    LoadedDatasetSource dataset,
  ) async {
    final record = await _apiLoadZenodoRecord(dataset);
    final files = record.files;
    final totalBytes = files.fold<int>(0, (value, file) => value + file.size);
    return <String, dynamic>{
      'kind': 'zenodo',
      'status': 'ready',
      'recordId': record.recordId,
      'title': record.title,
      'doi': record.doi,
      'doiUrl': record.doiUrl,
      'publicationDate': record.publicationDate,
      'version': record.version,
      'accessRight': record.accessRight,
      'recordUrl': record.recordUrl,
      'creatorCount': record.creators.length,
      'fileCount': files.length,
      'totalFileBytes': totalBytes,
      'files': files
          .take(100)
          .map(
            (file) => <String, dynamic>{
              'key': file.key,
              'size': file.size,
              'checksum': file.checksum,
              'contentUrl': file.contentUrl,
            },
          )
          .toList(growable: false),
      'hasMoreFiles': files.length > 100,
    };
  }

  Future<IndexSummary> _apiResolveIndexSummary(
      LoadedDatasetSource dataset) async {
    if (dataset.indexSummary != null) {
      return dataset.indexSummary!;
    }
    final source = _apiDatasetSourceInput(dataset).trim();
    if (source.isEmpty) {
      throw FormatException('Dataset source is empty.');
    }

    if (dataset.mode == ViewerMode.litdataIndex) {
      final httpUri = _parseHttpSourceUri(source);
      if (httpUri != null) {
        final resolved = await _resolveHttpLitdataSource(httpUri);
        final value = await _litdata.loadIndexFromBytes(
          resolved.indexBytes,
          indexName: resolved.indexName,
        );
        dataset.indexSummary = value;
        return value;
      }
      final remote = _parseRemoteDirectorySource(source);
      if (remote != null) {
        final resolved = await _resolveRemoteLitdataSource(remote);
        final value = await _litdata.loadIndexFromBytes(
          resolved.indexBytes,
          indexName: resolved.indexName,
        );
        _remoteLitdataSource = resolved;
        dataset.indexSummary = value;
        return value;
      }
      final value = await _litdata.loadIndex(_normalizeDatasetDir(source));
      dataset.indexSummary = value;
      return value;
    }

    if (dataset.mode == ViewerMode.litdataChunks) {
      List<String> chunkPaths = dataset.paths == null || dataset.paths!.isEmpty
          ? const <String>[]
          : List<String>.from(dataset.paths!);
      if (chunkPaths.isEmpty) {
        final normalized = _normalizeDatasetDir(source);
        chunkPaths = await _litdata
            .listChunkFiles(normalized)
            .catchError((_) => const <String>[]);
      }
      if (chunkPaths.isEmpty) {
        throw FormatException(
            'No LitData chunk paths available for ${dataset.id}.');
      }
      final value = await _litdata.loadChunkList(chunkPaths);
      dataset.indexSummary = value;
      return value;
    }

    if (dataset.mode == ViewerMode.mdsIndex) {
      final remote = _parseRemoteDirectorySource(source);
      if (remote != null) {
        final resolved = await _resolveRemoteMdsSource(remote);
        final value = await _mosaicml.loadIndexFromBytes(
          resolved.indexBytes,
          indexName: resolved.indexName,
        );
        dataset.indexSummary = value;
        return value;
      }
      final httpUri = _parseHttpSourceUri(source);
      if (httpUri != null) {
        final resolved = await _resolveHttpMdsSource(httpUri);
        final value = await _mosaicml.loadIndexFromBytes(
          resolved.indexBytes,
          indexName: resolved.indexName,
        );
        dataset.indexSummary = value;
        return value;
      }
      final value = await _mosaicml.loadIndex(_normalizeDatasetDir(source));
      dataset.indexSummary = value;
      return value;
    }

    throw FormatException(
      'Cannot resolve index summary for mode ${dataset.mode.name}.',
    );
  }

  Future<WdsDirSummary> _apiResolveWebdatasetSummary(
    LoadedDatasetSource dataset,
  ) async {
    if (dataset.wdsDirSummary != null) {
      return dataset.wdsDirSummary!;
    }
    final source = _apiDatasetSourceInput(dataset);
    if (source.trim().isEmpty) {
      throw FormatException('Dataset source is empty.');
    }
    final httpUri = _parseHttpSourceUri(source);
    if (httpUri != null) {
      final value = await _resolveHttpWebdatasetDirSummary(
        source: httpUri,
        sourceInput: source,
      );
      dataset.wdsDirSummary = value;
      return value;
    }
    final remote = _parseRemoteDirectorySource(source);
    if (remote != null) {
      final hintedFormat = _sourceRouter.detectFormatFromPath(remote.path);
      if (hintedFormat == DatasetSourceFormat.webdatasetShard) {
        final shards = <WdsShardSummary>[
          WdsShardSummary(
            filename: p.basename(remote.path).trim().isEmpty
                ? remote.path
                : p.basename(remote.path).trim(),
            path: remote.path,
            bytes: 0,
            exists: true,
          ),
        ];
        final value = WdsDirSummary(dirPath: source, shards: shards);
        dataset.wdsDirSummary = value;
        return value;
      }

      List<WdsShardSummary>? shards;
      final host = _findRemoteHost(remote.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remote.hostId}');
      }
      try {
        shards = (await _remoteDatasets.listEntries(
          host: host,
          directoryPath: remote.path,
        ))
            .where((entry) =>
                !entry.isDirectory && _looksLikeWebdatasetShardName(entry.name))
            .map(
              (entry) => WdsShardSummary(
                filename: entry.name,
                path: entry.path,
                bytes: entry.sizeBytes ?? 0,
                exists: true,
              ),
            )
            .toList(growable: false)
          ..sort((a, b) => a.filename.compareTo(b.filename));
      } catch (error) {
        throw FormatException(
          'Failed to load WebDataset shards from remote source: ${remote.path}; $error',
        );
      }

      final value = WdsDirSummary(dirPath: source, shards: shards);
      dataset.wdsDirSummary = value;
      return value;
    }

    final normalized = _normalizeDatasetDir(source);
    final value = await _webdataset.loadDir(normalized);
    dataset.wdsDirSummary = value;
    return value;
  }

  Future<List<LocalDirectoryItem>> _apiLoadLocalDirectoryItems(
    LoadedDatasetSource dataset,
  ) async {
    final source = _apiDatasetSourceInput(dataset);
    if (source.trim().isEmpty) return const <LocalDirectoryItem>[];
    final remote = _parseRemoteDirectorySource(source);
    if (remote != null) {
      return _loadRemoteDirectoryItems(
        hostId: remote.hostId,
        directoryPath: remote.path,
      );
    }
    final httpUri = _parseHttpSourceUri(source);
    if (httpUri != null) {
      return _resolveHttpDirectoryItems(httpUri);
    }
    final normalized = _normalizeDatasetDir(source);
    return _loadLocalDirectoryItems(normalized);
  }

  Future<HfDatasetPreview> _apiLoadHfPreview(
      LoadedDatasetSource dataset) async {
    if (dataset.hfPreview != null) {
      return dataset.hfPreview!;
    }
    final input = _apiDatasetSourceInput(dataset);
    if (input.isEmpty) {
      throw FormatException('Hugging Face dataset input is empty.');
    }
    final preview = await _huggingface.datasetPreview(
      input: input,
      config: dataset.selectedHfConfig,
      split: dataset.selectedHfSplit,
      offset: 0,
      length: _hfPageSize,
      token: hfToken,
      maxFeatureCount: _hfFeatureChunkSize,
    );
    dataset.hfPreview = preview;
    return preview;
  }

  Future<ZenodoRecordSummary> _apiLoadZenodoRecord(
    LoadedDatasetSource dataset,
  ) async {
    if (dataset.zenodoRecord != null) {
      return dataset.zenodoRecord!;
    }
    final input = _apiDatasetSourceInput(dataset);
    if (input.isEmpty) {
      throw FormatException('Zenodo dataset input is empty.');
    }
    final record = await _zenodo.recordSummary(input);
    dataset.zenodoRecord = record;
    return record;
  }

  Future<FieldPreview> apiPeekLitdataField({
    required LoadedDatasetSource dataset,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final resolvedChunk = chunkFilename.trim();
    if (resolvedChunk.isEmpty) {
      throw const FormatException('chunk filename is empty');
    }
    final summary = await _apiResolveIndexSummary(dataset);
    final source = _apiDatasetSourceInput(dataset).trim();
    if (source.isEmpty) {
      throw const FormatException('Dataset source is empty.');
    }

    final remote = _parseRemoteDirectorySource(source);
    if (remote != null) {
      final active = _activeRemoteLitdataRuntime();
      final resolved = _matchesRemoteLitdataSource(active, remote)
          ? active!
          : await _resolveRemoteLitdataSource(remote);
      _remoteLitdataSource = resolved;
      final host = _findRemoteHost(resolved.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${resolved.hostId}');
      }
      final remoteChunkPath =
          _joinRemoteDirectoryPath(resolved.directoryPath, resolvedChunk);
      return _peekLitdataFieldFromRemoteStreamWithRetry(
        host: host,
        remotePath: remoteChunkPath,
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        chunkFilename: resolvedChunk,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 3,
      );
    }

    final httpUri = _parseHttpSourceUri(source);
    if (httpUri != null) {
      final resolved = await _resolveHttpLitdataSource(httpUri);
      final chunkUri = _resolveHttpLitdataChunkUri(resolved, resolvedChunk);
      return _remoteOps.peekLitdataFieldFromStreamWithRetry(
        litdata: _litdata,
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        chunkFilename: resolvedChunk,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 3,
        openChunkStream: () => _httpDatasets.openRead(
          url: chunkUri,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    return _litdata.peekField(
      indexPath: summary.indexPath,
      chunkFilename: resolvedChunk,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  bool _matchesRemoteLitdataSource(
    _RemoteLitdataSource? source,
    _RemoteDirectorySource remote,
  ) {
    if (source == null) return false;
    if (source.hostId != remote.hostId) return false;
    final normalizedRequested = _normalizeRemoteDirectoryPath(remote.path);
    var requestedDirectory = normalizedRequested;
    final hintedName = p.basename(normalizedRequested).trim();
    if (_looksLikeLitdataIndexName(hintedName)) {
      requestedDirectory = _normalizeRemoteDirectoryPath(
        p.dirname(normalizedRequested),
      );
      if (requestedDirectory == '.') {
        requestedDirectory = '';
      }
    }
    final normalizedResolved = _normalizeRemoteDirectoryPath(
      source.directoryPath,
    );
    return requestedDirectory == normalizedResolved;
  }

  Future<ItemPage> apiListLitdataItemsPage({
    required LoadedDatasetSource dataset,
    required String chunkFilename,
    int offset = 0,
    int length = 200,
  }) async {
    final resolvedChunk = chunkFilename.trim();
    if (resolvedChunk.isEmpty) {
      throw const FormatException('chunk filename is empty');
    }
    final summary = await _apiResolveIndexSummary(dataset);
    final safeOffset = offset < 0 ? 0 : offset;
    final safeLength = length < 1 ? 1 : length;
    final source = _apiDatasetSourceInput(dataset).trim();

    final remote = _parseRemoteDirectorySource(source);
    final httpUri = _parseHttpSourceUri(source);
    if (remote != null || httpUri != null) {
      final chunk = _apiFindChunk(summary, resolvedChunk);
      return _apiBuildSyntheticItemPage(
        totalItems: chunk.chunkSize,
        offset: safeOffset,
        length: safeLength,
        fieldCount: summary.dataFormat.length,
        chunkBytes: chunk.chunkBytes,
      );
    }

    return _litdata.listChunkItemsPaged(
      summary.indexPath,
      resolvedChunk,
      offset: safeOffset,
      length: safeLength,
    );
  }

  Future<FieldPreview> apiPeekMdsField({
    required LoadedDatasetSource dataset,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final resolvedShard = shardFilename.trim();
    if (resolvedShard.isEmpty) {
      throw const FormatException('shard filename is empty');
    }
    final summary = await _apiResolveIndexSummary(dataset);
    final source = _apiDatasetSourceInput(dataset).trim();
    if (source.isEmpty) {
      throw const FormatException('Dataset source is empty.');
    }
    final compressed = _apiIsMdsCompressedShard(
      summary: summary,
      shardFilename: resolvedShard,
    );

    final remote = _parseRemoteDirectorySource(source);
    if (remote != null) {
      final resolved = await _resolveRemoteMdsSource(remote);
      final host = _findRemoteHost(resolved.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${resolved.hostId}');
      }
      final remoteShardPath = await _resolveRemoteMdsShardPath(
        host: host,
        directoryPath: resolved.directoryPath,
        shardFilename: resolvedShard,
        compressed: compressed,
      );
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          targetItemIndex: itemIndex,
        );
        return _mosaicml.peekFieldFromZstdCompressedStream(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          decodedShardCacheKey: _remoteCompressedMdsCacheKey(
            host: host,
            remotePath: remoteShardPath,
          ),
          openCompressedStream: (maxBytes) {
            final effectiveBytes = _apiResolvePreparedCompressedStreamMaxBytes(
                maxBytes, initialCompressedBytes);
            return _remoteDatasets.openReadFile(
              host: host,
              remotePath: remoteShardPath,
              maxBytes: effectiveBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            );
          },
        );
      }
      return _mosaicml.peekFieldFromRawStream(
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        shardFilename: resolvedShard,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        rawStream: _remoteDatasets.openReadFile(
          host: host,
          remotePath: remoteShardPath,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    final httpUri = _parseHttpSourceUri(source);
    if (httpUri != null) {
      final resolved = await _resolveHttpMdsSource(httpUri);
      final shardUri = _resolveHttpMdsShardUri(resolved, resolvedShard);
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          targetItemIndex: itemIndex,
        );
        return _mosaicml.peekFieldFromZstdCompressedStream(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          decodedShardCacheKey: _httpCompressedMdsCacheKey(shardUri),
          openCompressedStream: (maxBytes) {
            final effectiveBytes = _apiResolvePreparedCompressedStreamMaxBytes(
                maxBytes, initialCompressedBytes);
            return _httpDatasets.openRead(
              url: shardUri,
              maxBytes: effectiveBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            );
          },
        );
      }
      return _mosaicml.peekFieldFromRawStream(
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        shardFilename: resolvedShard,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        rawStream: _httpDatasets.openRead(
          url: shardUri,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    return _mosaicml.peekField(
      indexPath: summary.indexPath,
      shardFilename: resolvedShard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<ScanResult> apiScanShardTextFields({
    required LoadedDatasetSource dataset,
    required String shardFilename,
    required int textFieldIndex,
    int? idFieldIndex,
    int? audioFieldIndex,
  }) async {
    final resolvedShard = shardFilename.trim();
    if (resolvedShard.isEmpty) {
      throw const FormatException('shard filename is empty');
    }
    final summary = await _apiResolveIndexSummary(dataset);
    final source = _apiDatasetSourceInput(dataset).trim();
    if (source.isEmpty) {
      throw const FormatException('Dataset source is empty.');
    }
    final compressed = _apiIsMdsCompressedShard(
      summary: summary,
      shardFilename: resolvedShard,
    );

    final remote = _parseRemoteDirectorySource(source);
    if (remote != null) {
      final resolved = await _resolveRemoteMdsSource(remote);
      final host = _findRemoteHost(resolved.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${resolved.hostId}');
      }

      final remoteShardPath = await _resolveRemoteMdsShardPath(
        host: host,
        directoryPath: resolved.directoryPath,
        shardFilename: resolvedShard,
        compressed: compressed,
      );
      if (compressed) {
        return _mosaicml.scanTextFieldsFromZstdCompressedStream(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          textFieldIndex: textFieldIndex,
          idFieldIndex: idFieldIndex,
          audioFieldIndex: audioFieldIndex,
          decodedShardCacheKey: _remoteCompressedMdsCacheKey(
            host: host,
            remotePath: remoteShardPath,
          ),
          openCompressedStream: (maxBytes) {
            return _remoteDatasets.openReadFile(
              host: host,
              remotePath: remoteShardPath,
              maxBytes: maxBytes,
              onStatus: (message) {},
            );
          },
        );
      }

      // Uncompressed: read raw shard bytes via SMB and scan in memory.
      final rawBytes = await _remoteDatasets.readBytesFile(
        host: host,
        remotePath: remoteShardPath,
        onStatus: (message) {},
      );
      return _mosaicml.scanTextFieldsFromDecodedBytes(
        decodedBytes: rawBytes,
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        shardFilename: resolvedShard,
        textFieldIndex: textFieldIndex,
        idFieldIndex: idFieldIndex,
        audioFieldIndex: audioFieldIndex,
      );
    }

    return _mosaicml.scanTextFields(
      indexPath: summary.indexPath,
      shardFilename: resolvedShard,
      textFieldIndex: textFieldIndex,
      idFieldIndex: idFieldIndex,
      audioFieldIndex: audioFieldIndex,
    );
  }

  Future<PreparedFileResponse> apiPrepareMdsFieldFile({
    required LoadedDatasetSource dataset,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final resolvedShard = shardFilename.trim();
    if (resolvedShard.isEmpty) {
      throw const FormatException('shard filename is empty');
    }
    final summary = await _apiResolveIndexSummary(dataset);
    final source = _apiDatasetSourceInput(dataset).trim();
    if (source.isEmpty) {
      throw const FormatException('Dataset source is empty.');
    }
    final compressed = _apiIsMdsCompressedShard(
      summary: summary,
      shardFilename: resolvedShard,
    );

    final remote = _parseRemoteDirectorySource(source);
    if (remote != null) {
      final resolved = await _resolveRemoteMdsSource(remote);
      final host = _findRemoteHost(resolved.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${resolved.hostId}');
      }
      final remoteShardPath = await _resolveRemoteMdsShardPath(
        host: host,
        directoryPath: resolved.directoryPath,
        shardFilename: resolvedShard,
        compressed: compressed,
      );
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          targetItemIndex: itemIndex,
        );
        return _mosaicml.prepareFieldFileFromZstdCompressedStream(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          decodedShardCacheKey: _remoteCompressedMdsCacheKey(
            host: host,
            remotePath: remoteShardPath,
          ),
          openCompressedStream: (maxBytes) {
            final effectiveBytes = _apiResolvePreparedCompressedStreamMaxBytes(
                maxBytes, initialCompressedBytes);
            return _remoteDatasets.openReadFile(
              host: host,
              remotePath: remoteShardPath,
              maxBytes: effectiveBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            );
          },
        );
      }
      return _mosaicml.prepareFieldFileFromRawStream(
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        shardFilename: resolvedShard,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        rawStream: _remoteDatasets.openReadFile(
          host: host,
          remotePath: remoteShardPath,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    final httpUri = _parseHttpSourceUri(source);
    if (httpUri != null) {
      final resolved = await _resolveHttpMdsSource(httpUri);
      final shardUri = _resolveHttpMdsShardUri(resolved, resolvedShard);
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          targetItemIndex: itemIndex,
        );
        return _mosaicml.prepareFieldFileFromZstdCompressedStream(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          decodedShardCacheKey: _httpCompressedMdsCacheKey(shardUri),
          openCompressedStream: (maxBytes) {
            final effectiveBytes = _apiResolvePreparedCompressedStreamMaxBytes(
                maxBytes, initialCompressedBytes);
            return _httpDatasets.openRead(
              url: shardUri,
              maxBytes: effectiveBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            );
          },
        );
      }
      return _mosaicml.prepareFieldFileFromRawStream(
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        shardFilename: resolvedShard,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        rawStream: _httpDatasets.openRead(
          url: shardUri,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    return _mosaicml.prepareFieldFile(
      indexPath: summary.indexPath,
      shardFilename: resolvedShard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<PreparedMediaResponse> apiPrepareMdsFieldAudio({
    required LoadedDatasetSource dataset,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final resolvedShard = shardFilename.trim();
    if (resolvedShard.isEmpty) {
      throw const FormatException('shard filename is empty');
    }
    final summary = await _apiResolveIndexSummary(dataset);
    final source = _apiDatasetSourceInput(dataset).trim();
    if (source.isEmpty) {
      throw const FormatException('Dataset source is empty.');
    }
    final compressed = _apiIsMdsCompressedShard(
      summary: summary,
      shardFilename: resolvedShard,
    );

    final remote = _parseRemoteDirectorySource(source);
    if (remote != null) {
      final resolved = await _resolveRemoteMdsSource(remote);
      final host = _findRemoteHost(resolved.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${resolved.hostId}');
      }
      final remoteShardPath = await _resolveRemoteMdsShardPath(
        host: host,
        directoryPath: resolved.directoryPath,
        shardFilename: resolvedShard,
        compressed: compressed,
      );
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          targetItemIndex: itemIndex,
        );
        return _mosaicml.prepareAudioPreviewFromZstdCompressedStream(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          decodedShardCacheKey: _remoteCompressedMdsCacheKey(
            host: host,
            remotePath: remoteShardPath,
          ),
          openCompressedStream: (maxBytes) {
            final effectiveBytes = _apiResolveCompressedStreamMaxBytes(
                maxBytes, initialCompressedBytes);
            return _remoteDatasets.openReadFile(
              host: host,
              remotePath: remoteShardPath,
              maxBytes: effectiveBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            );
          },
        );
      }
      return _mosaicml.prepareAudioPreviewFromRawStream(
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        shardFilename: resolvedShard,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        rawStream: _remoteDatasets.openReadFile(
          host: host,
          remotePath: remoteShardPath,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    final httpUri = _parseHttpSourceUri(source);
    if (httpUri != null) {
      final resolved = await _resolveHttpMdsSource(httpUri);
      final shardUri = _resolveHttpMdsShardUri(resolved, resolvedShard);
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          targetItemIndex: itemIndex,
        );
        return _mosaicml.prepareAudioPreviewFromZstdCompressedStream(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          decodedShardCacheKey: _httpCompressedMdsCacheKey(shardUri),
          openCompressedStream: (maxBytes) {
            final effectiveBytes = _apiResolveCompressedStreamMaxBytes(
                maxBytes, initialCompressedBytes);
            return _httpDatasets.openRead(
              url: shardUri,
              maxBytes: effectiveBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            );
          },
        );
      }
      return _mosaicml.prepareAudioPreviewFromRawStream(
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        shardFilename: resolvedShard,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        rawStream: _httpDatasets.openRead(
          url: shardUri,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    return _mosaicml.prepareAudioPreview(
      indexPath: summary.indexPath,
      shardFilename: resolvedShard,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<ItemPage> apiListMdsItemsPage({
    required LoadedDatasetSource dataset,
    required String shardFilename,
    int offset = 0,
    int length = 200,
  }) async {
    final resolvedShard = shardFilename.trim();
    if (resolvedShard.isEmpty) {
      throw const FormatException('shard filename is empty');
    }
    final summary = await _apiResolveIndexSummary(dataset);
    final source = _apiDatasetSourceInput(dataset).trim();
    final safeOffset = offset < 0 ? 0 : offset;
    final safeLength = length < 1 ? 1 : length;
    final compressed = _apiIsMdsCompressedShard(
      summary: summary,
      shardFilename: resolvedShard,
    );

    final remote = _parseRemoteDirectorySource(source);
    if (remote != null) {
      final resolved = await _resolveRemoteMdsSource(remote);
      final host = _findRemoteHost(resolved.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${resolved.hostId}');
      }
      final remoteShardPath = await _resolveRemoteMdsShardPath(
        host: host,
        directoryPath: resolved.directoryPath,
        shardFilename: resolvedShard,
        compressed: compressed,
      );
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
        );
        return _mosaicml.listSamplesPagedFromZstdCompressedStream(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          offset: safeOffset,
          length: safeLength,
          decodedShardCacheKey: _remoteCompressedMdsCacheKey(
            host: host,
            remotePath: remoteShardPath,
          ),
          openCompressedStream: (maxBytes) {
            final effectiveBytes = _apiResolveCompressedStreamMaxBytes(
                maxBytes, initialCompressedBytes);
            return _remoteDatasets.openReadFile(
              host: host,
              remotePath: remoteShardPath,
              maxBytes: effectiveBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            );
          },
        );
      }
      return _mosaicml.listSamplesPagedFromRawStream(
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        shardFilename: resolvedShard,
        offset: safeOffset,
        length: safeLength,
        rawStream: _remoteDatasets.openReadFile(
          host: host,
          remotePath: remoteShardPath,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    final httpUri = _parseHttpSourceUri(source);
    if (httpUri != null) {
      final resolved = await _resolveHttpMdsSource(httpUri);
      final shardUri = _resolveHttpMdsShardUri(resolved, resolvedShard);
      if (compressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
        );
        return _mosaicml.listSamplesPagedFromZstdCompressedStream(
          indexBytes: resolved.indexBytes,
          indexName: resolved.indexName,
          shardFilename: resolvedShard,
          offset: safeOffset,
          length: safeLength,
          decodedShardCacheKey: _httpCompressedMdsCacheKey(shardUri),
          openCompressedStream: (maxBytes) {
            final effectiveBytes = _apiResolveCompressedStreamMaxBytes(
                maxBytes, initialCompressedBytes);
            return _httpDatasets.openRead(
              url: shardUri,
              maxBytes: effectiveBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            );
          },
        );
      }
      return _mosaicml.listSamplesPagedFromRawStream(
        indexBytes: resolved.indexBytes,
        indexName: resolved.indexName,
        shardFilename: resolvedShard,
        offset: safeOffset,
        length: safeLength,
        rawStream: _httpDatasets.openRead(
          url: shardUri,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    return _mosaicml.listSamplesPaged(
      indexPath: summary.indexPath,
      shardFilename: resolvedShard,
      offset: safeOffset,
      length: safeLength,
    );
  }

  Future<WdsSampleListResponse> apiListWebdatasetSamplesPage({
    required LoadedDatasetSource dataset,
    required String shardFilename,
    int offset = 0,
    int length = 200,
    bool computeTotal = false,
  }) async {
    final resolvedShard = shardFilename.trim();
    if (resolvedShard.isEmpty) {
      throw const FormatException('shard filename is empty');
    }
    final source = _apiDatasetSourceInput(dataset).trim();
    if (source.isEmpty) {
      throw const FormatException('Dataset source is empty.');
    }
    final safeOffset = offset < 0 ? 0 : offset;
    final safeLength = length < 1 ? 1 : length;

    final remote = _parseRemoteDirectorySource(source);
    final httpUri = _parseHttpSourceUri(source);
    if (remote != null || httpUri != null) {
      return _webdataset.listSamplesFromStream(
        shardStream: _apiOpenWebdatasetShardStream(
          dataset: dataset,
          shardFilename: resolvedShard,
        ),
        shardFilename: resolvedShard,
        offset: safeOffset,
        length: safeLength,
        computeTotal: computeTotal,
      );
    }

    return _webdataset.listSamples(
      dirPath: _normalizeDatasetDir(source),
      shardFilename: resolvedShard,
      offset: safeOffset,
      length: safeLength,
      computeTotal: computeTotal,
    );
  }

  Future<FieldPreview> apiPeekWebdatasetMember({
    required LoadedDatasetSource dataset,
    required String shardFilename,
    required String memberPath,
  }) async {
    final resolvedShard = shardFilename.trim();
    final resolvedMember = memberPath.trim();
    if (resolvedShard.isEmpty) {
      throw const FormatException('shard filename is empty');
    }
    if (resolvedMember.isEmpty) {
      throw const FormatException('member path is empty');
    }
    final source = _apiDatasetSourceInput(dataset).trim();
    if (source.isEmpty) {
      throw const FormatException('Dataset source is empty.');
    }
    final remote = _parseRemoteDirectorySource(source);
    final httpUri = _parseHttpSourceUri(source);
    if (remote != null || httpUri != null) {
      return _webdataset.peekMemberFromStream(
        shardStream: _apiOpenWebdatasetShardStream(
          dataset: dataset,
          shardFilename: resolvedShard,
        ),
        shardFilename: resolvedShard,
        memberPath: resolvedMember,
      );
    }
    return _webdataset.peekMember(
      dirPath: _normalizeDatasetDir(source),
      shardFilename: resolvedShard,
      memberPath: resolvedMember,
    );
  }

  Future<PreparedFileResponse> apiPrepareLocalDirectoryFieldFile({
    required String path,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final resolvedPath = path.trim();
    if (resolvedPath.isEmpty) {
      throw const FormatException('localDirectory path is empty');
    }

    if (isLocalDirectoryMdsShardPath(resolvedPath)) {
      final explicitRemoteShard = _parseRemoteDirectorySource(resolvedPath);
      final remote = explicitRemoteShard ?? _activeRemoteDirectorySource;
      final httpUri = _parseHttpSourceUri(resolvedPath);
      final lowerPath = resolvedPath.toLowerCase();
      final isRemoteExtCompressed = (remote != null || httpUri != null) &&
          (lowerPath.endsWith('.zst') || lowerPath.endsWith('.zstd'));
      final source = await _resolveLocalDirectoryMdsSource(resolvedPath);

      if (remote == null && httpUri == null) {
        return _mosaicml.prepareFieldFile(
          indexPath: source.indexPath!,
          shardFilename: source.shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
        );
      }

      final initialRemotePath = explicitRemoteShard != null
          ? _normalizeRemoteDirectoryPath(explicitRemoteShard.path)
          : _normalizeRemoteDirectoryPath(resolvedPath);
      final (:isCompressed, :remotePath) =
          await _resolveRemoteMdsCompression(
        isCompressed: isRemoteExtCompressed,
        remotePath: initialRemotePath,
        indexBytes: source.indexBytes,
        indexName: source.indexName,
        shardFilename: source.shardFilename,
      );

      if (httpUri != null) {
        if (isCompressed) {
          final initialCompressedBytes =
              await _estimateRemoteCompressedMdsScanBytes(
            indexBytes: source.indexBytes,
            indexName: source.indexName,
            shardFilename: source.shardFilename,
            targetItemIndex: itemIndex,
          );
          return _mosaicml.prepareFieldFileFromZstdCompressedStream(
            indexBytes: source.indexBytes,
            indexName: source.indexName,
            shardFilename: source.shardFilename,
            itemIndex: itemIndex,
            fieldIndex: fieldIndex,
            decodedShardCacheKey: _httpCompressedMdsCacheKey(httpUri),
            openCompressedStream: (maxBytes) {
              final effectiveBytes =
                  _apiResolvePreparedCompressedStreamMaxBytes(
                      maxBytes, initialCompressedBytes);
              return _httpDatasets.openRead(
                url: httpUri,
                maxBytes: effectiveBytes,
                onStatus: (message) {
                  statusMessage = message;
                },
              );
            },
          );
        }
        return _mosaicml.prepareFieldFileFromRawStream(
          indexBytes: source.indexBytes,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          rawStream: _httpDatasets.openRead(
            url: httpUri,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }

      final remoteSource = remote;
      if (remoteSource == null) {
        throw const FormatException('Remote source is not active.');
      }
      final host = _findRemoteHost(remoteSource.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remoteSource.hostId}');
      }
      if (isCompressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: source.indexBytes,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          targetItemIndex: itemIndex,
        );
        return _mosaicml.prepareFieldFileFromZstdCompressedStream(
          indexBytes: source.indexBytes,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          decodedShardCacheKey: _remoteCompressedMdsCacheKey(
            host: host,
            remotePath: remotePath,
          ),
          openCompressedStream: (maxBytes) {
            final effectiveBytes = _apiResolvePreparedCompressedStreamMaxBytes(
                maxBytes, initialCompressedBytes);
            return _remoteDatasets.openReadFile(
              host: host,
              remotePath: remotePath,
              maxBytes: effectiveBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            );
          },
        );
      }
      return _mosaicml.prepareFieldFileFromRawStream(
        indexBytes: source.indexBytes,
        indexName: source.indexName,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        rawStream: _remoteDatasets.openReadFile(
          host: host,
          remotePath: remotePath,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    final remoteLike = _parseRemoteDirectorySource(resolvedPath) != null ||
        _parseHttpSourceUri(resolvedPath) != null;
    if (remoteLike) {
      final bytes = await readDirectoryFileBytes(resolvedPath);
      final stagedPath = await _stageRemoteFileForOpen(
        remotePath: resolvedPath,
        bytes: bytes,
      );
      return PreparedFileResponse(
        path: stagedPath,
        size: bytes.length,
        ext: _normalizeLocalExt(resolvedPath),
      );
    }

    final normalizedPath = resolvedPath.startsWith('file://')
        ? Uri.parse(resolvedPath).toFilePath()
        : resolvedPath;
    final file = File(normalizedPath);
    if (!await file.exists()) {
      throw const FormatException('Selected file does not exist.');
    }
    final stat = await file.stat();
    return PreparedFileResponse(
      path: file.path,
      size: stat.size,
      ext: _normalizeLocalExt(resolvedPath),
    );
  }

  Future<PreparedMediaResponse> apiPrepareLocalDirectoryFieldAudio({
    required String path,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final resolvedPath = path.trim();
    if (resolvedPath.isEmpty) {
      throw const FormatException('localDirectory path is empty');
    }

    if (isLocalDirectoryMdsShardPath(resolvedPath)) {
      final explicitRemoteShard = _parseRemoteDirectorySource(resolvedPath);
      final remote = explicitRemoteShard ?? _activeRemoteDirectorySource;
      final httpUri = _parseHttpSourceUri(resolvedPath);
      final lowerPath = resolvedPath.toLowerCase();
      final isRemoteExtCompressed = (remote != null || httpUri != null) &&
          (lowerPath.endsWith('.zst') || lowerPath.endsWith('.zstd'));
      final source = await _resolveLocalDirectoryMdsSource(resolvedPath);

      if (remote == null && httpUri == null) {
        return _mosaicml.prepareAudioPreview(
          indexPath: source.indexPath!,
          shardFilename: source.shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
        );
      }

      final initialRemotePath = explicitRemoteShard != null
          ? _normalizeRemoteDirectoryPath(explicitRemoteShard.path)
          : _normalizeRemoteDirectoryPath(resolvedPath);
      final (:isCompressed, :remotePath) =
          await _resolveRemoteMdsCompression(
        isCompressed: isRemoteExtCompressed,
        remotePath: initialRemotePath,
        indexBytes: source.indexBytes,
        indexName: source.indexName,
        shardFilename: source.shardFilename,
      );

      if (httpUri != null) {
        if (isCompressed) {
          final initialCompressedBytes =
              await _estimateRemoteCompressedMdsScanBytes(
            indexBytes: source.indexBytes,
            indexName: source.indexName,
            shardFilename: source.shardFilename,
            targetItemIndex: itemIndex,
          );
          return _mosaicml.prepareAudioPreviewFromZstdCompressedStream(
            indexBytes: source.indexBytes,
            indexName: source.indexName,
            shardFilename: source.shardFilename,
            itemIndex: itemIndex,
            fieldIndex: fieldIndex,
            decodedShardCacheKey: _httpCompressedMdsCacheKey(httpUri),
            openCompressedStream: (maxBytes) {
              final effectiveBytes =
                  _apiResolvePreparedCompressedStreamMaxBytes(
                      maxBytes, initialCompressedBytes);
              return _httpDatasets.openRead(
                url: httpUri,
                maxBytes: effectiveBytes,
                onStatus: (message) {
                  statusMessage = message;
                },
              );
            },
          );
        }
        return _mosaicml.prepareAudioPreviewFromRawStream(
          indexBytes: source.indexBytes,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          rawStream: _httpDatasets.openRead(
            url: httpUri,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }

      final remoteSource = remote;
      if (remoteSource == null) {
        throw const FormatException('Remote source is not active.');
      }
      final host = _findRemoteHost(remoteSource.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remoteSource.hostId}');
      }
      if (isCompressed) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: source.indexBytes,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          targetItemIndex: itemIndex,
        );
        return _mosaicml.prepareAudioPreviewFromZstdCompressedStream(
          indexBytes: source.indexBytes,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          decodedShardCacheKey: _remoteCompressedMdsCacheKey(
            host: host,
            remotePath: remotePath,
          ),
          openCompressedStream: (maxBytes) {
            final effectiveBytes = _apiResolvePreparedCompressedStreamMaxBytes(
                maxBytes, initialCompressedBytes);
            return _remoteDatasets.openReadFile(
              host: host,
              remotePath: remotePath,
              maxBytes: effectiveBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            );
          },
        );
      }
      return _mosaicml.prepareAudioPreviewFromRawStream(
        indexBytes: source.indexBytes,
        indexName: source.indexName,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        rawStream: _remoteDatasets.openReadFile(
          host: host,
          remotePath: remotePath,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }

    final bytes = await readDirectoryFileBytes(resolvedPath);
    return PreparedMediaResponse(
      bytes: bytes,
      size: bytes.length,
      ext: _normalizeLocalExt(resolvedPath),
    );
  }

  Future<ZenodoRecordSummary> apiLoadZenodoRecordSummary(
    LoadedDatasetSource dataset,
  ) async {
    return _apiLoadZenodoRecord(dataset);
  }

  Future<FieldPreview> apiPeekZenodoFile({
    required String contentUrl,
  }) {
    return _zenodo.peekFile(contentUrl);
  }

  Future<List<ZenodoZipEntrySummary>> apiZenodoZipListEntries({
    required String contentUrl,
    required String filename,
  }) {
    return _zenodo.zipListEntries(
      contentUrl: contentUrl,
      filename: filename,
    );
  }

  Future<ZenodoTarEntryListResponse> apiZenodoTarListEntriesPaged({
    required String contentUrl,
    required String filename,
    int offset = 0,
    int length = 50,
  }) {
    return _zenodo.tarListEntriesPaged(
      contentUrl: contentUrl,
      filename: filename,
      offset: offset,
      length: length,
    );
  }

  Future<FieldPreview> apiZenodoZipPeekEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) {
    return _zenodo.zipPeekEntry(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
    );
  }

  Future<FieldPreview> apiZenodoTarPeekEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) {
    return _zenodo.tarPeekEntry(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
    );
  }

  ChunkSummary _apiFindChunk(IndexSummary summary, String chunkFilename) {
    final normalized = chunkFilename.trim();
    for (final chunk in summary.chunks) {
      if (chunk.filename == normalized) {
        return chunk;
      }
    }
    throw FormatException('Unknown chunk/shard filename: $chunkFilename');
  }

  ItemPage _apiBuildSyntheticItemPage({
    required int totalItems,
    required int offset,
    required int length,
    required int fieldCount,
    required int chunkBytes,
  }) {
    final safeTotal = totalItems < 0 ? 0 : totalItems;
    final safeOffset = offset < 0 ? 0 : offset;
    final safeLength = length < 1 ? 1 : length;
    final start = safeOffset.clamp(0, safeTotal).toInt();
    final end = (start + safeLength).clamp(0, safeTotal).toInt();
    final itemBytes =
        safeTotal > 0 && chunkBytes > 0 ? chunkBytes ~/ safeTotal : 0;
    final fields = List<FieldMeta>.generate(
      fieldCount < 0 ? 0 : fieldCount,
      (index) => FieldMeta(fieldIndex: index, size: 0),
      growable: false,
    );
    final items = <ItemMeta>[];
    for (var index = start; index < end; index += 1) {
      items.add(
        ItemMeta(
          itemIndex: index,
          totalBytes: itemBytes,
          fields: fields,
        ),
      );
    }
    return ItemPage(
      offset: start,
      length: safeLength,
      items: items,
      partial: end < safeTotal,
      numItemsTotal: safeTotal,
    );
  }

  bool _apiIsMdsCompressedShard({
    required IndexSummary summary,
    required String shardFilename,
  }) {
    final lower = shardFilename.trim().toLowerCase();
    if (lower.endsWith('.zst') || lower.endsWith('.zstd')) {
      return true;
    }
    final compression = summary.compression?.trim().toLowerCase();
    if (compression == null || compression.isEmpty) {
      return false;
    }
    return compression == 'zstd' ||
        compression == 'zst' ||
        compression.contains('zstd');
  }

  int? _apiResolveCompressedStreamMaxBytes(
    int? requestedBytes,
    int initialBytes,
  ) {
    if (initialBytes <= 0) {
      return requestedBytes;
    }
    if (requestedBytes == null || requestedBytes <= 0) {
      return initialBytes;
    }
    return requestedBytes < initialBytes ? initialBytes : requestedBytes;
  }

  int? _apiResolvePreparedCompressedStreamMaxBytes(
    int? requestedBytes,
    int initialBytes,
  ) {
    if (requestedBytes == null || requestedBytes <= 0) {
      return null;
    }
    return _apiResolveCompressedStreamMaxBytes(requestedBytes, initialBytes);
  }

  Stream<List<int>> _apiOpenWebdatasetShardStream({
    required LoadedDatasetSource dataset,
    required String shardFilename,
  }) {
    final source = _apiDatasetSourceInput(dataset).trim();
    if (source.isEmpty) {
      throw const FormatException('Dataset source is empty.');
    }
    final requestedShard = shardFilename.trim();
    if (requestedShard.isEmpty) {
      throw const FormatException('shard filename is empty');
    }
    final remote = _parseRemoteDirectorySource(source);
    if (remote != null) {
      final host = _findRemoteHost(remote.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remote.hostId}');
      }
      final hintedFormat = _sourceRouter.detectFormatFromPath(remote.path);
      final remoteShardPath = hintedFormat ==
              DatasetSourceFormat.webdatasetShard
          ? () {
              final directName = p.basename(remote.path).trim();
              if (directName.toLowerCase() == requestedShard.toLowerCase()) {
                return remote.path;
              }
              final parent = _normalizeRemoteDirectoryPath(
                p.dirname(remote.path),
              );
              return _joinRemoteDirectoryPath(
                parent == '.' ? '' : parent,
                requestedShard,
              );
            }()
          : _joinRemoteDirectoryPath(remote.path, requestedShard);
      return _remoteDatasets.openReadFile(
        host: host,
        remotePath: remoteShardPath,
        onStatus: (message) {
          statusMessage = message;
        },
      );
    }

    final httpUri = _parseHttpSourceUri(source);
    if (httpUri != null) {
      final format = _sourceRouter.detectFormatFromPath(httpUri.path);
      final shardUri = format == DatasetSourceFormat.webdatasetShard
          ? httpUri
          : _httpDatasets.resolveFromDirectory(
              _httpWebdatasetDirectoryUri(httpUri),
              requestedShard,
            );
      return _httpDatasets.openRead(
        url: shardUri,
        onStatus: (message) {
          statusMessage = message;
        },
      );
    }

    throw const FormatException('WebDataset streaming source is not active.');
  }

  List<Map<String, dynamic>> _takeSafeRows(
    List<dynamic> rows, {
    int count = 3,
  }) {
    final rowsToInclude = rows.length > count ? count : rows.length;
    final sampled = <Map<String, dynamic>>[];
    for (var i = 0; i < rowsToInclude; i += 1) {
      final row = rows[i];
      sampled
          .add(<String, dynamic>{'index': i, 'value': _safeToJsonValue(row)});
    }
    return sampled;
  }

  dynamic _safeToJsonValue(dynamic value) {
    if (value == null || value is String || value is num || value is bool) {
      return value;
    }
    if (value is DateTime) {
      return value.toIso8601String();
    }
    if (value is List) {
      return value.take(10).map(_safeToJsonValue).toList(growable: false);
    }
    if (value is Map) {
      final mapped = <String, dynamic>{};
      value.forEach((key, entry) {
        mapped[key.toString()] = _safeToJsonValue(entry);
      });
      return mapped;
    }
    return value.toString();
  }
}
