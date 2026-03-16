part of 'viewer_state.dart';

extension ViewerStateDataLoading on ViewerState {
  void _ensurePrimaryDataNotNull(ViewerMode sourceMode) {
    switch (sourceMode) {
      case ViewerMode.litdataIndex:
      case ViewerMode.litdataChunks:
      case ViewerMode.mdsIndex:
        indexSummary ??= _emptyIndexSummary();
        break;
      case ViewerMode.webdatasetDir:
        wdsDirSummary ??= _emptyWdsDirSummary();
        break;
      case ViewerMode.zenodo:
        zenodoRecord ??= _emptyZenodoRecord();
        break;
      case ViewerMode.huggingface:
        hfPreview ??= _emptyHfPreview();
        break;
      case ViewerMode.localDirectory:
        if (localDirectoryItems.isEmpty) {
          localDirectoryItems = const <LocalDirectoryItem>[];
        }
        break;
    }
  }

  String _formatBytes(int value) {
    if (value <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var bytes = value.toDouble();
    var unit = 0;
    while (bytes >= 1024 && unit < units.length - 1) {
      bytes /= 1024;
      unit += 1;
    }
    final decimals = bytes >= 10 || bytes < 1 ? 0 : 1;
    return '${bytes.toStringAsFixed(decimals)} ${units[unit]}';
  }

  IndexSummary _emptyIndexSummary() {
    return const IndexSummary(
      indexPath: '',
      rootDir: '',
      dataFormat: <String>[],
      compression: null,
      chunkSize: null,
      chunkBytes: null,
      configRaw: <String, dynamic>{},
      chunks: <ChunkSummary>[],
    );
  }

  WdsDirSummary _emptyWdsDirSummary() {
    return const WdsDirSummary(dirPath: '', shards: <WdsShardSummary>[]);
  }

  HfDatasetPreview _emptyHfPreview() {
    return const HfDatasetPreview(
      dataset: '',
      config: '',
      split: '',
      configs: <HfConfigSummary>[],
      offset: 0,
      length: 0,
      numRowsTotal: 0,
      partial: false,
      features: <HfFeature>[],
      rows: <dynamic>[],
      featureOffset: 0,
      featureCount: 0,
      totalFeatureCount: 0,
    );
  }

  ZenodoRecordSummary _emptyZenodoRecord() {
    return const ZenodoRecordSummary(
      recordId: 0,
      title: '',
      doi: null,
      doiUrl: null,
      publicationDate: null,
      version: null,
      accessRight: null,
      recordUrl: null,
      creators: <ZenodoCreator>[],
      files: <ZenodoFileSummary>[],
    );
  }

  List<ItemMeta> _emptyItemList() => const <ItemMeta>[];

  WdsSampleListResponse _emptyWdsSamples() {
    return const WdsSampleListResponse(
      offset: 0,
      length: 0,
      numSamplesTotal: 0,
      partial: false,
      samples: <WdsSampleInfo>[],
    );
  }

  FieldPreview _emptyFieldPreview() {
    return const FieldPreview(
      previewText: '',
      hexSnippet: '',
      guessedExt: 'bin',
      isBinary: false,
      size: 0,
    );
  }

  List<ZenodoZipEntrySummary> _emptyZenodoZipEntries() =>
      const <ZenodoZipEntrySummary>[];

  ZenodoTarEntryListResponse _emptyZenodoTarEntries() {
    return const ZenodoTarEntryListResponse(
      offset: 0,
      length: 0,
      entries: <ZenodoTarEntrySummary>[],
      partial: false,
      numEntriesTotal: 0,
    );
  }

  InlineMediaResponse _emptyInlineMedia() {
    return const InlineMediaResponse(base64: '', mime: '', size: 0, ext: '');
  }

  void _loadLitdataItems() {
    if (selectedChunkName == null || indexSummary == null) {
      litdataItemsFuture = null;
      return;
    }
    if (mode == ViewerMode.litdataIndex || mode == ViewerMode.litdataChunks) {
      final remoteSource = _activeRemoteLitdataRuntime();
      if (remoteSource != null) {
        final host = _findRemoteHost(remoteSource.hostId);
        if (host == null) {
          litdataItemsFuture = _captureFutureError(
            Future<List<ItemMeta>>.error(
              FormatException('Remote host not found: ${remoteSource.hostId}'),
            ),
            context: 'LitData items failed',
            fallback: _emptyItemList,
          );
          return;
        }
        final chunkPath = _joinRemoteDirectoryPath(
          remoteSource.directoryPath,
          selectedChunkName!,
        );
        litdataItemsFuture = _captureFutureError(
          _listLitdataItemsFromRemoteStreamWithRetry(
            host: host,
            remotePath: chunkPath,
            indexBytes: remoteSource.indexBytes,
            indexName: remoteSource.indexName,
            chunkFilename: selectedChunkName!,
          ),
          context: 'LitData items failed',
          fallback: _emptyItemList,
        );
        return;
      }
      final httpSource = _activeHttpLitdataRuntime();
      if (httpSource != null) {
        final chunkUri =
            _resolveHttpLitdataChunkUri(httpSource, selectedChunkName!);
        litdataItemsFuture = _captureFutureError(
          _remoteOps.listLitdataItemsFromStreamWithRetry(
            litdata: _litdata,
            indexBytes: httpSource.indexBytes,
            indexName: httpSource.indexName,
            chunkFilename: selectedChunkName!,
            maxAttempts: 3,
            openChunkStream: () => _httpDatasets.openRead(
              url: chunkUri,
              onStatus: (message) {
                statusMessage = message;
              },
            ),
          ),
          context: 'LitData items failed',
          fallback: _emptyItemList,
        );
        return;
      }
      litdataItemsFuture = _captureFutureError(
        _litdata.listChunkItems(
          indexSummary!.indexPath,
          selectedChunkName!,
        ),
        context: 'LitData items failed',
        fallback: _emptyItemList,
      );
    }
  }

  void _loadMdsItems() {
    if (selectedChunkName == null || indexSummary == null) {
      mdsItemsFuture = null;
      return;
    }
    if (mode == ViewerMode.mdsIndex) {
      final shardName = selectedChunkName!;
      final compressed = _isMdsCompressedShardForLoading(shardName);
      final remoteSource = _activeRemoteMdsRuntime();
      if (remoteSource != null) {
        final host = _findRemoteHost(remoteSource.hostId);
        if (host == null) {
          mdsItemsFuture = _captureFutureError(
            Future<List<ItemMeta>>.error(
              FormatException('Remote host not found: ${remoteSource.hostId}'),
            ),
            context: 'MosaicML samples failed',
            fallback: _emptyItemList,
          );
          return;
        }
        Future<List<ItemMeta>> loadCompressedRemoteItems() async {
          final remotePath = await _resolveRemoteMdsShardPath(
            host: host,
            directoryPath: remoteSource.directoryPath,
            shardFilename: shardName,
            compressed: true,
          );
          final initialCompressedBytes =
              await _estimateRemoteCompressedMdsScanBytes(
            indexBytes: remoteSource.indexBytes,
            indexName: remoteSource.indexName,
            shardFilename: shardName,
          );
          return _listMdsItemsFromRemoteCompressedStreamWithRetry(
            host: host,
            remotePath: remotePath,
            indexBytes: remoteSource.indexBytes,
            indexName: remoteSource.indexName,
            shardFilename: shardName,
            initialCompressedBytes: initialCompressedBytes,
            maxAttempts: 4,
          );
        }

        mdsItemsFuture = _captureFutureError(
          compressed
              ? loadCompressedRemoteItems()
              : () async {
                  final remotePath = await _resolveRemoteMdsShardPath(
                    host: host,
                    directoryPath: remoteSource.directoryPath,
                    shardFilename: shardName,
                    compressed: false,
                  );
                  return _listMdsItemsFromRemoteRawStreamWithRetry(
                    host: host,
                    remotePath: remotePath,
                    indexBytes: remoteSource.indexBytes,
                    indexName: remoteSource.indexName,
                    shardFilename: shardName,
                    maxAttempts: 4,
                  );
                }(),
          context: 'MosaicML samples failed',
          fallback: _emptyItemList,
        );
        return;
      }
      final httpSource = _activeHttpMdsRuntime();
      if (httpSource != null) {
        final shardUri = _resolveHttpMdsShardUri(httpSource, shardName);
        Future<List<ItemMeta>> loadCompressedHttpItems() async {
          final initialCompressedBytes =
              await _estimateRemoteCompressedMdsScanBytes(
            indexBytes: httpSource.indexBytes,
            indexName: httpSource.indexName,
            shardFilename: shardName,
          );
          return _remoteOps.listMdsItemsFromCompressedStreamWithRetry(
            mosaicml: _mosaicml,
            indexBytes: httpSource.indexBytes,
            indexName: httpSource.indexName,
            shardFilename: shardName,
            initialCompressedBytes: initialCompressedBytes,
            compressedShardCacheKey: _httpCompressedMdsCacheKey(shardUri),
            maxAttempts: 4,
            openCompressedStream: (maxBytes) => _httpDatasets.openRead(
              url: shardUri,
              maxBytes: maxBytes,
              onStatus: (message) {
                statusMessage = message;
              },
            ),
          );
        }

        mdsItemsFuture = _captureFutureError(
          compressed
              ? loadCompressedHttpItems()
              : _remoteOps.listMdsItemsFromRawStreamWithRetry(
                  mosaicml: _mosaicml,
                  indexBytes: httpSource.indexBytes,
                  indexName: httpSource.indexName,
                  shardFilename: shardName,
                  maxAttempts: 4,
                  openRawStream: () => _httpDatasets.openRead(
                    url: shardUri,
                    onStatus: (message) {
                      statusMessage = message;
                    },
                  ),
                ),
          context: 'MosaicML samples failed',
          fallback: _emptyItemList,
        );
        return;
      }
      mdsItemsFuture = _captureFutureError(
        _mosaicml.listSamples(
          indexPath: indexSummary!.indexPath,
          shardFilename: shardName,
        ),
        context: 'MosaicML samples failed',
        fallback: _emptyItemList,
      );
    }
  }

  bool _isMdsCompressedShardForLoading(String shardFilename) {
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

  void _loadWdsSamples() {
    if (selectedShardName == null || wdsDirSummary == null) {
      wdsSamplesFuture = null;
      return;
    }
    wdsSamplesFuture = _captureFutureError(
      () async {
        final useStreamingSource = _activeRemoteWebdatasetSource() != null ||
            _activeHttpWebdatasetShardUri() != null;
        final value = useStreamingSource
            ? await _webdataset.listSamplesFromStream(
                shardStream: _openWebdatasetShardStream(selectedShardName!),
                shardFilename: selectedShardName!,
                offset: wdsOffset,
                length: _wdsPageSize,
                computeTotal: false,
              )
            : await _webdataset.listSamples(
                dirPath: await _resolveWebdatasetDirPathForShard(
                  selectedShardName!,
                ),
                shardFilename: selectedShardName!,
                offset: wdsOffset,
                length: _wdsPageSize,
                computeTotal: false,
              );
        final total = value.numSamplesTotal;
        if (total != null && total > 0) {
          final lastOffset = ((total - 1) ~/ _wdsPageSize) * _wdsPageSize;
          if (value.offset > lastOffset) {
            wdsOffset = lastOffset;
            final corrected = useStreamingSource
                ? await _webdataset.listSamplesFromStream(
                    shardStream: _openWebdatasetShardStream(selectedShardName!),
                    shardFilename: selectedShardName!,
                    offset: wdsOffset,
                    length: _wdsPageSize,
                    computeTotal: false,
                  )
                : await _webdataset.listSamples(
                    dirPath: await _resolveWebdatasetDirPathForShard(
                      selectedShardName!,
                    ),
                    shardFilename: selectedShardName!,
                    offset: wdsOffset,
                    length: _wdsPageSize,
                    computeTotal: false,
                  );
            wdsSamples = corrected;
            _notifyStateChanged();
            return corrected;
          }
        }
        wdsSamples = value;
        _notifyStateChanged();
        return value;
      }(),
      context: 'WebDataset samples failed',
      fallback: _emptyWdsSamples,
    );
  }

  Future<T> _captureFutureError<T>(
    Future<T> future, {
    required String context,
    required T Function() fallback,
  }) {
    return future.catchError((error, stack) {
      AppLogger.error(context, tag: 'state', error: error, stackTrace: stack);
      statusMessage = error.toString();
      _notifyStateChanged();
      return fallback();
    });
  }

  void _refreshHfPreview() {
    final input = sourceInput.trim();
    if (input.isEmpty) return;

    final requestId = ++_hfFeatureLoadRequestId;
    final requestedConfig = hfConfigOverride?.trim();
    final requestedSplit = hfSplitOverride?.trim();
    final requestedOffset = hfOffset;
    final requestedToken = hfToken?.trim();

    AppLogger.info('Refresh Hugging Face preview "$input"', tag: 'state');
    hfPreviewFuture = _captureFutureError(
      _huggingface
          .datasetPreview(
        input: input,
        config: hfConfigOverride,
        split: hfSplitOverride,
        offset: hfOffset,
        length: _hfPageSize,
        token: hfToken,
        featureOffset: 0,
        maxFeatureCount: _hfFeatureChunkSize,
      )
          .then((value) {
        if (!_isActiveHfFeatureLoad(
          requestId: requestId,
          input: input,
          config: requestedConfig,
          split: requestedSplit,
          offset: requestedOffset,
          token: requestedToken,
        )) {
          return value;
        }
        hfPreview = value;
        if (value.configs.isNotEmpty) {
          hfConfigOptions = value.configs;
        } else {
          hfConfigOptions ??= [
            HfConfigSummary(config: value.config, splits: [value.split]),
          ];
        }
        hfConfigOverride = value.config;
        hfSplitOverride = value.split;
        _syncActiveDatasetSelection();
        _notifyStateChanged();

        if (_hfFlow.shouldLoadRemainingFeatures(value)) {
          _loadRemainingHfFeatureChunks(
            requestId: requestId,
            input: input,
            config: value.config,
            split: value.split,
            offset: value.offset,
            length: value.length,
            token: requestedToken,
            nextFeatureOffset: value.featureOffset + value.featureCount,
            totalFeatureCount: value.totalFeatureCount,
          );
        }
        _prefetchNextHfPage(
          requestId: requestId,
          input: input,
          config: value.config,
          split: value.split,
          token: requestedToken,
          currentOffset: value.offset,
          totalRows: value.numRowsTotal,
          currentPageSize: value.length,
        );
        return value;
      }),
      context: 'Hugging Face preview refresh failed',
      fallback: _emptyHfPreview,
    );
    _notifyStateChanged();
  }

  bool _isActiveHfFeatureLoad({
    required int requestId,
    required String input,
    required String? config,
    required String? split,
    required int offset,
    required String? token,
  }) {
    if (_hfFeatureLoadRequestId != requestId) return false;
    if (sourceInput.trim() != input) return false;
    if (hfOffset != offset) return false;
    if ((hfConfigOverride?.trim() ?? '') != (config?.trim() ?? '')) {
      return false;
    }
    if ((hfSplitOverride?.trim() ?? '') != (split?.trim() ?? '')) return false;
    if ((hfToken?.trim() ?? '') != (token ?? '')) return false;
    if (mode != ViewerMode.huggingface) return false;
    return true;
  }

  void _loadRemainingHfFeatureChunks({
    required int requestId,
    required String input,
    required String config,
    required String split,
    required int offset,
    required int length,
    required String? token,
    required int nextFeatureOffset,
    required int totalFeatureCount,
  }) {
    if (nextFeatureOffset >= totalFeatureCount) return;
    unawaited(_appendHfFeatureChunks(
      requestId: requestId,
      input: input,
      config: config,
      split: split,
      offset: offset,
      length: length,
      token: token,
      nextFeatureOffset: nextFeatureOffset,
      totalFeatureCount: totalFeatureCount,
      maxFeatureChunks: _hfVisibleFeatureChunkSize,
      prefetchNextColumn: true,
    ));
  }

  Future<void> _appendHfFeatureChunks({
    required int requestId,
    required String input,
    required String config,
    required String split,
    required int offset,
    required int length,
    required String? token,
    required int nextFeatureOffset,
    required int totalFeatureCount,
    required int maxFeatureChunks,
    required bool prefetchNextColumn,
  }) async {
    var featureOffset = nextFeatureOffset;
    var appendedChunks = 0;
    while (_isActiveHfFeatureLoad(
      requestId: requestId,
      input: input,
      config: config,
      split: split,
      offset: offset,
      token: token,
    )) {
      if (featureOffset >= totalFeatureCount) return;

      final chunk = await _huggingface
          .datasetPreview(
        input: input,
        config: config,
        split: split,
        offset: offset,
        length: length,
        token: token,
        featureOffset: featureOffset,
        maxFeatureCount: _hfFeatureChunkSize,
      )
          .catchError((_) {
        return _emptyHfPreview();
      });

      if (!_isActiveHfFeatureLoad(
        requestId: requestId,
        input: input,
        config: config,
        split: split,
        offset: offset,
        token: token,
      )) {
        return;
      }

      if (chunk.rows.isEmpty && chunk.features.isEmpty) {
        return;
      }

      if (chunk.featureCount <= 0) {
        return;
      }

      final current = hfPreview;
      if (current == null) return;
      final merged = _hfFlow.mergeFeatureChunk(current, chunk);
      if (merged == null) return;
      hfPreview = merged;
      _notifyStateChanged();
      appendedChunks += 1;

      if (_hfFlow.isFeatureLoadComplete(merged) ||
          appendedChunks >= maxFeatureChunks) {
        if (prefetchNextColumn &&
            !_hfFlow.isFeatureLoadComplete(merged) &&
            appendedChunks >= maxFeatureChunks) {
          final nextOffset = chunk.featureOffset + chunk.featureCount;
          _prefetchHfFeatureChunks(
            requestId: requestId,
            input: input,
            config: config,
            split: split,
            offset: offset,
            token: token,
            nextFeatureOffset: nextOffset,
            totalFeatureCount: totalFeatureCount,
          );
        }
        return;
      }
      final nextOffset = chunk.featureOffset + chunk.featureCount;
      if (nextOffset <= featureOffset) return;
      featureOffset = nextOffset;
    }
  }

  void _prefetchHfFeatureChunks({
    required int requestId,
    required String input,
    required String config,
    required String split,
    required int offset,
    required String? token,
    required int nextFeatureOffset,
    required int totalFeatureCount,
  }) {
    if (!_isActiveHfFeatureLoad(
      requestId: requestId,
      input: input,
      config: config,
      split: split,
      offset: offset,
      token: token,
    )) {
      return;
    }
    if (nextFeatureOffset >= totalFeatureCount) return;

    unawaited(
      _huggingface
          .datasetPreview(
            input: input,
            config: config,
            split: split,
            offset: offset,
            length: _hfPageSize,
            token: token,
            featureOffset: nextFeatureOffset,
            maxFeatureCount: _hfPrefetchFeatureChunkSize,
          )
          .catchError((_) => _emptyHfPreview()),
    );
  }

  void _prefetchNextHfPage({
    required int requestId,
    required String input,
    required String config,
    required String split,
    required String? token,
    required int currentOffset,
    required int totalRows,
    required int currentPageSize,
  }) {
    if (currentOffset + currentPageSize >= totalRows && totalRows > 0) return;
    if (!_isActiveHfFeatureLoad(
      requestId: requestId,
      input: input,
      config: config,
      split: split,
      offset: currentOffset,
      token: token,
    )) {
      return;
    }
    final nextOffset = currentOffset + currentPageSize;
    unawaited(
      _huggingface
          .datasetPreview(
            input: input,
            config: config,
            split: split,
            offset: nextOffset,
            length: _hfPageSize,
            token: token,
            featureOffset: 0,
            maxFeatureCount: _hfFeatureChunkSize,
          )
          .catchError((_) => _emptyHfPreview()),
    );
  }

  void _loadZenodoEntries() {
    final record = zenodoRecord;
    if (record == null || record.files.isEmpty) return;
    final file = record.files.firstWhere(
      (f) => f.key == zenodoSelectedFileKey,
      orElse: () => record.files.first,
    );
    final kind = _zenodoFlow.detectContainer(file.key);
    if (kind == ZenodoContainerKind.zip) {
      zenodoZipEntriesFuture = _captureFutureError(
        _zenodo
            .zipListEntries(
          contentUrl: file.contentUrl,
          filename: file.key,
        )
            .then((value) {
          zenodoZipEntries = value;
          _notifyStateChanged();
          return value;
        }),
        context: 'Zenodo ZIP entries failed',
        fallback: _emptyZenodoZipEntries,
      );
      zenodoTarEntriesFuture = null;
    } else if (kind == ZenodoContainerKind.tar) {
      zenodoTarEntriesFuture = _captureFutureError(
        _zenodo
            .tarListEntriesPaged(
          contentUrl: file.contentUrl,
          filename: file.key,
          offset: zenodoEntriesOffset,
          length: _zenodoTarPageSize,
        )
            .then((value) {
          zenodoTarEntries = value;
          _notifyStateChanged();
          return value;
        }),
        context: 'Zenodo TAR entries failed',
        fallback: _emptyZenodoTarEntries,
      );
      zenodoZipEntriesFuture = null;
    } else {
      zenodoZipEntriesFuture = null;
      zenodoTarEntriesFuture = null;
      zenodoFilePreviewFuture = _captureFutureError(
        _zenodo.peekFile(file.contentUrl),
        context: 'Zenodo file preview failed',
        fallback: _emptyFieldPreview,
      );
    }
  }

  void _loadZenodoEntryPreview() {
    final record = zenodoRecord;
    if (record == null || record.files.isEmpty) return;
    final file = record.files.firstWhere(
      (f) => f.key == zenodoSelectedFileKey,
      orElse: () => record.files.first,
    );
    if (zenodoSelectedEntryName == null) {
      zenodoEntryPreviewFuture = null;
      zenodoInlineMediaFuture = null;
      return;
    }
    final entryName = zenodoSelectedEntryName!;
    final kind = _zenodoFlow.detectContainer(file.key);
    if (kind == ZenodoContainerKind.zip) {
      zenodoEntryPreviewFuture = _captureFutureError(
        _zenodo.zipPeekEntry(
          contentUrl: file.contentUrl,
          filename: file.key,
          entryName: entryName,
        ),
        context: 'Zenodo ZIP preview failed',
        fallback: _emptyFieldPreview,
      );
      if (_zenodoFlow.isInlineMediaExt(entryName)) {
        zenodoInlineMediaFuture = _captureFutureError(
          _zenodo.zipInlineEntryMedia(
            contentUrl: file.contentUrl,
            filename: file.key,
            entryName: entryName,
          ),
          context: 'Zenodo ZIP media failed',
          fallback: _emptyInlineMedia,
        );
      } else {
        zenodoInlineMediaFuture = null;
      }
    } else {
      zenodoEntryPreviewFuture = _captureFutureError(
        _zenodo.tarPeekEntry(
          contentUrl: file.contentUrl,
          filename: file.key,
          entryName: entryName,
        ),
        context: 'Zenodo TAR preview failed',
        fallback: _emptyFieldPreview,
      );
      if (_zenodoFlow.isInlineMediaExt(entryName)) {
        zenodoInlineMediaFuture = _captureFutureError(
          _zenodo.tarInlineEntryMedia(
            contentUrl: file.contentUrl,
            filename: file.key,
            entryName: entryName,
          ),
          context: 'Zenodo TAR media failed',
          fallback: _emptyInlineMedia,
        );
      } else {
        zenodoInlineMediaFuture = null;
      }
    }
  }
}
