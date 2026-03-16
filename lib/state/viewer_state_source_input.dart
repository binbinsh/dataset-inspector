part of 'viewer_state.dart';

final Map<String, Future<List<LocalDirectoryItem>>>
    _localDirectoryItemsInFlight = <String, Future<List<LocalDirectoryItem>>>{};

extension ViewerStateSourceInput on ViewerState {
  void setSourceInput(String value) {
    sourceInput = value;
    _scheduleSourceDetection(value);
    _scheduleSessionPersist();
    _notifyStateChanged();
  }

  void _scheduleSourceDetection(String value) {
    _detectTimer?.cancel();
    final trimmed = value.trim();
    if (trimmed.isEmpty) {
      _setDetectedSource(null, notify: false);
      return;
    }
    if (_looksLikeHfInput(trimmed)) {
      _setDetectedSource(DetectedSourceKind.huggingface, notify: false);
      return;
    }
    if (_looksLikeZenodoInput(trimmed)) {
      _setDetectedSource(DetectedSourceKind.zenodo, notify: false);
      return;
    }
    if (_parseRemoteDirectorySource(trimmed) != null) {
      _setDetectedSource(DetectedSourceKind.detecting, notify: false);
      _detectTimer = Timer(const Duration(milliseconds: 320), () {
        _detectSource(trimmed);
      });
      return;
    }
    _setDetectedSource(DetectedSourceKind.detecting, notify: false);
    _detectTimer = Timer(const Duration(milliseconds: 320), () {
      _detectSource(trimmed);
    });
  }

  Future<void> _detectSource(String input) async {
    final requestId = ++_detectRequestId;
    if (input.isEmpty) {
      _setDetectedSource(null);
      return;
    }
    if (_looksLikeHfInput(input)) {
      _setDetectedSource(DetectedSourceKind.huggingface);
      return;
    }
    if (_looksLikeZenodoInput(input)) {
      _setDetectedSource(DetectedSourceKind.zenodo);
      return;
    }
    final httpUri = _parseHttpSourceUri(input);
    if (httpUri != null) {
      final kind = await _detectHttpSourceKind(httpUri);
      if (!_isDetectRequestActive(requestId, input)) return;
      _setDetectedSource(kind);
      return;
    }
    final remote = _parseRemoteDirectorySource(input);
    if (remote != null) {
      final kind = await _detectRemoteDirectorySourceKind(remote);
      if (!_isDetectRequestActive(requestId, input)) return;
      _setDetectedSource(kind);
      return;
    }

    try {
      final detected = await _webdataset.detectLocalDataset(input);
      if (!_isDetectRequestActive(requestId, input)) return;
      switch (detected.kind) {
        case LocalDatasetKind.litdataIndex:
          _setDetectedSource(DetectedSourceKind.litdataIndex);
        case LocalDatasetKind.mdsIndex:
          _setDetectedSource(DetectedSourceKind.mdsIndex);
        case LocalDatasetKind.webdatasetDir:
          _setDetectedSource(DetectedSourceKind.webdatasetDir);
      }
      return;
    } catch (error) {
      if (!_isDetectRequestActive(requestId, input)) return;
      final message = error.toString().toLowerCase();
      if (message.contains('path does not exist') ||
          message.contains('missing directory')) {
        _setDetectedSource(DetectedSourceKind.unknown);
        return;
      }
      if (_isPermissionDeniedError(error)) {
        _setDetectedSource(DetectedSourceKind.localDirectory);
        return;
      }

      final localDirectory = _normalizeDatasetDir(input);
      final localType = await FileSystemEntity.type(
        localDirectory,
        followLinks: true,
      ).onError((_, __) => FileSystemEntityType.notFound);
      if (localType == FileSystemEntityType.directory) {
        _setDetectedSource(DetectedSourceKind.localDirectory);
        return;
      }

      final chunkPaths = await _litdata
          .listChunkFiles(_normalizeDatasetDir(input))
          .catchError((_) => <String>[]);
      if (!_isDetectRequestActive(requestId, input)) return;
      if (chunkPaths.isNotEmpty) {
        _setDetectedSource(DetectedSourceKind.litdataChunks);
        return;
      }
      _setDetectedSource(DetectedSourceKind.litdataIndex);
    }
  }

  bool _isDetectRequestActive(int requestId, String input) {
    return requestId == _detectRequestId && input == sourceInput.trim();
  }

  bool _isPermissionDeniedError(Object error) {
    final message = error.toString().toLowerCase();
    return message.contains('permission denied') ||
        message.contains('operation not permitted') ||
        message.contains('errno = 13');
  }

  void _setDetectedSource(DetectedSourceKind? kind, {bool notify = true}) {
    if (detectedSource == kind) return;
    detectedSource = kind;
    if (notify) {
      _notifyStateChanged();
    }
  }

  void setChunkSelection(List<String> paths) {
    chunkSelection = paths;
    _notifyStateChanged();
  }

  Future<void> chooseIndexSource() async {
    try {
      final initialDirectory = await _resolvePickerInitialDirectory();
      final result = await FilePicker.platform.getDirectoryPath(
        initialDirectory: initialDirectory,
      );
      if (result == null || result.trim().isEmpty) return;
      setSourceInput(result.trim());
    } on PlatformException catch (err) {
      statusMessage = err.message ?? 'Failed to open directory picker.';
      _notifyStateChanged();
    }
  }

  Future<void> chooseAndScanDatasetFolder() async {
    try {
      final initialDirectory = await _resolvePickerInitialDirectory();
      final result = await FilePicker.platform.getDirectoryPath(
        initialDirectory: initialDirectory,
      );
      if (result == null || result.trim().isEmpty) return;
      await scanAndAddDatasetsFromFolder(result.trim());
    } on PlatformException catch (err) {
      statusMessage = err.message ?? 'Failed to open directory picker.';
      _notifyStateChanged();
    }
  }

  Future<String?> _resolvePickerInitialDirectory() async {
    final trimmed = sourceInput.trim();
    if (trimmed.isEmpty) return null;
    if (_looksLikeHfInput(trimmed) ||
        _looksLikeZenodoInput(trimmed) ||
        _parseHttpSourceUri(trimmed) != null) {
      return null;
    }
    final expanded = _expandHomePath(trimmed);
    if (!p.isAbsolute(expanded)) return null;
    final type = await FileSystemEntity.type(
      expanded,
      followLinks: true,
    ).onError((_, __) => FileSystemEntityType.notFound);
    if (type == FileSystemEntityType.directory) {
      return expanded;
    }
    if (type == FileSystemEntityType.file) {
      return File(expanded).parent.path;
    }
    return null;
  }

  String _expandHomePath(String input) {
    if (!input.startsWith('~')) return input;
    final home =
        Platform.environment['HOME'] ?? Platform.environment['USERPROFILE'];
    if (home == null || home.isEmpty) return input;
    if (input == '~') return home;
    if (input.startsWith('~/') || input.startsWith(r'~\')) {
      return p.join(home, input.substring(2));
    }
    return input;
  }

  Future<void> chooseChunkFiles() async {
    try {
      final result = await FilePicker.platform.pickFiles(allowMultiple: true);
      if (result == null) return;
      final paths =
          result.files.map((file) => file.path).whereType<String>().toList();
      if (paths.isEmpty) return;
      chunkSelection = paths;
      _notifyStateChanged();
    } on PlatformException catch (err) {
      statusMessage = err.message ?? 'Failed to pick files.';
      _notifyStateChanged();
    }
  }

  void triggerLoad(ViewerMode nextMode,
      {String? payload, List<String>? paths}) {
    requestId = DateTime.now().millisecondsSinceEpoch;
    mode = nextMode;
    selectedChunkName = null;
    selectedItemIndex = null;
    selectedFieldIndex = null;
    selectedShardName = null;
    wdsSelectedSampleKey = null;
    wdsSelectedMemberPath = null;
    wdsSelectedMemberName = null;
    hfConfigOverride = null;
    hfSplitOverride = null;
    hfOffset = 0;
    hfSelectedRowIndex = null;
    hfSelectedFieldName = null;
    hfConfigOptions = null;
    zenodoSelectedFileKey = null;
    zenodoSelectedEntryName = null;
    zenodoEntriesOffset = 0;
    wdsOffset = 0;
    statusMessage = null;
    _remoteLitdataSource = null;
    _httpLitdataSource = null;
    _remoteMdsSource = null;
    _httpMdsSource = null;

    indexSummary = null;
    wdsDirSummary = null;
    hfPreview = null;
    zenodoRecord = null;
    _clearFieldPreviewCache();

    if (nextMode == ViewerMode.litdataIndex) {
      final source = _normalizeDatasetDir(
        payload?.trim().isNotEmpty == true
            ? payload!.trim()
            : sourceInput.trim(),
      );
      if (source.isEmpty) return;
      final httpUri = _parseHttpSourceUri(source);
      if (httpUri != null) {
        indexFuture = _captureFutureError(
          () async {
            final resolved = await _resolveHttpLitdataSource(httpUri);
            _httpLitdataSource = resolved;
            final value = await _litdata.loadIndexFromBytes(
              resolved.indexBytes,
              indexName: resolved.indexName,
            );
            indexSummary = value;
            selectedChunkName =
                value.chunks.isNotEmpty ? value.chunks.first.filename : null;
            _loadLitdataItems();
            _syncActiveDatasetSelection();
            _notifyStateChanged();
            return value;
          }(),
          context: 'LitData load failed',
          fallback: _emptyIndexSummary,
        );
        _notifyStateChanged();
        return;
      }
      final remote = _parseRemoteDirectorySource(source);
      if (remote != null) {
        indexFuture = _captureFutureError(
          () async {
            final resolved = await _resolveRemoteLitdataSource(remote);
            _remoteLitdataSource = resolved;
            final value = await _litdata.loadIndexFromBytes(
              resolved.indexBytes,
              indexName: resolved.indexName,
            );
            indexSummary = value;
            selectedChunkName =
                value.chunks.isNotEmpty ? value.chunks.first.filename : null;
            _loadLitdataItems();
            _syncActiveDatasetSelection();
            _notifyStateChanged();
            return value;
          }(),
          context: 'LitData load failed',
          fallback: _emptyIndexSummary,
        );
        _notifyStateChanged();
        return;
      }
      indexFuture = _captureFutureError(
        _litdata.loadIndex(source).then((value) {
          indexSummary = value;
          _preferences.saveLastIndex(value.rootDir);
          selectedChunkName =
              value.chunks.isNotEmpty ? value.chunks.first.filename : null;
          _loadLitdataItems();
          _syncActiveDatasetSelection();
          _notifyStateChanged();
          return value;
        }),
        context: 'LitData load failed',
        fallback: _emptyIndexSummary,
      );
      _notifyStateChanged();
      return;
    }

    if (nextMode == ViewerMode.mdsIndex) {
      final source = _normalizeDatasetDir(
        payload?.trim().isNotEmpty == true
            ? payload!.trim()
            : sourceInput.trim(),
      );
      if (source.isEmpty) return;
      final httpUri = _parseHttpSourceUri(source);
      if (httpUri != null) {
        indexFuture = _captureFutureError(
          () async {
            final resolved = await _resolveHttpMdsSource(httpUri);
            _httpMdsSource = resolved;
            final value = await _mosaicml.loadIndexFromBytes(
              resolved.indexBytes,
              indexName: resolved.indexName,
            );
            indexSummary = value;
            selectedChunkName = _pickInitialMdsShard(
              value,
              preferredShard: resolved.preferredShardFilename,
            );
            _loadMdsItems();
            _syncActiveDatasetSelection();
            _notifyStateChanged();
            return value;
          }(),
          context: 'MosaicML load failed',
          fallback: _emptyIndexSummary,
        );
        _notifyStateChanged();
        return;
      }
      final remote = _parseRemoteDirectorySource(source);
      if (remote != null) {
        indexFuture = _captureFutureError(
          () async {
            final resolved = await _resolveRemoteMdsSource(remote);
            _remoteMdsSource = resolved;
            final value = await _mosaicml.loadIndexFromBytes(
              resolved.indexBytes,
              indexName: resolved.indexName,
            );
            indexSummary = value;
            selectedChunkName = _pickInitialMdsShard(
              value,
              preferredShard: resolved.preferredShardFilename,
            );
            _loadMdsItems();
            _syncActiveDatasetSelection();
            _notifyStateChanged();
            return value;
          }(),
          context: 'MosaicML load failed',
          fallback: _emptyIndexSummary,
        );
        _notifyStateChanged();
        return;
      }
      indexFuture = _captureFutureError(
        _mosaicml.loadIndex(source).then((value) {
          indexSummary = value;
          _preferences.saveLastIndex(value.rootDir);
          selectedChunkName = _pickInitialMdsShard(value);
          _loadMdsItems();
          _syncActiveDatasetSelection();
          _notifyStateChanged();
          return value;
        }),
        context: 'MosaicML load failed',
        fallback: _emptyIndexSummary,
      );
      _notifyStateChanged();
      return;
    }

    if (nextMode == ViewerMode.litdataChunks) {
      final selected = paths ?? chunkSelection;
      if (selected.isEmpty) return;
      indexFuture = _captureFutureError(
        _litdata.loadChunkList(selected).then((value) {
          indexSummary = value;
          selectedChunkName =
              value.chunks.isNotEmpty ? value.chunks.first.filename : null;
          _loadLitdataItems();
          _syncActiveDatasetSelection();
          _notifyStateChanged();
          return value;
        }),
        context: 'LitData chunk list failed',
        fallback: _emptyIndexSummary,
      );
      _notifyStateChanged();
      return;
    }

    if (nextMode == ViewerMode.webdatasetDir) {
      final source = _normalizeDatasetDir(
        payload?.trim().isNotEmpty == true
            ? payload!.trim()
            : sourceInput.trim(),
      );
      if (source.isEmpty) return;
      final httpUri = _parseHttpSourceUri(source);
      if (httpUri != null) {
        wdsDirFuture = _captureFutureError(
          _resolveHttpWebdatasetDirSummary(
            source: httpUri,
            sourceInput: source,
          ).then((value) {
            wdsDirSummary = value;
            selectedShardName =
                value.shards.isNotEmpty ? value.shards.first.filename : null;
            _loadWdsSamples();
            _syncActiveDatasetSelection();
            _notifyStateChanged();
            return value;
          }),
          context: 'WebDataset load failed',
          fallback: _emptyWdsDirSummary,
        );
        _notifyStateChanged();
        return;
      }
      final remote = _parseRemoteDirectorySource(source);
      if (remote != null) {
        final host = _findRemoteHost(remote.hostId);
        if (host == null) {
          statusMessage = 'Remote host not found: ${remote.hostId}';
          _notifyStateChanged();
          return;
        }
        wdsDirFuture = _captureFutureError(
          () async {
            final hintedFormat =
                _sourceRouter.detectFormatFromPath(remote.path);
            final shards = hintedFormat == DatasetSourceFormat.webdatasetShard
                ? <WdsShardSummary>[
                    WdsShardSummary(
                      filename: p.basename(remote.path).trim().isEmpty
                          ? remote.path
                          : p.basename(remote.path).trim(),
                      path: remote.path,
                      bytes: 0,
                      exists: true,
                    ),
                  ]
                : await () async {
                    final entries = await _remoteDatasets.listEntries(
                      host: host,
                      directoryPath: remote.path,
                      onStatus: (message) {
                        statusMessage = message;
                        _notifyStateChanged();
                      },
                    );
                    return entries
                        .where((entry) =>
                            !entry.isDirectory &&
                            _looksLikeWebdatasetShardName(entry.name))
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
                  }();
            final value = WdsDirSummary(
              // Keep remote source marker here; individual shard reads are
              // streamed directly from the remote host.
              dirPath: source,
              shards: shards,
            );
            wdsDirSummary = value;
            selectedShardName =
                value.shards.isNotEmpty ? value.shards.first.filename : null;
            _loadWdsSamples();
            _syncActiveDatasetSelection();
            _notifyStateChanged();
            return value;
          }(),
          context: 'WebDataset load failed',
          fallback: _emptyWdsDirSummary,
        );
        _notifyStateChanged();
        return;
      }
      wdsDirFuture = _captureFutureError(
        _webdataset.loadDir(source).then((value) {
          wdsDirSummary = value;
          selectedShardName =
              value.shards.isNotEmpty ? value.shards.first.filename : null;
          _loadWdsSamples();
          _syncActiveDatasetSelection();
          _notifyStateChanged();
          return value;
        }),
        context: 'WebDataset load failed',
        fallback: _emptyWdsDirSummary,
      );
      _notifyStateChanged();
      return;
    }

    if (nextMode == ViewerMode.localDirectory) {
      final source = payload?.trim().isNotEmpty == true
          ? payload!.trim()
          : sourceInput.trim();
      final httpUri = _parseHttpSourceUri(source);
      if (httpUri != null) {
        localDirectoryItemsFuture = _captureFutureError(
          _resolveHttpDirectoryItems(httpUri),
          context: 'HTTP source load failed',
          fallback: () => const <LocalDirectoryItem>[],
        ).then((items) {
          localDirectoryItems = items;
          registerLocalDirectoryItems(items);
          if (items.isEmpty) {
            selectedItemIndex = null;
            selectedChunkName = null;
            localFilePreviewFuture = null;
          } else {
            selectLocalDirectoryItem(0);
          }
          _syncActiveDatasetSelection();
          _notifyStateChanged();
          return items;
        });
        _notifyStateChanged();
        return;
      }
      final remote = _parseRemoteDirectorySource(source);
      if (remote != null) {
        localDirectoryItemsFuture = _captureFutureError(
          _loadRemoteDirectoryItems(
            hostId: remote.hostId,
            directoryPath: remote.path,
          ),
          context: 'Remote directory list failed',
          fallback: () => const <LocalDirectoryItem>[],
        ).then((items) {
          localDirectoryItems = items;
          registerLocalDirectoryItems(items);
          if (items.isEmpty) {
            selectedItemIndex = null;
            selectedChunkName = null;
            localFilePreviewFuture = null;
          } else {
            selectLocalDirectoryItem(0);
          }
          _syncActiveDatasetSelection();
          _notifyStateChanged();
          return items;
        });
        _notifyStateChanged();
        return;
      }
      final dirPath = _normalizeDatasetDir(source);
      if (dirPath.isEmpty) return;
      localDirectoryItemsFuture = _captureFutureError(
        _loadLocalDirectoryItems(dirPath),
        context: 'Local directory list failed',
        fallback: () => const <LocalDirectoryItem>[],
      ).then((items) {
        localDirectoryItems = items;
        registerLocalDirectoryItems(items);
        if (items.isEmpty) {
          selectedItemIndex = null;
          selectedChunkName = null;
          localFilePreviewFuture = null;
        } else {
          selectLocalDirectoryItem(0);
        }
        _syncActiveDatasetSelection();
        _notifyStateChanged();
        return items;
      });
      _notifyStateChanged();
      return;
    }

    if (nextMode == ViewerMode.zenodo) {
      final input = payload?.trim().isNotEmpty == true
          ? payload!.trim()
          : sourceInput.trim();
      if (input.isEmpty) return;
      zenodoRecordFuture = _captureFutureError(
        _zenodo.recordSummary(input).then((value) {
          zenodoRecord = value;
          zenodoSelectedFileKey =
              value.files.isNotEmpty ? value.files.first.key : null;
          _loadZenodoEntries();
          _syncActiveDatasetSelection();
          _notifyStateChanged();
          return value;
        }),
        context: 'Zenodo load failed',
        fallback: _emptyZenodoRecord,
      );
      _notifyStateChanged();
      return;
    }

    final input = payload?.trim().isNotEmpty == true
        ? payload!.trim()
        : sourceInput.trim();
    if (input.isEmpty) return;
    AppLogger.info('Load Hugging Face dataset "$input"', tag: 'state');
    hfOffset = 0;
    _refreshHfPreview();
  }

  String _normalizeDatasetDir(String input) {
    if (input.isEmpty) return input;
    if (_parseRemoteDirectorySource(input) != null) {
      return input.trim();
    }
    if (_parseHttpSourceUri(input) != null) {
      return input.trim();
    }
    final expanded = _expandHomePath(input);
    try {
      final type = FileSystemEntity.typeSync(expanded, followLinks: true);
      if (type == FileSystemEntityType.file) {
        return File(expanded).parent.path;
      }
    } catch (_) {}
    return expanded;
  }

  Future<void> loadFromSource() async {
    final input = sourceInput.trim();
    if (input.isEmpty) return;
    await addSource(input);
  }

  Future<bool> addSource(
    String input, {
    bool recordRecent = true,
  }) async {
    final trimmed = input.trim();
    if (trimmed.isEmpty) return false;
    final resolved = await _resolveLoadRequest(trimmed);
    return _addResolvedSource(resolved, recordRecent: recordRecent);
  }

  Future<void> scanAndAddDatasetsFromFolder(String rootPath) async {
    final trimmed = rootPath.trim();
    if (trimmed.isEmpty) return;
    final hadActiveDataset = activeDatasetId != null;
    String? firstAddedDatasetId;
    final scanJobId = ++_scanJobId;
    scanningDatasets = true;
    _scanCancelRequested = false;
    scanDiscoveredCount = 0;
    scanAddedCount = 0;
    statusMessage = 'Scanning datasets in $trimmed...';
    _notifyStateChanged();

    try {
      var canceled = false;
      await for (final dataset
          in _webdataset.discoverLocalDatasetsStream(trimmed)) {
        if (_scanCancelRequested || scanJobId != _scanJobId) {
          canceled = true;
          break;
        }
        scanDiscoveredCount += 1;
        final resolved = _resolvedLoadRequestFromDetected(dataset);
        final wasAdded = _registerResolvedSource(
          resolved,
          expanded: false,
          setActiveIfEmpty: false,
          notify: false,
        );
        if (wasAdded) {
          scanAddedCount += 1;
          firstAddedDatasetId ??=
              _datasetByIdentity(_datasetIdentity(resolved))?.id;
        }
        statusMessage =
            'Scanning $trimmed... found $scanDiscoveredCount, added $scanAddedCount';
        _notifyStateChanged();
      }

      if (_scanCancelRequested || scanJobId != _scanJobId) {
        canceled = true;
      }

      if (scanDiscoveredCount == 0) {
        statusMessage = canceled
            ? 'Scan canceled.'
            : 'No supported datasets found in $trimmed';
        return;
      }

      if (!hadActiveDataset && firstAddedDatasetId != null) {
        await activateDataset(firstAddedDatasetId);
      }

      if (canceled) {
        statusMessage =
            'Scan canceled: found $scanDiscoveredCount, added $scanAddedCount';
      } else {
        statusMessage = scanAddedCount > 0
            ? 'Scan complete: found $scanDiscoveredCount, added $scanAddedCount'
            : 'All discovered datasets were already open';
      }
    } catch (error) {
      statusMessage = error.toString();
    } finally {
      scanningDatasets = false;
      _scanCancelRequested = false;
      if (scanAddedCount > 0) {
        _scheduleSessionPersist();
      }
      _notifyStateChanged();
    }
  }

  Future<_ResolvedLoadRequest> _resolveLoadRequest(String input) async {
    if (_looksLikeHfInput(input)) {
      return _ResolvedLoadRequest(
        mode: ViewerMode.huggingface,
        sourceInput: input,
        payload: input,
      );
    }
    if (_looksLikeZenodoInput(input)) {
      return _ResolvedLoadRequest(
        mode: ViewerMode.zenodo,
        sourceInput: input,
        payload: input,
      );
    }
    final httpUri = _parseHttpSourceUri(input);
    if (httpUri != null) {
      return _resolveHttpLoadRequest(input, httpUri);
    }
    final remote = _parseRemoteDirectorySource(input);
    if (remote != null) {
      final host = _findRemoteHost(remote.hostId);
      if (host == null) {
        return _ResolvedLoadRequest(
          mode: ViewerMode.localDirectory,
          sourceInput: input,
          payload: input,
          paths: const <String>[],
        );
      }
      return _resolveRemoteLoadRequest(
        sourceInput: input,
        remote: remote,
        host: host,
      );
    }
    try {
      final detected = await _webdataset.detectLocalDataset(input);
      return _resolvedLoadRequestFromDetected(detected);
    } catch (error) {
      final normalizedDirectory = _normalizeDatasetDir(input);
      if (_isPermissionDeniedError(error)) {
        return _ResolvedLoadRequest(
          mode: ViewerMode.localDirectory,
          sourceInput: normalizedDirectory,
          payload: normalizedDirectory,
          paths: const <String>[],
        );
      }
      final dirType = await FileSystemEntity.type(
        normalizedDirectory,
        followLinks: true,
      ).catchError((_) => FileSystemEntityType.notFound);
      if (dirType == FileSystemEntityType.directory) {
        final entryPaths = await _listLocalDirectoryPaths(normalizedDirectory);
        return _ResolvedLoadRequest(
          mode: ViewerMode.localDirectory,
          sourceInput: normalizedDirectory,
          payload: normalizedDirectory,
          paths: entryPaths,
        );
      }
      final chunkPaths = await _litdata
          .listChunkFiles(_normalizeDatasetDir(input))
          .catchError((_) => <String>[]);
      if (chunkPaths.isNotEmpty) {
        return _ResolvedLoadRequest(
          mode: ViewerMode.litdataChunks,
          sourceInput: input,
          payload: input,
          paths: chunkPaths,
        );
      }
      return _ResolvedLoadRequest(
        mode: ViewerMode.litdataIndex,
        sourceInput: input,
        payload: input,
      );
    }
  }

  String? _pickInitialMdsShard(
    IndexSummary summary, {
    String? preferredShard,
  }) {
    final chunks = summary.chunks;
    if (chunks.isEmpty) return null;
    final preferred = preferredShard?.trim();
    if (preferred != null && preferred.isNotEmpty) {
      for (final chunk in chunks) {
        final name = chunk.filename.trim();
        if (name.toLowerCase() == preferred.toLowerCase()) {
          return name;
        }
      }
    }
    final first = chunks.first.filename.trim();
    return first.isEmpty ? null : first;
  }

  _ResolvedLoadRequest _resolvedLoadRequestFromDetected(
      LocalDatasetDetectResponse detected) {
    switch (detected.kind) {
      case LocalDatasetKind.litdataIndex:
        return _ResolvedLoadRequest(
          mode: ViewerMode.litdataIndex,
          sourceInput: detected.path,
          payload: detected.path,
        );
      case LocalDatasetKind.mdsIndex:
        return _ResolvedLoadRequest(
          mode: ViewerMode.mdsIndex,
          sourceInput: detected.path,
          payload: detected.path,
        );
      case LocalDatasetKind.webdatasetDir:
        return _ResolvedLoadRequest(
          mode: ViewerMode.webdatasetDir,
          sourceInput: detected.path,
          payload: detected.path,
        );
    }
  }

  Future<List<String>> _listLocalDirectoryPaths(String directoryPath) async {
    final entityType = await FileSystemEntity.type(
      directoryPath,
      followLinks: true,
    ).onError((_, __) => FileSystemEntityType.notFound);
    if (entityType != FileSystemEntityType.directory) {
      return const <String>[];
    }
    final dir = Directory(directoryPath);
    final entries = <String>[];
    try {
      await for (final entry in dir.list(followLinks: false)) {
        entries.add(entry.path);
      }
    } on FileSystemException catch (error, stack) {
      AppLogger.error(
        'Local directory list failed',
        tag: 'state',
        error: error,
        stackTrace: stack,
      );
      if (_isPermissionDeniedError(error)) {
        statusMessage =
            'Permission denied for "$directoryPath". Re-select the dataset directory.';
      }
      return const <String>[];
    }
    entries.sort((left, right) {
      final leftName = p.basename(left).toLowerCase();
      final rightName = p.basename(right).toLowerCase();
      return leftName.compareTo(rightName);
    });
    return entries;
  }

  Future<List<LocalDirectoryItem>> _loadLocalDirectoryItems(
    String directoryPath,
  ) async {
    final normalizedPath = _normalizeDatasetDir(directoryPath);
    final inFlight = _localDirectoryItemsInFlight[normalizedPath];
    if (inFlight != null) {
      return inFlight;
    }
    final future = () async {
      final entityType = await FileSystemEntity.type(
        normalizedPath,
        followLinks: true,
      ).onError((_, __) => FileSystemEntityType.notFound);
      if (entityType != FileSystemEntityType.directory) {
        return const <LocalDirectoryItem>[];
      }
      final dir = Directory(normalizedPath);

      final items = <LocalDirectoryItem>[];
      try {
        await for (final entity in dir.list(followLinks: false)) {
          FileStat stat;
          try {
            stat = await entity.stat();
          } on FileSystemException {
            continue;
          }
          final isDirectory = await _isLocalDirectory(entity.path, stat.type);
          final name = p.basename(entity.path);
          items.add(
            LocalDirectoryItem(
              name: name,
              path: entity.path,
              isDirectory: isDirectory,
              size: isDirectory ? null : stat.size,
              modifiedAt: stat.modified,
            ),
          );
        }
      } on FileSystemException catch (error, stack) {
        AppLogger.error(
          'Local directory list failed',
          tag: 'state',
          error: error,
          stackTrace: stack,
        );
        if (_isPermissionDeniedError(error)) {
          statusMessage =
              'Permission denied for "$normalizedPath". Re-select the dataset directory.';
        }
        return const <LocalDirectoryItem>[];
      }

      items.sort((left, right) {
        if (left.isDirectory != right.isDirectory) {
          return left.isDirectory ? -1 : 1;
        }
        return left.name.toLowerCase().compareTo(right.name.toLowerCase());
      });
      return items;
    }();
    _localDirectoryItemsInFlight[normalizedPath] = future;
    try {
      return await future;
    } finally {
      final current = _localDirectoryItemsInFlight[normalizedPath];
      if (identical(current, future)) {
        _localDirectoryItemsInFlight.remove(normalizedPath);
      }
    }
  }

  Future<List<LocalDirectoryItem>> _loadRemoteDirectoryItems({
    required String hostId,
    required String directoryPath,
  }) async {
    final host = _findRemoteHost(hostId);
    if (host == null) {
      throw FormatException('Remote host not found: $hostId');
    }
    final normalizedPath = _normalizeRemoteDirectoryPath(directoryPath);
    final hintedFormat = _sourceRouter.detectFormatFromPath(normalizedPath);
    if (hintedFormat == DatasetSourceFormat.webdatasetShard ||
        hintedFormat == DatasetSourceFormat.litdataIndex ||
        hintedFormat == DatasetSourceFormat.mdsShard ||
        hintedFormat == DatasetSourceFormat.parquetFile) {
      return <LocalDirectoryItem>[
        LocalDirectoryItem(
          name: p.basename(normalizedPath).trim().isEmpty
              ? normalizedPath
              : p.basename(normalizedPath).trim(),
          path: normalizedPath,
          isDirectory: false,
        ),
      ];
    }

    List<RemotePathEntry> entries = const <RemotePathEntry>[];
    try {
      entries = await _remoteDatasets.listEntries(
        host: host,
        directoryPath: normalizedPath,
        onStatus: (message) {
          statusMessage = message;
        },
      );
    } catch (_) {
      entries = const <RemotePathEntry>[];
    }
    if (entries.isNotEmpty) {
      return entries
          .map(
            (entry) => LocalDirectoryItem(
              name: entry.name,
              path: _normalizeRemoteDirectoryPath(entry.path),
              isDirectory: entry.isDirectory,
              size: entry.sizeBytes,
              modifiedAt: entry.modifiedAt,
            ),
          )
          .toList(growable: false);
    }

    if (normalizedPath.isEmpty) {
      return const <LocalDirectoryItem>[];
    }

    final leafName = p.basename(normalizedPath).trim();
    if (leafName.isEmpty) {
      return const <LocalDirectoryItem>[];
    }
    final parentPathRaw = _normalizeRemoteDirectoryPath(
      p.dirname(normalizedPath),
    );
    final parentPath = parentPathRaw == '.' ? '' : parentPathRaw;
    try {
      final parentEntries = await _remoteDatasets.listEntries(
        host: host,
        directoryPath: parentPath,
        onStatus: (message) {
          statusMessage = message;
        },
      );
      for (final entry in parentEntries) {
        if (entry.name != leafName) continue;
        return <LocalDirectoryItem>[
          LocalDirectoryItem(
            name: entry.name,
            path: _normalizeRemoteDirectoryPath(entry.path),
            isDirectory: entry.isDirectory,
            size: entry.sizeBytes,
            modifiedAt: entry.modifiedAt,
          ),
        ];
      }
    } catch (_) {}

    return const <LocalDirectoryItem>[];
  }

  Future<bool> _isLocalDirectory(String path, FileSystemEntityType type) async {
    if (type == FileSystemEntityType.directory) {
      return true;
    }
    if (type != FileSystemEntityType.link) {
      return false;
    }
    try {
      final resolved = await FileSystemEntity.type(
        path,
        followLinks: true,
      );
      return resolved == FileSystemEntityType.directory;
    } catch (_) {
      return false;
    }
  }

  String _normalizeLocalExt(String path) {
    return _localFileFlow.normalizeExt(path);
  }

  Future<bool> _addResolvedSource(
    _ResolvedLoadRequest resolved, {
    required bool recordRecent,
    bool notify = true,
    bool awaitPrimaryLoad = true,
  }) async {
    final source = resolved.sourceInput.trim();
    if (source.isEmpty) return false;
    if (recordRecent) {
      await _recordRecentSource(source, notify: false);
    }

    sourceInput = source;
    _scheduleSourceDetection(source);

    final identity = _datasetIdentity(resolved);
    final existing = _datasetByIdentity(identity);
    if (existing != null) {
      await activateDataset(existing.id);
      return false;
    }

    triggerLoad(resolved.mode,
        payload: resolved.payload, paths: resolved.paths);

    // Register the dataset immediately so it appears in the explorer
    // while data loads in the background.
    final dataset = _upsertOpenedDataset(resolved);
    if (dataset == null) {
      if (notify) {
        _notifyStateChanged();
      }
      return false;
    }
    activeDatasetId = dataset.id;
    _notifyStateChanged();

    if (awaitPrimaryLoad) {
      await _awaitPrimaryLoad(resolved.mode);

      // If loading failed, the .then() callback never ran so the summary
      // fields are still null. Fill them with empty values so the UI
      // shows "No entries" instead of spinning forever.
      _ensurePrimaryDataNotNull(resolved.mode);

      _syncActiveDatasetSelection();
      if (notify) {
        _notifyStateChanged();
      }
    }
    return true;
  }
}
