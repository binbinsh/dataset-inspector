part of 'viewer_state.dart';

extension ViewerStateSessionSelection on ViewerState {
  Future<void> _recordRecentSource(String input, {bool notify = true}) async {
    final trimmed = input.trim();
    if (trimmed.isEmpty) return;
    final updated = <String>[
      trimmed,
      ...recentSources.where((source) => source != trimmed),
    ];
    if (updated.length > _recentSourceLimit) {
      updated.removeRange(_recentSourceLimit, updated.length);
    }
    recentSources = updated;
    await _preferences.saveRecentSources(updated);
    if (notify) {
      _notifyStateChanged();
    }
  }

  LoadedDatasetSource? _datasetById(String datasetId) {
    for (final dataset in openedDatasets) {
      if (dataset.id == datasetId) return dataset;
    }
    return null;
  }

  void cancelDatasetScan() {
    if (!scanningDatasets) return;
    _scanCancelRequested = true;
    statusMessage =
        'Stopping scan... found $scanDiscoveredCount, added $scanAddedCount';
    _notifyStateChanged();
  }

  void setAllDatasetsExpanded(bool expanded) {
    if (openedDatasets.isEmpty) return;
    var changed = false;
    for (final dataset in openedDatasets) {
      if (dataset.expanded == expanded) continue;
      dataset.expanded = expanded;
      changed = true;
    }
    if (changed) {
      _scheduleSessionPersist();
      _notifyStateChanged();
    }
  }

  void toggleDatasetExpanded(String datasetId) {
    final dataset = _datasetById(datasetId);
    if (dataset == null) return;
    dataset.expanded = !dataset.expanded;
    _scheduleSessionPersist();
    _notifyStateChanged();
  }

  Future<void> activateDataset(String datasetId) async {
    final dataset = _datasetById(datasetId);
    if (dataset == null) return;
    if (activeDatasetId == dataset.id && mode == dataset.mode) return;

    activeDatasetId = dataset.id;
    sourceInput = dataset.sourceInput;
    _scheduleSourceDetection(sourceInput);
    _notifyStateChanged();

    triggerLoad(dataset.mode, payload: dataset.payload, paths: dataset.paths);
    await _awaitPrimaryLoad(dataset.mode);
    _ensurePrimaryDataNotNull(dataset.mode);
    _restorePrimarySelectionForDataset(dataset);
    _syncActiveDatasetSelection();
    _notifyStateChanged();
  }

  Future<void> removeDataset(String datasetId) async {
    final removingActive = activeDatasetId == datasetId;
    openedDatasets =
        openedDatasets.where((dataset) => dataset.id != datasetId).toList();
    if (!removingActive) {
      _scheduleSessionPersist();
      _notifyStateChanged();
      return;
    }
    if (openedDatasets.isEmpty) {
      activeDatasetId = null;
      _clearLoadedViewState();
      _scheduleSessionPersist();
      _notifyStateChanged();
      return;
    }
    final fallback = openedDatasets.last;
    await activateDataset(fallback.id);
  }

  Future<void> activateDatasetChunk(
      String datasetId, String chunkFilename) async {
    await activateDataset(datasetId);
    selectChunk(chunkFilename);
  }

  Future<void> activateDatasetShard(
      String datasetId, String shardFilename) async {
    await activateDataset(datasetId);
    selectWdsShard(shardFilename);
  }

  Future<void> activateDatasetHfConfig(
    String datasetId, {
    required String config,
    required String split,
  }) async {
    await activateDataset(datasetId);
    setHfConfigSplit(config, split);
  }

  Future<void> activateDatasetZenodoFile(
      String datasetId, String fileKey) async {
    await activateDataset(datasetId);
    selectZenodoFile(fileKey);
  }

  void _clearLoadedViewState() {
    mode = null;
    requestId = DateTime.now().millisecondsSinceEpoch;
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

    indexFuture = null;
    indexSummary = null;
    litdataItemsFuture = null;
    mdsItemsFuture = null;
    fieldPreviewFuture = null;
    mdsFieldPreviewFuture = null;
    audioPreviewFuture = null;

    wdsDirFuture = null;
    wdsDirSummary = null;
    wdsSamplesFuture = null;
    wdsSamples = null;
    wdsPreviewFuture = null;
    wdsAudioPreviewFuture = null;

    hfPreviewFuture = null;
    hfPreview = null;

    zenodoRecordFuture = null;
    zenodoRecord = null;
    zenodoFilePreviewFuture = null;
    zenodoZipEntriesFuture = null;
    zenodoZipEntries = null;
    zenodoTarEntriesFuture = null;
    zenodoTarEntries = null;
    zenodoEntryPreviewFuture = null;
    zenodoInlineMediaFuture = null;

    localDirectoryItems = [];
    localDirectoryItemsFuture = null;
    localFilePreviewFuture = null;
    _localDirectoryItemCache.clear();
    _remoteLitdataSource = null;
    _httpLitdataSource = null;
    _remoteMdsSource = null;
    _httpMdsSource = null;

    _clearFieldPreviewCache();
  }

  void _syncDatasetFromCurrentState(LoadedDatasetSource dataset) {
    dataset.selectedChunkName = selectedChunkName;
    dataset.selectedShardName = selectedShardName;
    dataset.selectedHfConfig = hfConfigOverride;
    dataset.selectedHfSplit = hfSplitOverride;
    dataset.selectedZenodoFileKey = zenodoSelectedFileKey;

    if (mode == ViewerMode.huggingface) {
      dataset.hfPreview = hfPreview;
      dataset.hfConfigOptions = hfConfigOptions;
      return;
    }
    if (mode == ViewerMode.zenodo) {
      dataset.zenodoRecord = zenodoRecord;
      return;
    }
    if (mode == ViewerMode.webdatasetDir) {
      dataset.wdsDirSummary = wdsDirSummary;
      return;
    }
    dataset.indexSummary = indexSummary;
  }

  void _syncActiveDatasetSelection() {
    final dataset = activeDataset;
    if (dataset == null) return;
    _syncDatasetFromCurrentState(dataset);
    _scheduleSessionPersist();
  }

  void _scheduleSessionPersist(
      {Duration delay = const Duration(milliseconds: 600)}) {
    if (_restoringSession) return;
    _sessionPersistTimer?.cancel();
    _sessionPersistTimer = Timer(delay, () {
      unawaited(_persistSessionSnapshot());
    });
  }

  Future<void> _persistSessionSnapshot() async {
    final snapshot = _buildSessionSnapshot();
    try {
      await _preferences.saveViewerSession(snapshot);
    } catch (error, stack) {
      AppLogger.error(
        'Failed to persist viewer session',
        tag: 'state',
        error: error,
        stackTrace: stack,
      );
    }
  }

  ViewerSessionSnapshot _buildSessionSnapshot() {
    final datasets = <PersistedDatasetSource>[];
    for (final dataset in openedDatasets) {
      datasets.add(PersistedDatasetSource(
        mode: dataset.mode.name,
        sourceInput: dataset.sourceInput,
        payload: dataset.payload,
        paths: dataset.paths == null ? null : List<String>.from(dataset.paths!),
        expanded: dataset.expanded,
        identity: dataset.identity,
        selectedChunkName: dataset.selectedChunkName,
        selectedShardName: dataset.selectedShardName,
        selectedHfConfig: dataset.selectedHfConfig,
        selectedHfSplit: dataset.selectedHfSplit,
        selectedZenodoFileKey: dataset.selectedZenodoFileKey,
      ));
    }
    final active = activeDataset;
    final trimmedInput = sourceInput.trim();
    return ViewerSessionSnapshot(
      datasets: datasets,
      activeIdentity: active?.identity,
      sourceInput: trimmedInput.isEmpty ? null : trimmedInput,
    );
  }

  Future<void> _restoreSession() async {
    final session = await _preferences.readViewerSession();
    if (session == null) return;

    if (session.datasets.isEmpty) {
      final draft = session.sourceInput?.trim() ?? '';
      if (draft.isNotEmpty) {
        sourceInput = draft;
        _scheduleSourceDetection(sourceInput);
      }
      return;
    }

    _restoringSession = true;
    try {
      openedDatasets = [];
      activeDatasetId = null;

      final restoredByIdentity = <String, LoadedDatasetSource>{};
      for (final persisted in session.datasets) {
        final mode = _parseViewerMode(persisted.mode);
        if (mode == null) continue;
        final resolved = _ResolvedLoadRequest(
          mode: mode,
          sourceInput: persisted.sourceInput,
          payload: persisted.payload,
          paths: persisted.paths,
        );
        final added = _registerResolvedSource(
          resolved,
          expanded: persisted.expanded,
          setActiveIfEmpty: false,
          notify: false,
        );
        if (!added) continue;
        final dataset = _datasetByIdentity(_datasetIdentity(resolved));
        if (dataset == null) continue;
        _applyPersistedSelection(dataset, persisted);
        restoredByIdentity[persisted.identity ?? dataset.identity] = dataset;
      }

      LoadedDatasetSource? active;
      final activeIdentity = session.activeIdentity;
      if (activeIdentity != null && activeIdentity.trim().isNotEmpty) {
        active = restoredByIdentity[activeIdentity.trim()];
      }
      active ??= openedDatasets.isNotEmpty ? openedDatasets.first : null;
      if (active != null) {
        await activateDataset(active.id);
      } else {
        final draft = session.sourceInput?.trim() ?? '';
        if (draft.isNotEmpty) {
          sourceInput = draft;
          _scheduleSourceDetection(sourceInput);
        }
        _notifyStateChanged();
      }
    } finally {
      _restoringSession = false;
    }
    _scheduleSessionPersist();
  }

  void _applyPersistedSelection(
    LoadedDatasetSource dataset,
    PersistedDatasetSource persisted,
  ) {
    dataset.selectedChunkName = persisted.selectedChunkName;
    dataset.selectedShardName = persisted.selectedShardName;
    dataset.selectedHfConfig = persisted.selectedHfConfig;
    dataset.selectedHfSplit = persisted.selectedHfSplit;
    dataset.selectedZenodoFileKey = persisted.selectedZenodoFileKey;
  }

  ViewerMode? _parseViewerMode(String? name) {
    if (name == null) return null;
    for (final mode in ViewerMode.values) {
      if (mode.name == name) return mode;
    }
    return null;
  }

  void _restorePrimarySelectionForDataset(LoadedDatasetSource dataset) {
    if (dataset.mode == ViewerMode.huggingface) {
      final config = dataset.selectedHfConfig;
      final split = dataset.selectedHfSplit;
      if (config != null && split != null) {
        final same = hfConfigOverride == config && hfSplitOverride == split;
        hfConfigOverride = config;
        hfSplitOverride = split;
        if (!same) {
          _refreshHfPreview();
        }
      }
      return;
    }
    if (dataset.mode == ViewerMode.zenodo) {
      if (dataset.selectedZenodoFileKey != null) {
        selectZenodoFile(dataset.selectedZenodoFileKey);
      }
      return;
    }
    if (dataset.mode == ViewerMode.webdatasetDir) {
      if (dataset.selectedShardName != null) {
        selectWdsShard(dataset.selectedShardName);
      }
      return;
    }
    if (dataset.mode == ViewerMode.localDirectory &&
        dataset.selectedChunkName != null) {
      final idx = localDirectoryItems.indexWhere(
        (item) => item.path == dataset.selectedChunkName,
      );
      if (idx >= 0) {
        selectLocalDirectoryItem(idx);
        return;
      }
    }
    if (dataset.selectedChunkName != null) {
      selectChunk(dataset.selectedChunkName);
    }
  }

  void selectChunk(String? filename) {
    selectedChunkName = filename;
    selectedItemIndex = null;
    selectedFieldIndex = null;
    fieldPreviewFuture = null;
    mdsFieldPreviewFuture = null;
    _clearFieldPreviewCache();
    _loadLitdataItems();
    _loadMdsItems();
    _syncActiveDatasetSelection();
    _notifyStateChanged();
  }

  void selectItem(int? idx, {int? fieldCount}) {
    if (mode == ViewerMode.localDirectory) {
      final selectedPath = selectedChunkName;
      final selected = selectedLocalDirectoryItem;
      final isMdsSelection = (selectedPath != null &&
              selectedPath.isNotEmpty &&
              _looksLikeMdsShardPath(selectedPath)) ||
          (selected != null &&
              !selected.isDirectory &&
              _looksLikeMdsShardPath(selected.path));
      final isRemoteCompressedMdsSelection = isRemoteDirectoryMode &&
          selectedPath != null &&
          selectedPath.isNotEmpty &&
          (selectedPath.toLowerCase().endsWith('.zst') ||
              selectedPath.toLowerCase().endsWith('.zstd'));
      if (!isMdsSelection) {
        selectLocalDirectoryItem(idx);
        return;
      }
      selectedItemIndex = idx;
      if (idx == null) {
        selectedFieldIndex = null;
        localFilePreviewFuture = null;
        _notifyStateChanged();
        return;
      }
      if (isRemoteCompressedMdsSelection) {
        // Keep field selection stable across item switches, but avoid
        // auto-preview on remote compressed shards to prevent slow UI updates.
        final keepField = selectedFieldIndex != null &&
            fieldCount != null &&
            selectedFieldIndex! >= 0 &&
            selectedFieldIndex! < fieldCount;
        if (keepField) {
          final fieldIndex = selectedFieldIndex!;
          selectField(fieldIndex);
          return;
        }
        selectedFieldIndex = null;
        localFilePreviewFuture = null;
        _notifyStateChanged();
        return;
      }
      final keepField = selectedFieldIndex != null &&
          fieldCount != null &&
          selectedFieldIndex! >= 0 &&
          selectedFieldIndex! < fieldCount;
      if (keepField) {
        final fieldIndex = selectedFieldIndex!;
        selectField(fieldIndex);
        return;
      }
      selectedFieldIndex = null;
      localFilePreviewFuture = null;
      _notifyStateChanged();
      return;
    }
    selectedItemIndex = idx;
    if (idx == null) {
      selectedFieldIndex = null;
      fieldPreviewFuture = null;
      mdsFieldPreviewFuture = null;
      _clearFieldPreviewCache();
      _notifyStateChanged();
      return;
    }

    final keepField = selectedFieldIndex != null &&
        fieldCount != null &&
        selectedFieldIndex! >= 0 &&
        selectedFieldIndex! < fieldCount;

    if (keepField && selectedChunkName != null) {
      final fieldIndex = selectedFieldIndex!;
      selectField(fieldIndex);
      return;
    }

    selectedFieldIndex = null;
    fieldPreviewFuture = null;
    mdsFieldPreviewFuture = null;
    _clearFieldPreviewCache();
    if (fieldCount != null) {
      _primeFieldPreviewsForItem(fieldCount);
    }
    _notifyStateChanged();
  }

  void selectLocalDirectoryItem(int? idx) {
    if (idx == null) {
      selectLocalDirectoryItemByPath(null);
      return;
    }
    if (idx < 0 || idx >= localDirectoryItems.length) {
      selectLocalDirectoryItemByPath(null);
      return;
    }
    selectLocalDirectoryItemByPath(localDirectoryItems[idx].path);
  }

  void selectLocalDirectoryItemByPath(String? path) {
    if (mode != ViewerMode.localDirectory) {
      return;
    }
    selectedChunkName = path;
    selectedItemIndex = null;
    selectedFieldIndex = null;
    localFilePreviewFuture = null;
    if (path == null || path.isEmpty) {
      _notifyStateChanged();
      return;
    }
    final idx = localDirectoryItems.indexWhere((item) => item.path == path);
    LocalDirectoryItem? selectedItem;
    if (idx >= 0 && idx < localDirectoryItems.length) {
      selectedItem = localDirectoryItems[idx];
      selectedItemIndex = idx;
    } else {
      selectedItem = _localDirectoryItemCache[path];
      if (selectedItem == null) {
        selectedItem = _fallbackLocalDirectoryItem(path);
      }
    }
    if (selectedItem != null && !selectedItem.isDirectory) {
      if (_looksLikeMdsShardPath(selectedItem.path)) {
        selectedItemIndex = null;
        localFilePreviewFuture = null;
      } else {
        localFilePreviewFuture = _captureFutureError(
          _readDirectoryFilePreview(path),
          context: 'Local file preview failed',
          fallback: _emptyFieldPreview,
        );
      }
    }
    _syncActiveDatasetSelection();
      _notifyStateChanged();
  }

  LocalDirectoryItem? _fallbackLocalDirectoryItem(String path) {
    final remote = _activeRemoteDirectorySource;
    final httpUri = _parseHttpSourceUri(path);
    if (remote != null || httpUri != null) {
      return null;
    }
    final normalized = p.normalize(path);
    try {
      final type = FileSystemEntity.typeSync(normalized, followLinks: true);
      if (type != FileSystemEntityType.file) return null;
      final stat = FileStat.statSync(normalized);
      final fallback = LocalDirectoryItem(
        name: p.basename(normalized),
        path: normalized,
        isDirectory: false,
        size: stat.size,
        modifiedAt: stat.modified,
      );
      _localDirectoryItemCache[normalized] = fallback;
      _localDirectoryItemCache[path] = fallback;
      return fallback;
    } catch (_) {
      return null;
    }
  }

  List<LocalDirectoryField> localSelectedFileFields() {
    final item = selectedLocalDirectoryItem;
    if (item == null) return const <LocalDirectoryField>[];
    final size = item.size != null ? item.size! : 0;
    return <LocalDirectoryField>[
      LocalDirectoryField(name: 'Name', value: item.name),
      LocalDirectoryField(
        name: 'Type',
        value: item.isDirectory ? 'Directory' : 'File',
      ),
      LocalDirectoryField(name: 'Path', value: item.path),
      LocalDirectoryField(
        name: 'Size',
        value: item.size == null ? '-' : _formatBytes(size),
      ),
      LocalDirectoryField(
        name: 'Modified',
        value: item.modifiedAt == null
            ? '-'
            : item.modifiedAt!.toLocal().toString(),
      ),
    ];
  }

  LocalDirectoryItem? get selectedLocalDirectoryItem {
    final index = selectedItemIndex;
    if (mode != ViewerMode.localDirectory) {
      return null;
    }
    final path = selectedChunkName;
    if (path == null) {
      return null;
    }
    if (index != null &&
        index >= 0 &&
        index < localDirectoryItems.length &&
        localDirectoryItems[index].path == path) {
      return localDirectoryItems[index];
    }
    for (final item in localDirectoryItems) {
      if (item.path == path) {
        return item;
      }
    }
    return _localDirectoryItemCache[path];
  }

  String? get localSelectedFileName {
    return selectedLocalDirectoryItem?.name;
  }

  LocalDirectoryField? localSelectedFileField() {
    final item = selectedLocalDirectoryItem;
    if (item == null || selectedFieldIndex == null) return null;
    final fields = localSelectedFileFields();
    final index = selectedFieldIndex!;
    if (index < 0 || index >= fields.length) return null;
    return fields[index];
  }

  Future<FieldPreview> _readDirectoryFilePreview(String path) async {
    final remote = _activeRemoteDirectorySource;
    if (remote != null) {
      return _readRemoteFilePreview(
        remotePath: path,
      );
    }
    return _readLocalFilePreview(path);
  }

  Future<FieldPreview> _readRemoteFilePreview({
    required String remotePath,
  }) async {
    final bytes = await readDirectoryFileBytes(
      remotePath,
      maxBytes: _localDirPreviewBytes,
    );
    return _localFileFlow.buildRemotePreview(
      path: remotePath,
      bytes: bytes,
      hexSnippetBytes: _localDirHexSnippetBytes,
    );
  }

  Future<FieldPreview> _readLocalFilePreview(String path) async {
    return _localFileFlow.readLocalFilePreview(
      path: path,
      emptyPreview: _emptyFieldPreview,
      previewBytes: _localDirPreviewBytes,
      hexSnippetBytes: _localDirHexSnippetBytes,
    );
  }
}
