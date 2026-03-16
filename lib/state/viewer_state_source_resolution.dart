part of 'viewer_state.dart';

extension ViewerStateSourceResolution on ViewerState {
  Future<void> _awaitPrimaryLoad(ViewerMode sourceMode) async {
    if (sourceMode == ViewerMode.litdataIndex ||
        sourceMode == ViewerMode.litdataChunks ||
        sourceMode == ViewerMode.mdsIndex) {
      final future = indexFuture;
      if (future != null) await future;
      return;
    }
    if (sourceMode == ViewerMode.webdatasetDir) {
      final future = wdsDirFuture;
      if (future != null) await future;
      return;
    }
    if (sourceMode == ViewerMode.localDirectory) {
      final future = localDirectoryItemsFuture;
      if (future != null) await future;
      return;
    }
    if (sourceMode == ViewerMode.zenodo) {
      final future = zenodoRecordFuture;
      if (future != null) await future;
      return;
    }
    final future = hfPreviewFuture;
    if (future != null) await future;
  }

  LoadedDatasetSource? _upsertOpenedDataset(
    _ResolvedLoadRequest resolved,
  ) {
    final currentMode = mode;
    if (currentMode == null) return null;
    final identity = _datasetIdentity(resolved);
    LoadedDatasetSource? existing;
    for (final dataset in openedDatasets) {
      if (dataset.identity == identity) {
        existing = dataset;
        break;
      }
    }
    final dataset = existing ??
        LoadedDatasetSource(
          id: _nextDatasetId(),
          identity: identity,
          sourceInput: resolved.sourceInput,
          mode: currentMode,
          label: _datasetLabel(resolved),
          payload: resolved.payload,
          paths: resolved.paths == null
              ? null
              : List<String>.from(resolved.paths!),
        );
    if (existing == null) {
      openedDatasets = [...openedDatasets, dataset];
    }
    _syncDatasetFromCurrentState(dataset);
    return dataset;
  }

  bool _registerResolvedSource(
    _ResolvedLoadRequest resolved, {
    bool expanded = true,
    bool setActiveIfEmpty = true,
    bool notify = true,
  }) {
    final identity = _datasetIdentity(resolved);
    if (_datasetByIdentity(identity) != null) {
      return false;
    }

    final dataset = LoadedDatasetSource(
      id: _nextDatasetId(),
      identity: identity,
      sourceInput: resolved.sourceInput,
      mode: resolved.mode,
      label: _datasetLabel(resolved),
      payload: resolved.payload,
      paths: resolved.paths == null ? null : List<String>.from(resolved.paths!),
      expanded: expanded,
    );
    openedDatasets = [...openedDatasets, dataset];
    if (setActiveIfEmpty && activeDatasetId == null) {
      activeDatasetId = dataset.id;
    }
    if (notify) {
      _notifyStateChanged();
    }
    return true;
  }

  String _datasetIdentity(_ResolvedLoadRequest resolved) {
    if (resolved.mode == ViewerMode.litdataChunks) {
      final sorted = [...?resolved.paths]..sort();
      return '${resolved.mode.name}:${sorted.join('|')}';
    }
    if (_parseRemoteDirectorySource(resolved.sourceInput) != null) {
      return '${resolved.mode.name}:${resolved.sourceInput.trim()}';
    }
    final payload = resolved.payload?.trim();
    final source =
        payload != null && payload.isNotEmpty ? payload : resolved.sourceInput;
    return '${resolved.mode.name}:${_normalizedSourceIdentity(resolved.mode, source)}';
  }

  String _normalizedSourceIdentity(ViewerMode sourceMode, String source) {
    final trimmed = source.trim();
    if (trimmed.isEmpty) return '';
    if (_parseRemoteDirectorySource(trimmed) != null) {
      return trimmed;
    }
    if (_parseHttpSourceUri(trimmed) != null) {
      return trimmed;
    }
    if (sourceMode == ViewerMode.huggingface ||
        sourceMode == ViewerMode.zenodo) {
      return trimmed;
    }
    final expanded = _expandHomePath(trimmed);
    if (p.isAbsolute(expanded)) {
      return p.normalize(expanded);
    }
    return p.normalize(p.absolute(expanded));
  }

  LoadedDatasetSource? _datasetByIdentity(String identity) {
    for (final dataset in openedDatasets) {
      if (dataset.identity == identity) return dataset;
    }
    return null;
  }

  String _datasetLabel(_ResolvedLoadRequest resolved) {
    final remote = _parseRemoteDirectorySource(resolved.sourceInput);
    if (remote != null) {
      final host = _findRemoteHost(remote.hostId);
      final hostLabel = host?.label.trim().isNotEmpty == true
          ? host!.label.trim()
          : remote.hostId;
      final suffix = remote.path.isEmpty ? '/' : '/${remote.path}';
      return '$hostLabel$suffix';
    }
    if (_parseHttpSourceUri(resolved.sourceInput) != null) {
      return resolved.sourceInput.trim();
    }
    final payload = resolved.payload?.trim();
    final source = payload != null && payload.isNotEmpty
        ? payload
        : resolved.sourceInput.trim();
    if (resolved.mode == ViewerMode.huggingface) {
      return _huggingFaceLabel(source);
    }
    if (resolved.mode == ViewerMode.zenodo) {
      return _zenodoLabel(source);
    }
    return _normalizedSourceIdentity(resolved.mode, source);
  }

  Future<_ResolvedLoadRequest> _resolveRemoteLoadRequest({
    required String sourceInput,
    required _RemoteDirectorySource remote,
    required RemoteHostConfig host,
  }) async {
    final fallback = _ResolvedLoadRequest(
      mode: ViewerMode.localDirectory,
      sourceInput: sourceInput,
      payload: sourceInput,
      paths: const <String>[],
    );
    if (host.id.trim().isEmpty) {
      return fallback;
    }
    final hintedFormat = _sourceRouter.detectFormatFromPath(remote.path);
    if (hintedFormat == DatasetSourceFormat.webdatasetShard) {
      return _ResolvedLoadRequest(
        mode: ViewerMode.webdatasetDir,
        sourceInput: sourceInput,
        payload: sourceInput,
      );
    }
    if (hintedFormat == DatasetSourceFormat.litdataIndex) {
      try {
        await _resolveRemoteLitdataSource(remote);
        return _ResolvedLoadRequest(
          mode: ViewerMode.litdataIndex,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      } catch (_) {}
      try {
        await _resolveRemoteMdsSource(remote);
        return _ResolvedLoadRequest(
          mode: ViewerMode.mdsIndex,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      } catch (_) {}
    }
    if (hintedFormat == DatasetSourceFormat.mdsShard) {
      try {
        await _resolveRemoteMdsSource(remote);
        return _ResolvedLoadRequest(
          mode: ViewerMode.mdsIndex,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      } catch (_) {
        return _ResolvedLoadRequest(
          mode: ViewerMode.localDirectory,
          sourceInput: sourceInput,
          payload: sourceInput,
          paths: <String>[remote.path],
        );
      }
    }
    if (hintedFormat == DatasetSourceFormat.parquetFile) {
      return _ResolvedLoadRequest(
        mode: ViewerMode.localDirectory,
        sourceInput: sourceInput,
        payload: sourceInput,
        paths: <String>[remote.path],
      );
    }
    final detectedKind = await _detectRemoteDirectorySourceKind(remote);
    switch (detectedKind) {
      case DetectedSourceKind.webdatasetDir:
        return _ResolvedLoadRequest(
          mode: ViewerMode.webdatasetDir,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      case DetectedSourceKind.litdataIndex:
      case DetectedSourceKind.litdataChunks:
        return _ResolvedLoadRequest(
          mode: ViewerMode.litdataIndex,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      case DetectedSourceKind.mdsIndex:
        return _ResolvedLoadRequest(
          mode: ViewerMode.mdsIndex,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      case DetectedSourceKind.localDirectory:
      case DetectedSourceKind.unknown:
      case DetectedSourceKind.detecting:
      case DetectedSourceKind.huggingface:
      case DetectedSourceKind.zenodo:
        return fallback;
    }
  }

  List<String> _activeSourceCandidates() {
    final active = activeDataset;
    return <String>[
      if (active?.sourceInput.trim().isNotEmpty == true)
        active!.sourceInput.trim(),
      if (active?.payload?.trim().isNotEmpty == true) active!.payload!.trim(),
      if (sourceInput.trim().isNotEmpty) sourceInput.trim(),
    ];
  }

  _RemoteDirectorySource? _activeRemoteWebdatasetSource() {
    if (mode != ViewerMode.webdatasetDir) return null;
    for (final candidate in _activeSourceCandidates()) {
      final parsed = _parseRemoteDirectorySource(candidate);
      if (parsed != null) return parsed;
    }
    return null;
  }

  Uri? _activeHttpWebdatasetSourceUri() {
    if (mode != ViewerMode.webdatasetDir) return null;
    for (final candidate in _activeSourceCandidates()) {
      final uri = _parseHttpSourceUri(candidate);
      if (uri != null) return uri;
    }
    return null;
  }

  Uri _httpWebdatasetDirectoryUri(Uri source) {
    return _httpProbeDirectoryUri(source);
  }

  Uri _httpProbeDirectoryUri(Uri source) {
    final format = _sourceRouter.detectFormatFromPath(source.path);
    if (format == DatasetSourceFormat.webdatasetShard ||
        format == DatasetSourceFormat.mdsShard ||
        format == DatasetSourceFormat.litdataIndex ||
        format == DatasetSourceFormat.parquetFile) {
      return _httpDatasets.parentDirectoryUri(source);
    }
    return _ensureHttpDirectoryUri(source);
  }

  Uri? _activeHttpWebdatasetShardUri([String? shardFilename]) {
    final source = _activeHttpWebdatasetSourceUri();
    if (source == null) return null;
    final format = _sourceRouter.detectFormatFromPath(source.path);
    if (format == DatasetSourceFormat.webdatasetShard) {
      return source;
    }
    final selected = (shardFilename ?? selectedShardName)?.trim();
    if (selected == null || selected.isEmpty) return null;
    final directoryUri = _httpWebdatasetDirectoryUri(source);
    return _httpDatasets.resolveFromDirectory(directoryUri, selected);
  }

  String _joinRemoteDirectoryPath(String left, String right) {
    final leftNorm = _normalizeRemoteDirectoryPath(left);
    final rightNorm = _normalizeRemoteDirectoryPath(right);
    if (leftNorm.isEmpty) return rightNorm;
    if (rightNorm.isEmpty) return leftNorm;
    return '$leftNorm/$rightNorm';
  }

  String _remoteCompressedMdsCacheKey({
    required RemoteHostConfig host,
    required String remotePath,
  }) {
    final hostId = host.id.trim();
    final normalizedPath = _normalizeRemoteDirectoryPath(remotePath);
    return 'remote-mds-zstd:${host.type.name}:$hostId:$normalizedPath';
  }

  String _httpCompressedMdsCacheKey(Uri shardUri) {
    final normalized = shardUri.replace(fragment: '').toString();
    return 'http-mds-zstd:$normalized';
  }

  Future<_RemoteLitdataSource> _resolveRemoteLitdataSource(
    _RemoteDirectorySource remote,
  ) async {
    final host = _findRemoteHost(remote.hostId);
    if (host == null) {
      throw FormatException('Remote host not found: ${remote.hostId}');
    }
    final normalized = _normalizeRemoteDirectoryPath(remote.path);
    var directoryPath = normalized;
    final hintedName = p.basename(normalized).trim();
    final candidates = <String>[
      'index.json',
      'index.json.zstd',
      'index.json.zst',
      '0.index.json',
      '0.index.json.zstd',
      '0.index.json.zst',
    ];
    if (_looksLikeLitdataIndexName(hintedName)) {
      directoryPath =
          _normalizeRemoteDirectoryPath(p.dirname(normalized)) == '.'
              ? ''
              : _normalizeRemoteDirectoryPath(p.dirname(normalized));
      candidates.insert(0, hintedName);
    }

    Uint8List? indexBytes;
    String? indexName;
    for (final candidate in candidates) {
      final remotePath = _joinRemoteDirectoryPath(directoryPath, candidate);
      try {
        final bytes = await _remoteDatasets.readBytesFile(
          host: host,
          remotePath: remotePath,
          onStatus: (message) {
            statusMessage = message;
          },
        );
        await _litdata.loadIndexFromBytes(bytes, indexName: candidate);
        indexBytes = bytes;
        indexName = candidate;
        break;
      } catch (_) {}
    }
    if (indexBytes == null || indexName == null) {
      final display = directoryPath.isEmpty ? '/' : '/$directoryPath';
      throw FormatException(
          'LitData index not found in remote directory: $display');
    }

    return _RemoteLitdataSource(
      hostId: remote.hostId,
      directoryPath: directoryPath,
      indexName: indexName,
      indexBytes: indexBytes,
    );
  }

  Future<_RemoteMdsSource> _resolveRemoteMdsSource(
    _RemoteDirectorySource remote,
  ) async {
    final host = _findRemoteHost(remote.hostId);
    if (host == null) {
      throw FormatException('Remote host not found: ${remote.hostId}');
    }
    final normalized = _normalizeRemoteDirectoryPath(remote.path);
    var directoryPath = normalized;
    final hintedName = p.basename(normalized).trim();
    String? preferredShardFilename;
    final candidates = <String>[
      ...DatasetSourceRoutingService.mdsIndexCandidates
    ];
    if (_sourceRouter.isMdsShardName(hintedName)) {
      directoryPath =
          _normalizeRemoteDirectoryPath(p.dirname(normalized)) == '.'
              ? ''
              : _normalizeRemoteDirectoryPath(p.dirname(normalized));
      preferredShardFilename = hintedName;
    } else if (_looksLikeLitdataIndexName(hintedName)) {
      directoryPath =
          _normalizeRemoteDirectoryPath(p.dirname(normalized)) == '.'
              ? ''
              : _normalizeRemoteDirectoryPath(p.dirname(normalized));
      candidates.insert(0, hintedName);
    }

    Uint8List? indexBytes;
    String? indexName;
    for (final candidate in candidates) {
      final remotePath = _joinRemoteDirectoryPath(directoryPath, candidate);
      try {
        final bytes = await _remoteDatasets.readBytesFile(
          host: host,
          remotePath: remotePath,
          onStatus: (message) {
            statusMessage = message;
          },
        );
        await _mosaicml.loadIndexFromBytes(bytes, indexName: candidate);
        indexBytes = bytes;
        indexName = candidate;
        break;
      } catch (_) {}
    }
    if (indexBytes == null || indexName == null) {
      final display = directoryPath.isEmpty ? '/' : '/$directoryPath';
      throw FormatException(
          'MDS index not found in remote directory: $display');
    }

    return _RemoteMdsSource(
      hostId: remote.hostId,
      directoryPath: directoryPath,
      indexName: indexName,
      indexBytes: indexBytes,
      preferredShardFilename: preferredShardFilename,
    );
  }

  _RemoteLitdataSource? _activeRemoteLitdataRuntime() {
    if (!(mode == ViewerMode.litdataIndex ||
        mode == ViewerMode.litdataChunks)) {
      return null;
    }
    return _remoteLitdataSource;
  }

  _HttpLitdataSource? _activeHttpLitdataRuntime() {
    if (!(mode == ViewerMode.litdataIndex ||
        mode == ViewerMode.litdataChunks)) {
      return null;
    }
    return _httpLitdataSource;
  }

  _RemoteMdsSource? _activeRemoteMdsRuntime() {
    if (mode != ViewerMode.mdsIndex) {
      return null;
    }
    return _remoteMdsSource;
  }

  _HttpMdsSource? _activeHttpMdsRuntime() {
    if (mode != ViewerMode.mdsIndex) {
      return null;
    }
    return _httpMdsSource;
  }

  Uri? _parseHttpSourceUri(String input) {
    return _sourceRouter.tryParseHttpUrl(input);
  }

  Uri _ensureHttpDirectoryUri(Uri source) {
    if (source.path.endsWith('/')) {
      return source.replace(query: null, fragment: null);
    }
    return source.replace(
      path: '${source.path}/',
      query: null,
      fragment: null,
    );
  }

  Future<_HttpLitdataSource> _resolveHttpLitdataSource(Uri source) async {
    final hintedName = p.basename(source.path).trim();
    final candidates = DatasetSourceRoutingService.litdataIndexCandidates;
    final hintedIsIndex = _looksLikeLitdataIndexName(hintedName);
    final directoryUri = hintedIsIndex
        ? _httpDatasets.parentDirectoryUri(source)
        : _httpProbeDirectoryUri(source);

    Uri? indexUri;
    Uint8List? indexBytes;
    String? indexName;

    if (hintedIsIndex) {
      try {
        final bytes = await _httpDatasets.readBytes(
          url: source,
          onStatus: (message) {
            statusMessage = message;
          },
        );
        await _litdata.loadIndexFromBytes(bytes, indexName: hintedName);
        indexBytes = bytes;
        indexUri = source;
        indexName = hintedName;
      } catch (_) {}
    }
    if (indexBytes == null || indexUri == null || indexName == null) {
      for (final candidate in candidates) {
        final candidateUri = _httpDatasets.resolveFromDirectory(
          directoryUri,
          candidate,
        );
        try {
          final bytes = await _httpDatasets.readBytes(
            url: candidateUri,
            onStatus: (message) {
              statusMessage = message;
            },
          );
          await _litdata.loadIndexFromBytes(bytes, indexName: candidate);
          indexBytes = bytes;
          indexUri = candidateUri;
          indexName = candidate;
          break;
        } catch (_) {}
      }
    }

    if (indexBytes == null || indexUri == null || indexName == null) {
      throw FormatException('LitData index not found at $source');
    }

    return _HttpLitdataSource(
      directoryUri: directoryUri,
      indexUri: indexUri,
      indexName: indexName,
      indexBytes: indexBytes,
    );
  }

  Uri _resolveHttpLitdataChunkUri(
    _HttpLitdataSource source,
    String chunkFilename,
  ) {
    return _httpDatasets.resolveFromDirectory(
        source.directoryUri, chunkFilename);
  }

  Future<_HttpMdsSource> _resolveHttpMdsSource(Uri source) async {
    final hintedName = p.basename(source.path).trim();
    final hintedFormat = _sourceRouter.detectFormatFromPath(source.path);
    final hintedIsIndex = _looksLikeLitdataIndexName(hintedName);
    final hintedIsShard = hintedFormat == DatasetSourceFormat.mdsShard;
    final directoryUri = (hintedIsIndex || hintedIsShard)
        ? _httpDatasets.parentDirectoryUri(source)
        : _httpProbeDirectoryUri(source);

    final candidates = <String>[
      ...DatasetSourceRoutingService.mdsIndexCandidates
    ];
    if (hintedIsIndex) {
      candidates.insert(0, hintedName);
    }

    Uri? indexUri;
    Uint8List? indexBytes;
    String? indexName;
    for (final candidate in candidates) {
      final candidateUri = _httpDatasets.resolveFromDirectory(
        directoryUri,
        candidate,
      );
      try {
        final bytes = await _httpDatasets.readBytes(
          url: candidateUri,
          onStatus: (message) {
            statusMessage = message;
          },
        );
        await _mosaicml.loadIndexFromBytes(bytes, indexName: candidate);
        indexUri = candidateUri;
        indexBytes = bytes;
        indexName = candidate;
        break;
      } catch (_) {}
    }

    if (indexUri == null || indexBytes == null || indexName == null) {
      throw FormatException('MDS index not found at $source');
    }

    return _HttpMdsSource(
      directoryUri: directoryUri,
      indexUri: indexUri,
      indexName: indexName,
      indexBytes: indexBytes,
      preferredShardFilename: hintedIsShard ? hintedName : null,
    );
  }

  Uri _resolveHttpMdsShardUri(
    _HttpMdsSource source,
    String shardFilename,
  ) {
    return _httpDatasets.resolveFromDirectory(
      source.directoryUri,
      shardFilename,
    );
  }

  Future<Uint8List?> _tryReadHttpBytes(
    Uri url, {
    int? maxBytes,
  }) async {
    try {
      return await _httpDatasets.readBytes(
        url: url,
        maxBytes: maxBytes,
        onStatus: (message) {
          statusMessage = message;
        },
      );
    } catch (_) {
      return null;
    }
  }

  Future<List<String>> _probeHttpWebdatasetShards(Uri source) async {
    final directoryUri = _httpProbeDirectoryUri(source);
    for (final candidate
        in DatasetSourceRoutingService.webdatasetManifestCandidates) {
      final manifestUri = _httpDatasets.resolveFromDirectory(
        directoryUri,
        candidate,
      );
      final bytes = await _tryReadHttpBytes(
        manifestUri,
        maxBytes: 8 * 1024 * 1024,
      );
      if (bytes == null || bytes.isEmpty) continue;
      try {
        final shards = _sourceRouter.parseWebdatasetShardsFromManifest(
          manifestName: candidate,
          bytes: bytes,
        );
        if (shards.isNotEmpty) {
          return shards;
        }
      } catch (_) {}
    }
    return const <String>[];
  }

  Future<List<String>> _probeHttpLitdataChunks(Uri source) async {
    try {
      final resolved = await _resolveHttpLitdataSource(source);
      final summary = await _litdata.loadIndexFromBytes(
        resolved.indexBytes,
        indexName: resolved.indexName,
      );
      return summary.chunks
          .map((chunk) => chunk.filename.trim())
          .where((name) => name.isNotEmpty)
          .toList(growable: false);
    } catch (_) {
      return const <String>[];
    }
  }

  Future<List<String>> _probeHttpMdsShards(Uri source) async {
    final directoryUri = _httpWebdatasetDirectoryUri(source);
    for (final candidate in DatasetSourceRoutingService.mdsIndexCandidates) {
      final indexUri = _httpDatasets.resolveFromDirectory(
        directoryUri,
        candidate,
      );
      final bytes = await _tryReadHttpBytes(
        indexUri,
        maxBytes: 8 * 1024 * 1024,
      );
      if (bytes == null || bytes.isEmpty) continue;
      try {
        final summary = await _mosaicml.loadIndexFromBytes(
          bytes,
          indexName: candidate,
        );
        final shards = summary.chunks
            .map((chunk) => chunk.filename.trim())
            .where((name) => _sourceRouter.isMdsShardName(name))
            .toList(growable: false);
        if (shards.isNotEmpty) {
          return shards;
        }
      } catch (_) {}
    }
    return const <String>[];
  }

  Future<DetectedSourceKind> _detectHttpSourceKind(Uri source) async {
    final format = _sourceRouter.detectFormatFromPath(source.path);
    if (format == DatasetSourceFormat.litdataIndex) {
      try {
        await _resolveHttpLitdataSource(source);
        return DetectedSourceKind.litdataIndex;
      } catch (_) {}
      try {
        await _resolveHttpMdsSource(source);
        return DetectedSourceKind.mdsIndex;
      } catch (_) {}
      return DetectedSourceKind.unknown;
    }
    if (format != DatasetSourceFormat.directory &&
        format != DatasetSourceFormat.unknown &&
        format != DatasetSourceFormat.litdataChunk) {
      return _detectedSourceKindFromFormat(format);
    }

    final webdataset = await _probeHttpWebdatasetShards(source);
    if (webdataset.isNotEmpty) {
      return DetectedSourceKind.webdatasetDir;
    }
    final litdata = await _probeHttpLitdataChunks(source);
    if (litdata.isNotEmpty) {
      return DetectedSourceKind.litdataIndex;
    }
    final mds = await _probeHttpMdsShards(source);
    if (mds.isNotEmpty) {
      return DetectedSourceKind.mdsIndex;
    }
    return DetectedSourceKind.localDirectory;
  }

  Future<_ResolvedLoadRequest> _resolveHttpLoadRequest(
    String sourceInput,
    Uri sourceUri,
  ) async {
    final format = _sourceRouter.detectFormatFromPath(sourceUri.path);
    if (format == DatasetSourceFormat.webdatasetShard) {
      return _ResolvedLoadRequest(
        mode: ViewerMode.webdatasetDir,
        sourceInput: sourceInput,
        payload: sourceInput,
      );
    }
    if (format == DatasetSourceFormat.litdataIndex) {
      try {
        await _resolveHttpLitdataSource(sourceUri);
        return _ResolvedLoadRequest(
          mode: ViewerMode.litdataIndex,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      } catch (_) {}
      try {
        await _resolveHttpMdsSource(sourceUri);
        return _ResolvedLoadRequest(
          mode: ViewerMode.mdsIndex,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      } catch (_) {}
    }
    if (format == DatasetSourceFormat.parquetFile) {
      return _ResolvedLoadRequest(
        mode: ViewerMode.localDirectory,
        sourceInput: sourceInput,
        payload: sourceInput,
        paths: const <String>[],
      );
    }
    if (format == DatasetSourceFormat.mdsShard) {
      try {
        await _resolveHttpMdsSource(sourceUri);
        return _ResolvedLoadRequest(
          mode: ViewerMode.mdsIndex,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      } catch (_) {
        return _ResolvedLoadRequest(
          mode: ViewerMode.localDirectory,
          sourceInput: sourceInput,
          payload: sourceInput,
          paths: const <String>[],
        );
      }
    }

    final detectedKind = await _detectHttpSourceKind(sourceUri);
    switch (detectedKind) {
      case DetectedSourceKind.webdatasetDir:
        return _ResolvedLoadRequest(
          mode: ViewerMode.webdatasetDir,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      case DetectedSourceKind.litdataIndex:
      case DetectedSourceKind.litdataChunks:
        return _ResolvedLoadRequest(
          mode: ViewerMode.litdataIndex,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      case DetectedSourceKind.mdsIndex:
        return _ResolvedLoadRequest(
          mode: ViewerMode.mdsIndex,
          sourceInput: sourceInput,
          payload: sourceInput,
        );
      case DetectedSourceKind.localDirectory:
      case DetectedSourceKind.unknown:
      case DetectedSourceKind.detecting:
      case DetectedSourceKind.huggingface:
      case DetectedSourceKind.zenodo:
        return _ResolvedLoadRequest(
          mode: ViewerMode.localDirectory,
          sourceInput: sourceInput,
          payload: sourceInput,
          paths: const <String>[],
        );
    }
  }

  Future<WdsDirSummary> _resolveHttpWebdatasetDirSummary({
    required Uri source,
    required String sourceInput,
  }) async {
    final format = _sourceRouter.detectFormatFromPath(source.path);
    if (format == DatasetSourceFormat.webdatasetShard) {
      final fileName = p.basename(source.path).trim().isEmpty
          ? 'dataset.tar'
          : p.basename(source.path).trim();
      return WdsDirSummary(
        dirPath: sourceInput,
        shards: <WdsShardSummary>[
          WdsShardSummary(
            filename: fileName,
            path: sourceInput,
            bytes: 0,
            exists: true,
          ),
        ],
      );
    }

    final directoryUri = _httpWebdatasetDirectoryUri(source);
    final shards = await _probeHttpWebdatasetShards(source);
    if (shards.isEmpty) {
      throw FormatException('WebDataset manifest not found at $directoryUri');
    }
    final summaries = shards
        .map(
          (name) => WdsShardSummary(
            filename: name,
            path: _httpDatasets
                .resolveFromDirectory(directoryUri, name)
                .toString(),
            bytes: 0,
            exists: true,
          ),
        )
        .toList(growable: false)
      ..sort((a, b) => a.filename.compareTo(b.filename));
    return WdsDirSummary(dirPath: sourceInput, shards: summaries);
  }

  Future<List<LocalDirectoryItem>> _resolveHttpDirectoryItems(
      Uri source) async {
    final entries = <String, LocalDirectoryItem>{};

    void addFile(String name, Uri uri) {
      final trimmed = name.trim();
      if (trimmed.isEmpty) return;
      final key = uri.toString();
      entries[key] = LocalDirectoryItem(
        name: trimmed,
        path: key,
        isDirectory: false,
      );
    }

    final format = _sourceRouter.detectFormatFromPath(source.path);
    if (format == DatasetSourceFormat.parquetFile ||
        format == DatasetSourceFormat.webdatasetShard ||
        format == DatasetSourceFormat.mdsShard ||
        format == DatasetSourceFormat.litdataIndex) {
      addFile(
        p.basename(source.path).trim().isEmpty
            ? source.host
            : p.basename(source.path).trim(),
        source,
      );
    }

    if (format == DatasetSourceFormat.parquetFile ||
        format == DatasetSourceFormat.webdatasetShard ||
        format == DatasetSourceFormat.mdsShard) {
      final simple = entries.values.toList(growable: false)
        ..sort(
          (left, right) =>
              left.name.toLowerCase().compareTo(right.name.toLowerCase()),
        );
      return simple;
    }

    final directoryUri = _httpWebdatasetDirectoryUri(source);
    final webdataset = await _probeHttpWebdatasetShards(source);
    for (final shard in webdataset) {
      addFile(shard, _httpDatasets.resolveFromDirectory(directoryUri, shard));
    }

    final litdata = await _probeHttpLitdataChunks(source);
    if (litdata.isNotEmpty) {
      for (final indexName
          in DatasetSourceRoutingService.litdataIndexCandidates) {
        final indexUri =
            _httpDatasets.resolveFromDirectory(directoryUri, indexName);
        final exists = await _tryReadHttpBytes(indexUri, maxBytes: 1);
        if (exists != null) {
          addFile(indexName, indexUri);
          break;
        }
      }
      for (final chunk in litdata) {
        addFile(chunk, _httpDatasets.resolveFromDirectory(directoryUri, chunk));
      }
    }

    final mds = await _probeHttpMdsShards(source);
    if (mds.isNotEmpty) {
      for (final indexName in DatasetSourceRoutingService.mdsIndexCandidates) {
        final indexUri =
            _httpDatasets.resolveFromDirectory(directoryUri, indexName);
        final exists = await _tryReadHttpBytes(indexUri, maxBytes: 1);
        if (exists != null) {
          addFile(indexName, indexUri);
          break;
        }
      }
      for (final shard in mds) {
        addFile(shard, _httpDatasets.resolveFromDirectory(directoryUri, shard));
      }
    }

    if (entries.isEmpty) {
      addFile(
        p.basename(source.path).trim().isEmpty
            ? source.host
            : p.basename(source.path).trim(),
        source,
      );
    }
    final items = entries.values.toList(growable: false)
      ..sort(
        (left, right) =>
            left.name.toLowerCase().compareTo(right.name.toLowerCase()),
      );
    return items;
  }

  Stream<List<int>> _openWebdatasetShardStream(String shardFilename) {
    final remote = _activeRemoteWebdatasetSource();
    if (remote != null) {
      final requestedShard = shardFilename.trim();
      if (requestedShard.isEmpty) {
        throw const FormatException('WebDataset shard filename is empty.');
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
      final host = _findRemoteHost(remote.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remote.hostId}');
      }
      return _remoteDatasets.openReadFile(
        host: host,
        remotePath: remoteShardPath,
        onStatus: (message) {
          statusMessage = message;
          _notifyStateChanged();
        },
      );
    }
    final httpUri = _activeHttpWebdatasetShardUri(shardFilename);
    if (httpUri != null) {
      return _httpDatasets.openRead(
        url: httpUri,
        onStatus: (message) {
          statusMessage = message;
          _notifyStateChanged();
        },
      );
    }
    throw const FormatException('WebDataset streaming source is not active.');
  }

  Future<String> _resolveWebdatasetDirPathForShard(
    String shardFilename,
  ) async {
    final summary = wdsDirSummary;
    if (summary == null) {
      throw const FormatException('WebDataset directory is not loaded.');
    }
    if (shardFilename.trim().isEmpty) {
      throw const FormatException('shard filename is empty');
    }
    return summary.dirPath;
  }

  Future<DetectedSourceKind> _detectRemoteDirectorySourceKind(
    _RemoteDirectorySource remote,
  ) async {
    final hintedFormat = _sourceRouter.detectFormatFromPath(remote.path);
    if (hintedFormat == DatasetSourceFormat.litdataIndex) {
      try {
        await _resolveRemoteLitdataSource(remote);
        return DetectedSourceKind.litdataIndex;
      } catch (_) {}
      try {
        await _resolveRemoteMdsSource(remote);
        return DetectedSourceKind.mdsIndex;
      } catch (_) {}
      return DetectedSourceKind.unknown;
    }
    if (hintedFormat != DatasetSourceFormat.directory &&
        hintedFormat != DatasetSourceFormat.unknown &&
        hintedFormat != DatasetSourceFormat.litdataChunk) {
      return _detectedSourceKindFromFormat(hintedFormat);
    }
    final host = _findRemoteHost(remote.hostId);
    if (host == null) {
      return DetectedSourceKind.localDirectory;
    }
    try {
      final entries = await _remoteDatasets.listEntries(
        host: host,
        directoryPath: remote.path,
      );
      final files = entries
          .where((entry) => !entry.isDirectory)
          .map((entry) => entry.name)
          .toList(growable: false);
      final format = _sourceRouter.detectFormatFromEntries(files);
      return _detectedSourceKindFromFormat(format);
    } catch (_) {}
    return DetectedSourceKind.localDirectory;
  }

  bool _looksLikeWebdatasetShardName(String filename) {
    return _sourceRouter.isWebdatasetShardName(filename);
  }

  bool _looksLikeLitdataIndexName(String filename) {
    return _sourceRouter.isLitdataIndexName(filename);
  }

  DetectedSourceKind _detectedSourceKindFromFormat(DatasetSourceFormat format) {
    switch (format) {
      case DatasetSourceFormat.webdatasetShard:
        return DetectedSourceKind.webdatasetDir;
      case DatasetSourceFormat.litdataIndex:
      case DatasetSourceFormat.litdataChunk:
        return DetectedSourceKind.litdataIndex;
      case DatasetSourceFormat.mdsShard:
        return DetectedSourceKind.mdsIndex;
      case DatasetSourceFormat.parquetFile:
      case DatasetSourceFormat.directory:
        return DetectedSourceKind.localDirectory;
      case DatasetSourceFormat.unknown:
        return DetectedSourceKind.unknown;
    }
  }

  String _huggingFaceLabel(String input) {
    if (input.startsWith('hf://datasets/')) {
      final raw = input.substring('hf://datasets/'.length);
      final parts = raw.split('/').where((part) => part.isNotEmpty).toList();
      if (parts.length >= 2) {
        return '${parts[0]}/${parts[1]}';
      }
      return raw;
    }
    final uri = Uri.tryParse(input);
    if (uri != null) {
      final segments =
          uri.pathSegments.where((segment) => segment.isNotEmpty).toList();
      for (var index = 0; index < segments.length; index += 1) {
        if (segments[index] != 'datasets') continue;
        if (segments.length > index + 2) {
          return '${segments[index + 1]}/${segments[index + 2]}';
        }
      }
    }
    return input;
  }

  String _zenodoLabel(String input) {
    final uri = Uri.tryParse(input);
    if (uri == null) return 'Zenodo';
    final segments =
        uri.pathSegments.where((segment) => segment.isNotEmpty).toList();
    for (var index = 0; index < segments.length; index += 1) {
      if ((segments[index] == 'record' || segments[index] == 'records') &&
          segments.length > index + 1) {
        return 'Zenodo ${segments[index + 1]}';
      }
    }
    return 'Zenodo';
  }

  String _nextDatasetId() {
    return DateTime.now().microsecondsSinceEpoch.toString();
  }
}
