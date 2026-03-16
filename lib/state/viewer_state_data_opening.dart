part of 'viewer_state.dart';

extension ViewerStateDataOpening on ViewerState {
  Future<OpenLeafResponse> litdataOpenField({String? openerAppPath}) async {
    if (indexSummary == null ||
        selectedChunkName == null ||
        selectedItemIndex == null ||
        selectedFieldIndex == null) {
      throw const FormatException('No field selected.');
    }
    final remoteSource = _activeRemoteLitdataRuntime();
    if (remoteSource != null) {
      final host = _findRemoteHost(remoteSource.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remoteSource.hostId}');
      }
      final chunkPath = _joinRemoteDirectoryPath(
          remoteSource.directoryPath, selectedChunkName!);
      final prepared = await _prepareLitdataFileFromRemoteStreamWithRetry(
        host: host,
        remotePath: chunkPath,
        indexBytes: remoteSource.indexBytes,
        indexName: remoteSource.indexName,
        chunkFilename: selectedChunkName!,
        itemIndex: selectedItemIndex!,
        fieldIndex: selectedFieldIndex!,
      );
      final targetPath = prepared.path;
      final size = prepared.size;
      final ext = prepared.ext;
      if (openerAppPath != null && openerAppPath.trim().isNotEmpty) {
        return _openLocalWithApplication(
          openerAppPath.trim(),
          targetPath,
          size: size,
          ext: ext,
        );
      }
      final opened = await OpenFilex.open(targetPath);
      final noDefaultApp = opened.type == ResultType.noAppToOpen;
      if (!noDefaultApp) {
        return OpenLeafResponse(
          path: targetPath,
          size: size,
          ext: ext,
          opened: opened.type == ResultType.done,
          needsOpener: false,
          message: opened.message,
        );
      }
      return OpenLeafResponse(
        path: targetPath,
        size: size,
        ext: ext,
        opened: false,
        needsOpener: true,
        message: 'No default application found, choose an app to open it.',
      );
    }
    final httpSource = _activeHttpLitdataRuntime();
    if (httpSource != null) {
      final chunkUri =
          _resolveHttpLitdataChunkUri(httpSource, selectedChunkName!);
      final prepared = await _remoteOps.prepareLitdataFileFromStreamWithRetry(
        litdata: _litdata,
        indexBytes: httpSource.indexBytes,
        indexName: httpSource.indexName,
        chunkFilename: selectedChunkName!,
        itemIndex: selectedItemIndex!,
        fieldIndex: selectedFieldIndex!,
        maxAttempts: 2,
        openChunkStream: () => _httpDatasets.openRead(
          url: chunkUri,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
      final targetPath = prepared.path;
      final size = prepared.size;
      final ext = prepared.ext;
      if (openerAppPath != null && openerAppPath.trim().isNotEmpty) {
        return _openLocalWithApplication(
          openerAppPath.trim(),
          targetPath,
          size: size,
          ext: ext,
        );
      }
      final opened = await OpenFilex.open(targetPath);
      final noDefaultApp = opened.type == ResultType.noAppToOpen;
      if (!noDefaultApp) {
        return OpenLeafResponse(
          path: targetPath,
          size: size,
          ext: ext,
          opened: opened.type == ResultType.done,
          needsOpener: false,
          message: opened.message,
        );
      }
      return OpenLeafResponse(
        path: targetPath,
        size: size,
        ext: ext,
        opened: false,
        needsOpener: true,
        message: 'No default application found, choose an app to open it.',
      );
    }
    return _litdata.openLeaf(
      indexPath: indexSummary!.indexPath,
      chunkFilename: selectedChunkName!,
      itemIndex: selectedItemIndex!,
      fieldIndex: selectedFieldIndex!,
      openerAppPath: openerAppPath,
    );
  }

  Future<PreparedMediaResponse> litdataPrepareAudio({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final remoteSource = _activeRemoteLitdataRuntime();
    if (remoteSource != null) {
      final host = _findRemoteHost(remoteSource.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remoteSource.hostId}');
      }
      final chunkPath =
          _joinRemoteDirectoryPath(remoteSource.directoryPath, chunkFilename);
      return _prepareLitdataAudioFromRemoteStreamWithRetry(
        host: host,
        remotePath: chunkPath,
        indexBytes: remoteSource.indexBytes,
        indexName: remoteSource.indexName,
        chunkFilename: chunkFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }
    final httpSource = _activeHttpLitdataRuntime();
    if (httpSource != null) {
      final chunkUri = _resolveHttpLitdataChunkUri(httpSource, chunkFilename);
      return _remoteOps.prepareLitdataAudioFromStreamWithRetry(
        litdata: _litdata,
        indexBytes: httpSource.indexBytes,
        indexName: httpSource.indexName,
        chunkFilename: chunkFilename,
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
    return _litdata.prepareAudioPreview(
      indexPath: indexPath,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<PreparedFileResponse> litdataPrepareFile({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final remoteSource = _activeRemoteLitdataRuntime();
    if (remoteSource != null) {
      final host = _findRemoteHost(remoteSource.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remoteSource.hostId}');
      }
      final chunkPath =
          _joinRemoteDirectoryPath(remoteSource.directoryPath, chunkFilename);
      return _prepareLitdataFileFromRemoteStreamWithRetry(
        host: host,
        remotePath: chunkPath,
        indexBytes: remoteSource.indexBytes,
        indexName: remoteSource.indexName,
        chunkFilename: chunkFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }
    final httpSource = _activeHttpLitdataRuntime();
    if (httpSource != null) {
      final chunkUri = _resolveHttpLitdataChunkUri(httpSource, chunkFilename);
      return _remoteOps.prepareLitdataFileFromStreamWithRetry(
        litdata: _litdata,
        indexBytes: httpSource.indexBytes,
        indexName: httpSource.indexName,
        chunkFilename: chunkFilename,
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
    return _litdata.prepareFieldFile(
      indexPath: indexPath,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<OpenLeafResponse> mosaicmlOpenField({String? openerAppPath}) async {
    if (indexSummary == null ||
        selectedChunkName == null ||
        selectedItemIndex == null ||
        selectedFieldIndex == null) {
      throw const FormatException('No field selected.');
    }
    final shardFilename = selectedChunkName!;
    final itemIndex = selectedItemIndex!;
    final fieldIndex = selectedFieldIndex!;
    final remoteSource = _activeRemoteMdsRuntime();
    if (remoteSource != null) {
      final host = _findRemoteHost(remoteSource.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remoteSource.hostId}');
      }
      final compressed = _isMdsCompressedShardForOpen(shardFilename);
      final remotePath = await _resolveRemoteMdsShardPath(
        host: host,
        directoryPath: remoteSource.directoryPath,
        shardFilename: shardFilename,
        compressed: compressed,
      );
      final prepared = compressed
          ? await _prepareMdsFileFromRemoteCompressedStreamWithRetry(
              host: host,
              remotePath: remotePath,
              indexBytes: remoteSource.indexBytes,
              indexName: remoteSource.indexName,
              shardFilename: shardFilename,
              itemIndex: itemIndex,
              fieldIndex: fieldIndex,
              maxAttempts: 3,
            )
          : await _prepareMdsFileFromRemoteRawStreamWithRetry(
              host: host,
              remotePath: remotePath,
              indexBytes: remoteSource.indexBytes,
              indexName: remoteSource.indexName,
              shardFilename: shardFilename,
              itemIndex: itemIndex,
              fieldIndex: fieldIndex,
              maxAttempts: 3,
            );
      return _openPreparedFile(prepared, openerAppPath: openerAppPath);
    }
    final httpSource = _activeHttpMdsRuntime();
    if (httpSource != null) {
      final shardUri = _resolveHttpMdsShardUri(httpSource, shardFilename);
      final prepared = _isMdsCompressedShardForOpen(shardFilename)
          ? await _remoteOps.prepareMdsFileFromCompressedStreamWithRetry(
              mosaicml: _mosaicml,
              indexBytes: httpSource.indexBytes,
              indexName: httpSource.indexName,
              shardFilename: shardFilename,
              itemIndex: itemIndex,
              fieldIndex: fieldIndex,
              compressedShardCacheKey: _httpCompressedMdsCacheKey(shardUri),
              maxAttempts: 3,
              openCompressedStream: (maxBytes) => _httpDatasets.openRead(
                url: shardUri,
                maxBytes: maxBytes,
                onStatus: (message) {
                  statusMessage = message;
                },
              ),
            )
          : await _remoteOps.prepareMdsFileFromRawStreamWithRetry(
              mosaicml: _mosaicml,
              indexBytes: httpSource.indexBytes,
              indexName: httpSource.indexName,
              shardFilename: shardFilename,
              itemIndex: itemIndex,
              fieldIndex: fieldIndex,
              maxAttempts: 3,
              openRawStream: () => _httpDatasets.openRead(
                url: shardUri,
                onStatus: (message) {
                  statusMessage = message;
                },
              ),
            );
      return _openPreparedFile(prepared, openerAppPath: openerAppPath);
    }
    return _mosaicml.openLeaf(
      indexPath: indexSummary!.indexPath,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      openerAppPath: openerAppPath,
    );
  }

  Future<PreparedMediaResponse> mosaicmlPrepareAudio({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final remoteSource = _activeRemoteMdsRuntime();
    if (remoteSource != null) {
      final host = _findRemoteHost(remoteSource.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remoteSource.hostId}');
      }
      final compressed = _isMdsCompressedShardForOpen(shardFilename);
      final remotePath = await _resolveRemoteMdsShardPath(
        host: host,
        directoryPath: remoteSource.directoryPath,
        shardFilename: shardFilename,
        compressed: compressed,
      );
      if (compressed) {
        return _prepareMdsAudioFromRemoteCompressedStreamWithRetry(
          host: host,
          remotePath: remotePath,
          indexBytes: remoteSource.indexBytes,
          indexName: remoteSource.indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          maxAttempts: 3,
        );
      }
      return _prepareMdsAudioFromRemoteRawStreamWithRetry(
        host: host,
        remotePath: remotePath,
        indexBytes: remoteSource.indexBytes,
        indexName: remoteSource.indexName,
        shardFilename: shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 3,
      );
    }
    final httpSource = _activeHttpMdsRuntime();
    if (httpSource != null) {
      final shardUri = _resolveHttpMdsShardUri(httpSource, shardFilename);
      if (_isMdsCompressedShardForOpen(shardFilename)) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: httpSource.indexBytes,
          indexName: httpSource.indexName,
          shardFilename: shardFilename,
          targetItemIndex: itemIndex,
        );
        return _remoteOps.prepareMdsAudioFromCompressedStreamWithRetry(
          mosaicml: _mosaicml,
          indexBytes: httpSource.indexBytes,
          indexName: httpSource.indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          initialCompressedBytes: initialCompressedBytes,
          compressedShardCacheKey: _httpCompressedMdsCacheKey(shardUri),
          maxAttempts: 3,
          openCompressedStream: (maxBytes) => _httpDatasets.openRead(
            url: shardUri,
            maxBytes: maxBytes,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }
      return _remoteOps.prepareMdsAudioFromRawStreamWithRetry(
        mosaicml: _mosaicml,
        indexBytes: httpSource.indexBytes,
        indexName: httpSource.indexName,
        shardFilename: shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 3,
        openRawStream: () => _httpDatasets.openRead(
          url: shardUri,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }
    return _mosaicml.prepareAudioPreview(
      indexPath: indexPath,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<PreparedFileResponse> mosaicmlPrepareFile({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final remoteSource = _activeRemoteMdsRuntime();
    if (remoteSource != null) {
      final host = _findRemoteHost(remoteSource.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remoteSource.hostId}');
      }
      final compressed = _isMdsCompressedShardForOpen(shardFilename);
      final remotePath = await _resolveRemoteMdsShardPath(
        host: host,
        directoryPath: remoteSource.directoryPath,
        shardFilename: shardFilename,
        compressed: compressed,
      );
      if (compressed) {
        return _prepareMdsFileFromRemoteCompressedStreamWithRetry(
          host: host,
          remotePath: remotePath,
          indexBytes: remoteSource.indexBytes,
          indexName: remoteSource.indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          maxAttempts: 3,
        );
      }
      return _prepareMdsFileFromRemoteRawStreamWithRetry(
        host: host,
        remotePath: remotePath,
        indexBytes: remoteSource.indexBytes,
        indexName: remoteSource.indexName,
        shardFilename: shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 3,
      );
    }
    final httpSource = _activeHttpMdsRuntime();
    if (httpSource != null) {
      final shardUri = _resolveHttpMdsShardUri(httpSource, shardFilename);
      if (_isMdsCompressedShardForOpen(shardFilename)) {
        final initialCompressedBytes =
            await _estimateRemoteCompressedMdsScanBytes(
          indexBytes: httpSource.indexBytes,
          indexName: httpSource.indexName,
          shardFilename: shardFilename,
          targetItemIndex: itemIndex,
        );
        return _remoteOps.prepareMdsFileFromCompressedStreamWithRetry(
          mosaicml: _mosaicml,
          indexBytes: httpSource.indexBytes,
          indexName: httpSource.indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          initialCompressedBytes: initialCompressedBytes,
          compressedShardCacheKey: _httpCompressedMdsCacheKey(shardUri),
          maxAttempts: 3,
          openCompressedStream: (maxBytes) => _httpDatasets.openRead(
            url: shardUri,
            maxBytes: maxBytes,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }
      return _remoteOps.prepareMdsFileFromRawStreamWithRetry(
        mosaicml: _mosaicml,
        indexBytes: httpSource.indexBytes,
        indexName: httpSource.indexName,
        shardFilename: shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 3,
        openRawStream: () => _httpDatasets.openRead(
          url: shardUri,
          onStatus: (message) {
            statusMessage = message;
          },
        ),
      );
    }
    return _mosaicml.prepareFieldFile(
      indexPath: indexPath,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<OpenLeafResponse> webdatasetOpenMember({String? openerAppPath}) async {
    if (wdsDirSummary == null ||
        selectedShardName == null ||
        wdsSelectedMemberPath == null) {
      throw const FormatException('No member selected.');
    }
    if (_activeRemoteWebdatasetSource() != null ||
        _activeHttpWebdatasetShardUri() != null) {
      return _webdataset.openMemberFromStream(
        shardStream: _openWebdatasetShardStream(selectedShardName!),
        shardFilename: selectedShardName!,
        memberPath: wdsSelectedMemberPath!,
        openerAppPath: openerAppPath,
      );
    }
    final dirPath = await _resolveWebdatasetDirPathForShard(selectedShardName!);
    return _webdataset.openMember(
      dirPath: dirPath,
      shardFilename: selectedShardName!,
      memberPath: wdsSelectedMemberPath!,
      openerAppPath: openerAppPath,
    );
  }

  Future<PreparedMediaResponse> webdatasetPrepareAudio({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
  }) async {
    if (_activeRemoteWebdatasetSource() != null ||
        _activeHttpWebdatasetShardUri() != null) {
      return _webdataset.prepareAudioPreviewFromStream(
        shardStream: _openWebdatasetShardStream(shardFilename),
        shardFilename: shardFilename,
        memberPath: memberPath,
      );
    }
    final effectiveDirPath = await _resolveWebdatasetDirPathForShard(
      shardFilename,
    );
    return _webdataset.prepareAudioPreview(
      dirPath: effectiveDirPath,
      shardFilename: shardFilename,
      memberPath: memberPath,
    );
  }

  Future<PreparedFileResponse> webdatasetPrepareFile({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
  }) async {
    if (_activeRemoteWebdatasetSource() != null ||
        _activeHttpWebdatasetShardUri() != null) {
      return _webdataset.prepareMemberFileFromStream(
        shardStream: _openWebdatasetShardStream(shardFilename),
        shardFilename: shardFilename,
        memberPath: memberPath,
      );
    }
    final effectiveDirPath = await _resolveWebdatasetDirPathForShard(
      shardFilename,
    );
    return _webdataset.prepareMemberFile(
      dirPath: effectiveDirPath,
      shardFilename: shardFilename,
      memberPath: memberPath,
    );
  }

  Future<OpenLeafResponse> localOpenSelectedFile({
    String? openerAppPath,
  }) async {
    if (_hasLocalDirectoryMdsFieldSelection()) {
      return _localOpenSelectedMdsField(openerAppPath: openerAppPath);
    }
    final item = selectedLocalDirectoryItem;
    if (item == null) {
      throw const FormatException('No file selected.');
    }

    final ext = _normalizeLocalExt(item.path);
    final size = item.size ?? 0;
    final remote = _activeRemoteDirectorySource;
    final isHttpSource = _parseHttpSourceUri(item.path) != null;
    final targetPath = (remote == null && !isHttpSource)
        ? item.path
        : await _stageRemoteFileForOpen(
            remotePath: item.path,
            bytes: await readDirectoryFileBytes(item.path),
          );
    if (openerAppPath != null && openerAppPath.trim().isNotEmpty) {
      return _openLocalWithApplication(
        openerAppPath.trim(),
        targetPath,
        size: size,
        ext: ext,
      );
    }

    final opened = await OpenFilex.open(targetPath);
    final noDefaultApp = opened.type == ResultType.noAppToOpen;
    if (!noDefaultApp) {
      return OpenLeafResponse(
        path: targetPath,
        size: size,
        ext: ext,
        opened: opened.type == ResultType.done,
        needsOpener: false,
        message: opened.message,
      );
    }

    return OpenLeafResponse(
      path: targetPath,
      size: size,
      ext: ext,
      opened: false,
      needsOpener: true,
      message: 'No default application found, choose an app to open it.',
    );
  }

  Future<PreparedMediaResponse> localPrepareSelectedAudio() async {
    if (_hasLocalDirectoryMdsFieldSelection()) {
      return _localPrepareSelectedMdsAudio();
    }
    final item = selectedLocalDirectoryItem;
    if (item == null || item.isDirectory) {
      throw const FormatException('No audio file selected.');
    }
    final bytes = await readDirectoryFileBytes(item.path);
    return PreparedMediaResponse(
      bytes: bytes,
      size: bytes.length,
      ext: _normalizeLocalExt(item.path),
    );
  }

  Future<PreparedFileResponse> localPrepareSelectedFile() async {
    if (_hasLocalDirectoryMdsFieldSelection()) {
      return _localPrepareSelectedMdsFile();
    }
    final item = selectedLocalDirectoryItem;
    if (item == null || item.isDirectory) {
      throw const FormatException('No file selected.');
    }
    final remote = _activeRemoteDirectorySource;
    final isHttpSource = _parseHttpSourceUri(item.path) != null;
    if (remote != null || isHttpSource) {
      final bytes = await readDirectoryFileBytes(item.path);
      final stagedPath = await _stageRemoteFileForOpen(
        remotePath: item.path,
        bytes: bytes,
      );
      return PreparedFileResponse(
        path: stagedPath,
        size: bytes.length,
        ext: _normalizeLocalExt(item.path),
      );
    }
    final file = File(item.path);
    if (!await file.exists()) {
      throw const FormatException('Selected file does not exist.');
    }
    final stat = await file.stat();
    return PreparedFileResponse(
      path: item.path,
      size: stat.size,
      ext: _normalizeLocalExt(item.path),
    );
  }

  bool _hasLocalDirectoryMdsFieldSelection() {
    if (mode != ViewerMode.localDirectory) {
      return false;
    }
    final item = selectedLocalDirectoryItem;
    return item != null &&
        !item.isDirectory &&
        _looksLikeMdsShardPath(item.path) &&
        selectedItemIndex != null &&
        selectedFieldIndex != null;
  }

  Future<PreparedMediaResponse> _localPrepareSelectedMdsAudio() async {
    final item = selectedLocalDirectoryItem;
    final itemIndex = selectedItemIndex;
    final fieldIndex = selectedFieldIndex;
    if (item == null ||
        item.isDirectory ||
        itemIndex == null ||
        fieldIndex == null) {
      throw const FormatException('No MDS field selected.');
    }
    final remote = _activeRemoteDirectorySource;
    final httpUri = _parseHttpSourceUri(item.path);
    final lowerPath = item.path.trim().toLowerCase();
    final isRemoteExtCompressed = (remote != null || httpUri != null) &&
        (lowerPath.endsWith('.zst') || lowerPath.endsWith('.zstd'));
    final source = await _resolveLocalDirectoryMdsSource(item.path);
    if (remote == null && httpUri == null) {
      return _mosaicml.prepareAudioPreview(
        indexPath: source.indexPath!,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }

    final (:isCompressed, :remotePath) = await _resolveRemoteMdsCompression(
      isCompressed: isRemoteExtCompressed,
      remotePath: _normalizeRemoteDirectoryPath(item.path),
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
        return _remoteOps.prepareMdsAudioFromCompressedStreamWithRetry(
          mosaicml: _mosaicml,
          indexBytes: source.indexBytes,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          initialCompressedBytes: initialCompressedBytes,
          compressedShardCacheKey: _httpCompressedMdsCacheKey(httpUri),
          maxAttempts: 3,
          openCompressedStream: (maxBytes) => _httpDatasets.openRead(
            url: httpUri,
            maxBytes: maxBytes,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }
      return _remoteOps.prepareMdsAudioFromRawStreamWithRetry(
        mosaicml: _mosaicml,
        indexBytes: source.indexBytes,
        indexName: source.indexName,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 3,
        openRawStream: () => _httpDatasets.openRead(
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
      return _prepareMdsAudioFromRemoteCompressedStreamWithRetry(
        host: host,
        remotePath: remotePath,
        indexBytes: source.indexBytes,
        indexName: source.indexName,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 3,
      );
    }
    return _prepareMdsAudioFromRemoteRawStreamWithRetry(
      host: host,
      remotePath: remotePath,
      indexBytes: source.indexBytes,
      indexName: source.indexName,
      shardFilename: source.shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxAttempts: 3,
    );
  }

  Future<PreparedFileResponse> _localPrepareSelectedMdsFile() async {
    final item = selectedLocalDirectoryItem;
    final itemIndex = selectedItemIndex;
    final fieldIndex = selectedFieldIndex;
    if (item == null ||
        item.isDirectory ||
        itemIndex == null ||
        fieldIndex == null) {
      throw const FormatException('No MDS field selected.');
    }
    final remote = _activeRemoteDirectorySource;
    final httpUri = _parseHttpSourceUri(item.path);
    final lowerPath = item.path.trim().toLowerCase();
    final isRemoteExtCompressed = (remote != null || httpUri != null) &&
        (lowerPath.endsWith('.zst') || lowerPath.endsWith('.zstd'));
    final source = await _resolveLocalDirectoryMdsSource(item.path);
    if (remote == null && httpUri == null) {
      return _mosaicml.prepareFieldFile(
        indexPath: source.indexPath!,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }

    final (:isCompressed, :remotePath) = await _resolveRemoteMdsCompression(
      isCompressed: isRemoteExtCompressed,
      remotePath: _normalizeRemoteDirectoryPath(item.path),
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
        return _remoteOps.prepareMdsFileFromCompressedStreamWithRetry(
          mosaicml: _mosaicml,
          indexBytes: source.indexBytes,
          indexName: source.indexName,
          shardFilename: source.shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          initialCompressedBytes: initialCompressedBytes,
          compressedShardCacheKey: _httpCompressedMdsCacheKey(httpUri),
          maxAttempts: 3,
          openCompressedStream: (maxBytes) => _httpDatasets.openRead(
            url: httpUri,
            maxBytes: maxBytes,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      }
      return _remoteOps.prepareMdsFileFromRawStreamWithRetry(
        mosaicml: _mosaicml,
        indexBytes: source.indexBytes,
        indexName: source.indexName,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 3,
        openRawStream: () => _httpDatasets.openRead(
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
      return _prepareMdsFileFromRemoteCompressedStreamWithRetry(
        host: host,
        remotePath: remotePath,
        indexBytes: source.indexBytes,
        indexName: source.indexName,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
        maxAttempts: 3,
      );
    }
    return _prepareMdsFileFromRemoteRawStreamWithRetry(
      host: host,
      remotePath: remotePath,
      indexBytes: source.indexBytes,
      indexName: source.indexName,
      shardFilename: source.shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      maxAttempts: 3,
    );
  }

  Future<PreparedMediaResponse>
      _prepareMdsAudioFromRemoteCompressedStreamWithRetry({
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
    return _remoteOps.prepareMdsAudioFromCompressedStreamWithRetry(
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

  Future<PreparedMediaResponse> _prepareMdsAudioFromRemoteRawStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    int maxAttempts = 2,
  }) async {
    return _remoteOps.prepareMdsAudioFromRawStreamWithRetry(
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

  Future<PreparedFileResponse>
      _prepareMdsFileFromRemoteCompressedStreamWithRetry({
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
    return _remoteOps.prepareMdsFileFromCompressedStreamWithRetry(
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

  Future<PreparedFileResponse> _prepareMdsFileFromRemoteRawStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    int maxAttempts = 2,
  }) async {
    return _remoteOps.prepareMdsFileFromRawStreamWithRetry(
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

  Future<OpenLeafResponse> _localOpenSelectedMdsField({
    String? openerAppPath,
  }) async {
    final prepared = await _localPrepareSelectedMdsFile();
    final targetPath = prepared.path;
    final size = prepared.size;
    final ext = prepared.ext;
    if (openerAppPath != null && openerAppPath.trim().isNotEmpty) {
      return _openLocalWithApplication(
        openerAppPath.trim(),
        targetPath,
        size: size,
        ext: ext,
      );
    }
    final opened = await OpenFilex.open(targetPath);
    final noDefaultApp = opened.type == ResultType.noAppToOpen;
    if (!noDefaultApp) {
      return OpenLeafResponse(
        path: targetPath,
        size: size,
        ext: ext,
        opened: opened.type == ResultType.done,
        needsOpener: false,
        message: opened.message,
      );
    }
    return OpenLeafResponse(
      path: targetPath,
      size: size,
      ext: ext,
      opened: false,
      needsOpener: true,
      message: 'No default application found, choose an app to open it.',
    );
  }

  Future<OpenLeafResponse> _openPreparedFile(
    PreparedFileResponse prepared, {
    String? openerAppPath,
  }) async {
    final targetPath = prepared.path;
    final size = prepared.size;
    final ext = prepared.ext;
    if (openerAppPath != null && openerAppPath.trim().isNotEmpty) {
      return _openLocalWithApplication(
        openerAppPath.trim(),
        targetPath,
        size: size,
        ext: ext,
      );
    }
    final opened = await OpenFilex.open(targetPath);
    final noDefaultApp = opened.type == ResultType.noAppToOpen;
    if (!noDefaultApp) {
      return OpenLeafResponse(
        path: targetPath,
        size: size,
        ext: ext,
        opened: opened.type == ResultType.done,
        needsOpener: false,
        message: opened.message,
      );
    }
    return OpenLeafResponse(
      path: targetPath,
      size: size,
      ext: ext,
      opened: false,
      needsOpener: true,
      message: 'No default application found, choose an app to open it.',
    );
  }

  bool _isMdsCompressedShardForOpen(String shardFilename) {
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

  Future<String> _stageRemoteFileForOpen({
    required String remotePath,
    required Uint8List bytes,
  }) async {
    final httpUri = _parseHttpSourceUri(remotePath);
    final fileName =
        (httpUri != null ? p.basename(httpUri.path) : p.basename(remotePath))
            .trim();
    final safeName = fileName.isEmpty ? 'remote_file.bin' : fileName;
    final tempDir =
        await Directory.systemTemp.createTemp('dataset_inspector_remote_');
    final staged = File(p.join(tempDir.path, safeName));
    await staged.writeAsBytes(bytes, flush: true);
    return staged.path;
  }

  Future<OpenLeafResponse> _openLocalWithApplication(
    String appPath,
    String targetPath, {
    required int size,
    required String ext,
  }) async {
    final command = appPath.trim();
    if (command.isEmpty) {
      return OpenLeafResponse(
        path: targetPath,
        size: size,
        ext: ext,
        opened: false,
        needsOpener: true,
        message: 'No application selected.',
      );
    }
    final normalized = targetPath.trim();
    if (normalized.isEmpty) {
      return OpenLeafResponse(
        path: targetPath,
        size: size,
        ext: ext,
        opened: false,
        needsOpener: true,
        message: 'No file selected.',
      );
    }

    try {
      final result = Platform.isMacOS && command.toLowerCase().endsWith('.app')
          ? await Process.run('open', ['-a', command, normalized])
          : await Process.run(command, [normalized]);
      final opened = result.exitCode == 0;
      return OpenLeafResponse(
        path: normalized,
        size: size,
        ext: ext,
        opened: opened,
        needsOpener: !opened,
        message: opened
            ? normalized
            : 'Failed to open with "$command": ${result.stderr}',
      );
    } on ProcessException catch (error) {
      return OpenLeafResponse(
        path: normalized,
        size: size,
        ext: ext,
        opened: false,
        needsOpener: true,
        message: 'Failed to open with "$command": $error',
      );
    }
  }
}
