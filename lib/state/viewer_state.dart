import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:path/path.dart' as p;
import 'package:open_filex/open_filex.dart';

import '../models/common.dart';
import '../models/huggingface.dart';
import '../models/remote_host.dart';
import '../models/viewer_persistence.dart';
import '../models/webdataset.dart';
import '../models/zenodo.dart';
import '../services/app_logger.dart';
import '../services/dataset_source_routing_service.dart';
import '../services/hf_preview_flow_service.dart';
import '../services/huggingface_service.dart';
import '../services/http_dataset_service.dart';
import '../services/litdata_service.dart';
import '../services/local_file_preview_flow_service.dart';
import '../services/mosaicml_service.dart';
import '../services/parquet_preview_service.dart';
import '../services/preferences_service.dart';
import '../services/remote_dataset_service.dart';
import '../services/remote_dataset_ops_service.dart';
import '../services/update_service.dart';
import '../services/webdataset_service.dart';
import '../services/zenodo_service.dart';
import '../services/zenodo_preview_flow_service.dart';

part 'viewer_state_data_io.dart';
part 'viewer_state_data_loading.dart';
part 'viewer_state_data_opening.dart';
part 'viewer_state_source_input.dart';
part 'viewer_state_source_resolution.dart';
part 'viewer_state_session_selection.dart';
part 'viewer_state_api_bridge.dart';

enum ViewerMode {
  litdataIndex,
  litdataChunks,
  mdsIndex,
  webdatasetDir,
  localDirectory,
  huggingface,
  zenodo,
}

enum DetectedSourceKind {
  detecting,
  huggingface,
  zenodo,
  litdataIndex,
  litdataChunks,
  mdsIndex,
  webdatasetDir,
  localDirectory,
  unknown,
}

const _wdsPageSize = 50;
const _hfPageSize = 50;
const _hfFeatureChunkSize = 64;
const _hfVisibleFeatureChunkSize = 64;
const _hfPrefetchFeatureChunkSize = 64;
const _zenodoTarPageSize = 50;
const _recentSourceLimit = 10;
const _localDirPreviewBytes = 64 * 1024;
const _localDirHexSnippetBytes = 2048;
const _remoteTextPreviewBytes = 4 * 1024 * 1024;
const _remoteSourceScheme = 'remote';
const _remoteCompressedMdsListSampleWindow = 200;
const _remoteCompressedMdsListScanBytesMin = 4 * 1024 * 1024;
const _remoteCompressedMdsListScanBytesMax = 256 * 1024 * 1024;
const _apiDefaultHost = '127.0.0.1';
const _apiDefaultPort = 9292;
const _apiDefaultMaxConcurrency = 32;
const _apiMaxAllowedConcurrency = 64;

typedef StartApiServerCallback = Future<int> Function({
  required String host,
  required int port,
  required int maxConcurrency,
});
typedef StopApiServerCallback = Future<void> Function();

class LoadedDatasetSource {
  LoadedDatasetSource({
    required this.id,
    required this.identity,
    required this.sourceInput,
    required this.mode,
    required this.label,
    this.payload,
    this.paths,
    this.expanded = true,
    this.indexSummary,
    this.wdsDirSummary,
    this.hfPreview,
    this.hfConfigOptions,
    this.zenodoRecord,
    this.selectedChunkName,
    this.selectedShardName,
    this.selectedHfConfig,
    this.selectedHfSplit,
    this.selectedZenodoFileKey,
  });

  final String id;
  final String identity;
  final String sourceInput;
  final ViewerMode mode;
  final String label;
  final String? payload;
  final List<String>? paths;
  bool expanded;

  IndexSummary? indexSummary;
  WdsDirSummary? wdsDirSummary;
  HfDatasetPreview? hfPreview;
  List<HfConfigSummary>? hfConfigOptions;
  ZenodoRecordSummary? zenodoRecord;

  String? selectedChunkName;
  String? selectedShardName;
  String? selectedHfConfig;
  String? selectedHfSplit;
  String? selectedZenodoFileKey;
}

class _ResolvedLoadRequest {
  const _ResolvedLoadRequest({
    required this.mode,
    required this.sourceInput,
    this.payload,
    this.paths,
  });

  final ViewerMode mode;
  final String sourceInput;
  final String? payload;
  final List<String>? paths;
}

class _RemoteDirectorySource {
  const _RemoteDirectorySource({
    required this.hostId,
    required this.path,
  });

  final String hostId;
  final String path;
}

class _RemoteLitdataSource {
  const _RemoteLitdataSource({
    required this.hostId,
    required this.directoryPath,
    required this.indexName,
    required this.indexBytes,
  });

  final String hostId;
  final String directoryPath;
  final String indexName;
  final Uint8List indexBytes;
}

class _HttpLitdataSource {
  const _HttpLitdataSource({
    required this.directoryUri,
    required this.indexUri,
    required this.indexName,
    required this.indexBytes,
  });

  final Uri directoryUri;
  final Uri indexUri;
  final String indexName;
  final Uint8List indexBytes;
}

class _RemoteMdsSource {
  const _RemoteMdsSource({
    required this.hostId,
    required this.directoryPath,
    required this.indexName,
    required this.indexBytes,
    this.preferredShardFilename,
  });

  final String hostId;
  final String directoryPath;
  final String indexName;
  final Uint8List indexBytes;
  final String? preferredShardFilename;
}

class _HttpMdsSource {
  const _HttpMdsSource({
    required this.directoryUri,
    required this.indexUri,
    required this.indexName,
    required this.indexBytes,
    this.preferredShardFilename,
  });

  final Uri directoryUri;
  final Uri indexUri;
  final String indexName;
  final Uint8List indexBytes;
  final String? preferredShardFilename;
}

class ViewerState extends ChangeNotifier {
  ViewerState({
    LitDataService? litdata,
    MosaicmlService? mosaicml,
    WebdatasetService? webdataset,
    HuggingfaceService? huggingface,
    ZenodoService? zenodo,
    DatasetSourceRoutingService? sourceRouter,
    HttpDatasetService? httpDatasets,
    HfPreviewFlowService? hfFlow,
    ZenodoPreviewFlowService? zenodoFlow,
    LocalFilePreviewFlowService? localFileFlow,
    ParquetPreviewService? parquetPreview,
    RemoteDatasetOpsService? remoteOps,
    RemoteDatasetService? remoteDatasets,
    PreferencesService? preferences,
    UpdateService? updates,
  })  : _litdata = litdata ?? LitDataService(),
        _mosaicml = mosaicml ?? MosaicmlService(),
        _webdataset = webdataset ?? WebdatasetService(),
        _huggingface = huggingface ?? HuggingfaceService(),
        _zenodo = zenodo ?? ZenodoService(),
        _sourceRouter = sourceRouter ?? const DatasetSourceRoutingService(),
        _httpDatasets = httpDatasets ?? HttpDatasetService(),
        _hfFlow = hfFlow ?? const HfPreviewFlowService(),
        _zenodoFlow = zenodoFlow ?? const ZenodoPreviewFlowService(),
        _localFileFlow = localFileFlow ?? const LocalFilePreviewFlowService(),
        _parquetPreview = parquetPreview ?? ParquetPreviewService(),
        _remoteOps = remoteOps ?? RemoteDatasetOpsService(),
        _remoteDatasets = remoteDatasets ?? RemoteDatasetService(),
        _preferences = preferences ?? PreferencesService(),
        _updates = updates ?? UpdateService() {
    // Warmup DuckDB in background on app start
    _huggingface.warmup();
  }

  final LitDataService _litdata;
  final MosaicmlService _mosaicml;
  final WebdatasetService _webdataset;
  final HuggingfaceService _huggingface;
  final ZenodoService _zenodo;
  final DatasetSourceRoutingService _sourceRouter;
  final HttpDatasetService _httpDatasets;
  final HfPreviewFlowService _hfFlow;
  final ZenodoPreviewFlowService _zenodoFlow;
  final LocalFilePreviewFlowService _localFileFlow;
  final ParquetPreviewService _parquetPreview;
  final RemoteDatasetOpsService _remoteOps;
  final RemoteDatasetService _remoteDatasets;
  final PreferencesService _preferences;
  final UpdateService _updates;
  StartApiServerCallback? _startApiServer;
  StopApiServerCallback? _stopApiServer;

  String sourceInput = '';
  DetectedSourceKind? detectedSource;
  Timer? _detectTimer;
  int _detectRequestId = 0;
  Timer? _sessionPersistTimer;
  bool _restoringSession = false;
  List<String> chunkSelection = [];
  List<LoadedDatasetSource> openedDatasets = [];
  String? activeDatasetId;
  bool scanningDatasets = false;
  int scanDiscoveredCount = 0;
  int scanAddedCount = 0;
  int _scanJobId = 0;
  bool _scanCancelRequested = false;
  ViewerMode? mode;
  int requestId = 0;

  String? selectedChunkName;
  int? selectedItemIndex;
  int? selectedFieldIndex;

  String? selectedShardName;
  String? wdsSelectedSampleKey;
  String? wdsSelectedMemberPath;
  String? wdsSelectedMemberName;

  String? hfConfigOverride;
  String? hfSplitOverride;
  int hfOffset = 0;
  int? hfSelectedRowIndex;
  String? hfSelectedFieldName;

  String? zenodoSelectedFileKey;
  String? zenodoSelectedEntryName;
  int zenodoEntriesOffset = 0;

  int wdsOffset = 0;
  String? statusMessage;
  int _hfFeatureLoadRequestId = 0;

  String? hfToken;
  List<RemoteHostConfig> remoteHosts = const <RemoteHostConfig>[];
  List<String> recentSources = [];
  bool apiEnabled = false;
  String apiHost = _apiDefaultHost;
  int apiPort = _apiDefaultPort;
  int apiMaxConcurrency = _apiDefaultMaxConcurrency;
  bool apiRunning = false;
  int? apiRunningPort;
  String? apiRuntimeHost;
  String? apiRuntimeError;
  _RemoteLitdataSource? _remoteLitdataSource;
  _HttpLitdataSource? _httpLitdataSource;
  _RemoteMdsSource? _remoteMdsSource;
  _HttpMdsSource? _httpMdsSource;

  Future<IndexSummary>? indexFuture;
  IndexSummary? indexSummary;
  Future<List<ItemMeta>>? litdataItemsFuture;
  Future<List<ItemMeta>>? mdsItemsFuture;
  Future<FieldPreview>? fieldPreviewFuture;
  Future<FieldPreview>? mdsFieldPreviewFuture;
  Future<PreparedMediaResponse>? audioPreviewFuture;

  Future<WdsDirSummary>? wdsDirFuture;
  WdsDirSummary? wdsDirSummary;
  Future<WdsSampleListResponse>? wdsSamplesFuture;
  WdsSampleListResponse? wdsSamples;
  Future<FieldPreview>? wdsPreviewFuture;
  Future<PreparedMediaResponse>? wdsAudioPreviewFuture;

  Future<HfDatasetPreview>? hfPreviewFuture;
  HfDatasetPreview? hfPreview;
  List<HfConfigSummary>? hfConfigOptions;

  Future<ZenodoRecordSummary>? zenodoRecordFuture;
  ZenodoRecordSummary? zenodoRecord;
  Future<FieldPreview>? zenodoFilePreviewFuture;
  Future<List<ZenodoZipEntrySummary>>? zenodoZipEntriesFuture;
  List<ZenodoZipEntrySummary>? zenodoZipEntries;
  Future<ZenodoTarEntryListResponse>? zenodoTarEntriesFuture;
  ZenodoTarEntryListResponse? zenodoTarEntries;
  Future<FieldPreview>? zenodoEntryPreviewFuture;
  Future<InlineMediaResponse>? zenodoInlineMediaFuture;

  List<LocalDirectoryItem> localDirectoryItems = [];
  Future<List<LocalDirectoryItem>>? localDirectoryItemsFuture;
  Future<FieldPreview>? localFilePreviewFuture;
  final Map<String, LocalDirectoryItem> _localDirectoryItemCache =
      <String, LocalDirectoryItem>{};

  Future<UpdateInfo?>? updateCheckFuture;

  Map<int, FieldPreview> litdataFieldPreviewByIndex = {};
  Map<int, FieldPreview> mdsFieldPreviewByIndex = {};
  int _fieldPreviewRequestId = 0;

  void _notifyStateChanged() {
    notifyListeners();
  }

  Future<void> bootstrap() async {
    final recent = await _preferences.readRecentSources();
    if (recent.isNotEmpty) {
      recentSources = recent;
    } else {
      final lastIndex = await _preferences.readLastIndex();
      if (lastIndex != null && lastIndex.trim().isNotEmpty) {
        recentSources = [lastIndex.trim()];
      }
    }
    hfToken = await _preferences.readHfToken();
    remoteHosts = await _preferences.readRemoteHosts();
    apiEnabled = await _preferences.readApiEnabled();
    apiHost = (await _preferences.readApiHost()) ?? _apiDefaultHost;
    apiPort = _normalizeApiPort(
      await _preferences.readApiPort() ?? _apiDefaultPort,
    );
    apiMaxConcurrency = _normalizeApiConcurrency(
      await _preferences.readApiMaxConcurrency() ?? _apiDefaultMaxConcurrency,
    );
    updateCheckFuture = _updates.checkForUpdate();
    await _restoreSession();
    notifyListeners();
  }

  void setApiServerCallbacks({
    required StartApiServerCallback startServer,
    required StopApiServerCallback stopServer,
  }) {
    _startApiServer = startServer;
    _stopApiServer = stopServer;
  }

  String get apiEndpointHost => apiRuntimeHost ?? apiHost;
  int get apiEndpointPort => apiRunningPort ?? apiPort;
  String get apiEndpoint =>
      '${apiEnabled ? apiEndpointHost : apiHost}:$apiEndpointPort';

  Future<UpdateInfo?> checkForUpdateNow() async {
    final future = _updates.checkForUpdate();
    updateCheckFuture = future;
    notifyListeners();
    return future;
  }

  @override
  void dispose() {
    _detectTimer?.cancel();
    _sessionPersistTimer?.cancel();
    super.dispose();
  }

  bool get isHuggingFaceDetected =>
      detectedSource == DetectedSourceKind.huggingface;

  bool isDatasetActive(String datasetId) => activeDatasetId == datasetId;

  LoadedDatasetSource? get activeDataset {
    final activeId = activeDatasetId;
    if (activeId == null) return null;
    for (final dataset in openedDatasets) {
      if (dataset.id == activeId) return dataset;
    }
    return null;
  }

  String get detectedSourceLabel {
    switch (detectedSource) {
      case DetectedSourceKind.detecting:
        return 'Detecting...';
      case DetectedSourceKind.huggingface:
        return 'Hugging Face';
      case DetectedSourceKind.zenodo:
        return 'Zenodo';
      case DetectedSourceKind.litdataIndex:
        return 'LitData';
      case DetectedSourceKind.litdataChunks:
        return 'LitData chunks';
      case DetectedSourceKind.mdsIndex:
        return 'MosaicML';
      case DetectedSourceKind.webdatasetDir:
        return 'WebDataset';
      case DetectedSourceKind.localDirectory:
        return 'Local directory';
      case DetectedSourceKind.unknown:
        return 'Unknown';
      case null:
        return 'Auto';
    }
  }

  Future<File> downloadUpdate(UpdateInfo update,
      {void Function(int, int?)? onProgress}) async {
    return _updates.download(update, onProgress: onProgress);
  }

  Future<void> installUpdate(File file) async {
    await _updates.installUpdate(file);
  }

  Future<String?> chooseOpenerApp() async {
    try {
      final result = await FilePicker.platform.pickFiles(type: FileType.any);
      if (result == null || result.files.isEmpty) return null;
      final path = result.files.first.path;
      if (path == null || path.isEmpty) return null;
      return path;
    } on PlatformException catch (err) {
      statusMessage = err.message ?? 'Failed to pick an app.';
      notifyListeners();
      return null;
    }
  }

  Future<String?> preferredOpenerForExt(String ext) async {
    return _preferences.readPreferredOpenerForExt(ext);
  }

  Future<void> savePreferredOpenerForExt(String ext, String appPath) async {
    await _preferences.savePreferredOpenerForExt(ext, appPath);
  }

  Future<void> saveHfToken(String token) async {
    final trimmed = token.trim();
    hfToken = trimmed.isEmpty ? null : trimmed;
    if (hfToken == null) {
      await _preferences.clearHfToken();
    } else {
      await _preferences.saveHfToken(trimmed);
    }
    notifyListeners();
  }

  Future<void> clearHfToken() async {
    hfToken = null;
    await _preferences.clearHfToken();
    notifyListeners();
  }

  Future<bool> applyApiSettings({
    bool? enabled,
    String? host,
    int? port,
    int? maxConcurrency,
    bool applyRuntime = true,
  }) async {
    final nextEnabled = enabled ?? apiEnabled;
    final nextHost =
        (host?.trim().isEmpty ?? true) ? _apiDefaultHost : host!.trim();
    final nextPort = _normalizeApiPort(port ?? apiPort);
    final nextConcurrency = _normalizeApiConcurrency(
      maxConcurrency ?? apiMaxConcurrency,
    );
    final settingsChanged = nextEnabled != apiEnabled ||
        nextHost != apiHost ||
        nextPort != apiPort ||
        nextConcurrency != apiMaxConcurrency;
    apiEnabled = nextEnabled;
    apiHost = nextHost;
    apiPort = nextPort;
    apiMaxConcurrency = nextConcurrency;
    await _preferences.saveApiEnabled(apiEnabled);
    await _preferences.saveApiHost(apiHost);
    await _preferences.saveApiPort(apiPort);
    await _preferences.saveApiMaxConcurrency(apiMaxConcurrency);
    notifyListeners();

    if (!applyRuntime) return settingsChanged;
    if (!apiEnabled) {
      if (apiRunning) {
        await _stopApiServerInternal();
      }
      return settingsChanged;
    }

    if (apiRunning) {
      if (!settingsChanged) {
        return false;
      }
      await _stopApiServerInternal();
    }
    final started = await _startApiServerInternal();
    return started;
  }

  Future<bool> startApiServer() async {
    if (apiEnabled) {
      if (apiRunning) {
        return false;
      }
      return _startApiServerInternal();
    }
    apiEnabled = true;
    await _preferences.saveApiEnabled(true);
    notifyListeners();
    return _startApiServerInternal();
  }

  Future<void> stopApiServer() async {
    apiEnabled = false;
    await _preferences.saveApiEnabled(false);
    await _stopApiServerInternal();
    notifyListeners();
  }

  Future<bool> _startApiServerInternal() async {
    final start = _startApiServer;
    if (start == null) {
      apiRuntimeError = 'API control is not available.';
      notifyListeners();
      return false;
    }
    try {
      apiRuntimeError = null;
      final resolvedPort = await start(
        host: apiHost,
        port: apiPort,
        maxConcurrency: apiMaxConcurrency,
      );
      apiRunning = true;
      apiRunningPort = resolvedPort;
      apiRuntimeHost = apiHost;
      notifyListeners();
      return true;
    } catch (error, stack) {
      AppLogger.error(
        'Failed to start API server',
        tag: 'api',
        error: error,
        stackTrace: stack,
      );
      apiRuntimeError = error.toString();
      apiRunning = false;
      apiRunningPort = null;
      apiRuntimeHost = null;
      notifyListeners();
      statusMessage = 'Failed to start API server.';
      notifyListeners();
      return false;
    }
  }

  Future<void> _stopApiServerInternal() async {
    final stop = _stopApiServer;
    if (stop == null) {
      apiRunning = false;
      apiRunningPort = null;
      apiRuntimeHost = null;
      notifyListeners();
      return;
    }
    try {
      await stop();
    } catch (error, stack) {
      AppLogger.error(
        'Failed to stop API server',
        tag: 'api',
        error: error,
        stackTrace: stack,
      );
      apiRuntimeError = error.toString();
    } finally {
      apiRunning = false;
      apiRunningPort = null;
      apiRuntimeHost = null;
      notifyListeners();
    }
  }

  int _normalizeApiPort(int value) {
    if (value < 1 || value > 65535) {
      return _apiDefaultPort;
    }
    return value;
  }

  int _normalizeApiConcurrency(int value) {
    if (value < 1) return 1;
    if (value > _apiMaxAllowedConcurrency) {
      return _apiMaxAllowedConcurrency;
    }
    return value;
  }

  Future<void> saveRemoteHosts(List<RemoteHostConfig> hosts) async {
    final normalized = <RemoteHostConfig>[];
    final seenIds = <String>{};
    for (final host in hosts) {
      if (!host.isValid) continue;
      final id = host.id.trim();
      if (id.isEmpty || !seenIds.add(id)) continue;
      normalized.add(host);
    }
    if (_remoteHostsEqual(normalized, remoteHosts)) {
      return;
    }
    remoteHosts = normalized;
    await _preferences.saveRemoteHosts(normalized);
    notifyListeners();
  }

  Future<bool> addSourceFromRemoteHost({
    required String hostId,
    required String datasetPath,
    bool recordRecent = true,
  }) async {
    final normalizedId = hostId.trim();
    if (normalizedId.isEmpty) return false;
    RemoteHostConfig? host;
    for (final item in remoteHosts) {
      if (item.id.trim() == normalizedId) {
        host = item;
        break;
      }
    }
    if (host == null) {
      statusMessage = 'Remote host not found: $normalizedId';
      notifyListeners();
      return false;
    }
    try {
      final remoteSource = _buildRemoteSourceInput(
        normalizedId,
        datasetPath,
      );
      final remote = _parseRemoteDirectorySource(remoteSource);
      final resolved = (remote == null)
          ? _ResolvedLoadRequest(
              mode: ViewerMode.localDirectory,
              sourceInput: remoteSource,
              payload: remoteSource,
              paths: const <String>[],
            )
          : await _resolveRemoteLoadRequest(
              sourceInput: remoteSource,
              remote: remote,
              host: host,
            );
      final added = await _addResolvedSource(
        resolved,
        recordRecent: recordRecent,
        awaitPrimaryLoad: false,
      );
      final identity = _datasetIdentity(resolved);
      final exists = _datasetByIdentity(identity) != null;
      final opened = added || exists;
      if (opened) {
        final normalizedPath = _normalizeRemoteDirectoryPath(datasetPath);
        final pathLabel = normalizedPath.isEmpty ? '/' : '/$normalizedPath';
        statusMessage = added
            ? 'Opened remote dataset: ${host.label} ($pathLabel)'
            : 'Remote dataset already open: ${host.label} ($pathLabel)';
        notifyListeners();
      }
      return opened;
    } catch (error) {
      statusMessage = error.toString();
      notifyListeners();
      return false;
    }
  }

  bool get isRemoteDirectoryMode => _activeRemoteDirectorySource != null;

  String _buildRemoteSourceInput(String hostId, String datasetPath) {
    final normalizedHost = hostId.trim();
    if (normalizedHost.isEmpty) {
      throw const FormatException('Remote host ID is required.');
    }
    final normalizedPath = _normalizeRemoteDirectoryPath(datasetPath);
    final segments = normalizedPath.isEmpty
        ? const <String>[]
        : normalizedPath
            .split('/')
            .where((segment) => segment.trim().isNotEmpty)
            .toList(growable: false);
    return Uri(
      scheme: _remoteSourceScheme,
      host: normalizedHost,
      pathSegments: segments,
    ).toString();
  }

  _RemoteDirectorySource? _parseRemoteDirectorySource(String input) {
    final trimmed = input.trim();
    if (trimmed.isEmpty) return null;
    final uri = Uri.tryParse(trimmed);
    if (uri == null || uri.scheme != _remoteSourceScheme) {
      return null;
    }
    final hostId = uri.host.trim();
    if (hostId.isEmpty) {
      return null;
    }
    final path = _normalizeRemoteDirectoryPath(
      uri.pathSegments.map(Uri.decodeComponent).join('/'),
    );
    return _RemoteDirectorySource(hostId: hostId, path: path);
  }

  String _normalizeRemoteDirectoryPath(String value) {
    final trimmed = value.trim().replaceAll('\\', '/');
    if (trimmed.isEmpty) return '';
    var normalized = trimmed;
    while (normalized.startsWith('/')) {
      normalized = normalized.substring(1);
    }
    while (normalized.endsWith('/')) {
      normalized = normalized.substring(0, normalized.length - 1);
    }
    return normalized;
  }

  Future<int> _estimateRemoteCompressedMdsScanBytes({
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    int targetItemIndex = 0,
    int targetSampleWindow = _remoteCompressedMdsListSampleWindow,
  }) async {
    if (indexBytes == null) {
      return _remoteCompressedMdsListScanBytesMin;
    }
    final normalizedShard = _normalizeChunkName(shardFilename);
    if (normalizedShard.isEmpty) {
      return _remoteCompressedMdsListScanBytesMin;
    }
    final candidates = _compressedMdsChunkCandidates(normalizedShard);
    if (candidates.isEmpty) {
      return _remoteCompressedMdsListScanBytesMin;
    }

    IndexSummary summary;
    try {
      summary = await _mosaicml.loadIndexFromBytes(
        indexBytes,
        indexName: indexName,
      );
    } catch (_) {
      return _remoteCompressedMdsListScanBytesMin;
    }

    ChunkSummary? selectedChunk;
    for (final chunk in summary.chunks) {
      final filename = _normalizeChunkName(chunk.filename);
      final basename = _normalizeChunkName(p.basename(chunk.path));
      if (candidates.contains(filename) || candidates.contains(basename)) {
        selectedChunk = chunk;
        break;
      }
    }
    if (selectedChunk == null) {
      return _remoteCompressedMdsListScanBytesMin;
    }

    final totalSamples = selectedChunk.chunkSize;
    final totalBytes = selectedChunk.chunkBytes;
    final normalizedTargetIndex = targetItemIndex < 0
        ? 0
        : (targetItemIndex >= totalSamples
            ? (totalSamples <= 0 ? 0 : totalSamples - 1)
            : targetItemIndex);
    final requestedWindow = targetSampleWindow < 1 ? 1 : targetSampleWindow;
    final sampleWindowToLoad = totalSamples <= 0
        ? requestedWindow
        : normalizedTargetIndex + requestedWindow;

    if (totalBytes <= 0) {
      return _remoteCompressedMdsListScanBytesMin;
    }
    if (totalBytes <= _remoteCompressedMdsListScanBytesMax) {
      return totalBytes;
    }
    if (totalSamples <= 0) {
      return _remoteCompressedMdsListScanBytesMax;
    }

    final targetSamples = sampleWindowToLoad < 0
        ? 1
        : (sampleWindowToLoad > totalSamples
            ? totalSamples
            : sampleWindowToLoad);
    final ratio = targetSamples / totalSamples;
    var estimated = ((totalBytes * ratio) * 1.5).ceil() + 128 * 1024;
    if (estimated < _remoteCompressedMdsListScanBytesMin) {
      estimated = _remoteCompressedMdsListScanBytesMin;
    } else if (estimated > _remoteCompressedMdsListScanBytesMax) {
      estimated = _remoteCompressedMdsListScanBytesMax;
    }
    return estimated;
  }

  String _normalizeChunkName(String value) {
    return value
        .trim()
        .replaceAll('\\', '/')
        .toLowerCase()
        .trim()
        .replaceAll(RegExp(r'/+'), '/');
  }

  Set<String> _compressedMdsChunkCandidates(String shardFilename) {
    final lower = _normalizeChunkName(shardFilename).trim();
    if (lower.isEmpty) return const <String>{};
    final strippedZstd = lower.endsWith('.zstd') || lower.endsWith('.zst')
        ? lower.substring(0, lower.lastIndexOf('.'))
        : lower;
    final withoutSuffix = strippedZstd.endsWith('.zstd')
        ? strippedZstd.substring(0, strippedZstd.lastIndexOf('.'))
        : strippedZstd;
    final candidates = <String>{
      lower,
      strippedZstd,
      withoutSuffix,
    };
    candidates.removeWhere((value) => value.isEmpty);
    return candidates;
  }

  Set<String> _compressedMdsRemotePathCandidates(String shardFilename) {
    final base = _compressedMdsChunkCandidates(shardFilename);
    if (base.isEmpty) return const <String>{};
    final expanded = <String>{...base};
    for (final candidate in base) {
      if (!candidate.endsWith('.zstd')) {
        expanded.add('$candidate.zstd');
      }
      if (!candidate.endsWith('.zst')) {
        expanded.add('$candidate.zst');
      }
    }
    expanded.removeWhere((value) => value.isEmpty);
    return expanded;
  }

  Future<String> _resolveRemoteMdsShardPath({
    required RemoteHostConfig host,
    required String directoryPath,
    required String shardFilename,
    required bool compressed,
  }) async {
    final normalizedDirectory = _normalizeRemoteDirectoryPath(directoryPath);
    final normalizedShard = shardFilename.trim();
    if (normalizedShard.isEmpty) {
      throw const FormatException('shard filename is empty');
    }
    if (!compressed) {
      return _joinRemoteDirectoryPath(normalizedDirectory, normalizedShard);
    }
    final candidates = _compressedMdsRemotePathCandidates(normalizedShard);
    try {
      final entries = await _remoteDatasets.listEntries(
        host: host,
        directoryPath: normalizedDirectory,
        onStatus: (message) {
          statusMessage = message;
        },
      );
      for (final entry in entries) {
        if (entry.isDirectory) continue;
        final normalizedName = _normalizeChunkName(entry.name);
        final normalizedPath = _normalizeChunkName(p.basename(entry.path));
        if (candidates.contains(normalizedName) ||
            candidates.contains(normalizedPath)) {
          return _normalizeRemoteDirectoryPath(entry.path);
        }
      }
    } catch (_) {}

    final normalizedLower = _normalizeChunkName(normalizedShard);
    if (!normalizedLower.endsWith('.zstd')) {
      return _joinRemoteDirectoryPath(
        normalizedDirectory,
        '$normalizedShard.zstd',
      );
    }
    return _joinRemoteDirectoryPath(normalizedDirectory, normalizedShard);
  }

  _RemoteDirectorySource? get _activeRemoteDirectorySource {
    if (mode != ViewerMode.localDirectory) {
      return null;
    }
    final active =
        activeDatasetId == null ? null : _datasetById(activeDatasetId!);
    final candidates = <String>[
      if (active?.payload != null && active!.payload!.trim().isNotEmpty)
        active.payload!.trim(),
      if (active?.sourceInput.trim().isNotEmpty == true)
        active!.sourceInput.trim(),
      if (sourceInput.trim().isNotEmpty) sourceInput.trim(),
    ];
    for (final candidate in candidates) {
      final parsed = _parseRemoteDirectorySource(candidate);
      if (parsed != null) {
        return parsed;
      }
    }
    return null;
  }

  Future<RemoteHostConnectionResult> testRemoteHostConnection(
    RemoteHostConfig host, {
    bool verifyWrite = true,
  }) {
    return _remoteDatasets.testConnection(
      host: host,
      verifyWrite: verifyWrite,
      onStatus: (message) {
        statusMessage = message;
        notifyListeners();
      },
    );
  }

  Future<List<RemotePathEntry>> listRemoteHostEntries({
    required String hostId,
    String directoryPath = '',
  }) async {
    final host = _findRemoteHost(hostId);
    if (host == null) {
      throw FormatException('Remote host not found: $hostId');
    }
    return _remoteDatasets.listEntries(
      host: host,
      directoryPath: directoryPath,
      onStatus: (message) {
        statusMessage = message;
      },
    );
  }

  Future<void> writeTextToRemoteHost({
    required String hostId,
    required String remotePath,
    required String content,
    bool overwrite = true,
  }) async {
    final host = _findRemoteHost(hostId);
    if (host == null) {
      throw FormatException('Remote host not found: $hostId');
    }
    await _remoteDatasets.writeTextFile(
      host: host,
      remotePath: remotePath,
      content: content,
      overwrite: overwrite,
      onStatus: (message) {
        statusMessage = message;
        notifyListeners();
      },
    );
  }

  RemoteHostConfig? findRemoteHostById(String hostId) {
    return _findRemoteHost(hostId);
  }

  RemoteHostConfig? _findRemoteHost(String hostId) {
    final normalized = hostId.trim();
    if (normalized.isEmpty) return null;
    for (final host in remoteHosts) {
      if (host.id.trim() == normalized) {
        return host;
      }
    }
    return null;
  }

  bool _remoteHostsEqual(
    List<RemoteHostConfig> left,
    List<RemoteHostConfig> right,
  ) {
    if (left.length != right.length) return false;
    for (var i = 0; i < left.length; i += 1) {
      if (left[i] != right[i]) return false;
    }
    return true;
  }

  Future<OpenLeafResponse> huggingfaceOpenField({
    required String input,
    required String config,
    required String split,
    required int rowIndex,
    required String fieldName,
    bool openWithSystem = true,
    String? openerAppPath,
  }) async {
    AppLogger.info(
      'Open Hugging Face field row=$rowIndex field="$fieldName" config="$config" split="$split"',
      tag: 'state',
    );
    try {
      return await _huggingface.openField(
        input: input,
        config: config,
        split: split,
        rowIndex: rowIndex,
        fieldName: fieldName,
        openWithSystem: openWithSystem,
        openerAppPath: openerAppPath,
        token: hfToken,
      );
    } catch (error, stack) {
      AppLogger.error('Hugging Face open field failed',
          tag: 'state', error: error, stackTrace: stack);
      rethrow;
    }
  }

  Future<OpenLeafResponse> zenodoOpenFile({
    required String contentUrl,
    required String filename,
    String? openerAppPath,
  }) async {
    return _zenodo.openFile(
      contentUrl: contentUrl,
      filename: filename,
      openerAppPath: openerAppPath,
    );
  }

  Future<OpenLeafResponse> zenodoZipOpenEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
    String? openerAppPath,
  }) async {
    return _zenodo.zipOpenEntry(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
      openerAppPath: openerAppPath,
    );
  }

  Future<OpenLeafResponse> zenodoTarOpenEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
    String? openerAppPath,
  }) async {
    return _zenodo.tarOpenEntry(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
      openerAppPath: openerAppPath,
    );
  }

  Future<PreparedMediaResponse> zenodoPrepareSelectedEntryMedia() async {
    final record = zenodoRecord;
    if (record == null || record.files.isEmpty) {
      throw const FormatException('No Zenodo record loaded.');
    }
    final entryName = zenodoSelectedEntryName;
    if (entryName == null || entryName.isEmpty) {
      throw const FormatException('No entry selected.');
    }
    final file = record.files.firstWhere(
      (f) => f.key == zenodoSelectedFileKey,
      orElse: () => record.files.first,
    );
    final lower = file.key.toLowerCase();
    if (lower.endsWith('.zip')) {
      return _zenodo.zipPrepareEntryMedia(
        contentUrl: file.contentUrl,
        filename: file.key,
        entryName: entryName,
      );
    }
    return _zenodo.tarPrepareEntryMedia(
      contentUrl: file.contentUrl,
      filename: file.key,
      entryName: entryName,
    );
  }

  Future<PreparedMediaResponse> zenodoPrepareSelectedFileMedia() async {
    final record = zenodoRecord;
    if (record == null || record.files.isEmpty) {
      throw const FormatException('No Zenodo record loaded.');
    }
    final file = record.files.firstWhere(
      (f) => f.key == zenodoSelectedFileKey,
      orElse: () => record.files.first,
    );
    return _zenodo.prepareFileMedia(
      contentUrl: file.contentUrl,
      filename: file.key,
    );
  }

  bool _looksLikeHfInput(String value) {
    if (value.startsWith('hf://datasets/')) return true;
    if (value.startsWith('https://huggingface.co/datasets/') ||
        value.startsWith('http://huggingface.co/datasets/')) {
      return true;
    }
    if (value.startsWith('https://hf.co/datasets/') ||
        value.startsWith('http://hf.co/datasets/')) {
      return true;
    }
    return false;
  }

  bool _looksLikeZenodoInput(String value) {
    final uri = Uri.tryParse(value);
    if (uri == null) return false;
    final host = uri.host.toLowerCase();
    if (!(host == 'zenodo.org' || host.endsWith('.zenodo.org'))) return false;
    final segments = uri.pathSegments.where((s) => s.isNotEmpty).toList();
    for (var i = 0; i < segments.length; i += 1) {
      if (segments[i] == 'records' || segments[i] == 'record') return true;
    }
    return false;
  }
}
