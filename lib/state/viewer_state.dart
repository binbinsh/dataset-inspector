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
import '../services/huggingface_service.dart';
import '../services/litdata_service.dart';
import '../services/mosaicml_service.dart';
import '../services/preferences_service.dart';
import '../services/remote_dataset_service.dart';
import '../services/update_service.dart';
import '../services/webdataset_service.dart';
import '../services/zenodo_service.dart';

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

class ViewerState extends ChangeNotifier {
  ViewerState({
    LitDataService? litdata,
    MosaicmlService? mosaicml,
    WebdatasetService? webdataset,
    HuggingfaceService? huggingface,
    ZenodoService? zenodo,
    RemoteDatasetService? remoteDatasets,
    PreferencesService? preferences,
    UpdateService? updates,
  })  : _litdata = litdata ?? LitDataService(),
        _mosaicml = mosaicml ?? MosaicmlService(),
        _webdataset = webdataset ?? WebdatasetService(),
        _huggingface = huggingface ?? HuggingfaceService(),
        _zenodo = zenodo ?? ZenodoService(),
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
  final RemoteDatasetService _remoteDatasets;
  final PreferencesService _preferences;
  final UpdateService _updates;

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
  int? remoteCacheQuotaMb;
  List<String> recentSources = [];

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
    remoteCacheQuotaMb = await _preferences.readRemoteCacheQuotaMb();
    updateCheckFuture = _updates.checkForUpdate();
    await _restoreSession();
    notifyListeners();
  }

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

  Future<void> saveRemoteCacheQuotaMb(int? quotaMb) async {
    final normalized = quotaMb == null || quotaMb <= 0 ? null : quotaMb;
    if (normalized == remoteCacheQuotaMb) {
      return;
    }
    remoteCacheQuotaMb = normalized;
    await _preferences.saveRemoteCacheQuotaMb(normalized);
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

  Future<RemoteCacheStats> loadRemoteCacheStats({
    RemoteHostConfig? host,
  }) {
    return _remoteDatasets.loadCacheStats(
      host: host,
      allHosts: remoteHosts,
    );
  }

  Future<void> clearRemoteCache({
    RemoteHostConfig? host,
  }) async {
    await _remoteDatasets.clearCache(
      host: host,
      allHosts: remoteHosts,
    );
    if (host == null) {
      statusMessage = 'Remote cache cleared.';
    } else {
      statusMessage = 'Remote cache cleared for ${host.label}.';
    }
    notifyListeners();
  }

  Future<void> enforceRemoteCacheQuota() async {
    final quotaMb = remoteCacheQuotaMb;
    if (quotaMb == null || quotaMb <= 0) return;
    await _remoteDatasets.enforceCacheQuota(
      maxBytes: quotaMb * 1024 * 1024,
      allHosts: remoteHosts,
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

  void setSourceInput(String value) {
    sourceInput = value;
    _scheduleSourceDetection(value);
    _scheduleSessionPersist();
    notifyListeners();
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
      notifyListeners();
    }
  }

  void setChunkSelection(List<String> paths) {
    chunkSelection = paths;
    notifyListeners();
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
      notifyListeners();
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
      notifyListeners();
    }
  }

  Future<String?> _resolvePickerInitialDirectory() async {
    final trimmed = sourceInput.trim();
    if (trimmed.isEmpty) return null;
    if (_looksLikeHfInput(trimmed) || _looksLikeZenodoInput(trimmed)) {
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
      notifyListeners();
    } on PlatformException catch (err) {
      statusMessage = err.message ?? 'Failed to pick files.';
      notifyListeners();
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

    indexSummary = null;
    wdsDirSummary = null;
    hfPreview = null;
    zenodoRecord = null;
    _clearFieldPreviewCache();

    if (nextMode == ViewerMode.litdataIndex) {
      final indexPath = _normalizeDatasetDir(
        payload?.trim().isNotEmpty == true
            ? payload!.trim()
            : sourceInput.trim(),
      );
      if (indexPath.isEmpty) return;
      indexFuture = _captureFutureError(
        _litdata.loadIndex(indexPath).then((value) {
          indexSummary = value;
          _preferences.saveLastIndex(value.rootDir);
          selectedChunkName =
              value.chunks.isNotEmpty ? value.chunks.first.filename : null;
          _loadLitdataItems();
          _syncActiveDatasetSelection();
          notifyListeners();
          return value;
        }),
        context: 'LitData load failed',
        fallback: _emptyIndexSummary,
      );
      notifyListeners();
      return;
    }

    if (nextMode == ViewerMode.mdsIndex) {
      final indexPath = _normalizeDatasetDir(
        payload?.trim().isNotEmpty == true
            ? payload!.trim()
            : sourceInput.trim(),
      );
      if (indexPath.isEmpty) return;
      indexFuture = _captureFutureError(
        _mosaicml.loadIndex(indexPath).then((value) {
          indexSummary = value;
          _preferences.saveLastIndex(value.rootDir);
          selectedChunkName =
              value.chunks.isNotEmpty ? value.chunks.first.filename : null;
          _loadMdsItems();
          _syncActiveDatasetSelection();
          notifyListeners();
          return value;
        }),
        context: 'MosaicML load failed',
        fallback: _emptyIndexSummary,
      );
      notifyListeners();
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
          notifyListeners();
          return value;
        }),
        context: 'LitData chunk list failed',
        fallback: _emptyIndexSummary,
      );
      notifyListeners();
      return;
    }

    if (nextMode == ViewerMode.webdatasetDir) {
      final source = _normalizeDatasetDir(
        payload?.trim().isNotEmpty == true
            ? payload!.trim()
            : sourceInput.trim(),
      );
      if (source.isEmpty) return;
      final remote = _parseRemoteDirectorySource(source);
      if (remote != null) {
        final host = _findRemoteHost(remote.hostId);
        if (host == null) {
          statusMessage = 'Remote host not found: ${remote.hostId}';
          notifyListeners();
          return;
        }
        wdsDirFuture = _captureFutureError(
          () async {
            final entries = await _remoteDatasets.listEntries(
              host: host,
              directoryPath: remote.path,
              onStatus: (message) {
                statusMessage = message;
                notifyListeners();
              },
            );
            final shards = entries
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
            final value = WdsDirSummary(
              // Keep remote source marker here; individual shard loads resolve
              // into local cache lazily.
              dirPath: source,
              shards: shards,
            );
            wdsDirSummary = value;
            selectedShardName =
                value.shards.isNotEmpty ? value.shards.first.filename : null;
            _loadWdsSamples();
            _syncActiveDatasetSelection();
            notifyListeners();
            return value;
          }(),
          context: 'WebDataset load failed',
          fallback: _emptyWdsDirSummary,
        );
        notifyListeners();
        return;
      }
      wdsDirFuture = _captureFutureError(
        _webdataset.loadDir(source).then((value) {
          wdsDirSummary = value;
          selectedShardName =
              value.shards.isNotEmpty ? value.shards.first.filename : null;
          _loadWdsSamples();
          _syncActiveDatasetSelection();
          notifyListeners();
          return value;
        }),
        context: 'WebDataset load failed',
        fallback: _emptyWdsDirSummary,
      );
      notifyListeners();
      return;
    }

    if (nextMode == ViewerMode.localDirectory) {
      final source = payload?.trim().isNotEmpty == true
          ? payload!.trim()
          : sourceInput.trim();
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
          notifyListeners();
          return items;
        });
        notifyListeners();
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
        notifyListeners();
        return items;
      });
      notifyListeners();
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
          notifyListeners();
          return value;
        }),
        context: 'Zenodo load failed',
        fallback: _emptyZenodoRecord,
      );
      notifyListeners();
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
    notifyListeners();

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
        notifyListeners();
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
      notifyListeners();
    }
  }

  Future<_ResolvedLoadRequest> _resolveLoadRequest(String input) async {
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
    final entries = await _remoteDatasets.listEntries(
      host: host,
      directoryPath: normalizedPath,
      onStatus: (message) {
        statusMessage = message;
      },
    );
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
    final name = p.basename(path);
    final dot = name.lastIndexOf('.');
    if (dot <= 0 || dot >= name.length - 1) return 'bin';
    return name.substring(dot + 1).toLowerCase();
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
        notifyListeners();
      }
      return false;
    }
    activeDatasetId = dataset.id;
    notifyListeners();

    if (awaitPrimaryLoad) {
      await _awaitPrimaryLoad(resolved.mode);

      // If loading failed, the .then() callback never ran so the summary
      // fields are still null. Fill them with empty values so the UI
      // shows "No entries" instead of spinning forever.
      _ensurePrimaryDataNotNull(resolved.mode);

      _syncActiveDatasetSelection();
      if (notify) {
        notifyListeners();
      }
    }
    return true;
  }

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
      notifyListeners();
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
    final detectedKind = await _detectRemoteDirectorySourceKind(remote);
    if (detectedKind != DetectedSourceKind.webdatasetDir) {
      return fallback;
    }
    if (host.type != RemoteHostType.samba) {
      // Remote WebDataset lazy shard fetch is currently implemented for Samba.
      // Keep other remote providers in directory mode.
      return fallback;
    }
    return _ResolvedLoadRequest(
      mode: ViewerMode.webdatasetDir,
      sourceInput: sourceInput,
      payload: sourceInput,
    );
  }

  _RemoteDirectorySource? _activeRemoteWebdatasetSource() {
    if (mode != ViewerMode.webdatasetDir) return null;
    final active = activeDataset;
    final candidates = <String>[
      if (active?.sourceInput.trim().isNotEmpty == true)
        active!.sourceInput.trim(),
      if (active?.payload?.trim().isNotEmpty == true) active!.payload!.trim(),
      if (sourceInput.trim().isNotEmpty) sourceInput.trim(),
    ];
    for (final candidate in candidates) {
      final parsed = _parseRemoteDirectorySource(candidate);
      if (parsed != null) return parsed;
    }
    return null;
  }

  String _joinRemoteDirectoryPath(String left, String right) {
    final leftNorm = _normalizeRemoteDirectoryPath(left);
    final rightNorm = _normalizeRemoteDirectoryPath(right);
    if (leftNorm.isEmpty) return rightNorm;
    if (rightNorm.isEmpty) return leftNorm;
    return '$leftNorm/$rightNorm';
  }

  Stream<List<int>> _openRemoteWebdatasetShardStream(String shardFilename) {
    final remote = _activeRemoteWebdatasetSource();
    if (remote == null) {
      throw const FormatException('Remote WebDataset source is not active.');
    }
    final host = _findRemoteHost(remote.hostId);
    if (host == null) {
      throw FormatException('Remote host not found: ${remote.hostId}');
    }
    final remoteShardPath =
        _joinRemoteDirectoryPath(remote.path, shardFilename);
    return _remoteDatasets.openReadFile(
      host: host,
      remotePath: remoteShardPath,
      onStatus: (message) {
        statusMessage = message;
        notifyListeners();
      },
    );
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
    final host = _findRemoteHost(remote.hostId);
    if (host == null) {
      return DetectedSourceKind.localDirectory;
    }
    try {
      final entries = await _remoteDatasets.listEntries(
        host: host,
        directoryPath: remote.path,
      );
      if (entries.any((entry) =>
          !entry.isDirectory && _looksLikeWebdatasetShardName(entry.name))) {
        return DetectedSourceKind.webdatasetDir;
      }
    } catch (_) {}
    return DetectedSourceKind.localDirectory;
  }

  bool _looksLikeWebdatasetShardName(String filename) {
    final name = filename.toLowerCase();
    return name.endsWith('.tar') ||
        name.endsWith('.tar.gz') ||
        name.endsWith('.tgz') ||
        name.endsWith('.tar.zst') ||
        name.endsWith('.tar.zstd');
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
      notifyListeners();
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
    notifyListeners();
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
      notifyListeners();
    }
  }

  void toggleDatasetExpanded(String datasetId) {
    final dataset = _datasetById(datasetId);
    if (dataset == null) return;
    dataset.expanded = !dataset.expanded;
    _scheduleSessionPersist();
    notifyListeners();
  }

  Future<void> activateDataset(String datasetId) async {
    final dataset = _datasetById(datasetId);
    if (dataset == null) return;
    if (activeDatasetId == dataset.id && mode == dataset.mode) return;

    activeDatasetId = dataset.id;
    sourceInput = dataset.sourceInput;
    _scheduleSourceDetection(sourceInput);
    notifyListeners();

    triggerLoad(dataset.mode, payload: dataset.payload, paths: dataset.paths);
    await _awaitPrimaryLoad(dataset.mode);
    _ensurePrimaryDataNotNull(dataset.mode);
    _restorePrimarySelectionForDataset(dataset);
    _syncActiveDatasetSelection();
    notifyListeners();
  }

  Future<void> removeDataset(String datasetId) async {
    final removingActive = activeDatasetId == datasetId;
    openedDatasets =
        openedDatasets.where((dataset) => dataset.id != datasetId).toList();
    if (!removingActive) {
      _scheduleSessionPersist();
      notifyListeners();
      return;
    }
    if (openedDatasets.isEmpty) {
      activeDatasetId = null;
      _clearLoadedViewState();
      _scheduleSessionPersist();
      notifyListeners();
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
        notifyListeners();
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
    notifyListeners();
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
        notifyListeners();
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
        notifyListeners();
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
      notifyListeners();
      return;
    }
    selectedItemIndex = idx;
    if (idx == null) {
      selectedFieldIndex = null;
      fieldPreviewFuture = null;
      mdsFieldPreviewFuture = null;
      _clearFieldPreviewCache();
      notifyListeners();
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
    notifyListeners();
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
      notifyListeners();
      return;
    }
    final idx = localDirectoryItems.indexWhere((item) => item.path == path);
    LocalDirectoryItem? selectedItem;
    if (idx >= 0 && idx < localDirectoryItems.length) {
      selectedItem = localDirectoryItems[idx];
      selectedItemIndex = idx;
    } else {
      selectedItem = _localDirectoryItemCache[path];
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
    notifyListeners();
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

  bool _isLocalBinary(String ext, List<int> bytes) {
    final knownText = {
      'txt',
      'json',
      'yaml',
      'yml',
      'csv',
      'tsv',
      'md',
      'toml',
      'ini',
      'cfg',
      'log',
      'rtf',
      'xml',
      'html',
      'css',
      'js',
      'ts',
      'dart',
      'py',
      'java',
      'cpp',
      'c',
      'h',
      'go',
      'rs',
      'sh',
      'bat',
      'ps1',
    };
    if (knownText.contains(ext)) return false;

    const knownBinary = {
      'png',
      'jpg',
      'jpeg',
      'gif',
      'webp',
      'bmp',
      'svg',
      'wav',
      'mp3',
      'flac',
      'ogg',
      'm4a',
      'opus',
      'aac',
      'sph',
      'mp4',
      'webm',
      'mov',
      'zip',
      'tar',
      'gz',
      'bz2',
      'xz',
      'bin',
      'dat',
    };
    if (knownBinary.contains(ext)) return true;

    var control = 0;
    for (var i = 0; i < bytes.length; i += 1) {
      final value = bytes[i];
      if (value == 0) return true;
      if ((value < 0x07 && value != 0x09 && value != 0x0a && value != 0x0d) ||
          value == 0x0b) {
        control += 1;
      }
    }
    return control > 0 && control >= (bytes.length * 0.1).round();
  }

  String _hexChunkSuffix(List<int> bytes) {
    if (bytes.length <= _localDirHexSnippetBytes) return '';
    return '\n\n(truncated ${bytes.length - _localDirHexSnippetBytes} bytes)';
  }

  String _toHexSnippet(List<int> bytes,
      {int maxBytes = _localDirHexSnippetBytes}) {
    final limit = bytes.length < maxBytes ? bytes.length : maxBytes;
    final buffer = StringBuffer();
    for (var i = 0; i < limit; i += 1) {
      final value = bytes[i];
      buffer.write(value.toRadixString(16).padLeft(2, '0'));
      if (i + 1 < limit) buffer.write(' ');
    }
    return buffer.toString();
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
    final ext = _normalizeLocalExt(remotePath);
    final isBinary = _isLocalBinary(ext, bytes);
    if (isBinary) {
      final hex = _toHexSnippet(bytes, maxBytes: _localDirHexSnippetBytes);
      return FieldPreview(
        previewText: '',
        hexSnippet: 'Read ${bytes.length} bytes\n$hex',
        guessedExt: ext,
        isBinary: true,
        size: bytes.length,
      );
    }
    final previewText = utf8.decode(bytes, allowMalformed: true);
    return FieldPreview(
      previewText: previewText,
      hexSnippet: '',
      guessedExt: ext,
      isBinary: false,
      size: bytes.length,
    );
  }

  Future<FieldPreview> _readLocalFilePreview(String path) async {
    final file = File(path);
    FileStat? stat;
    try {
      stat = await file.stat();
    } catch (_) {
      stat = null;
    }
    if (stat == null || stat.type != FileSystemEntityType.file) {
      return _emptyFieldPreview();
    }
    List<int> rawBytes;
    try {
      rawBytes = await file.readAsBytes();
    } catch (_) {
      throw const FormatException('Unable to read file.');
    }
    final bytes = rawBytes.length > _localDirPreviewBytes
        ? rawBytes.sublist(0, _localDirPreviewBytes)
        : rawBytes;
    final lowerName = p.basename(path).toLowerCase();
    final ext = lowerName.contains('.')
        ? lowerName.substring(lowerName.lastIndexOf('.') + 1)
        : 'bin';
    final isBinary = _isLocalBinary(ext, rawBytes);
    if (isBinary) {
      final hex = _toHexSnippet(rawBytes, maxBytes: _localDirHexSnippetBytes);
      final fileSizeLabel = rawBytes.length.toString();
      return FieldPreview(
        previewText: '',
        hexSnippet:
            'Size: $fileSizeLabel bytes${_hexChunkSuffix(rawBytes)}\n$hex',
        guessedExt: ext,
        isBinary: true,
        size: rawBytes.length,
      );
    }

    final previewText = utf8.decode(bytes, allowMalformed: true);
    return FieldPreview(
      previewText: rawBytes.length > _localDirPreviewBytes
          ? '$previewText\n\n(first $_localDirPreviewBytes bytes)'
          : previewText,
      hexSnippet: '',
      guessedExt: ext,
      isBinary: false,
      size: rawBytes.length,
    );
  }

  Future<Uint8List> readDirectoryFileBytes(
    String path, {
    int? maxBytes,
  }) async {
    final remote = _activeRemoteDirectorySource;
    if (remote != null) {
      final host = _findRemoteHost(remote.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remote.hostId}');
      }
      return _remoteDatasets.readBytesFile(
        host: host,
        remotePath: _normalizeRemoteDirectoryPath(path),
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

  Future<List<ItemMeta>> listLocalDirectoryMdsItems(String shardPath) async {
    final remote = _activeRemoteDirectorySource;
    final lowerPath = shardPath.trim().toLowerCase();
    final isRemoteCompressed = remote != null &&
        (lowerPath.endsWith('.zst') || lowerPath.endsWith('.zstd'));
    final source = await _resolveLocalDirectoryMdsSource(
      shardPath,
      requireLocalShard: !isRemoteCompressed,
    );
    try {
      if (isRemoteCompressed) {
        final remoteSource = remote;
        final host = _findRemoteHost(remoteSource.hostId);
        if (host == null) {
          throw FormatException(
              'Remote host not found: ${remoteSource.hostId}');
        }
        final remoteShardPath = _normalizeRemoteDirectoryPath(shardPath);
        final items = await _listMdsItemsFromRemoteCompressedStreamWithRetry(
          host: host,
          remotePath: remoteShardPath,
          indexPath: source.indexPath,
          shardFilename: source.shardFilename,
          maxAttempts: 4,
        );
        return items;
      }
      return await _mosaicml.listSamples(
        indexPath: source.indexPath,
        shardFilename: source.shardFilename,
      );
    } catch (error) {
      if (!_looksLikeMdsCorruption(error.toString())) {
        rethrow;
      }
      final refreshed = await _resolveLocalDirectoryMdsSource(
        shardPath,
        forceRefreshRemoteShard: true,
        requireLocalShard: !isRemoteCompressed,
      );
      if (isRemoteCompressed) {
        final remoteSource = remote;
        final host = _findRemoteHost(remoteSource.hostId);
        if (host == null) {
          throw FormatException(
              'Remote host not found: ${remoteSource.hostId}');
        }
        final remoteShardPath = _normalizeRemoteDirectoryPath(shardPath);
        final items = await _listMdsItemsFromRemoteCompressedStreamWithRetry(
          host: host,
          remotePath: remoteShardPath,
          indexPath: refreshed.indexPath,
          shardFilename: refreshed.shardFilename,
          maxAttempts: 5,
        );
        return items;
      }
      return _mosaicml.listSamples(
        indexPath: refreshed.indexPath,
        shardFilename: refreshed.shardFilename,
      );
    }
  }

  Future<FieldPreview> peekLocalDirectoryMdsField({
    required String shardPath,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    final remote = _activeRemoteDirectorySource;
    final lowerPath = shardPath.trim().toLowerCase();
    final isRemoteCompressed = remote != null &&
        (lowerPath.endsWith('.zst') || lowerPath.endsWith('.zstd'));
    final source = await _resolveLocalDirectoryMdsSource(
      shardPath,
      requireLocalShard: !isRemoteCompressed,
    );
    Future<FieldPreview> peekRemoteCompressedField({
      required String indexPath,
      required String shardFilename,
    }) async {
      final remoteSource = remote;
      if (remoteSource == null) {
        throw const FormatException('Remote directory source is not active.');
      }
      final host = _findRemoteHost(remoteSource.hostId);
      if (host == null) {
        throw FormatException('Remote host not found: ${remoteSource.hostId}');
      }
      return _peekMdsFieldFromRemoteCompressedStreamWithRetry(
        host: host,
        remotePath: _normalizeRemoteDirectoryPath(shardPath),
        indexPath: indexPath,
        shardFilename: shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }

    try {
      if (isRemoteCompressed) {
        return await peekRemoteCompressedField(
          indexPath: source.indexPath,
          shardFilename: source.shardFilename,
        );
      }
      return await _mosaicml.peekField(
        indexPath: source.indexPath,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    } catch (error) {
      if (!_looksLikeMdsCorruption(error.toString())) {
        rethrow;
      }
      final refreshed = await _resolveLocalDirectoryMdsSource(
        shardPath,
        forceRefreshRemoteShard: true,
        requireLocalShard: !isRemoteCompressed,
      );
      if (isRemoteCompressed) {
        return await peekRemoteCompressedField(
          indexPath: refreshed.indexPath,
          shardFilename: refreshed.shardFilename,
        );
      }
      return _mosaicml.peekField(
        indexPath: refreshed.indexPath,
        shardFilename: refreshed.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }
  }

  bool isLocalDirectoryMdsShardPath(String path) {
    return _looksLikeMdsShardPath(path);
  }

  Future<List<String>> localDirectoryMdsFieldFormats(String shardPath) async {
    final source = await _resolveLocalDirectoryMdsSource(
      shardPath,
      requireLocalShard: false,
    );
    final summary = await _mosaicml.loadIndex(source.indexPath);
    final raw = summary.configRaw['columnEncodings'];
    if (raw is List) {
      return raw.map((value) => value.toString()).toList(growable: false);
    }
    return List<String>.from(summary.dataFormat, growable: false);
  }

  bool _looksLikeMdsCorruption(String message) {
    final lower = message.toLowerCase();
    return lower.contains('zstderror') ||
        lower.contains('failed to stream decode') ||
        lower.contains('decoding error') ||
        lower.contains('corruption') ||
        lower.contains('malformed shard');
  }

  bool _looksLikeRemoteStreamInstability(String message) {
    final lower = message.toLowerCase();
    return lower.contains('socketexception') ||
        lower.contains('broken pipe') ||
        lower.contains("can't read 4 from socket") ||
        lower.contains('streamsink is closed') ||
        lower.contains('connection reset') ||
        lower.contains('write failed');
  }

  bool _looksLikeMdsShardPath(String path) {
    final name = p.basename(path).trim().toLowerCase();
    return name.endsWith('.mds') ||
        name.endsWith('.mds.zst') ||
        name.endsWith('.mds.zstd') ||
        name == 'mds.zst' ||
        name == 'mds.zstd';
  }

  Future<({String indexPath, String shardFilename})>
      _resolveLocalDirectoryMdsSource(
    String shardPath, {
    bool forceRefreshRemoteShard = false,
    bool requireLocalShard = true,
  }) async {
    final trimmed = shardPath.trim();
    if (trimmed.isEmpty) {
      throw const FormatException('MDS shard path is empty.');
    }

    final remote = _activeRemoteDirectorySource;
    if (remote == null) {
      final normalizedPath = _normalizeDatasetDir(trimmed);
      final shardName = p.basename(normalizedPath).trim();
      if (shardName.isEmpty || !_looksLikeMdsShardPath(shardName)) {
        throw FormatException('Not an MDS shard file: $normalizedPath');
      }
      final parentDir = File(normalizedPath).parent.path;
      return (indexPath: parentDir, shardFilename: shardName);
    }

    final normalizedShardPath = _normalizeRemoteDirectoryPath(trimmed);
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
    final host = _findRemoteHost(remote.hostId);
    if (host == null) {
      throw FormatException('Remote host not found: ${remote.hostId}');
    }

    final indexCandidates = <String>[
      'index.json',
      'index.json.zstd',
      'index.json.zst',
    ];
    final cacheKey = base64Url
        .encode(utf8.encode('${remote.hostId}:$parentDir'))
        .replaceAll('=', '');
    final cacheRoot = Directory(
      p.join(
        Directory.systemTemp.path,
        'dataset_inspector',
        'remote_mds',
        cacheKey,
      ),
    );
    await cacheRoot.create(recursive: true);

    String? indexFileName;
    File? localIndexFile;
    if (!forceRefreshRemoteShard) {
      for (final candidate in indexCandidates) {
        final existing = File(p.join(cacheRoot.path, candidate));
        if (await existing.exists()) {
          indexFileName = candidate;
          localIndexFile = existing;
          break;
        }
      }
    }

    if (indexFileName == null || localIndexFile == null) {
      Uint8List? indexBytes;
      for (final candidate in indexCandidates) {
        final remoteIndexPath = _joinRemoteDirectoryPath(parentDir, candidate);
        try {
          indexBytes = await _remoteDatasets.readBytesFile(
            host: host,
            remotePath: remoteIndexPath,
            onStatus: (message) {
              statusMessage = message;
            },
          );
          indexFileName = candidate;
          break;
        } catch (_) {}
      }
      if (indexBytes == null || indexFileName == null) {
        final display = parentDir.isEmpty ? '/' : '/$parentDir';
        throw FormatException(
            'MDS index not found in remote directory: $display');
      }
      for (final candidate in indexCandidates) {
        if (candidate == indexFileName) continue;
        final existing = File(p.join(cacheRoot.path, candidate));
        if (await existing.exists()) {
          try {
            await existing.delete();
          } catch (_) {}
        }
      }
      localIndexFile = File(p.join(cacheRoot.path, indexFileName));
      await localIndexFile.writeAsBytes(indexBytes, flush: true);
    }

    if (requireLocalShard) {
      final localShardPath = File(p.join(cacheRoot.path, shardName));
      var shouldDownload =
          forceRefreshRemoteShard || !await localShardPath.exists();
      int? expectedBytes;
      if (shouldDownload || forceRefreshRemoteShard) {
        expectedBytes = await _lookupRemoteDirectoryFileSize(
          host: host,
          directoryPath: parentDir,
          filename: shardName,
        );
      }
      if (!shouldDownload && expectedBytes != null) {
        final currentSize = await localShardPath
            .stat()
            .then((s) => s.size)
            .onError((_, __) => -1);
        shouldDownload = currentSize != expectedBytes;
      }
      if (shouldDownload) {
        await _downloadRemoteMdsShardToPath(
          host: host,
          remotePath: normalizedShardPath,
          destination: localShardPath,
          expectedBytes: expectedBytes,
        );
      }
    }

    return (indexPath: cacheRoot.path, shardFilename: shardName);
  }

  Future<void> _downloadRemoteMdsShardToPath({
    required RemoteHostConfig host,
    required String remotePath,
    required File destination,
    required int? expectedBytes,
  }) async {
    final parent = destination.parent;
    if (!await parent.exists()) {
      await parent.create(recursive: true);
    }
    final tmpPath = File('${destination.path}.part');
    if (await tmpPath.exists()) {
      try {
        await tmpPath.delete();
      } catch (_) {}
    }
    if (await destination.exists()) {
      try {
        await destination.delete();
      } catch (_) {}
    }

    final sink = tmpPath.openWrite();
    var downloaded = 0;
    try {
      await for (final chunk in _remoteDatasets.openReadFile(
        host: host,
        remotePath: remotePath,
        onStatus: (message) {
          statusMessage = message;
        },
      )) {
        if (chunk.isEmpty) continue;
        downloaded += chunk.length;
        sink.add(chunk);
      }
      await sink.flush();
    } finally {
      try {
        await sink.close();
      } catch (_) {}
    }

    if (expectedBytes != null && downloaded != expectedBytes) {
      if (await tmpPath.exists()) {
        try {
          await tmpPath.delete();
        } catch (_) {}
      }
      throw FormatException(
        'Remote shard download incomplete: expected $expectedBytes bytes, got $downloaded bytes.',
      );
    }
    await tmpPath.rename(destination.path);
  }

  Future<int?> _lookupRemoteDirectoryFileSize({
    required RemoteHostConfig host,
    required String directoryPath,
    required String filename,
  }) async {
    try {
      final siblings = await _remoteDatasets.listEntries(
        host: host,
        directoryPath: directoryPath,
        onStatus: (message) {
          statusMessage = message;
        },
      );
      for (final entry in siblings) {
        if (!entry.isDirectory && entry.name == filename) {
          return entry.sizeBytes;
        }
      }
    } catch (_) {}
    return null;
  }

  Future<List<ItemMeta>> _listMdsItemsFromRemoteCompressedStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required String indexPath,
    required String shardFilename,
    int maxAttempts = 3,
  }) async {
    final attempts = maxAttempts < 1 ? 1 : maxAttempts;
    Object? lastError;
    StackTrace? lastStack;
    for (var attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        return await _mosaicml.listSamplesFromZstdCompressedStream(
          indexPath: indexPath,
          shardFilename: shardFilename,
          compressedStream: _remoteDatasets.openReadFile(
            host: host,
            remotePath: remotePath,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      } catch (error, stackTrace) {
        final detail = error.toString();
        if (!_looksLikeMdsCorruption(detail) &&
            !_looksLikeRemoteStreamInstability(detail)) {
          rethrow;
        }
        lastError = error;
        lastStack = stackTrace;
        if (attempt < attempts) {
          await Future<void>.delayed(
            Duration(milliseconds: 120 * attempt),
          );
        }
      }
    }
    final error = lastError;
    if (error != null) {
      Error.throwWithStackTrace(error, lastStack ?? StackTrace.current);
    }
    throw const FormatException('Failed to load MDS items from remote stream.');
  }

  Future<FieldPreview> _peekMdsFieldFromRemoteCompressedStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    int maxAttempts = 2,
  }) async {
    final attempts = maxAttempts < 1 ? 1 : maxAttempts;
    Object? lastError;
    StackTrace? lastStack;
    for (var attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        return await _mosaicml.peekFieldFromZstdCompressedStream(
          indexPath: indexPath,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          compressedStream: _remoteDatasets.openReadFile(
            host: host,
            remotePath: remotePath,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      } catch (error, stackTrace) {
        final detail = error.toString();
        if (!_looksLikeMdsCorruption(detail) &&
            !_looksLikeRemoteStreamInstability(detail)) {
          rethrow;
        }
        lastError = error;
        lastStack = stackTrace;
        if (attempt < attempts) {
          await Future<void>.delayed(
            Duration(milliseconds: 120 * attempt),
          );
        }
      }
    }
    final error = lastError;
    if (error != null) {
      Error.throwWithStackTrace(error, lastStack ?? StackTrace.current);
    }
    throw const FormatException(
        'Failed to preview MDS field from remote stream.');
  }

  void registerLocalDirectoryItems(Iterable<LocalDirectoryItem> items) {
    for (final item in items) {
      _localDirectoryItemCache[item.path] = item;
    }
  }

  Future<List<LocalDirectoryItem>> listLocalDirectoryItems(
    String directoryPath,
  ) {
    final remote = _activeRemoteDirectorySource;
    if (remote != null) {
      final normalizedPath = _normalizeRemoteDirectoryPath(directoryPath);
      return _loadRemoteDirectoryItems(
        hostId: remote.hostId,
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
        if (idx == null) {
          localFilePreviewFuture = null;
        }
        notifyListeners();
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
      notifyListeners();
      return;
    }
    selectedFieldIndex = idx;
    if (idx == null || selectedItemIndex == null || selectedChunkName == null) {
      fieldPreviewFuture = null;
      mdsFieldPreviewFuture = null;
      notifyListeners();
      return;
    }
    if (mode == ViewerMode.litdataIndex || mode == ViewerMode.litdataChunks) {
      if (indexSummary == null) {
        fieldPreviewFuture = null;
      } else {
        fieldPreviewFuture = _captureFutureError(
          _litdata.peekField(
            indexPath: indexSummary!.indexPath,
            chunkFilename: selectedChunkName!,
            itemIndex: selectedItemIndex!,
            fieldIndex: idx,
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
          _mosaicml.peekField(
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
    notifyListeners();
  }

  void selectWdsShard(String? filename) {
    selectedShardName = filename;
    wdsSelectedSampleKey = null;
    wdsSelectedMemberPath = null;
    wdsSelectedMemberName = null;
    wdsOffset = 0;
    _loadWdsSamples();
    _syncActiveDatasetSelection();
    notifyListeners();
  }

  void selectWdsSample(String? key, {List<WdsFieldInfo>? fields}) {
    wdsSelectedSampleKey = key;
    if (key == null) {
      wdsSelectedMemberPath = null;
      wdsSelectedMemberName = null;
      wdsPreviewFuture = null;
      notifyListeners();
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
    notifyListeners();
  }

  void selectWdsMember(String? memberPath, {String? memberName}) {
    wdsSelectedMemberPath = memberPath;
    wdsSelectedMemberName = memberName ?? wdsSelectedMemberName;
    if (memberPath == null ||
        selectedShardName == null ||
        wdsDirSummary == null) {
      wdsPreviewFuture = null;
      notifyListeners();
      return;
    }
    wdsPreviewFuture = _captureFutureError(
      () async {
        if (_activeRemoteWebdatasetSource() != null) {
          return _webdataset.peekMemberFromStream(
            shardStream: _openRemoteWebdatasetShardStream(selectedShardName!),
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
    notifyListeners();
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
    notifyListeners();
  }

  void selectHfField(String? fieldName) {
    hfSelectedFieldName = fieldName;
    notifyListeners();
  }

  void selectZenodoFile(String? key) {
    zenodoSelectedFileKey = key;
    zenodoSelectedEntryName = null;
    zenodoEntriesOffset = 0;
    _loadZenodoEntries();
    _syncActiveDatasetSelection();
    notifyListeners();
  }

  void selectZenodoEntry(String? name) {
    zenodoSelectedEntryName = name;
    _loadZenodoEntryPreview();
    notifyListeners();
  }

  void setZenodoEntriesOffset(int offset) {
    zenodoEntriesOffset = offset < 0 ? 0 : offset;
    zenodoSelectedEntryName = null;
    _loadZenodoEntries();
    _syncActiveDatasetSelection();
    notifyListeners();
  }

  void setWdsOffset(int offset) {
    wdsOffset = offset < 0 ? 0 : offset;
    _loadWdsSamples();
    notifyListeners();
  }

  void setStatusMessage(String? message) {
    statusMessage = message;
    notifyListeners();
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
    notifyListeners();

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
      notifyListeners();
    }
  }

  Future<FieldPreview> _safeLitdataPreview(
    String indexPath,
    String chunkName,
    int itemIndex,
    int fieldIndex,
  ) async {
    try {
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
      return await _mosaicml.peekField(
        indexPath: indexPath,
        shardFilename: chunkName,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    } catch (_) {
      return _emptyFieldPreview();
    }
  }

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
      mdsItemsFuture = _captureFutureError(
        _mosaicml.listSamples(
          indexPath: indexSummary!.indexPath,
          shardFilename: selectedChunkName!,
        ),
        context: 'MosaicML samples failed',
        fallback: _emptyItemList,
      );
    }
  }

  void _loadWdsSamples() {
    if (selectedShardName == null || wdsDirSummary == null) {
      wdsSamplesFuture = null;
      return;
    }
    wdsSamplesFuture = _captureFutureError(
      () async {
        final value = _activeRemoteWebdatasetSource() != null
            ? await _webdataset.listSamplesFromStream(
                shardStream:
                    _openRemoteWebdatasetShardStream(selectedShardName!),
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
            final corrected = _activeRemoteWebdatasetSource() != null
                ? await _webdataset.listSamplesFromStream(
                    shardStream:
                        _openRemoteWebdatasetShardStream(selectedShardName!),
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
            notifyListeners();
            return corrected;
          }
        }
        wdsSamples = value;
        notifyListeners();
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
      notifyListeners();
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
        notifyListeners();

        if (value.totalFeatureCount > 0 &&
            value.featureCount < value.totalFeatureCount) {
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
    notifyListeners();
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
      final merged = _mergeHfFeatureChunk(current, chunk);
      if (merged == null) return;
      hfPreview = merged;
      notifyListeners();
      appendedChunks += 1;

      if (_isHfFeatureLoadComplete(merged) ||
          appendedChunks >= maxFeatureChunks) {
        if (prefetchNextColumn &&
            !_isHfFeatureLoadComplete(merged) &&
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

  HfDatasetPreview? _mergeHfFeatureChunk(
    HfDatasetPreview current,
    HfDatasetPreview chunk,
  ) {
    if (current.rows.isEmpty) {
      return chunk;
    }

    final mergedRows = <Map<String, dynamic>>[];
    final mergedFeatures = <HfFeature>[...current.features];
    final featureNames = <String>{
      for (final feature in current.features) feature.name,
    };
    var chunkRowsAdded = 0;

    final rows = current.rows.length >= chunk.rows.length
        ? current.rows.length
        : chunk.rows.length;

    for (var index = 0; index < rows; index += 1) {
      final currentRow = <String, dynamic>{};
      final mapIndex = index < current.rows.length ? current.rows[index] : null;
      if (mapIndex is Map<String, dynamic>) {
        currentRow.addAll(mapIndex);
      }
      if (index < chunk.rows.length) {
        final chunkRow = chunk.rows[index];
        if (chunkRow is Map<String, dynamic>) {
          currentRow.addAll(chunkRow);
          chunkRowsAdded += 1;
        }
      }
      mergedRows.add(currentRow);
    }

    if (chunkRowsAdded == 0 && chunk.featureCount > 0) return null;

    for (final feature in chunk.features) {
      if (feature.name.isEmpty) continue;
      if (featureNames.add(feature.name)) {
        mergedFeatures.add(feature);
      }
    }

    return HfDatasetPreview(
      dataset: current.dataset,
      config: current.config,
      split: current.split,
      configs: current.configs,
      offset: current.offset,
      length: current.length,
      numRowsTotal: current.numRowsTotal,
      partial: mergedFeatures.length < current.totalFeatureCount,
      features: mergedFeatures,
      rows: mergedRows,
      featureOffset: current.featureOffset,
      featureCount: mergedFeatures.length,
      totalFeatureCount: current.totalFeatureCount,
    );
  }

  bool _isHfFeatureLoadComplete(HfDatasetPreview preview) {
    final total = preview.totalFeatureCount;
    return total > 0 && preview.features.length >= total;
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
    final name = file.key.toLowerCase();
    if (name.endsWith('.zip')) {
      zenodoZipEntriesFuture = _captureFutureError(
        _zenodo
            .zipListEntries(
          contentUrl: file.contentUrl,
          filename: file.key,
        )
            .then((value) {
          zenodoZipEntries = value;
          notifyListeners();
          return value;
        }),
        context: 'Zenodo ZIP entries failed',
        fallback: _emptyZenodoZipEntries,
      );
      zenodoTarEntriesFuture = null;
    } else if (name.endsWith('.tar') ||
        name.endsWith('.tar.gz') ||
        name.endsWith('.tgz') ||
        name.endsWith('.tar.zst') ||
        name.endsWith('.tar.zstd')) {
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
          notifyListeners();
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
    if (file.key.toLowerCase().endsWith('.zip')) {
      zenodoEntryPreviewFuture = _captureFutureError(
        _zenodo.zipPeekEntry(
          contentUrl: file.contentUrl,
          filename: file.key,
          entryName: entryName,
        ),
        context: 'Zenodo ZIP preview failed',
        fallback: _emptyFieldPreview,
      );
      if (_isInlineMediaExt(entryName)) {
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
      if (_isInlineMediaExt(entryName)) {
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

  bool _isInlineMediaExt(String name) {
    final lower = name.toLowerCase();
    final ext = lower.contains('.') ? lower.split('.').last : '';
    return {
      'png',
      'jpg',
      'jpeg',
      'gif',
      'webp',
      'bmp',
      'svg',
      'wav',
      'mp3',
      'flac',
      'm4a',
      'ogg',
      'opus',
      'aac',
      'sph',
    }.contains(ext);
  }

  Future<OpenLeafResponse> litdataOpenField({String? openerAppPath}) async {
    if (indexSummary == null ||
        selectedChunkName == null ||
        selectedItemIndex == null ||
        selectedFieldIndex == null) {
      throw const FormatException('No field selected.');
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
    return _mosaicml.openLeaf(
      indexPath: indexSummary!.indexPath,
      shardFilename: selectedChunkName!,
      itemIndex: selectedItemIndex!,
      fieldIndex: selectedFieldIndex!,
      openerAppPath: openerAppPath,
    );
  }

  Future<PreparedMediaResponse> mosaicmlPrepareAudio({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) async {
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
    if (_activeRemoteWebdatasetSource() != null) {
      return _webdataset.openMemberFromStream(
        shardStream: _openRemoteWebdatasetShardStream(selectedShardName!),
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
    if (_activeRemoteWebdatasetSource() != null) {
      return _webdataset.prepareAudioPreviewFromStream(
        shardStream: _openRemoteWebdatasetShardStream(shardFilename),
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
    if (_activeRemoteWebdatasetSource() != null) {
      return _webdataset.prepareMemberFileFromStream(
        shardStream: _openRemoteWebdatasetShardStream(shardFilename),
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
    final targetPath = remote == null
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
    if (remote != null) {
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
    final lowerPath = item.path.trim().toLowerCase();
    final isRemoteCompressed = remote != null &&
        (lowerPath.endsWith('.zst') || lowerPath.endsWith('.zstd'));
    final source = await _resolveLocalDirectoryMdsSource(
      item.path,
      requireLocalShard: !isRemoteCompressed,
    );
    if (!isRemoteCompressed) {
      return _mosaicml.prepareAudioPreview(
        indexPath: source.indexPath,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }

    final host = _findRemoteHost(remote.hostId);
    if (host == null) {
      throw FormatException('Remote host not found: ${remote.hostId}');
    }
    return _prepareMdsAudioFromRemoteCompressedStreamWithRetry(
      host: host,
      remotePath: _normalizeRemoteDirectoryPath(item.path),
      indexPath: source.indexPath,
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
    final lowerPath = item.path.trim().toLowerCase();
    final isRemoteCompressed = remote != null &&
        (lowerPath.endsWith('.zst') || lowerPath.endsWith('.zstd'));
    final source = await _resolveLocalDirectoryMdsSource(
      item.path,
      requireLocalShard: !isRemoteCompressed,
    );
    if (!isRemoteCompressed) {
      return _mosaicml.prepareFieldFile(
        indexPath: source.indexPath,
        shardFilename: source.shardFilename,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }

    final host = _findRemoteHost(remote.hostId);
    if (host == null) {
      throw FormatException('Remote host not found: ${remote.hostId}');
    }
    return _prepareMdsFileFromRemoteCompressedStreamWithRetry(
      host: host,
      remotePath: _normalizeRemoteDirectoryPath(item.path),
      indexPath: source.indexPath,
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
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    int maxAttempts = 2,
  }) async {
    final attempts = maxAttempts < 1 ? 1 : maxAttempts;
    Object? lastError;
    StackTrace? lastStack;
    for (var attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        return await _mosaicml.prepareAudioPreviewFromZstdCompressedStream(
          indexPath: indexPath,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          compressedStream: _remoteDatasets.openReadFile(
            host: host,
            remotePath: remotePath,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      } catch (error, stackTrace) {
        final detail = error.toString();
        if (!_looksLikeMdsCorruption(detail) &&
            !_looksLikeRemoteStreamInstability(detail)) {
          rethrow;
        }
        lastError = error;
        lastStack = stackTrace;
        if (attempt < attempts) {
          await Future<void>.delayed(
            Duration(milliseconds: 120 * attempt),
          );
        }
      }
    }
    final error = lastError;
    if (error != null) {
      Error.throwWithStackTrace(error, lastStack ?? StackTrace.current);
    }
    throw const FormatException(
        'Failed to prepare MDS audio from remote stream.');
  }

  Future<PreparedFileResponse>
      _prepareMdsFileFromRemoteCompressedStreamWithRetry({
    required RemoteHostConfig host,
    required String remotePath,
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    int maxAttempts = 2,
  }) async {
    final attempts = maxAttempts < 1 ? 1 : maxAttempts;
    Object? lastError;
    StackTrace? lastStack;
    for (var attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        return await _mosaicml.prepareFieldFileFromZstdCompressedStream(
          indexPath: indexPath,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          compressedStream: _remoteDatasets.openReadFile(
            host: host,
            remotePath: remotePath,
            onStatus: (message) {
              statusMessage = message;
            },
          ),
        );
      } catch (error, stackTrace) {
        final detail = error.toString();
        if (!_looksLikeMdsCorruption(detail) &&
            !_looksLikeRemoteStreamInstability(detail)) {
          rethrow;
        }
        lastError = error;
        lastStack = stackTrace;
        if (attempt < attempts) {
          await Future<void>.delayed(
            Duration(milliseconds: 120 * attempt),
          );
        }
      }
    }
    final error = lastError;
    if (error != null) {
      Error.throwWithStackTrace(error, lastStack ?? StackTrace.current);
    }
    throw const FormatException(
        'Failed to prepare MDS file from remote stream.');
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

  Future<String> _stageRemoteFileForOpen({
    required String remotePath,
    required Uint8List bytes,
  }) async {
    final fileName = p.basename(remotePath).trim();
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

  Future<OpenLeafResponse> huggingfaceOpenField({
    required String input,
    required String config,
    required String split,
    required int rowIndex,
    required String fieldName,
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
