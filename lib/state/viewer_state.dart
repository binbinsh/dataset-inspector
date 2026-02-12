import 'dart:async';
import 'dart:io';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:path/path.dart' as p;

import '../models/common.dart';
import '../models/huggingface.dart';
import '../models/webdataset.dart';
import '../models/zenodo.dart';
import '../services/app_logger.dart';
import '../services/huggingface_service.dart';
import '../services/litdata_service.dart';
import '../services/mosaicml_service.dart';
import '../services/preferences_service.dart';
import '../services/update_service.dart';
import '../services/webdataset_service.dart';
import '../services/zenodo_service.dart';

enum ViewerMode {
  litdataIndex,
  litdataChunks,
  mdsIndex,
  webdatasetDir,
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
  unknown,
}

const _wdsPageSize = 50;
const _hfPageSize = 50;
const _zenodoTarPageSize = 50;
const _recentSourceLimit = 10;

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

class ViewerState extends ChangeNotifier {
  ViewerState({
    LitDataService? litdata,
    MosaicmlService? mosaicml,
    WebdatasetService? webdataset,
    HuggingfaceService? huggingface,
    ZenodoService? zenodo,
    PreferencesService? preferences,
    UpdateService? updates,
  })  : _litdata = litdata ?? LitDataService(),
        _mosaicml = mosaicml ?? MosaicmlService(),
        _webdataset = webdataset ?? WebdatasetService(),
        _huggingface = huggingface ?? HuggingfaceService(),
        _zenodo = zenodo ?? ZenodoService(),
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
  final PreferencesService _preferences;
  final UpdateService _updates;

  String sourceInput = '';
  DetectedSourceKind? detectedSource;
  Timer? _detectTimer;
  int _detectRequestId = 0;
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

  String? hfToken;
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
    updateCheckFuture = _updates.checkForUpdate();
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
      final result = await FilePicker.pickFiles(type: FileType.any);
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

  void setSourceInput(String value) {
    sourceInput = value;
    _scheduleSourceDetection(value);
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
      final result = await FilePicker.getDirectoryPath(
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
      final result = await FilePicker.getDirectoryPath(
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
    if (_looksLikeHfInput(trimmed) || _looksLikeZenodoInput(trimmed))
      return null;
    final expanded = _expandHomePath(trimmed);
    if (!p.isAbsolute(expanded)) return null;
    final type = await FileSystemEntity.type(expanded, followLinks: true);
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
      final result = await FilePicker.pickFiles(allowMultiple: true);
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
      final dirPath = _normalizeDatasetDir(
        payload?.trim().isNotEmpty == true
            ? payload!.trim()
            : sourceInput.trim(),
      );
      if (dirPath.isEmpty) return;
      wdsDirFuture = _captureFutureError(
        _webdataset.loadDir(dirPath).then((value) {
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
    hfPreviewFuture = _captureFutureError(
      _huggingface
          .datasetPreview(
        input: input,
        config: hfConfigOverride,
        split: hfSplitOverride,
        offset: hfOffset,
        length: _hfPageSize,
        token: hfToken,
      )
          .then((value) {
        hfPreview = value;
        if (value.configs.isNotEmpty) {
          hfConfigOptions = value.configs;
        } else if (hfConfigOptions == null) {
          hfConfigOptions = [
            HfConfigSummary(config: value.config, splits: [value.split])
          ];
        }
        hfConfigOverride = value.config;
        hfSplitOverride = value.split;
        _syncActiveDatasetSelection();
        notifyListeners();
        return value;
      }),
      context: 'Hugging Face preview failed',
      fallback: _emptyHfPreview,
    );
    notifyListeners();
  }

  String _normalizeDatasetDir(String input) {
    if (input.isEmpty) return input;
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
      notifyListeners();
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
    try {
      final detected = await _webdataset.detectLocalDataset(input);
      return _resolvedLoadRequestFromDetected(detected);
    } catch (_) {
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

  Future<bool> _addResolvedSource(
    _ResolvedLoadRequest resolved, {
    required bool recordRecent,
    bool notify = true,
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
    await _awaitPrimaryLoad(resolved.mode);

    final dataset = _upsertOpenedDataset(resolved);
    if (dataset == null) {
      if (notify) {
        notifyListeners();
      }
      return false;
    }
    activeDatasetId = dataset.id;
    _syncActiveDatasetSelection();
    if (notify) {
      notifyListeners();
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
    final payload = resolved.payload?.trim();
    final source =
        payload != null && payload.isNotEmpty ? payload : resolved.sourceInput;
    return '${resolved.mode.name}:${_normalizedSourceIdentity(resolved.mode, source)}';
  }

  String _normalizedSourceIdentity(ViewerMode sourceMode, String source) {
    final trimmed = source.trim();
    if (trimmed.isEmpty) return '';
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
    final normalized = _normalizedSourceIdentity(resolved.mode, source);
    final base = p.basename(normalized);
    if (base.isNotEmpty) return base;
    return normalized;
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
      notifyListeners();
    }
  }

  void toggleDatasetExpanded(String datasetId) {
    final dataset = _datasetById(datasetId);
    if (dataset == null) return;
    dataset.expanded = !dataset.expanded;
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
    _restorePrimarySelectionForDataset(dataset);
    _syncActiveDatasetSelection();
    notifyListeners();
  }

  Future<void> removeDataset(String datasetId) async {
    final removingActive = activeDatasetId == datasetId;
    openedDatasets =
        openedDatasets.where((dataset) => dataset.id != datasetId).toList();
    if (!removingActive) {
      notifyListeners();
      return;
    }
    if (openedDatasets.isEmpty) {
      activeDatasetId = null;
      _clearLoadedViewState();
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

  void selectField(int? idx) {
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
      _webdataset.peekMember(
        dirPath: wdsDirSummary!.dirPath,
        shardFilename: selectedShardName!,
        memberPath: memberPath,
      ),
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
        final value = await _webdataset.listSamples(
          dirPath: wdsDirSummary!.dirPath,
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
            final corrected = await _webdataset.listSamples(
              dirPath: wdsDirSummary!.dirPath,
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
      )
          .then((value) {
        hfPreview = value;
        if (value.configs.isNotEmpty) {
          hfConfigOptions = value.configs;
        } else if (hfConfigOptions == null) {
          hfConfigOptions = [
            HfConfigSummary(config: value.config, splits: [value.split])
          ];
        }
        hfConfigOverride = value.config;
        hfSplitOverride = value.split;
        _syncActiveDatasetSelection();
        notifyListeners();
        return value;
      }),
      context: 'Hugging Face preview refresh failed',
      fallback: _emptyHfPreview,
    );
    notifyListeners();
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
    return _webdataset.openMember(
      dirPath: wdsDirSummary!.dirPath,
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
    return _webdataset.prepareAudioPreview(
      dirPath: dirPath,
      shardFilename: shardFilename,
      memberPath: memberPath,
    );
  }

  Future<PreparedFileResponse> webdatasetPrepareFile({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
  }) async {
    return _webdataset.prepareMemberFile(
      dirPath: dirPath,
      shardFilename: shardFilename,
      memberPath: memberPath,
    );
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
