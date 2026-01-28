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

  bool get isHuggingFaceDetected => detectedSource == DetectedSourceKind.huggingface;

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

  Future<File> downloadUpdate(UpdateInfo update, {void Function(int, int?)? onProgress}) async {
    return _updates.download(update, onProgress: onProgress);
  }

  Future<void> installUpdate(File file) async {
    await _updates.installUpdate(file);
  }

  Future<String?> chooseOpenerApp() async {
    try {
      final result = await FilePicker.pickFiles(type: FileType.any);
      if (result == null || result.paths.isEmpty) return null;
      return result.paths.first;
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
      if (message.contains('path does not exist') || message.contains('missing directory')) {
        _setDetectedSource(DetectedSourceKind.unknown);
        return;
      }
      final chunkPaths = await _litdata.listChunkFiles(input).catchError((_) => <String>[]);
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

  Future<String?> _resolvePickerInitialDirectory() async {
    final trimmed = sourceInput.trim();
    if (trimmed.isEmpty) return null;
    if (_looksLikeHfInput(trimmed) || _looksLikeZenodoInput(trimmed)) return null;
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
    final home = Platform.environment['HOME'] ?? Platform.environment['USERPROFILE'];
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
      final paths = result.paths.whereType<String>().toList();
      if (paths.isEmpty) return;
      chunkSelection = paths;
      notifyListeners();
    } on PlatformException catch (err) {
      statusMessage = err.message ?? 'Failed to pick files.';
      notifyListeners();
    }
  }

  void triggerLoad(ViewerMode nextMode, {String? payload, List<String>? paths}) {
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
        payload?.trim().isNotEmpty == true ? payload!.trim() : sourceInput.trim(),
      );
      if (indexPath.isEmpty) return;
      indexFuture = _captureFutureError(
        _litdata.loadIndex(indexPath).then((value) {
          indexSummary = value;
          _preferences.saveLastIndex(value.rootDir);
          selectedChunkName = value.chunks.isNotEmpty ? value.chunks.first.filename : null;
          _loadLitdataItems();
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
        payload?.trim().isNotEmpty == true ? payload!.trim() : sourceInput.trim(),
      );
      if (indexPath.isEmpty) return;
      indexFuture = _captureFutureError(
        _mosaicml.loadIndex(indexPath).then((value) {
          indexSummary = value;
          _preferences.saveLastIndex(value.rootDir);
          selectedChunkName = value.chunks.isNotEmpty ? value.chunks.first.filename : null;
          _loadMdsItems();
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
          selectedChunkName = value.chunks.isNotEmpty ? value.chunks.first.filename : null;
          _loadLitdataItems();
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
        payload?.trim().isNotEmpty == true ? payload!.trim() : sourceInput.trim(),
      );
      if (dirPath.isEmpty) return;
      wdsDirFuture = _captureFutureError(
        _webdataset.loadDir(dirPath).then((value) {
          wdsDirSummary = value;
          selectedShardName = value.shards.isNotEmpty ? value.shards.first.filename : null;
          _loadWdsSamples();
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
      final input = payload?.trim().isNotEmpty == true ? payload!.trim() : sourceInput.trim();
      if (input.isEmpty) return;
      zenodoRecordFuture = _captureFutureError(
        _zenodo.recordSummary(input).then((value) {
          zenodoRecord = value;
          zenodoSelectedFileKey = value.files.isNotEmpty ? value.files.first.key : null;
          _loadZenodoEntries();
          notifyListeners();
          return value;
        }),
        context: 'Zenodo load failed',
        fallback: _emptyZenodoRecord,
      );
      notifyListeners();
      return;
    }

    final input = payload?.trim().isNotEmpty == true ? payload!.trim() : sourceInput.trim();
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
          hfConfigOptions = [HfConfigSummary(config: value.config, splits: [value.split])];
        }
        hfConfigOverride = value.config;
        hfSplitOverride = value.split;
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
    await _recordRecentSource(input);
    if (_looksLikeHfInput(input)) {
      triggerLoad(ViewerMode.huggingface, payload: input);
      return;
    }
    if (_looksLikeZenodoInput(input)) {
      triggerLoad(ViewerMode.zenodo, payload: input);
      return;
    }
    try {
      final detected = await _webdataset.detectLocalDataset(input);
      switch (detected.kind) {
        case LocalDatasetKind.litdataIndex:
          triggerLoad(ViewerMode.litdataIndex, payload: detected.path);
        case LocalDatasetKind.mdsIndex:
          triggerLoad(ViewerMode.mdsIndex, payload: detected.path);
        case LocalDatasetKind.webdatasetDir:
          triggerLoad(ViewerMode.webdatasetDir, payload: detected.path);
      }
    } catch (_) {
      final chunkPaths = await _litdata.listChunkFiles(input).catchError((_) => <String>[]);
      if (chunkPaths.isNotEmpty) {
        triggerLoad(ViewerMode.litdataChunks, paths: chunkPaths);
        return;
      }
      triggerLoad(ViewerMode.litdataIndex, payload: input);
    }
  }

  Future<void> _recordRecentSource(String input) async {
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
    notifyListeners();
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
    if (memberPath == null || selectedShardName == null || wdsDirSummary == null) {
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

  List<ZenodoZipEntrySummary> _emptyZenodoZipEntries() => const <ZenodoZipEntrySummary>[];

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
          hfConfigOptions = [HfConfigSummary(config: value.config, splits: [value.split])];
        }
        hfConfigOverride = value.config;
        hfSplitOverride = value.split;
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
        _zenodo.zipListEntries(
          contentUrl: file.contentUrl,
          filename: file.key,
        ).then((value) {
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
        _zenodo.tarListEntriesPaged(
          contentUrl: file.contentUrl,
          filename: file.key,
          offset: zenodoEntriesOffset,
          length: _zenodoTarPageSize,
        ).then((value) {
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
    if (wdsDirSummary == null || selectedShardName == null || wdsSelectedMemberPath == null) {
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
      AppLogger.error('Hugging Face open field failed', tag: 'state', error: error, stackTrace: stack);
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
    if (value.startsWith('https://hf.co/datasets/') || value.startsWith('http://hf.co/datasets/')) {
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
