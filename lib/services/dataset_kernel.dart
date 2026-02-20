import '../models/common.dart';
import '../models/huggingface.dart';
import '../models/webdataset.dart';
import '../models/zenodo.dart';
import 'huggingface_service.dart';
import 'litdata_service.dart';
import 'mosaicml_service.dart';
import 'webdataset_service.dart';
import 'zenodo_service.dart';

enum DatasetKind {
  litdata,
  mds,
  webdataset,
  huggingface,
  zenodo,
}

enum DatasetCapability {
  workspace,
  detectSource,
  loadSource,
  listContainers,
  listRecords,
  previewField,
  openField,
  prepareAudio,
  prepareFile,
  listConfigs,
  listArchiveEntries,
  previewArchiveEntry,
  openArchiveEntry,
  prepareArchiveMedia,
  inlineArchiveMedia,
  previewFile,
  openFile,
  prepareFileMedia,
}

enum DatasetSourceKind {
  huggingface,
  zenodo,
  litdataIndex,
  litdataChunks,
  mdsIndex,
  webdatasetDir,
  unknown,
}

class DatasetSourceDetection {
  const DatasetSourceDetection({
    required this.kind,
    this.resolvedPath,
    this.chunkPaths = const <String>[],
  });

  final DatasetSourceKind kind;
  final String? resolvedPath;
  final List<String> chunkPaths;
}

class DatasetCapabilityMatrix {
  DatasetCapabilityMatrix._(Map<DatasetKind, Set<DatasetCapability>> values)
      : _values = values.map(
            (key, value) => MapEntry(key, Set<DatasetCapability>.of(value)));

  factory DatasetCapabilityMatrix.defaults() {
    return DatasetCapabilityMatrix._({
      DatasetKind.litdata: {
        DatasetCapability.workspace,
        DatasetCapability.detectSource,
        DatasetCapability.loadSource,
        DatasetCapability.listContainers,
        DatasetCapability.listRecords,
        DatasetCapability.previewField,
        DatasetCapability.openField,
        DatasetCapability.prepareAudio,
        DatasetCapability.prepareFile,
      },
      DatasetKind.mds: {
        DatasetCapability.workspace,
        DatasetCapability.detectSource,
        DatasetCapability.loadSource,
        DatasetCapability.listContainers,
        DatasetCapability.listRecords,
        DatasetCapability.previewField,
        DatasetCapability.openField,
        DatasetCapability.prepareAudio,
        DatasetCapability.prepareFile,
      },
      DatasetKind.webdataset: {
        DatasetCapability.workspace,
        DatasetCapability.detectSource,
        DatasetCapability.loadSource,
        DatasetCapability.listContainers,
        DatasetCapability.listRecords,
        DatasetCapability.previewField,
        DatasetCapability.openField,
        DatasetCapability.prepareAudio,
        DatasetCapability.prepareFile,
      },
      DatasetKind.huggingface: {
        DatasetCapability.workspace,
        DatasetCapability.detectSource,
        DatasetCapability.loadSource,
        DatasetCapability.listConfigs,
        DatasetCapability.listRecords,
        DatasetCapability.openField,
      },
      DatasetKind.zenodo: {
        DatasetCapability.workspace,
        DatasetCapability.detectSource,
        DatasetCapability.loadSource,
        DatasetCapability.listContainers,
        DatasetCapability.listRecords,
        DatasetCapability.previewFile,
        DatasetCapability.openFile,
        DatasetCapability.prepareFileMedia,
        DatasetCapability.listArchiveEntries,
        DatasetCapability.previewArchiveEntry,
        DatasetCapability.openArchiveEntry,
        DatasetCapability.prepareArchiveMedia,
        DatasetCapability.inlineArchiveMedia,
      },
    });
  }

  final Map<DatasetKind, Set<DatasetCapability>> _values;

  bool supports(DatasetKind kind, DatasetCapability capability) {
    return _values[kind]?.contains(capability) ?? false;
  }

  Set<DatasetCapability> enabledFor(DatasetKind kind) {
    return Set<DatasetCapability>.unmodifiable(
        _values[kind] ?? const <DatasetCapability>{});
  }

  Map<DatasetKind, Set<DatasetCapability>> snapshot() {
    return _values
        .map((key, value) => MapEntry(key, Set<DatasetCapability>.of(value)));
  }

  void enable(DatasetKind kind, DatasetCapability capability) {
    final set = _values.putIfAbsent(kind, () => <DatasetCapability>{});
    set.add(capability);
  }

  void disable(DatasetKind kind, DatasetCapability capability) {
    final set = _values.putIfAbsent(kind, () => <DatasetCapability>{});
    set.remove(capability);
  }

  void replace(DatasetKind kind, Iterable<DatasetCapability> capabilities) {
    _values[kind] = Set<DatasetCapability>.of(capabilities);
  }
}

class DatasetKernel {
  DatasetKernel({
    LitDataService? litdata,
    MosaicmlService? mosaicml,
    WebdatasetService? webdataset,
    HuggingfaceService? huggingface,
    ZenodoService? zenodo,
    DatasetCapabilityMatrix? capabilityMatrix,
  })  : _litdata = litdata ?? LitDataService(),
        _mosaicml = mosaicml ?? MosaicmlService(),
        _webdataset = webdataset ?? WebdatasetService(),
        _huggingface = huggingface ?? HuggingfaceService(),
        _zenodo = zenodo ?? ZenodoService(),
        capabilities = capabilityMatrix ?? DatasetCapabilityMatrix.defaults();

  final LitDataService _litdata;
  final MosaicmlService _mosaicml;
  final WebdatasetService _webdataset;
  final HuggingfaceService _huggingface;
  final ZenodoService _zenodo;
  final DatasetCapabilityMatrix capabilities;

  static bool looksLikeHuggingFaceInput(String value) {
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

  static bool looksLikeZenodoInput(String value) {
    final uri = Uri.tryParse(value);
    if (uri == null) return false;
    final host = uri.host.toLowerCase();
    if (!(host == 'zenodo.org' || host.endsWith('.zenodo.org'))) return false;
    final segments =
        uri.pathSegments.where((segment) => segment.isNotEmpty).toList();
    for (final segment in segments) {
      if (segment == 'records' || segment == 'record') {
        return true;
      }
    }
    return false;
  }

  Set<DatasetCapability> enabledCapabilitiesFor(DatasetKind kind) {
    return capabilities.enabledFor(kind);
  }

  bool supports(DatasetKind kind, DatasetCapability capability) {
    return capabilities.supports(kind, capability);
  }

  Future<void> warmupHuggingface() {
    return _huggingface.warmup();
  }

  Future<DatasetSourceDetection> detectSource(String input) async {
    final trimmed = input.trim();
    if (trimmed.isEmpty) {
      throw const FormatException('Input is empty.');
    }
    if (looksLikeHuggingFaceInput(trimmed)) {
      _requireCapability(
          DatasetKind.huggingface, DatasetCapability.detectSource);
      return const DatasetSourceDetection(kind: DatasetSourceKind.huggingface);
    }
    if (looksLikeZenodoInput(trimmed)) {
      _requireCapability(DatasetKind.zenodo, DatasetCapability.detectSource);
      return const DatasetSourceDetection(kind: DatasetSourceKind.zenodo);
    }
    try {
      final detected = await _webdataset.detectLocalDataset(trimmed);
      switch (detected.kind) {
        case LocalDatasetKind.litdataIndex:
          _requireCapability(
              DatasetKind.litdata, DatasetCapability.detectSource);
          return DatasetSourceDetection(
            kind: DatasetSourceKind.litdataIndex,
            resolvedPath: detected.path,
          );
        case LocalDatasetKind.mdsIndex:
          _requireCapability(DatasetKind.mds, DatasetCapability.detectSource);
          return DatasetSourceDetection(
            kind: DatasetSourceKind.mdsIndex,
            resolvedPath: detected.path,
          );
        case LocalDatasetKind.webdatasetDir:
          _requireCapability(
              DatasetKind.webdataset, DatasetCapability.detectSource);
          return DatasetSourceDetection(
            kind: DatasetSourceKind.webdatasetDir,
            resolvedPath: detected.path,
          );
      }
    } catch (error) {
      final message = error.toString().toLowerCase();
      if (message.contains('path does not exist') ||
          message.contains('missing directory')) {
        return const DatasetSourceDetection(kind: DatasetSourceKind.unknown);
      }
      final chunkPaths =
          await _litdata.listChunkFiles(trimmed).catchError((_) => <String>[]);
      if (chunkPaths.isNotEmpty) {
        _requireCapability(DatasetKind.litdata, DatasetCapability.detectSource);
        return DatasetSourceDetection(
          kind: DatasetSourceKind.litdataChunks,
          resolvedPath: trimmed,
          chunkPaths: chunkPaths,
        );
      }
      _requireCapability(DatasetKind.litdata, DatasetCapability.detectSource);
      return DatasetSourceDetection(
        kind: DatasetSourceKind.litdataIndex,
        resolvedPath: trimmed,
      );
    }
  }

  Future<IndexSummary> loadLitdataIndex(String indexPath) {
    _requireCapability(DatasetKind.litdata, DatasetCapability.loadSource);
    return _litdata.loadIndex(indexPath);
  }

  Future<IndexSummary> loadLitdataChunks(List<String> paths) {
    _requireCapability(DatasetKind.litdata, DatasetCapability.loadSource);
    return _litdata.loadChunkList(paths);
  }

  Future<List<String>> listLitdataChunkFiles(String dirPath) {
    _requireCapability(DatasetKind.litdata, DatasetCapability.listContainers);
    return _litdata.listChunkFiles(dirPath);
  }

  Future<List<ItemMeta>> listLitdataChunkItems(
      String indexPath, String chunkFilename) {
    _requireCapability(DatasetKind.litdata, DatasetCapability.listRecords);
    return _litdata.listChunkItems(indexPath, chunkFilename);
  }

  Future<ItemPage> listLitdataChunkItemsPaged({
    required String indexPath,
    required String chunkFilename,
    int? offset,
    int? length,
  }) {
    _requireCapability(DatasetKind.litdata, DatasetCapability.listRecords);
    return _litdata.listChunkItemsPaged(
      indexPath,
      chunkFilename,
      offset: offset ?? 0,
      length: length ?? 200,
    );
  }

  Future<FieldPreview> peekLitdataField({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
  }) {
    _requireCapability(DatasetKind.litdata, DatasetCapability.previewField);
    return _litdata.peekField(
      indexPath: indexPath,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<OpenLeafResponse> openLitdataField({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    String? openerAppPath,
  }) {
    _requireCapability(DatasetKind.litdata, DatasetCapability.openField);
    return _litdata.openLeaf(
      indexPath: indexPath,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      openerAppPath: openerAppPath,
    );
  }

  Future<PreparedMediaResponse> prepareLitdataAudio({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
  }) {
    _requireCapability(DatasetKind.litdata, DatasetCapability.prepareAudio);
    return _litdata.prepareAudioPreview(
      indexPath: indexPath,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<PreparedFileResponse> prepareLitdataFile({
    required String indexPath,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
  }) {
    _requireCapability(DatasetKind.litdata, DatasetCapability.prepareFile);
    return _litdata.prepareFieldFile(
      indexPath: indexPath,
      chunkFilename: chunkFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<IndexSummary> loadMdsIndex(String indexPath) {
    _requireCapability(DatasetKind.mds, DatasetCapability.loadSource);
    return _mosaicml.loadIndex(indexPath);
  }

  Future<List<ItemMeta>> listMdsSamples({
    required String indexPath,
    required String shardFilename,
  }) {
    _requireCapability(DatasetKind.mds, DatasetCapability.listRecords);
    return _mosaicml.listSamples(
        indexPath: indexPath, shardFilename: shardFilename);
  }

  Future<ItemPage> listMdsSamplesPaged({
    required String indexPath,
    required String shardFilename,
    int? offset,
    int? length,
  }) {
    _requireCapability(DatasetKind.mds, DatasetCapability.listRecords);
    return _mosaicml.listSamplesPaged(
      indexPath: indexPath,
      shardFilename: shardFilename,
      offset: offset ?? 0,
      length: length ?? 200,
    );
  }

  Future<FieldPreview> peekMdsField({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) {
    _requireCapability(DatasetKind.mds, DatasetCapability.previewField);
    return _mosaicml.peekField(
      indexPath: indexPath,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<OpenLeafResponse> openMdsField({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    String? openerAppPath,
  }) {
    _requireCapability(DatasetKind.mds, DatasetCapability.openField);
    return _mosaicml.openLeaf(
      indexPath: indexPath,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
      openerAppPath: openerAppPath,
    );
  }

  Future<PreparedMediaResponse> prepareMdsAudio({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) {
    _requireCapability(DatasetKind.mds, DatasetCapability.prepareAudio);
    return _mosaicml.prepareAudioPreview(
      indexPath: indexPath,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<PreparedFileResponse> prepareMdsFile({
    required String indexPath,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
  }) {
    _requireCapability(DatasetKind.mds, DatasetCapability.prepareFile);
    return _mosaicml.prepareFieldFile(
      indexPath: indexPath,
      shardFilename: shardFilename,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<WdsDirSummary> loadWebdatasetDir(String dirPath) {
    _requireCapability(DatasetKind.webdataset, DatasetCapability.loadSource);
    return _webdataset.loadDir(dirPath);
  }

  Future<WdsSampleListResponse> listWebdatasetSamples({
    required String dirPath,
    required String shardFilename,
    int? offset,
    int? length,
    bool? computeTotal,
  }) {
    _requireCapability(DatasetKind.webdataset, DatasetCapability.listRecords);
    return _webdataset.listSamples(
      dirPath: dirPath,
      shardFilename: shardFilename,
      offset: offset,
      length: length,
      computeTotal: computeTotal,
    );
  }

  Future<FieldPreview> peekWebdatasetMember({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
  }) {
    _requireCapability(DatasetKind.webdataset, DatasetCapability.previewField);
    return _webdataset.peekMember(
      dirPath: dirPath,
      shardFilename: shardFilename,
      memberPath: memberPath,
    );
  }

  Future<OpenLeafResponse> openWebdatasetMember({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
    String? openerAppPath,
  }) {
    _requireCapability(DatasetKind.webdataset, DatasetCapability.openField);
    return _webdataset.openMember(
      dirPath: dirPath,
      shardFilename: shardFilename,
      memberPath: memberPath,
      openerAppPath: openerAppPath,
    );
  }

  Future<PreparedMediaResponse> prepareWebdatasetAudio({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
  }) {
    _requireCapability(DatasetKind.webdataset, DatasetCapability.prepareAudio);
    return _webdataset.prepareAudioPreview(
      dirPath: dirPath,
      shardFilename: shardFilename,
      memberPath: memberPath,
    );
  }

  Future<PreparedFileResponse> prepareWebdatasetFile({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
  }) {
    _requireCapability(DatasetKind.webdataset, DatasetCapability.prepareFile);
    return _webdataset.prepareMemberFile(
      dirPath: dirPath,
      shardFilename: shardFilename,
      memberPath: memberPath,
    );
  }

  Future<HfDatasetPreview> previewHuggingfaceDataset({
    required String input,
    String? config,
    String? split,
    int? offset,
    int? length,
    String? token,
    bool useStreamingApi = false,
  }) {
    _requireCapability(DatasetKind.huggingface, DatasetCapability.loadSource);
    return _huggingface.datasetPreview(
      input: input,
      config: config,
      split: split,
      offset: offset,
      length: length,
      token: token,
      useStreamingApi: useStreamingApi,
    );
  }

  Future<List<String>> listHuggingfaceParquetFiles({
    required String input,
    String? config,
    String? split,
    String? token,
  }) {
    _requireCapability(DatasetKind.huggingface, DatasetCapability.loadSource);
    return _huggingface.listParquetFiles(
      input: input,
      config: config,
      split: split,
      token: token,
    );
  }

  Future<OpenLeafResponse> openHuggingfaceField({
    required String input,
    required String config,
    required String split,
    required int rowIndex,
    required String fieldName,
    String? openerAppPath,
    String? token,
  }) {
    _requireCapability(DatasetKind.huggingface, DatasetCapability.openField);
    return _huggingface.openField(
      input: input,
      config: config,
      split: split,
      rowIndex: rowIndex,
      fieldName: fieldName,
      openerAppPath: openerAppPath,
      token: token,
    );
  }

  Future<ZenodoRecordSummary> loadZenodoRecord(String input) {
    _requireCapability(DatasetKind.zenodo, DatasetCapability.loadSource);
    return _zenodo.recordSummary(input);
  }

  Future<FieldPreview> peekZenodoFile(String contentUrl) {
    _requireCapability(DatasetKind.zenodo, DatasetCapability.previewFile);
    return _zenodo.peekFile(contentUrl);
  }

  Future<OpenLeafResponse> openZenodoFile({
    required String contentUrl,
    required String filename,
    String? openerAppPath,
  }) {
    _requireCapability(DatasetKind.zenodo, DatasetCapability.openFile);
    return _zenodo.openFile(
      contentUrl: contentUrl,
      filename: filename,
      openerAppPath: openerAppPath,
    );
  }

  Future<PreparedMediaResponse> prepareZenodoFileMedia({
    required String contentUrl,
    required String filename,
  }) {
    _requireCapability(DatasetKind.zenodo, DatasetCapability.prepareFileMedia);
    return _zenodo.prepareFileMedia(contentUrl: contentUrl, filename: filename);
  }

  Future<List<ZenodoZipEntrySummary>> listZenodoZipEntries({
    required String contentUrl,
    required String filename,
  }) {
    _requireCapability(
        DatasetKind.zenodo, DatasetCapability.listArchiveEntries);
    return _zenodo.zipListEntries(contentUrl: contentUrl, filename: filename);
  }

  Future<FieldPreview> peekZenodoZipEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) {
    _requireCapability(
        DatasetKind.zenodo, DatasetCapability.previewArchiveEntry);
    return _zenodo.zipPeekEntry(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
    );
  }

  Future<OpenLeafResponse> openZenodoZipEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
    String? openerAppPath,
  }) {
    _requireCapability(DatasetKind.zenodo, DatasetCapability.openArchiveEntry);
    return _zenodo.zipOpenEntry(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
      openerAppPath: openerAppPath,
    );
  }

  Future<InlineMediaResponse> inlineZenodoZipEntryMedia({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) {
    _requireCapability(
        DatasetKind.zenodo, DatasetCapability.inlineArchiveMedia);
    return _zenodo.zipInlineEntryMedia(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
    );
  }

  Future<PreparedMediaResponse> prepareZenodoZipEntryMedia({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) {
    _requireCapability(
        DatasetKind.zenodo, DatasetCapability.prepareArchiveMedia);
    return _zenodo.zipPrepareEntryMedia(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
    );
  }

  Future<ZenodoTarEntryListResponse> listZenodoTarEntries({
    required String contentUrl,
    required String filename,
    int? offset,
    int? length,
  }) {
    _requireCapability(
        DatasetKind.zenodo, DatasetCapability.listArchiveEntries);
    return _zenodo.tarListEntriesPaged(
      contentUrl: contentUrl,
      filename: filename,
      offset: offset,
      length: length,
    );
  }

  Future<FieldPreview> peekZenodoTarEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) {
    _requireCapability(
        DatasetKind.zenodo, DatasetCapability.previewArchiveEntry);
    return _zenodo.tarPeekEntry(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
    );
  }

  Future<OpenLeafResponse> openZenodoTarEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
    String? openerAppPath,
  }) {
    _requireCapability(DatasetKind.zenodo, DatasetCapability.openArchiveEntry);
    return _zenodo.tarOpenEntry(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
      openerAppPath: openerAppPath,
    );
  }

  Future<InlineMediaResponse> inlineZenodoTarEntryMedia({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) {
    _requireCapability(
        DatasetKind.zenodo, DatasetCapability.inlineArchiveMedia);
    return _zenodo.tarInlineEntryMedia(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
    );
  }

  Future<PreparedMediaResponse> prepareZenodoTarEntryMedia({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) {
    _requireCapability(
        DatasetKind.zenodo, DatasetCapability.prepareArchiveMedia);
    return _zenodo.tarPrepareEntryMedia(
      contentUrl: contentUrl,
      filename: filename,
      entryName: entryName,
    );
  }

  void _requireCapability(DatasetKind kind, DatasetCapability capability) {
    if (capabilities.supports(kind, capability)) return;
    throw UnsupportedError(
      'Capability "${capability.name}" is disabled for dataset type "${kind.name}".',
    );
  }
}
