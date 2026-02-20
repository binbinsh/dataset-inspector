import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';

import '../models/common.dart';
import '../models/webdataset.dart';
import '../utils/audio.dart';
import '../utils/preview.dart';
import '../utils/tar_stream.dart';
import '../utils/zstd.dart';
import 'litdata_service.dart';
import 'mosaicml_service.dart';
import 'open_with_service.dart';

const _previewBytes = 16 * 1024;
const _maxListedSamples = 5000;
const _maxOpenBytes = 256 * 1024 * 1024;
const _wdsPreviewCacheMaxEntries = 2000;

enum _DatasetDetectConfidence {
  weak,
  strong,
}

class _DatasetDetectResult {
  const _DatasetDetectResult({
    required this.response,
    required this.confidence,
  });

  final LocalDatasetDetectResponse response;
  final _DatasetDetectConfidence confidence;
}

class WebdatasetService {
  WebdatasetService({
    OpenWithService? openWith,
    MosaicmlService? mosaicml,
    LitDataService? litdata,
  })  : _openWith = openWith ?? OpenWithService(),
        _mosaicml = mosaicml ?? MosaicmlService(),
        _litdata = litdata ?? LitDataService();

  final OpenWithService _openWith;
  final MosaicmlService _mosaicml;
  final LitDataService _litdata;
  final _WdsScanCache _cache = _WdsScanCache();

  Future<WdsDirSummary> loadDir(String dirPath) async {
    final (dir, shards) = _resolveShardDirAndList(dirPath);
    return WdsDirSummary(dirPath: dir.path, shards: shards);
  }

  Future<WdsSampleListResponse> listSamples({
    required String dirPath,
    required String shardFilename,
    int? offset,
    int? length,
    bool? computeTotal,
  }) async {
    final (dir, _) = _resolveShardDirAndList(dirPath);
    final trimmed = shardFilename.trim();
    if (trimmed.isEmpty) throw const FormatException('shard filename is empty');
    final shardPath = File('${dir.path}/$trimmed');
    if (!shardPath.existsSync()) {
      throw FormatException('shard does not exist: ${shardPath.path}');
    }
    if (!_looksLikeWdsShard(trimmed)) {
      throw const FormatException('file is not a supported WebDataset shard');
    }

    final pageOffset = offset ?? 0;
    final pageLength = (length ?? 200).clamp(1, _maxListedSamples).toInt();
    final wantTotal = computeTotal ?? false;

    final state = await _cache.getOrCreate(shardPath);
    final captureStart = pageOffset;
    final captureEnd = pageOffset + pageLength;
    await state.ensureScanned(
      pageOffset + pageLength,
      wantTotal,
      captureStart: captureStart,
      captureEnd: captureEnd,
    );

    final total = state.done ? state.currentSampleIndex : null;
    final start = pageOffset;
    final end =
        (pageOffset + pageLength).clamp(0, state.samples.length).toInt();
    final samples = start >= state.samples.length
        ? <WdsSampleInfo>[]
        : state.samples.sublist(start, end);

    return WdsSampleListResponse(
      offset: pageOffset,
      length: pageLength,
      numSamplesTotal: total,
      partial: !state.done,
      samples: samples,
    );
  }

  Future<WdsSampleListResponse> listSamplesFromStream({
    required Stream<List<int>> shardStream,
    required String shardFilename,
    int? offset,
    int? length,
    bool? computeTotal,
  }) async {
    final trimmed = shardFilename.trim();
    if (trimmed.isEmpty) throw const FormatException('shard filename is empty');
    if (!_looksLikeWdsShard(trimmed)) {
      throw const FormatException('file is not a supported WebDataset shard');
    }
    final pageOffset = offset ?? 0;
    final pageLength = (length ?? 200).clamp(1, _maxListedSamples).toInt();
    final wantTotal = computeTotal ?? false;
    final stream = _openShardStreamFromStream(shardStream, trimmed);
    return _listSamplesFromOpenShardStream(
      stream: stream,
      offset: pageOffset,
      length: pageLength,
      computeTotal: wantTotal,
    );
  }

  Future<FieldPreview> peekMember({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
  }) async {
    final shardPath = _resolveShardPath(dirPath, shardFilename);
    final normalized = normalizeTarPath(memberPath);
    if (normalized.isEmpty) throw const FormatException('member path is empty');

    final state = await _cache.getOrCreate(shardPath);
    final cached = state.cachedPreview(normalized);
    if (cached != null) return cached;

    final (data, size) =
        await _readMemberBytes(shardPath, normalized, _previewBytes);
    final previewText = previewUtf8Text(data);
    final guessedExt = _guessExtFromMember(normalized, data);
    return FieldPreview(
      previewText: previewText,
      hexSnippet: hexSnippet(data),
      guessedExt: guessedExt,
      isBinary: previewText == null,
      size: size,
    );
  }

  Future<FieldPreview> peekMemberFromStream({
    required Stream<List<int>> shardStream,
    required String shardFilename,
    required String memberPath,
  }) async {
    final normalized = normalizeTarPath(memberPath);
    if (normalized.isEmpty) throw const FormatException('member path is empty');
    final (data, size) = await _readMemberBytesFromStream(
      shardStream: shardStream,
      shardFilename: shardFilename,
      memberPath: normalized,
      limit: _previewBytes,
    );
    final previewText = previewUtf8Text(data);
    final guessedExt = _guessExtFromMember(normalized, data);
    return FieldPreview(
      previewText: previewText,
      hexSnippet: hexSnippet(data),
      guessedExt: guessedExt,
      isBinary: previewText == null,
      size: size,
    );
  }

  Future<OpenLeafResponse> openMember({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
    String? openerAppPath,
  }) async {
    final shardPath = _resolveShardPath(dirPath, shardFilename);
    final normalized = normalizeTarPath(memberPath);
    if (normalized.isEmpty) throw const FormatException('member path is empty');

    final (data, size) = await _readMemberBytes(shardPath, normalized, null);
    if (size > _maxOpenBytes) {
      throw FormatException('member too large to open ($size bytes)');
    }

    var ext = _guessExtFromMember(normalized, data) ?? 'bin';
    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector');
    await tempDir.create(recursive: true);
    final baseName = _sanitize('$shardFilename-$normalized');
    var out = File('${tempDir.path}/$baseName.$ext');
    await out.writeAsBytes(data, flush: true);

    if (ext == 'sph') {
      final wavOut = File('${tempDir.path}/$baseName.wav');
      try {
        await writeSphereAsWav(data, wavOut);
        out = wavOut;
        ext = 'wav';
      } on Exception catch (err) {
        final base = '${out.path} ($size bytes)';
        return OpenLeafResponse(
          path: out.path,
          size: size,
          ext: ext,
          opened: false,
          needsOpener: true,
          message: '$base · sph decode failed: $err · choose an app to open it',
        );
      }
    }

    final result = await _openWith.openFile(out.path, appPath: openerAppPath);
    final needsOpener = !result.opened;
    final message = result.opened
        ? 'Opened ${out.path} ($size bytes)'
        : 'Could not open ${out.path} · ${result.error ?? 'unknown error'}';

    return OpenLeafResponse(
      path: out.path,
      size: size,
      ext: ext,
      opened: result.opened,
      needsOpener: needsOpener,
      message: message,
    );
  }

  Future<OpenLeafResponse> openMemberFromStream({
    required Stream<List<int>> shardStream,
    required String shardFilename,
    required String memberPath,
    String? openerAppPath,
  }) async {
    final normalized = normalizeTarPath(memberPath);
    if (normalized.isEmpty) throw const FormatException('member path is empty');
    final (data, size) = await _readMemberBytesFromStream(
      shardStream: shardStream,
      shardFilename: shardFilename,
      memberPath: normalized,
      limit: null,
    );
    if (size > _maxOpenBytes) {
      throw FormatException('member too large to open ($size bytes)');
    }

    var ext = _guessExtFromMember(normalized, data) ?? 'bin';
    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector');
    await tempDir.create(recursive: true);
    final baseName = _sanitize('$shardFilename-$normalized');
    var out = File('${tempDir.path}/$baseName.$ext');
    await out.writeAsBytes(data, flush: true);

    if (ext == 'sph') {
      final wavOut = File('${tempDir.path}/$baseName.wav');
      try {
        await writeSphereAsWav(data, wavOut);
        out = wavOut;
        ext = 'wav';
      } on Exception catch (err) {
        final base = '${out.path} ($size bytes)';
        return OpenLeafResponse(
          path: out.path,
          size: size,
          ext: ext,
          opened: false,
          needsOpener: true,
          message: '$base · sph decode failed: $err · choose an app to open it',
        );
      }
    }

    final result = await _openWith.openFile(out.path, appPath: openerAppPath);
    final needsOpener = !result.opened;
    final message = result.opened
        ? 'Opened ${out.path} ($size bytes)'
        : 'Could not open ${out.path} · ${result.error ?? 'unknown error'}';

    return OpenLeafResponse(
      path: out.path,
      size: size,
      ext: ext,
      opened: result.opened,
      needsOpener: needsOpener,
      message: message,
    );
  }

  Future<PreparedMediaResponse> prepareAudioPreview({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
  }) async {
    return _prepareMemberBytes(
      dirPath: dirPath,
      shardFilename: shardFilename,
      memberPath: memberPath,
      convertSphereToWav: true,
    );
  }

  Future<PreparedMediaResponse> prepareAudioPreviewFromStream({
    required Stream<List<int>> shardStream,
    required String shardFilename,
    required String memberPath,
  }) async {
    var (data, _) = await _readMemberBytesFromStream(
      shardStream: shardStream,
      shardFilename: shardFilename,
      memberPath: memberPath,
      limit: null,
    );
    var ext = _guessExtFromMember(memberPath, data) ?? 'bin';
    if (ext == 'sph') {
      data = await decodeSphereToWavWithFallback(data);
      ext = 'wav';
    }
    return PreparedMediaResponse(bytes: data, size: data.length, ext: ext);
  }

  Future<PreparedFileResponse> prepareMemberFile({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
  }) async {
    return _prepareMemberFile(
      dirPath: dirPath,
      shardFilename: shardFilename,
      memberPath: memberPath,
      convertSphereToWav: false,
    );
  }

  Future<PreparedFileResponse> prepareMemberFileFromStream({
    required Stream<List<int>> shardStream,
    required String shardFilename,
    required String memberPath,
  }) async {
    final normalized = normalizeTarPath(memberPath);
    if (normalized.isEmpty) throw const FormatException('member path is empty');
    final (data, size) = await _readMemberBytesFromStream(
      shardStream: shardStream,
      shardFilename: shardFilename,
      memberPath: normalized,
      limit: null,
    );
    if (size > _maxOpenBytes) {
      throw FormatException('member too large to preview ($size bytes)');
    }

    var ext = _guessExtFromMember(normalized, data) ?? 'bin';
    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector');
    await tempDir.create(recursive: true);
    final baseName = _sanitize('$shardFilename-$normalized');
    var out = File('${tempDir.path}/$baseName.$ext');
    await out.writeAsBytes(data, flush: true);

    return PreparedFileResponse(path: out.path, size: size, ext: ext);
  }

  Future<PreparedFileResponse> _prepareMemberFile({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
    required bool convertSphereToWav,
  }) async {
    final shardPath = _resolveShardPath(dirPath, shardFilename);
    final normalized = normalizeTarPath(memberPath);
    if (normalized.isEmpty) throw const FormatException('member path is empty');

    final (data, size) = await _readMemberBytes(shardPath, normalized, null);
    if (size > _maxOpenBytes) {
      throw FormatException('member too large to preview ($size bytes)');
    }

    var ext = _guessExtFromMember(normalized, data) ?? 'bin';
    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector');
    await tempDir.create(recursive: true);
    final baseName = _sanitize('$shardFilename-$normalized');
    var out = File('${tempDir.path}/$baseName.$ext');
    await out.writeAsBytes(data, flush: true);

    if (convertSphereToWav && ext == 'sph') {
      final wavOut = File('${tempDir.path}/$baseName.wav');
      await writeSphereAsWav(data, wavOut);
      out = wavOut;
      ext = 'wav';
    }

    return PreparedFileResponse(path: out.path, size: size, ext: ext);
  }

  Future<PreparedMediaResponse> _prepareMemberBytes({
    required String dirPath,
    required String shardFilename,
    required String memberPath,
    required bool convertSphereToWav,
  }) async {
    final shardPath = _resolveShardPath(dirPath, shardFilename);
    final normalized = normalizeTarPath(memberPath);
    if (normalized.isEmpty) throw const FormatException('member path is empty');

    var (data, _) = await _readMemberBytes(shardPath, normalized, null);
    var ext = _guessExtFromMember(normalized, data) ?? 'bin';

    if (convertSphereToWav && ext == 'sph') {
      data = await decodeSphereToWavWithFallback(data);
      ext = 'wav';
    }

    return PreparedMediaResponse(bytes: data, size: data.length, ext: ext);
  }

  Future<LocalDatasetDetectResponse> detectLocalDataset(String path) async {
    final result = await _detectLocalDatasetDetailed(path);
    return result.response;
  }

  Future<_DatasetDetectResult> _detectLocalDatasetDetailed(String path) async {
    final trimmed = path.trim();
    if (trimmed.isEmpty) throw const FormatException('path is empty');
    final file = File(trimmed);

    if (await file.exists()) {
      final name = file.uri.pathSegments.last;
      if (_looksLikeWdsShard(name)) {
        final strong = await _looksLikeWebdatasetShard(file);
        return _DatasetDetectResult(
          response: LocalDatasetDetectResponse(
            kind: LocalDatasetKind.webdatasetDir,
            path: file.parent.path,
          ),
          confidence: strong
              ? _DatasetDetectConfidence.strong
              : _DatasetDetectConfidence.weak,
        );
      }
      if (_looksLikeMdsShard(name)) {
        final indexPath = await _detectMdsIndex(File(file.parent.path));
        if (indexPath != null) {
          return _DatasetDetectResult(
            response: LocalDatasetDetectResponse(
              kind: LocalDatasetKind.mdsIndex,
              path: File(indexPath).parent.path,
            ),
            confidence: _DatasetDetectConfidence.strong,
          );
        }
        throw FormatException(
            'no MosaicML MDS index.json found next to ${file.path}');
      }
      if (_looksLikeIndexFileName(name)) {
        final mdsIndex = await _detectMdsIndex(file);
        if (mdsIndex != null) {
          return _DatasetDetectResult(
            response: LocalDatasetDetectResponse(
              kind: LocalDatasetKind.mdsIndex,
              path: File(mdsIndex).parent.path,
            ),
            confidence: _DatasetDetectConfidence.strong,
          );
        }
        final litdataConfidence = await _detectLitdataIndexConfidence(file);
        if (litdataConfidence != null) {
          return _DatasetDetectResult(
            response: LocalDatasetDetectResponse(
              kind: LocalDatasetKind.litdataIndex,
              path: file.parent.path,
            ),
            confidence: litdataConfidence,
          );
        }
      }
    }

    final dir = Directory(trimmed);
    if (await dir.exists()) {
      final mdsFromDir = await _detectMdsIndex(File(dir.path));
      if (mdsFromDir != null) {
        return _DatasetDetectResult(
          response: LocalDatasetDetectResponse(
            kind: LocalDatasetKind.mdsIndex,
            path: File(mdsFromDir).parent.path,
          ),
          confidence: _DatasetDetectConfidence.strong,
        );
      }

      final indexFiles = await _listIndexFilesInDir(dir);
      _DatasetDetectConfidence? litdataConfidence;
      for (final indexFile in indexFiles) {
        final mdsFromIndex = await _detectMdsIndex(indexFile);
        if (mdsFromIndex != null) {
          return _DatasetDetectResult(
            response: LocalDatasetDetectResponse(
              kind: LocalDatasetKind.mdsIndex,
              path: File(mdsFromIndex).parent.path,
            ),
            confidence: _DatasetDetectConfidence.strong,
          );
        }

        final detectedLitdata = await _detectLitdataIndexConfidence(indexFile);
        if (detectedLitdata == null) continue;
        if (detectedLitdata == _DatasetDetectConfidence.strong) {
          return _DatasetDetectResult(
            response: LocalDatasetDetectResponse(
              kind: LocalDatasetKind.litdataIndex,
              path: dir.path,
            ),
            confidence: _DatasetDetectConfidence.strong,
          );
        }
        litdataConfidence ??= detectedLitdata;
      }
      if (litdataConfidence != null) {
        return _DatasetDetectResult(
          response: LocalDatasetDetectResponse(
            kind: LocalDatasetKind.litdataIndex,
            path: dir.path,
          ),
          confidence: litdataConfidence,
        );
      }

      final shards = await _listWdsShardsInDir(dir);
      if (shards.isNotEmpty) {
        var strong = false;
        final inspectCount = shards.length < 3 ? shards.length : 3;
        for (var i = 0; i < inspectCount; i += 1) {
          if (await _looksLikeWebdatasetShard(shards[i])) {
            strong = true;
            break;
          }
        }
        if (!strong && shards.length >= 2) {
          strong = _looksLikeWdsShardSequence(shards);
        }
        return _DatasetDetectResult(
          response: LocalDatasetDetectResponse(
            kind: LocalDatasetKind.webdatasetDir,
            path: dir.path,
          ),
          confidence: strong
              ? _DatasetDetectConfidence.strong
              : _DatasetDetectConfidence.weak,
        );
      }
      throw FormatException(
        'no LitData index.json, MDS index.json, or WebDataset shard found in ${dir.path}',
      );
    }

    throw FormatException('path does not exist: $trimmed');
  }

  Stream<LocalDatasetDetectResponse> discoverLocalDatasetsStream(
    String rootPath, {
    int maxDepth = 6,
  }) async* {
    final trimmed = rootPath.trim();
    if (trimmed.isEmpty) throw const FormatException('path is empty');

    final rootFile = File(trimmed);
    final rootDir = Directory(trimmed);
    if (await rootFile.exists() && !await rootDir.exists()) {
      final detected = await _detectLocalDatasetDetailed(trimmed);
      yield LocalDatasetDetectResponse(
        kind: detected.response.kind,
        path: _canonicalFsPath(detected.response.path),
      );
      return;
    }
    if (!await rootDir.exists()) {
      throw FormatException('path does not exist: $trimmed');
    }

    final queue = Queue<({Directory dir, int depth})>();
    queue.add((dir: rootDir, depth: 0));

    final visitedDirs = <String>{};
    final seenDatasets = <String>{};

    while (queue.isNotEmpty) {
      final current = queue.removeFirst();
      final dir = current.dir;
      final depth = current.depth;
      var detectedCurrentDirStrong = false;

      final dirPath = _canonicalFsPath(dir.path);
      if (!visitedDirs.add(dirPath)) continue;

      try {
        final detected = await _detectLocalDatasetDetailed(dir.path);
        detectedCurrentDirStrong =
            detected.confidence == _DatasetDetectConfidence.strong;
        final detectedPath = _canonicalFsPath(detected.response.path);
        final key = '${detected.response.kind.name}:$detectedPath';
        if (seenDatasets.add(key)) {
          final response = LocalDatasetDetectResponse(
              kind: detected.response.kind, path: detectedPath);
          yield response;
        }
      } catch (_) {}

      // Strongly detected dataset roots stop recursion; weak matches still recurse.
      if (detectedCurrentDirStrong) continue;
      if (depth >= maxDepth) continue;

      await for (final entry in dir.list(followLinks: false)) {
        if (entry is! Directory) continue;
        final name = entry.path.split(Platform.pathSeparator).last;
        if (_shouldSkipScanDirectory(name)) continue;
        queue.add((dir: entry, depth: depth + 1));
      }
    }

    // Keep deterministic traversal for stream consumers by exhausting queue.
    return;
  }

  Future<List<LocalDatasetDetectResponse>> discoverLocalDatasets(
    String rootPath, {
    int maxDepth = 6,
  }) async {
    final discovered = <LocalDatasetDetectResponse>[];
    await for (final detected
        in discoverLocalDatasetsStream(rootPath, maxDepth: maxDepth)) {
      discovered.add(detected);
    }
    discovered.sort((a, b) {
      final kindCompare = a.kind.index.compareTo(b.kind.index);
      if (kindCompare != 0) return kindCompare;
      return a.path.compareTo(b.path);
    });
    return discovered;
  }

  (Directory, List<WdsShardSummary>) _resolveShardDirAndList(String dirPath) {
    final file = File(dirPath);
    if (file.existsSync() &&
        file.statSync().type == FileSystemEntityType.file) {
      final filename = file.uri.pathSegments.last;
      if (!_looksLikeWdsShard(filename)) {
        throw const FormatException('file is not a supported WebDataset shard');
      }
      final bytes = file.statSync().size;
      return (
        file.parent,
        [
          WdsShardSummary(
            filename: filename,
            path: file.path,
            bytes: bytes,
            exists: true,
          ),
        ],
      );
    }

    final dir = Directory(dirPath);
    if (!dir.existsSync()) {
      throw FormatException('directory does not exist: ${dir.path}');
    }
    if (dir.statSync().type != FileSystemEntityType.directory) {
      throw const FormatException('path is not a directory');
    }

    final shards = dir
        .listSync()
        .whereType<File>()
        .where((file) => _looksLikeWdsShard(file.uri.pathSegments.last))
        .map((file) {
      final filename = file.uri.pathSegments.last;
      return WdsShardSummary(
        filename: filename,
        path: file.path,
        bytes: file.statSync().size,
        exists: file.existsSync(),
      );
    }).toList();
    shards.sort((a, b) => a.filename.compareTo(b.filename));
    return (dir, shards);
  }

  File _resolveShardPath(String dirPath, String shardFilename) {
    final (dir, _) = _resolveShardDirAndList(dirPath);
    final trimmed = shardFilename.trim();
    if (trimmed.isEmpty) throw const FormatException('shard filename is empty');
    if (!_looksLikeWdsShard(trimmed)) {
      throw const FormatException('file is not a supported WebDataset shard');
    }
    final shardPath = File('${dir.path}/$trimmed');
    if (!shardPath.existsSync()) {
      throw FormatException('shard does not exist: ${shardPath.path}');
    }
    return shardPath;
  }

  Stream<List<int>> _openShardStream(File shardPath) {
    final tarFile = _resolveTarFile(shardPath);
    if (tarFile != null) {
      return tarFile.openRead();
    }
    final filename = shardPath.uri.pathSegments.last.toLowerCase();
    if (filename.endsWith('.tar.gz') || filename.endsWith('.tgz')) {
      return gzip.decoder.bind(shardPath.openRead());
    }
    if (filename.endsWith('.tar.zst') || filename.endsWith('.tar.zstd')) {
      final decompressed = _decompressZstdToTemp(shardPath);
      return decompressed.openRead();
    }
    return shardPath.openRead();
  }

  Stream<List<int>> _openShardStreamFromStream(
    Stream<List<int>> source,
    String shardFilename,
  ) {
    final filename = shardFilename.toLowerCase();
    if (filename.endsWith('.tar')) {
      return source;
    }
    if (filename.endsWith('.tar.gz') || filename.endsWith('.tgz')) {
      return gzip.decoder.bind(source);
    }
    if (filename.endsWith('.tar.zst') || filename.endsWith('.tar.zstd')) {
      throw const FormatException(
        'Streaming .tar.zst/.tar.zstd shards is not supported yet.',
      );
    }
    return source;
  }

  File? _resolveTarFile(File shardPath) {
    final filename = shardPath.uri.pathSegments.last.toLowerCase();
    if (filename.endsWith('.tar')) {
      return shardPath;
    }
    if (filename.endsWith('.tar.zst') || filename.endsWith('.tar.zstd')) {
      return _decompressZstdToTemp(shardPath);
    }
    return null;
  }

  File _decompressZstdToTemp(File path) {
    final stat = path.statSync();
    final payload =
        '${path.path}:${stat.size}:${stat.modified.millisecondsSinceEpoch}';
    final key = sha1.convert(utf8.encode(payload)).toString();
    final outDir =
        Directory('${Directory.systemTemp.path}/dataset-inspector/wds-cache');
    outDir.createSync(recursive: true);
    final out = File('${outDir.path}/$key.tar');
    if (out.existsSync()) return out;
    final bytes = path.readAsBytesSync();
    final decoded = decodeZstd(bytes);
    out.writeAsBytesSync(decoded, flush: true);
    return out;
  }

  Future<(Uint8List, int)> _readMemberBytes(
    File shardPath,
    String memberPath,
    int? limit,
  ) async {
    final state = await _cache.getOrCreate(shardPath);
    final location = state.entryLocation(memberPath);
    final tarFile = state.tarFile;
    if (location != null && tarFile != null) {
      return _readMemberBytesAt(tarFile, location, limit);
    }
    final stream = _openShardStream(shardPath);
    final readerHandle = openTarStreamReader(stream);
    final tar = createTarStream(readerHandle);

    while (true) {
      final entry = await tar.nextWithBytes((meta) {
        if (meta.isDir) return 0;
        if (meta.path != memberPath) return 0;
        if (limit == null) return meta.size;
        return meta.size < limit ? meta.size : limit;
      });
      if (entry == null) {
        break;
      }
      if (entry.meta.isDir) continue;
      if (entry.meta.path != memberPath) {
        continue;
      }
      final bytes = entry.bytes ?? Uint8List(0);
      return (bytes, entry.meta.size);
    }

    throw FormatException('member not found in shard: $memberPath');
  }

  Future<(Uint8List, int)> _readMemberBytesFromStream({
    required Stream<List<int>> shardStream,
    required String shardFilename,
    required String memberPath,
    required int? limit,
  }) async {
    final normalized = normalizeTarPath(memberPath);
    if (normalized.isEmpty) throw const FormatException('member path is empty');
    final stream = _openShardStreamFromStream(shardStream, shardFilename);
    final readerHandle = openTarStreamReader(stream);
    final tar = createTarStream(readerHandle);

    while (true) {
      final entry = await tar.nextWithBytes((meta) {
        if (meta.isDir) return 0;
        if (meta.path != normalized) return 0;
        if (limit == null) return meta.size;
        return meta.size < limit ? meta.size : limit;
      });
      if (entry == null) break;
      if (entry.meta.isDir || entry.meta.path != normalized) continue;
      final bytes = entry.bytes ?? Uint8List(0);
      return (bytes, entry.meta.size);
    }
    throw FormatException('member not found in shard: $normalized');
  }

  Future<WdsSampleListResponse> _listSamplesFromOpenShardStream({
    required Stream<List<int>> stream,
    required int offset,
    required int length,
    required bool computeTotal,
  }) async {
    final tar = createTarStream(openTarStreamReader(stream));
    final pageOffset = offset < 0 ? 0 : offset;
    final pageLength = length.clamp(1, _maxListedSamples).toInt();
    final targetEnd = pageOffset + pageLength;
    final pageSamples = <WdsSampleInfo>[];

    String? currentKey;
    final currentFields = <WdsFieldInfo>[];
    var currentBytes = 0;
    var sampleIndex = 0;
    var done = false;
    var stoppedEarly = false;

    void flushCurrent() {
      final keyValue = currentKey;
      if (keyValue == null) {
        currentFields.clear();
        currentBytes = 0;
        return;
      }
      final fields = List<WdsFieldInfo>.from(currentFields);
      fields.sort((a, b) {
        final primary = a.name.compareTo(b.name);
        if (primary != 0) return primary;
        return a.memberPath.compareTo(b.memberPath);
      });
      if (sampleIndex >= pageOffset && pageSamples.length < pageLength) {
        pageSamples.add(
          WdsSampleInfo(
            sampleIndex: sampleIndex,
            key: keyValue,
            totalBytes: currentBytes,
            fields: fields,
          ),
        );
      }
      sampleIndex += 1;
      currentFields.clear();
      currentBytes = 0;
    }

    while (true) {
      final entry = await tar.nextWithBytes((_) => 0);
      if (entry == null) {
        done = true;
        break;
      }
      if (entry.meta.isDir) continue;
      final memberPath = entry.meta.path;
      final (key, fieldName) = _splitSampleKey(memberPath);
      if (currentKey != null && currentKey != key) {
        flushCurrent();
        if (!computeTotal &&
            sampleIndex >= targetEnd &&
            pageSamples.length >= pageLength) {
          stoppedEarly = true;
          currentKey = null;
          currentFields.clear();
          currentBytes = 0;
          break;
        }
      }
      if (currentKey != key) {
        currentKey = key;
      }
      currentBytes += entry.meta.size;
      currentFields.add(
        WdsFieldInfo(
          name: fieldName,
          memberPath: memberPath,
          size: entry.meta.size,
        ),
      );
    }

    if (!stoppedEarly) {
      flushCurrent();
    }

    final total = computeTotal && done ? sampleIndex : null;
    return WdsSampleListResponse(
      offset: pageOffset,
      length: pageLength,
      numSamplesTotal: total,
      partial: !done,
      samples: pageSamples,
    );
  }

  Future<(Uint8List, int)> _readMemberBytesAt(
    File tarFile,
    _TarEntryLocation location,
    int? limit,
  ) async {
    final size = location.size;
    final length = limit == null ? size : (limit < size ? limit : size);
    final raf = await tarFile.open();
    try {
      await raf.setPosition(location.dataOffset);
      final data = await raf.read(length);
      return (data, size);
    } finally {
      await raf.close();
    }
  }

  (String, String) _splitSampleKey(String memberPath) {
    final normalized = normalizeTarPath(memberPath);
    final parts = normalized.split('/');
    final base = parts.isNotEmpty ? parts.last : normalized;
    final dir =
        parts.length > 1 ? parts.sublist(0, parts.length - 1).join('/') : '';

    final dot = base.indexOf('.');
    String basePrefix;
    String suffix;
    if (dot > 0 && dot < base.length - 1) {
      basePrefix = base.substring(0, dot);
      suffix = base.substring(dot + 1);
    } else {
      basePrefix = base;
      suffix = '';
    }

    final key = dir.isEmpty ? basePrefix : '$dir/$basePrefix';
    final fieldName = suffix.isEmpty ? 'bin' : suffix.toLowerCase();
    return (key, fieldName);
  }

  bool _looksLikeWdsShard(String filename) {
    final name = filename.toLowerCase();
    return name.endsWith('.tar') ||
        name.endsWith('.tar.gz') ||
        name.endsWith('.tgz') ||
        name.endsWith('.tar.zst') ||
        name.endsWith('.tar.zstd');
  }

  bool _looksLikeMdsShard(String filename) {
    final name = filename.toLowerCase();
    return name.endsWith('.mds') ||
        name.endsWith('.mds.zst') ||
        name.endsWith('.mds.zstd');
  }

  bool _looksLikeIndexFileName(String filename) {
    final name = filename.toLowerCase();
    if (name == 'index.json' ||
        name == 'index.json.zstd' ||
        name == 'index.json.zst' ||
        name == '0.index.json' ||
        name == '0.index.json.zstd' ||
        name == '0.index.json.zst') {
      return true;
    }
    return name.endsWith('.index.json') || name.contains('.index.json.');
  }

  String _canonicalFsPath(String input) {
    return File(input).absolute.path;
  }

  bool _shouldSkipScanDirectory(String name) {
    final trimmed = name.trim();
    if (trimmed.isEmpty) return true;
    if (trimmed.startsWith('.')) return true;
    const excluded = {
      'build',
      '.dart_tool',
      'node_modules',
      '.venv',
      'venv',
      '__pycache__',
    };
    return excluded.contains(trimmed.toLowerCase());
  }

  Future<List<File>> _listIndexFilesInDir(Directory dir) async {
    final files = <File>[];
    final seen = <String>{};
    void appendIfNew(File file) {
      final key = _canonicalFsPath(file.path);
      if (seen.add(key)) {
        files.add(file);
      }
    }

    final candidates = [
      'index.json',
      'index.json.zstd',
      'index.json.zst',
      '0.index.json',
      '0.index.json.zstd',
      '0.index.json.zst',
    ];
    for (final name in candidates) {
      final file = File('${dir.path}/$name');
      if (await file.exists()) {
        appendIfNew(file);
      }
    }

    await for (final entry in dir.list(followLinks: false)) {
      if (entry is! File) continue;
      final name = entry.uri.pathSegments.last;
      if (_looksLikeIndexFileName(name)) {
        appendIfNew(entry);
      }
    }
    files.sort((a, b) => a.path.compareTo(b.path));
    return files;
  }

  Future<_DatasetDetectConfidence?> _detectLitdataIndexConfidence(
    File indexFile,
  ) async {
    try {
      final summary = await _litdata.loadIndex(indexFile.path);
      if (summary.chunks.isEmpty) {
        return _DatasetDetectConfidence.weak;
      }
      if (summary.chunks.any((chunk) => chunk.exists)) {
        return _DatasetDetectConfidence.strong;
      }
      if (summary.chunks
          .any((chunk) => _looksLikeLikelyLitdataChunk(chunk.filename))) {
        return _DatasetDetectConfidence.strong;
      }
      return _DatasetDetectConfidence.weak;
    } catch (_) {
      return null;
    }
  }

  bool _looksLikeLikelyLitdataChunk(String filename) {
    final name = filename.toLowerCase();
    if (name.contains('index.json')) return false;
    if (_looksLikeWdsShard(name) || _looksLikeMdsShard(name)) return false;
    return name.endsWith('.bin') ||
        name.contains('.bin.') ||
        name.endsWith('.zst') ||
        name.endsWith('.zstd');
  }

  Future<List<File>> _listWdsShardsInDir(Directory dir) async {
    final shards = <File>[];
    await for (final entry in dir.list(followLinks: false)) {
      if (entry is! File) continue;
      if (_looksLikeWdsShard(entry.uri.pathSegments.last)) {
        shards.add(entry);
      }
    }
    shards.sort((a, b) => a.path.compareTo(b.path));
    return shards;
  }

  bool _looksLikeWdsShardSequence(List<File> shards) {
    var matched = 0;
    for (final shard in shards) {
      final filename = shard.uri.pathSegments.last;
      final stem = _wdsShardStem(filename).toLowerCase();
      final isNumeric = RegExp(r'^\d{3,}$').hasMatch(stem);
      final isLabeled = RegExp(r'^(part|shard|chunk)[-_]?\d+$').hasMatch(stem);
      if (isNumeric || isLabeled) {
        matched += 1;
      }
      if (matched >= 2) return true;
    }
    return false;
  }

  String _wdsShardStem(String filename) {
    final lower = filename.toLowerCase();
    if (lower.endsWith('.tar.zstd')) {
      return filename.substring(0, filename.length - '.tar.zstd'.length);
    }
    if (lower.endsWith('.tar.zst')) {
      return filename.substring(0, filename.length - '.tar.zst'.length);
    }
    if (lower.endsWith('.tar.gz')) {
      return filename.substring(0, filename.length - '.tar.gz'.length);
    }
    if (lower.endsWith('.tgz')) {
      return filename.substring(0, filename.length - '.tgz'.length);
    }
    if (lower.endsWith('.tar')) {
      return filename.substring(0, filename.length - '.tar'.length);
    }
    return filename;
  }

  Future<bool> _looksLikeWebdatasetShard(File shardPath) async {
    try {
      final readerHandle = openTarStreamReader(_openShardStream(shardPath));
      final tar = createTarStream(readerHandle);
      final fieldsPerKey = <String, Set<String>>{};
      final distinctFields = <String>{};
      var nonDirectoryEntries = 0;

      while (nonDirectoryEntries < 40) {
        final entry = await tar.nextWithBytes((_) => 0);
        if (entry == null) break;
        if (entry.meta.isDir) continue;

        nonDirectoryEntries += 1;
        final (key, fieldName) = _splitSampleKey(entry.meta.path);
        if (key.isEmpty) continue;
        final fields = fieldsPerKey.putIfAbsent(key, () => <String>{});
        fields.add(fieldName);
        distinctFields.add(fieldName);
        if (fields.length >= 2) {
          return true;
        }

        if (nonDirectoryEntries >= 12 &&
            fieldsPerKey.length >= 8 &&
            distinctFields.length <= 3) {
          return true;
        }
      }
      return false;
    } catch (_) {
      return false;
    }
  }

  Future<String?> _detectMdsIndex(File path) async {
    try {
      final index = await _mosaicml.loadIndex(path.path);
      return index.indexPath;
    } catch (_) {
      return null;
    }
  }

  String _sanitize(String input) {
    final buffer = StringBuffer();
    for (final rune in input.runes) {
      final char = String.fromCharCode(rune);
      if (RegExp(r'[A-Za-z0-9]').hasMatch(char)) {
        buffer.write(char);
      } else {
        buffer.write('-');
      }
    }
    return buffer.toString();
  }
}

class _WdsScanCache {
  final Map<String, _ShardScanState> _states = {};

  Future<_ShardScanState> getOrCreate(File shardPath) async {
    final key = shardPath.path;
    final existing = _states[key];
    if (existing != null) return existing;
    final state = _ShardScanState(shardPath);
    _states[key] = state;
    return state;
  }
}

class _TarEntryLocation {
  const _TarEntryLocation({
    required this.dataOffset,
    required this.size,
  });

  final int dataOffset;
  final int size;
}

class _ShardScanState {
  _ShardScanState(File shardPath)
      : _tarFile = _resolveTarFileStatic(shardPath),
        _tar = createTarStream(
            openTarStreamReader(_openShardStreamStatic(shardPath)));

  final File? _tarFile;
  final TarStreamReader _tar;
  bool done = false;
  final List<WdsSampleInfo> samples = [];
  final Map<String, FieldPreview> _previewCache = {};
  final Queue<String> _previewLru = ListQueue();
  final Map<String, _TarEntryLocation> _entryIndex = {};
  String? _currentKey;
  final List<WdsFieldInfo> _currentFields = [];
  int _currentBytes = 0;
  int currentSampleIndex = 0;
  Future<void>? _scanFuture;

  File? get tarFile => _tarFile;

  Future<void> ensureScanned(
    int targetCount,
    bool computeTotal, {
    int? captureStart,
    int? captureEnd,
  }) async {
    while (true) {
      if (done) return;
      if (!computeTotal && samples.length >= targetCount) return;

      final pendingScan = _scanFuture;
      if (pendingScan != null) {
        await pendingScan;
        continue;
      }

      final scanRun = _runScan(
        targetCount,
        computeTotal,
        captureStart: captureStart,
        captureEnd: captureEnd,
      );
      _scanFuture = scanRun;

      try {
        await scanRun;
      } finally {
        if (identical(_scanFuture, scanRun)) {
          _scanFuture = null;
        }
      }
    }
  }

  Future<void> _runScan(
    int targetCount,
    bool computeTotal, {
    int? captureStart,
    int? captureEnd,
  }) async {
    final captureEnabled =
        captureStart != null && captureEnd != null && captureEnd > captureStart;
    final captureStartValue = captureStart ?? 0;
    final captureEndValue = captureEnd ?? 0;
    var stoppedEarly = false;

    while (!done) {
      final entry = captureEnabled
          ? await _tar.nextWithBytes((meta) {
              if (meta.isDir) return 0;
              if (currentSampleIndex + 1 < captureStartValue ||
                  currentSampleIndex >= captureEndValue) {
                return 0;
              }
              final (key, _) = _splitSampleKeyStatic(meta.path);
              final sampleIndex = _predictSampleIndexForKey(key);
              if (sampleIndex < captureStartValue ||
                  sampleIndex >= captureEndValue) {
                return 0;
              }
              return _previewBytes;
            })
          : await _tar.next();
      if (entry == null) {
        done = true;
        break;
      }

      if (entry.meta.isDir) continue;
      final memberPath = entry.meta.path;
      final (key, fieldName) = _splitSampleKeyStatic(memberPath);
      final size = entry.meta.size;
      _entryIndex.putIfAbsent(
        memberPath,
        () => _TarEntryLocation(dataOffset: entry.meta.dataOffset, size: size),
      );

      if (_currentKey != key) {
        _flushSample();
        _currentKey = key;
      }

      _currentBytes += size;
      _currentFields.add(WdsFieldInfo(
        name: fieldName,
        memberPath: memberPath,
        size: size,
      ));

      final bytes = entry.bytes;
      if (bytes != null) {
        final previewText = previewUtf8Text(bytes);
        final guessedExt = _guessExtFromMember(memberPath, bytes);
        _cachePreview(
          memberPath,
          FieldPreview(
            previewText: previewText,
            hexSnippet: hexSnippet(bytes),
            guessedExt: guessedExt,
            isBinary: previewText == null,
            size: size,
          ),
        );
      }

      if (!computeTotal && samples.length >= targetCount) {
        stoppedEarly = true;
        break;
      }
    }

    if (done && !stoppedEarly) {
      _flushSample();
    }
  }

  FieldPreview? cachedPreview(String name) {
    final cached = _previewCache[name];
    if (cached == null) return null;
    _previewLru.remove(name);
    _previewLru.add(name);
    return cached;
  }

  _TarEntryLocation? entryLocation(String memberPath) =>
      _entryIndex[memberPath];

  void _flushSample() {
    if (_currentKey == null) {
      _currentFields.clear();
      _currentBytes = 0;
      return;
    }
    final key = _currentKey!;
    final fields = List<WdsFieldInfo>.from(_currentFields);
    fields.sort((a, b) {
      final primary = a.name.compareTo(b.name);
      if (primary != 0) return primary;
      return a.memberPath.compareTo(b.memberPath);
    });
    samples.add(WdsSampleInfo(
      sampleIndex: currentSampleIndex,
      key: key,
      totalBytes: _currentBytes,
      fields: fields,
    ));
    _currentBytes = 0;
    _currentFields.clear();
    currentSampleIndex += 1;
  }

  int _predictSampleIndexForKey(String key) {
    if (_currentKey == null) return currentSampleIndex;
    if (_currentKey == key) return currentSampleIndex;
    return currentSampleIndex + 1;
  }

  void _cachePreview(String name, FieldPreview preview) {
    if (_previewCache.containsKey(name)) {
      _previewCache[name] = preview;
      _previewLru.remove(name);
      _previewLru.add(name);
      return;
    }
    _previewCache[name] = preview;
    _previewLru.add(name);
    while (_previewLru.length > _wdsPreviewCacheMaxEntries) {
      final oldest = _previewLru.removeFirst();
      _previewCache.remove(oldest);
    }
  }

  static Stream<List<int>> _openShardStreamStatic(File shardPath) {
    final tarFile = _resolveTarFileStatic(shardPath);
    if (tarFile != null) {
      return tarFile.openRead();
    }
    final filename = shardPath.uri.pathSegments.last.toLowerCase();
    if (filename.endsWith('.tar.gz') || filename.endsWith('.tgz')) {
      return gzip.decoder.bind(shardPath.openRead());
    }
    if (filename.endsWith('.tar.zst') || filename.endsWith('.tar.zstd')) {
      final decompressed = _decompressZstdToTempStatic(shardPath);
      return decompressed.openRead();
    }
    return shardPath.openRead();
  }

  static File? _resolveTarFileStatic(File shardPath) {
    final filename = shardPath.uri.pathSegments.last.toLowerCase();
    if (filename.endsWith('.tar')) {
      return shardPath;
    }
    if (filename.endsWith('.tar.zst') || filename.endsWith('.tar.zstd')) {
      return _decompressZstdToTempStatic(shardPath);
    }
    return null;
  }

  static File _decompressZstdToTempStatic(File path) {
    final stat = path.statSync();
    final payload =
        '${path.path}:${stat.size}:${stat.modified.millisecondsSinceEpoch}';
    final key = sha1.convert(utf8.encode(payload)).toString();
    final outDir =
        Directory('${Directory.systemTemp.path}/dataset-inspector/wds-cache');
    outDir.createSync(recursive: true);
    final out = File('${outDir.path}/$key.tar');
    if (out.existsSync()) return out;
    final bytes = path.readAsBytesSync();
    final decoded = decodeZstd(bytes);
    out.writeAsBytesSync(decoded, flush: true);
    return out;
  }

  static (String, String) _splitSampleKeyStatic(String memberPath) {
    final normalized = normalizeTarPath(memberPath);
    final parts = normalized.split('/');
    final base = parts.isNotEmpty ? parts.last : normalized;
    final dir =
        parts.length > 1 ? parts.sublist(0, parts.length - 1).join('/') : '';
    final dot = base.indexOf('.');
    String basePrefix;
    String suffix;
    if (dot > 0 && dot < base.length - 1) {
      basePrefix = base.substring(0, dot);
      suffix = base.substring(dot + 1);
    } else {
      basePrefix = base;
      suffix = '';
    }
    final key = dir.isEmpty ? basePrefix : '$dir/$basePrefix';
    final fieldName = suffix.isEmpty ? 'bin' : suffix.toLowerCase();
    return (key, fieldName);
  }
}

String? _guessExtFromMember(String memberPath, Uint8List data) {
  final ext = memberPath.split('.').last.trim().toLowerCase();
  if (ext.isNotEmpty && ext != memberPath) return ext;
  return _detectMagicExt(data);
}

String? _detectMagicExt(Uint8List data) {
  if (isSphereFile(data)) return 'sph';
  if (data.length >= 12 &&
      data[0] == 0x52 &&
      data[1] == 0x49 &&
      data[2] == 0x46 &&
      data[3] == 0x46 &&
      data[8] == 0x57 &&
      data[9] == 0x41 &&
      data[10] == 0x56 &&
      data[11] == 0x45) {
    return 'wav';
  }
  if (data.length >= 3 &&
      data[0] == 0x49 &&
      data[1] == 0x44 &&
      data[2] == 0x33) {
    return 'mp3';
  }
  if (data.length >= 2 && data[0] == 0xff && (data[1] & 0xe0) == 0xe0) {
    return 'mp3';
  }
  if (data.length >= 4 &&
      data[0] == 0x66 &&
      data[1] == 0x4c &&
      data[2] == 0x61 &&
      data[3] == 0x43) {
    return 'flac';
  }
  if (data.length >= 8 &&
      data[0] == 0x89 &&
      data[1] == 0x50 &&
      data[2] == 0x4e &&
      data[3] == 0x47) {
    return 'png';
  }
  if (data.length >= 3 &&
      data[0] == 0xff &&
      data[1] == 0xd8 &&
      data[2] == 0xff) {
    return 'jpg';
  }
  if (data.length >= 6 &&
      data[0] == 0x47 &&
      data[1] == 0x49 &&
      data[2] == 0x46 &&
      data[3] == 0x38) {
    return 'gif';
  }
  if (data.length >= 12 &&
      data[0] == 0x52 &&
      data[1] == 0x49 &&
      data[2] == 0x46 &&
      data[3] == 0x46 &&
      data[8] == 0x57 &&
      data[9] == 0x45 &&
      data[10] == 0x42 &&
      data[11] == 0x50) {
    return 'webp';
  }
  final text = previewUtf8Text(data);
  if (text != null && text.trim().isNotEmpty) return 'txt';
  return null;
}
