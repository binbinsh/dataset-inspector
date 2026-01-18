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
import 'mosaicml_service.dart';
import 'open_with_service.dart';

const _previewBytes = 16 * 1024;
const _maxListedSamples = 5000;
const _maxOpenBytes = 256 * 1024 * 1024;
const _wdsPreviewCacheMaxEntries = 2000;

class WebdatasetService {
  WebdatasetService({
    OpenWithService? openWith,
    MosaicmlService? mosaicml,
  })  : _openWith = openWith ?? OpenWithService(),
        _mosaicml = mosaicml ?? MosaicmlService();

  final OpenWithService _openWith;
  final MosaicmlService _mosaicml;
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
    final end = (pageOffset + pageLength).clamp(0, state.samples.length).toInt();
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

    final (data, size) = await _readMemberBytes(shardPath, normalized, _previewBytes);
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
    final baseName = _sanitize('${shardFilename}-${normalized}');
    var out = File('${tempDir.path}/$baseName.$ext');
    await out.writeAsBytes(data, flush: true);

    if (ext == 'sph') {
      final wavOut = File('${tempDir.path}/$baseName.wav');
      try {
        await writeSphereAsWav(data, wavOut);
        out = wavOut;
        ext = 'wav';
      } on Exception catch (err) {
        final base = '${out.path} (${size} bytes)';
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
    final baseName = _sanitize('${shardFilename}-${normalized}');
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
    final trimmed = path.trim();
    if (trimmed.isEmpty) throw const FormatException('path is empty');
    final file = File(trimmed);

    if (await file.exists()) {
      final name = file.uri.pathSegments.last;
      if (_looksLikeWdsShard(name)) {
        return LocalDatasetDetectResponse(kind: LocalDatasetKind.webdatasetDir, path: file.parent.path);
      }
      if (_looksLikeMdsShard(name)) {
        final indexPath = await _detectMdsIndex(File(file.parent.path));
        if (indexPath != null) {
          return LocalDatasetDetectResponse(
            kind: LocalDatasetKind.mdsIndex,
            path: File(indexPath).parent.path,
          );
        }
        throw FormatException('no MosaicML MDS index.json found next to ${file.path}');
      }
      if (name.toLowerCase().contains('index.json')) {
        final indexPath = await _detectMdsIndex(file);
        if (indexPath != null) {
          return LocalDatasetDetectResponse(
            kind: LocalDatasetKind.mdsIndex,
            path: File(indexPath).parent.path,
          );
        }
      }
      if (_looksLikeLitdataFile(name)) {
        return LocalDatasetDetectResponse(kind: LocalDatasetKind.litdataIndex, path: file.parent.path);
      }
    }

    final dir = Directory(trimmed);
    if (await dir.exists()) {
      final litdataIndex = await _findLitdataIndexInDir(dir);
      if (litdataIndex != null) {
        final mdsIndex = await _detectMdsIndex(File(litdataIndex.path));
        if (mdsIndex != null) {
          return LocalDatasetDetectResponse(
            kind: LocalDatasetKind.mdsIndex,
            path: File(mdsIndex).parent.path,
          );
        }
        return LocalDatasetDetectResponse(kind: LocalDatasetKind.litdataIndex, path: dir.path);
      }
      if (await _hasWdsShardsInDir(dir)) {
        return LocalDatasetDetectResponse(kind: LocalDatasetKind.webdatasetDir, path: dir.path);
      }
      throw FormatException(
        'no LitData index.json, MDS index.json, or WebDataset shard found in ${dir.path}',
      );
    }

    throw FormatException('path does not exist: $trimmed');
  }

  (Directory, List<WdsShardSummary>) _resolveShardDirAndList(String dirPath) {
    final file = File(dirPath);
    if (file.existsSync() && file.statSync().type == FileSystemEntityType.file) {
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
    final payload = '${path.path}:${stat.size}:${stat.modified.millisecondsSinceEpoch}';
    final key = sha1.convert(utf8.encode(payload)).toString();
    final outDir = Directory('${Directory.systemTemp.path}/dataset-inspector/wds-cache');
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
    final dir = parts.length > 1 ? parts.sublist(0, parts.length - 1).join('/') : '';

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
    return name.endsWith('.mds') || name.endsWith('.mds.zst') || name.endsWith('.mds.zstd');
  }

  bool _looksLikeLitdataFile(String filename) {
    final name = filename.toLowerCase();
    if (name.contains('index.json')) return true;
    if (name.endsWith('.bin') || name.contains('.bin.')) return true;
    if (name.endsWith('.zst') && !_looksLikeWdsShard(name)) return true;
    return false;
  }

  Future<bool> _hasWdsShardsInDir(Directory dir) async {
    await for (final entry in dir.list(followLinks: false)) {
      if (entry is! File) continue;
      if (_looksLikeWdsShard(entry.uri.pathSegments.last)) {
        return true;
      }
    }
    return false;
  }

  Future<File?> _findLitdataIndexInDir(Directory dir) async {
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
      if (await file.exists()) return file;
    }
    File? best;
    await for (final entry in dir.list(followLinks: false)) {
      if (entry is! File) continue;
      final path = entry.path;
      if (path.endsWith('.index.json') || path.contains('.index.json.')) {
        if (best == null || path.compareTo(best!.path) < 0) {
          best = entry;
        }
      }
    }
    return best;
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
  _ShardScanState(this._shardPath)
      : _tarFile = _resolveTarFileStatic(_shardPath),
        _tar = createTarStream(openTarStreamReader(_openShardStreamStatic(_shardPath)));

  final File _shardPath;
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

  File? get tarFile => _tarFile;

  Future<void> ensureScanned(
    int targetCount,
    bool computeTotal, {
    int? captureStart,
    int? captureEnd,
  }) async {
    if (done) return;
    if (!computeTotal && samples.length >= targetCount) return;

    final captureEnabled =
        captureStart != null && captureEnd != null && captureEnd > captureStart;
    var stoppedEarly = false;
    while (!done) {
      final entry = captureEnabled
          ? await _tar.nextWithBytes((meta) {
              if (meta.isDir) return 0;
              if (currentSampleIndex + 1 < captureStart! || currentSampleIndex >= captureEnd!) {
                return 0;
              }
              final (key, _) = _splitSampleKeyStatic(meta.path);
              final sampleIndex = _predictSampleIndexForKey(key);
              if (sampleIndex < captureStart || sampleIndex >= captureEnd) return 0;
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

  _TarEntryLocation? entryLocation(String memberPath) => _entryIndex[memberPath];

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
    final payload = '${path.path}:${stat.size}:${stat.modified.millisecondsSinceEpoch}';
    final key = sha1.convert(utf8.encode(payload)).toString();
    final outDir = Directory('${Directory.systemTemp.path}/dataset-inspector/wds-cache');
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
    final dir = parts.length > 1 ? parts.sublist(0, parts.length - 1).join('/') : '';
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
  if (data.length >= 3 && data[0] == 0x49 && data[1] == 0x44 && data[2] == 0x33) {
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
  if (data.length >= 3 && data[0] == 0xff && data[1] == 0xd8 && data[2] == 0xff) {
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
