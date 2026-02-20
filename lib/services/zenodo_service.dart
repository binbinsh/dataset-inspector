import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:http/http.dart' as http;

import '../models/common.dart';
import '../models/zenodo.dart';
import '../utils/audio.dart';
import '../utils/preview.dart';
import '../utils/tar_stream.dart';
import '../utils/zstd.dart';
import 'open_with_service.dart';
import 'remote_zip_service.dart';

const _userAgent = 'dataset-inspector/2.3.1 (flutter)';
const _requestTimeout = Duration(seconds: 30);
const _peekBytes = 64 * 1024;
const _maxInlineDownloadBytes = 50 * 1024 * 1024;
const _tarMaxEntries = 250000;
const _tarInlineMediaMaxBytes = 128 * 1024 * 1024;
const _tarDefaultPageSize = 50;
const _tarMaxPageSize = 200;
const _tarMediaCacheItemMaxBytes = 32 * 1024 * 1024;
const _tarMediaCacheTotalMaxBytes = 256 * 1024 * 1024;

class ZenodoService {
  ZenodoService({OpenWithService? openWith, http.Client? client})
      : _openWith = openWith ?? OpenWithService(),
        _client = client ?? http.Client() {
    _remoteZip = RemoteZipService(
      client: _client,
      allowUrl: _allowedContentUrl,
      userAgent: _userAgent,
      previewMaxCompressedBytes: remoteZipPreviewMaxCompressedBytes,
      inlineMaxCompressedBytes: remoteZipInlineMaxBytes,
    );
  }

  final OpenWithService _openWith;
  final http.Client _client;
  late final RemoteZipService _remoteZip;
  final _TarScanCache _tarCache = _TarScanCache();

  Future<ZenodoRecordSummary> recordSummary(String input) async {
    final (baseUrl, recordId) = _extractRecordId(input);
    final apiUrl = _apiRecordUrl(baseUrl, recordId);
    final record = await _getJson(apiUrl);

    final metadata = record['metadata'] as Map<String, dynamic>? ?? {};
    final creatorsRaw = metadata['creators'] as List<dynamic>? ?? [];
    final creators = creatorsRaw.map((entry) {
      final map = entry as Map<String, dynamic>;
      return ZenodoCreator(
        name: map['name']?.toString() ?? '',
        affiliation: map['affiliation']?.toString(),
        orcid: map['orcid']?.toString(),
      );
    }).toList();

    final links = record['links'] as Map<String, dynamic>? ?? {};
    final recordUrl = links['self_doi_html']?.toString() ??
        links['preview_html']?.toString() ??
        links['self_html']?.toString();

    final filesRaw = record['files'] as List<dynamic>? ?? [];
    final files = <ZenodoFileSummary>[];
    for (final entry in filesRaw) {
      final map = entry as Map<String, dynamic>;
      final links = map['links'] as Map<String, dynamic>? ?? {};
      final content = links['self']?.toString();
      if (content == null || content.isEmpty) continue;
      final uri = Uri.tryParse(content);
      if (uri == null || !_allowedContentUrl(uri)) continue;
      files.add(ZenodoFileSummary(
        key: map['key']?.toString() ?? '',
        size: (map['size'] as num?)?.toInt() ?? 0,
        checksum: map['checksum']?.toString(),
        contentUrl: content,
      ));
    }

    return ZenodoRecordSummary(
      recordId: (record['id'] as num).toInt(),
      title: metadata['title']?.toString() ?? '',
      doi: record['doi']?.toString(),
      doiUrl: record['doi_url']?.toString(),
      publicationDate: metadata['publication_date']?.toString(),
      version: metadata['version']?.toString(),
      accessRight: metadata['access_right']?.toString(),
      recordUrl: recordUrl,
      creators: creators,
      files: files,
    );
  }

  Future<FieldPreview> peekFile(String contentUrl) async {
    final url = _parseContentUrl(contentUrl);
    final (data, totalSize) = await _rangeRequest(url, 0, _peekBytes - 1);
    final previewText = previewUtf8Text(data);
    final guessedExt = _extFromFilename(_fileNameFromUrl(url)) ?? _inferBasicExt(data);
    return FieldPreview(
      previewText: previewText,
      hexSnippet: hexSnippet(data),
      guessedExt: guessedExt,
      isBinary: previewText == null,
      size: (totalSize ?? 0).clamp(0, 0xFFFFFFFF).toInt(),
    );
  }

  Future<PreparedMediaResponse> prepareFileMedia({
    required String contentUrl,
    required String filename,
  }) async {
    final url = _parseContentUrl(contentUrl);
    final name = filename.trim();
    if (name.isEmpty) throw const FormatException('Missing filename.');
    final (_, totalSize) = await _rangeRequest(url, 0, 0);
    final size = totalSize ?? 0;
    if (size == 0 || size > _maxInlineDownloadBytes) {
      throw const FormatException('File too large for preview.');
    }
    final bytes = await _download(url);
    final ext =
        _extFromFilename(name) ?? _extFromFilename(_fileNameFromUrl(url)) ?? _inferBasicExt(bytes) ?? 'bin';
    return PreparedMediaResponse(bytes: bytes, size: bytes.length, ext: ext);
  }

  Future<OpenLeafResponse> openFile({
    required String contentUrl,
    required String filename,
    String? openerAppPath,
  }) async {
    final url = _parseContentUrl(contentUrl);
    final name = filename.trim();
    if (name.isEmpty) throw const FormatException('Missing filename.');

    final (_, totalSize) = await _rangeRequest(url, 0, 0);
    final size = totalSize ?? 0;
    final ext = _extFromFilename(name) ?? _extFromFilename(_fileNameFromUrl(url)) ?? 'bin';

    if (size == 0 || size > _maxInlineDownloadBytes) {
      final opened = await _openWith.openUrl(url.toString()).then((_) => true).catchError((_) => false);
      final message = opened
          ? 'Opened download URL (${size.clamp(0, 0xFFFFFFFF).toInt()} bytes) in your browser.'
          : 'Unable to open download URL.';
      return OpenLeafResponse(
        path: url.toString(),
        size: size.clamp(0, 0xFFFFFFFF).toInt(),
        ext: ext,
        opened: opened,
        needsOpener: false,
        message: message,
      );
    }

    final bytes = await _download(url);
    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector/zenodo');
    await tempDir.create(recursive: true);
    final recordId = _recordIdFromContentUrl(url) ?? 'unknown';
    final sanitized = _sanitize(name);
    final stem = sanitized.contains('.') ? sanitized.split('.').first : sanitized;
    final out = File('${tempDir.path}/${_sanitize(url.host)}-r$recordId-$stem.$ext');
    await out.writeAsBytes(bytes, flush: true);

    final result = await _openWith.openFile(out.path, appPath: openerAppPath);
    final base = '${out.path} (${bytes.length} bytes)';
    final needsOpener = !result.opened && result.error != null;
    var message = base;
    if (needsOpener) {
      message = '$base · no default app found, choose an app to open it';
    }
    return OpenLeafResponse(
      path: out.path,
      size: bytes.length,
      ext: ext,
      opened: result.opened,
      needsOpener: needsOpener,
      message: message,
    );
  }

  Future<List<ZenodoZipEntrySummary>> zipListEntries({
    required String contentUrl,
    required String filename,
  }) async {
    final entries = await _remoteZip.listEntries(
      contentUrl: contentUrl,
      filename: filename,
    );
    return entries
        .map((entry) => ZenodoZipEntrySummary(
              name: entry.name,
              method: entry.method,
              compressedSize: entry.compressedSize,
              uncompressedSize: entry.uncompressedSize,
              isDir: entry.isDir,
            ))
        .toList();
  }

  Future<FieldPreview> zipPeekEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) async {
    final name = filename.trim();
    if (name.isEmpty) throw const FormatException('Missing filename.');
    if (!_looksLikeZip(name)) throw const FormatException('Selected file is not a ZIP archive.');
    return _remoteZip.peekEntry(
      contentUrl: contentUrl,
      entryName: entryName,
      previewBytes: _peekBytes,
    );
  }

  Future<OpenLeafResponse> zipOpenEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
    String? openerAppPath,
  }) async {
    final name = filename.trim();
    if (name.isEmpty) throw const FormatException('Missing filename.');
    if (!_looksLikeZip(name)) throw const FormatException('Selected file is not a ZIP archive.');
    final url = _parseContentUrl(contentUrl);
    final bytes = await _remoteZip.readEntryBytes(
      contentUrl: contentUrl,
      entryName: entryName,
      limitBytes: remoteZipInlineMaxBytes,
    );
    final ext = _extFromFilename(entryName) ?? _inferBasicExt(bytes) ?? 'bin';
    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector/zenodo');
    await tempDir.create(recursive: true);
    final recordId = _recordIdFromContentUrl(url) ?? 'unknown';
    final baseName = _sanitize('${_fileNameFromUrl(url)}-$entryName');
    final out = File('${tempDir.path}/${_sanitize(url.host)}-r$recordId-$baseName.$ext');
    await out.writeAsBytes(bytes, flush: true);

    final result = await _openWith.openFile(out.path, appPath: openerAppPath);
    final base = '${out.path} (${bytes.length} bytes)';
    final needsOpener = !result.opened && result.error != null;
    var message = base;
    if (needsOpener) {
      message = '$base · no default app found, choose an app to open it';
    }
    return OpenLeafResponse(
      path: out.path,
      size: bytes.length,
      ext: ext,
      opened: result.opened,
      needsOpener: needsOpener,
      message: message,
    );
  }

  Future<InlineMediaResponse> zipInlineEntryMedia({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) async {
    final name = filename.trim();
    if (name.isEmpty) throw const FormatException('Missing filename.');
    if (!_looksLikeZip(name)) throw const FormatException('Selected file is not a ZIP archive.');
    final bytes = await _remoteZip.readEntryBytes(
      contentUrl: contentUrl,
      entryName: entryName,
      limitBytes: remoteZipInlineMaxBytes,
    );
    final ext = _extFromFilename(entryName) ?? _inferBasicExt(bytes) ?? 'bin';
    final mime = _mimeForExt(ext);
    final base64 = base64Encode(bytes);

    return InlineMediaResponse(
      base64: base64,
      mime: mime,
      size: bytes.length,
      ext: ext,
    );
  }

  Future<PreparedMediaResponse> zipPrepareEntryMedia({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) async {
    final name = filename.trim();
    if (name.isEmpty) throw const FormatException('Missing filename.');
    if (!_looksLikeZip(name)) throw const FormatException('Selected file is not a ZIP archive.');
    final bytes = await _remoteZip.readEntryBytes(
      contentUrl: contentUrl,
      entryName: entryName,
      limitBytes: remoteZipInlineMaxBytes,
    );
    final ext = _extFromFilename(entryName) ?? _inferBasicExt(bytes) ?? 'bin';
    return PreparedMediaResponse(bytes: bytes, size: bytes.length, ext: ext);
  }

  Future<ZenodoTarEntryListResponse> tarListEntriesPaged({
    required String contentUrl,
    required String filename,
    int? offset,
    int? length,
  }) async {
    final name = filename.trim();
    if (name.isEmpty) throw const FormatException('Missing filename.');
    if (!_looksLikeTar(name)) {
      throw const FormatException('Selected file is not a supported TAR archive.');
    }

    final pageOffset = (offset ?? 0).clamp(0, 0x7FFFFFFF).toInt();
    final pageLength = (length ?? _tarDefaultPageSize).clamp(1, _tarMaxPageSize).toInt();

    final state = await _tarCache.getOrCreate(_client, contentUrl, name);
    final captureStart = pageOffset;
    final captureEnd = pageOffset + pageLength;
    await state.ensureScannedForPage(captureEnd, captureStart, captureEnd);

    final total = state.done ? state.entries.length : null;
    final end = captureEnd.clamp(0, state.entries.length).toInt();
    final page = pageOffset >= state.entries.length
        ? <ZenodoTarEntrySummary>[]
        : state.entries.sublist(pageOffset, end);

    return ZenodoTarEntryListResponse(
      offset: pageOffset,
      length: pageLength,
      entries: page,
      partial: !state.done,
      numEntriesTotal: total,
    );
  }

  Future<FieldPreview> tarPeekEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) async {
    final name = filename.trim();
    if (!_looksLikeTar(name)) {
      throw const FormatException('Selected file is not a supported TAR archive.');
    }
    final state = await _tarCache.getOrCreate(_client, contentUrl, name);
    final cached = state.cachedPreview(entryName);
    if (cached != null) return cached;

    final url = _parseContentUrl(contentUrl);
    final entry = await _readTarEntry(url, name, entryName, _peekBytes);
    return entry.preview;
  }

  Future<OpenLeafResponse> tarOpenEntry({
    required String contentUrl,
    required String filename,
    required String entryName,
    String? openerAppPath,
  }) async {
    final name = filename.trim();
    if (!_looksLikeTar(name)) {
      throw const FormatException('Selected file is not a supported TAR archive.');
    }
    final url = _parseContentUrl(contentUrl);
    final entry = await _readTarEntry(url, name, entryName, _tarInlineMediaMaxBytes);

    final ext = _extFromFilename(entryName) ?? _inferBasicExt(entry.bytes) ?? 'bin';
    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector/zenodo');
    await tempDir.create(recursive: true);
    final recordId = _recordIdFromContentUrl(url) ?? 'unknown';
    final baseName = _sanitize(entryName);
    final out = File('${tempDir.path}/${_sanitize(url.host)}-r$recordId-$baseName.$ext');
    await out.writeAsBytes(entry.bytes, flush: true);

    final result = await _openWith.openFile(out.path, appPath: openerAppPath);
    final base = '${out.path} (${entry.bytes.length} bytes)';
    final needsOpener = !result.opened && result.error != null;
    var message = base;
    if (needsOpener) {
      message = '$base · no default app found, choose an app to open it';
    }
    return OpenLeafResponse(
      path: out.path,
      size: entry.bytes.length,
      ext: ext,
      opened: result.opened,
      needsOpener: needsOpener,
      message: message,
    );
  }

  Future<InlineMediaResponse> tarInlineEntryMedia({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) async {
    final name = filename.trim();
    if (!_looksLikeTar(name)) {
      throw const FormatException('Selected file is not a supported TAR archive.');
    }

    final state = await _tarCache.getOrCreate(_client, contentUrl, name);
    final cached = state.cachedMedia(entryName);
    if (cached != null) {
      return InlineMediaResponse(
        base64: cached.base64,
        mime: cached.mime,
        size: cached.size,
        ext: cached.ext,
      );
    }

    final url = _parseContentUrl(contentUrl);
    final entry = await _readTarEntry(url, name, entryName, _tarInlineMediaMaxBytes);
    final ext = _extFromFilename(entryName) ?? _inferBasicExt(entry.bytes) ?? 'bin';
    final mime = _mimeForExt(ext);
    final base64 = base64Encode(entry.bytes);

    return InlineMediaResponse(
      base64: base64,
      mime: mime,
      size: entry.bytes.length,
      ext: ext,
    );
  }

  Future<PreparedMediaResponse> tarPrepareEntryMedia({
    required String contentUrl,
    required String filename,
    required String entryName,
  }) async {
    final name = filename.trim();
    if (!_looksLikeTar(name)) {
      throw const FormatException('Selected file is not a supported TAR archive.');
    }
    final url = _parseContentUrl(contentUrl);
    final entry = await _readTarEntry(url, name, entryName, _tarInlineMediaMaxBytes);
    final ext = _extFromFilename(entryName) ?? _inferBasicExt(entry.bytes) ?? 'bin';
    return PreparedMediaResponse(bytes: entry.bytes, size: entry.bytes.length, ext: ext);
  }

  Future<_TarEntryRead> _readTarEntry(
    Uri url,
    String filename,
    String entryName,
    int maxBytes,
  ) async {
    final stream = await _openRemoteTarStream(url, filename);
    final tar = createTarStream(openTarStreamReader(stream));
    while (true) {
      final entry = await tar.nextWithBytes((meta) {
        if (meta.isDir) return 0;
        if (meta.path != entryName) return 0;
        return meta.size < maxBytes ? meta.size : maxBytes;
      });
      if (entry == null) {
        break;
      }
      if (entry.meta.isDir) continue;
      if (entry.meta.path != entryName) continue;
      if (entry.meta.size > maxBytes) {
        throw FormatException('Entry too large to read (${entry.meta.size} bytes).');
      }
      final bytes = entry.bytes ?? Uint8List(0);
      final previewText = previewUtf8Text(bytes);
      final guessedExt = _extFromFilename(entryName) ?? _inferBasicExt(bytes);
      final preview = FieldPreview(
        previewText: previewText,
        hexSnippet: hexSnippet(bytes),
        guessedExt: guessedExt,
        isBinary: previewText == null,
        size: entry.meta.size.clamp(0, 0xFFFFFFFF).toInt(),
      );
      return _TarEntryRead(bytes: bytes, preview: preview);
    }
    throw FormatException('Entry not found in TAR: $entryName');
  }

  Future<Stream<List<int>>> _openRemoteTarStream(Uri url, String filenameHint) async {
    final request = http.Request('GET', url);
    request.headers[HttpHeaders.userAgentHeader] = _userAgent;
    final response = await _client.send(request).timeout(_requestTimeout);
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw Exception('HTTP ${response.statusCode} from $url');
    }

    final name = filenameHint.toLowerCase();
    if (name.endsWith('.tar.gz') || name.endsWith('.tgz')) {
      return gzip.decoder.bind(response.stream);
    }
    if (name.endsWith('.tar.zst') || name.endsWith('.tar.zstd')) {
      final bytes = await response.stream.fold<BytesBuilder>(BytesBuilder(), (b, c) {
        b.add(c);
        return b;
      }).then((b) => b.toBytes());
      final decoded = decodeZstd(bytes);
      return Stream<List<int>>.fromIterable([decoded]);
    }
    return response.stream;
  }

  Uri _parseContentUrl(String contentUrl) {
    final trimmed = contentUrl.trim();
    if (trimmed.isEmpty) throw const FormatException('Missing content URL.');
    final url = Uri.tryParse(trimmed);
    if (url == null) throw const FormatException('Invalid Zenodo content URL.');
    if (!_allowedContentUrl(url)) {
      throw const FormatException('Blocked content URL.');
    }
    return url;
  }

  Future<Map<String, dynamic>> _getJson(Uri url) async {
    final response = await _client.get(url, headers: {HttpHeaders.userAgentHeader: _userAgent});
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw Exception('HTTP ${response.statusCode} from $url');
    }
    return jsonDecode(response.body) as Map<String, dynamic>;
  }

  (String, int) _extractRecordId(String input) {
    final trimmed = input.trim();
    final url = Uri.tryParse(trimmed);
    if (url == null) {
      throw const FormatException('Provide a Zenodo record URL like https://zenodo.org/records/<id>.');
    }
    if (!_validateZenodoUrl(url)) {
      throw const FormatException('Unsupported Zenodo URL. Expected https://zenodo.org/records/<id>.');
    }
    final recordId = _extractRecordIdFromUrl(url);
    if (recordId == null) {
      throw const FormatException('Unsupported Zenodo URL. Expected https://zenodo.org/records/<id>.');
    }
    return ('${url.scheme}://${url.host}', recordId);
  }

  Uri _apiRecordUrl(String baseUrl, int recordId) {
    return Uri.parse('$baseUrl/api/records/$recordId');
  }

  bool _validateZenodoUrl(Uri url) {
    if (url.scheme != 'https' && url.scheme != 'http') return false;
    final host = url.host.toLowerCase();
    return host == 'zenodo.org' || host.endsWith('.zenodo.org');
  }

  int? _extractRecordIdFromUrl(Uri url) {
    final segments = url.pathSegments.where((s) => s.isNotEmpty).toList();
    for (var i = 0; i < segments.length; i += 1) {
      if (segments[i] != 'records' && segments[i] != 'record') continue;
      if (i + 1 >= segments.length) return null;
      return int.tryParse(segments[i + 1]);
    }
    return null;
  }

  bool _allowedContentUrl(Uri url) {
    final host = url.host.toLowerCase();
    return host == 'zenodo.org' || host.endsWith('.zenodo.org');
  }

  String? _extFromFilename(String name) {
    final trimmed = name.trim();
    final base = trimmed.split('/').last;
    if (!base.contains('.')) return null;
    final ext = base.split('.').last.trim().toLowerCase();
    return ext.isEmpty ? null : ext;
  }

  bool _looksLikeZip(String filename) {
    return _extFromFilename(filename) == 'zip';
  }

  bool _looksLikeTar(String filename) {
    final lower = filename.trim().toLowerCase();
    return lower.endsWith('.tar') ||
        lower.endsWith('.tar.gz') ||
        lower.endsWith('.tgz') ||
        lower.endsWith('.tar.zst') ||
        lower.endsWith('.tar.zstd');
  }

  String? _recordIdFromContentUrl(Uri url) {
    final segments = url.pathSegments;
    final idx = segments.indexOf('records');
    if (idx >= 0 && idx + 1 < segments.length) {
      return segments[idx + 1];
    }
    return null;
  }

  String _fileNameFromUrl(Uri url) {
    final segments = url.pathSegments;
    final idx = segments.indexOf('files');
    if (idx >= 0 && idx + 1 < segments.length) {
      return segments[idx + 1];
    }
    return segments.isNotEmpty ? segments.last : 'file';
  }

  String _mimeForExt(String ext) {
    switch (ext.toLowerCase()) {
      case 'mp4':
        return 'video/mp4';
      case 'wav':
        return 'audio/wav';
      case 'mp3':
        return 'audio/mpeg';
      case 'flac':
        return 'audio/flac';
      case 'm4a':
        return 'audio/mp4';
      case 'ogg':
        return 'audio/ogg';
      case 'opus':
        return 'audio/opus';
      case 'aac':
        return 'audio/aac';
      case 'png':
        return 'image/png';
      case 'jpg':
      case 'jpeg':
        return 'image/jpeg';
      case 'gif':
        return 'image/gif';
      case 'webp':
        return 'image/webp';
      default:
        return 'application/octet-stream';
    }
  }

  String? _inferBasicExt(Uint8List data) {
    if (isSphereFile(data)) return 'sph';
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
    if (data.length >= 6 && data[0] == 0x47 && data[1] == 0x49 && data[2] == 0x46) {
      return 'gif';
    }
    return null;
  }

  String _sanitize(String input) {
    final buffer = StringBuffer();
    for (final rune in input.runes) {
      final c = String.fromCharCode(rune);
      if (RegExp(r'[A-Za-z0-9_.+-]').hasMatch(c)) {
        buffer.write(c);
      } else {
        buffer.write('_');
      }
    }
    return buffer.toString();
  }

  Future<(Uint8List, int?)> _rangeRequest(Uri url, int start, int endInclusive) async {
    final response = await _client.get(
      url,
      headers: {
        HttpHeaders.rangeHeader: 'bytes=$start-$endInclusive',
        HttpHeaders.userAgentHeader: _userAgent,
      },
    );
    if (response.statusCode != 200 && response.statusCode != 206) {
      throw Exception('HTTP ${response.statusCode} from $url');
    }
    final totalSize = _parseContentRangeTotal(response.headers['content-range']);
    return (response.bodyBytes, totalSize);
  }

  int? _parseContentRangeTotal(String? value) {
    if (value == null) return null;
    final parts = value.split('/');
    if (parts.length != 2) return null;
    if (parts[1] == '*') return null;
    return int.tryParse(parts[1]);
  }

  Future<Uint8List> _download(Uri url) async {
    final res = await _client.get(url, headers: {HttpHeaders.userAgentHeader: _userAgent});
    if (res.statusCode < 200 || res.statusCode >= 300) {
      throw Exception('download HTTP ${res.statusCode} from $url');
    }
    return res.bodyBytes;
  }
}

class _TarEntryRead {
  _TarEntryRead({required this.bytes, required this.preview});

  final Uint8List bytes;
  final FieldPreview preview;
}

class _TarScanCache {
  final Map<String, _TarScanState> _states = {};

  Future<_TarScanState> getOrCreate(http.Client client, String contentUrl, String filename) async {
    final key = contentUrl.trim();
    if (key.isEmpty) throw const FormatException('Missing content URL.');
    final name = filename.trim();
    if (name.isEmpty) throw const FormatException('Missing filename.');
    if (!_looksLikeTarStatic(name)) {
      throw const FormatException('Selected file is not a supported TAR archive.');
    }
    final existing = _states[key];
    if (existing != null) return existing;
    final url = Uri.tryParse(key);
    if (url == null) throw const FormatException('Invalid Zenodo content URL.');
    if (!(url.host == 'zenodo.org' || url.host.endsWith('.zenodo.org'))) {
      throw const FormatException('Blocked content URL.');
    }
    final state = _TarScanState(client: client, url: url, filename: name);
    _states[key] = state;
    return state;
  }

  static bool _looksLikeTarStatic(String filename) {
    final lower = filename.trim().toLowerCase();
    return lower.endsWith('.tar') ||
        lower.endsWith('.tar.gz') ||
        lower.endsWith('.tgz') ||
        lower.endsWith('.tar.zst') ||
        lower.endsWith('.tar.zstd');
  }
}

class _TarScanState {
  _TarScanState({required http.Client client, required this.url, required this.filename})
      : _tar = createTarStream(openTarStreamReader(_openRemoteTarStreamStatic(client, url, filename)));

  final Uri url;
  final String filename;
  final TarStreamReader _tar;
  bool done = false;
  final List<ZenodoTarEntrySummary> entries = [];
  final Map<String, FieldPreview> _previews = {};
  final Map<String, _CachedMedia> _mediaCache = {};
  final Queue<String> _mediaLru = ListQueue();
  int _mediaTotal = 0;

  Future<void> ensureScannedForPage(int target, int captureStart, int captureEnd) async {
    while (!done && entries.length < target) {
      final idx = entries.length;
      final capture = idx >= captureStart && idx < captureEnd;
      final entry = await _tar.nextWithBytes((meta) {
        if (!capture || meta.isDir) return 0;
        final ext = _extFromFilenameStatic(meta.path) ?? '';
        final isMedia = _isMediaExt(ext);
        if (isMedia && meta.size > 0 && meta.size <= _tarMediaCacheItemMaxBytes) {
          return meta.size;
        }
        return _peekBytes;
      });
      if (entry == null) {
        done = true;
        break;
      }

      final summary = ZenodoTarEntrySummary(
        name: entry.meta.path,
        size: entry.meta.size,
        isDir: entry.meta.isDir,
      );
      entries.add(summary);
      if (entries.length >= _tarMaxEntries) {
        throw const FormatException('TAR contains too many entries to list.');
      }

      if (entry.bytes != null && !entry.meta.isDir) {
        final bytes = entry.bytes!;
        final previewBytes = bytes.length > _peekBytes ? bytes.sublist(0, _peekBytes) : bytes;
        final previewText = previewUtf8Text(previewBytes);
        final guessedExt = _extFromFilenameStatic(entry.meta.path) ?? _inferBasicExtStatic(previewBytes);
        final preview = FieldPreview(
          previewText: previewText,
          hexSnippet: hexSnippet(previewBytes),
          guessedExt: guessedExt,
          isBinary: previewText == null,
          size: entry.meta.size.clamp(0, 0xFFFFFFFF).toInt(),
        );
        _previews[entry.meta.path] = preview;

        if (bytes.length == entry.meta.size && entry.meta.size <= _tarMediaCacheItemMaxBytes) {
          final ext = _extFromFilenameStatic(entry.meta.path) ?? 'bin';
          final mime = _mimeForExtStatic(ext);
          _cacheMedia(entry.meta.path, ext, mime, bytes);
        }
      }
    }
  }

  FieldPreview? cachedPreview(String name) => _previews[name];

  _CachedMedia? cachedMedia(String name) {
    final cached = _mediaCache[name];
    if (cached == null) return null;
    _mediaLru.remove(name);
    _mediaLru.add(name);
    return cached;
  }

  void _cacheMedia(String name, String ext, String mime, Uint8List bytes) {
    if (bytes.length > _tarMediaCacheItemMaxBytes) return;
    while (_mediaTotal + bytes.length > _tarMediaCacheTotalMaxBytes && _mediaLru.isNotEmpty) {
      final oldest = _mediaLru.removeFirst();
      final removed = _mediaCache.remove(oldest);
      if (removed != null) {
        _mediaTotal -= removed.size;
      }
    }
    _mediaCache[name] = _CachedMedia(
      ext: ext,
      mime: mime,
      base64: base64Encode(bytes),
      size: bytes.length,
    );
    _mediaLru.add(name);
    _mediaTotal += bytes.length;
  }

  static Stream<List<int>> _openRemoteTarStreamStatic(
    http.Client client,
    Uri url,
    String filenameHint,
  ) {
    final controller = StreamController<List<int>>();
    () async {
      try {
        final request = http.Request('GET', url);
        request.headers[HttpHeaders.userAgentHeader] = _userAgent;
        final response = await client.send(request).timeout(_requestTimeout);
        if (response.statusCode < 200 || response.statusCode >= 300) {
          controller.addError(Exception('HTTP ${response.statusCode} from $url'));
          await controller.close();
          return;
        }
        final name = filenameHint.toLowerCase();
        Stream<List<int>> stream = response.stream;
        if (name.endsWith('.tar.gz') || name.endsWith('.tgz')) {
          stream = gzip.decoder.bind(stream);
        } else if (name.endsWith('.tar.zst') || name.endsWith('.tar.zstd')) {
          final bytes = await stream.fold<BytesBuilder>(BytesBuilder(), (b, c) {
            b.add(c);
            return b;
          }).then((b) => b.toBytes());
          final decoded = decodeZstd(bytes);
          stream = Stream<List<int>>.fromIterable([decoded]);
        }
        await for (final chunk in stream) {
          controller.add(chunk);
        }
      } catch (err) {
        controller.addError(err);
      } finally {
        await controller.close();
      }
    }();
    return controller.stream;
  }

  static bool _isMediaExt(String ext) {
    switch (ext.toLowerCase()) {
      case 'mp4':
      case 'wav':
      case 'mp3':
      case 'flac':
      case 'm4a':
      case 'ogg':
      case 'opus':
      case 'aac':
      case 'sph':
        return true;
      default:
        return false;
    }
  }

  static String? _extFromFilenameStatic(String name) {
    final trimmed = name.trim();
    final base = trimmed.split('/').last;
    if (!base.contains('.')) return null;
    final ext = base.split('.').last.trim().toLowerCase();
    return ext.isEmpty ? null : ext;
  }

  static String? _inferBasicExtStatic(Uint8List data) {
    if (isSphereFile(data)) return 'sph';
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
    return null;
  }

  static String _mimeForExtStatic(String ext) {
    switch (ext.toLowerCase()) {
      case 'mp4':
        return 'video/mp4';
      case 'wav':
        return 'audio/wav';
      case 'mp3':
        return 'audio/mpeg';
      case 'flac':
        return 'audio/flac';
      case 'm4a':
        return 'audio/mp4';
      case 'ogg':
        return 'audio/ogg';
      case 'opus':
        return 'audio/opus';
      case 'aac':
        return 'audio/aac';
      default:
        return 'application/octet-stream';
    }
  }
}

class _CachedMedia {
  _CachedMedia({
    required this.ext,
    required this.mime,
    required this.base64,
    required this.size,
  });

  final String ext;
  final String mime;
  final String base64;
  final int size;
}
