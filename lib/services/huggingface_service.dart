import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:http/http.dart' as http;

import '../models/common.dart';
import '../models/huggingface.dart';
import '../utils/audio.dart';
import 'app_logger.dart';
import 'open_with_service.dart';

const _datasetsServerBase = 'https://datasets-server.huggingface.co/';
const _defaultRows = 50;
const _maxRows = 100;
const _maxInlineText = 10 * 1024 * 1024;
const _logBodyLimit = 1024;

class HuggingfaceService {
  HuggingfaceService({OpenWithService? openWith, http.Client? client})
      : _openWith = openWith ?? OpenWithService(),
        _client = client ?? http.Client();

  final OpenWithService _openWith;
  final http.Client _client;

  Future<HfDatasetPreview> datasetPreview({
    required String input,
    String? config,
    String? split,
    int? offset,
    int? length,
    String? token,
  }) async {
    var dataset = _extractRepoId(input);
    final pageOffset = offset ?? 0;
    final pageLength = (length ?? _defaultRows).clamp(1, _maxRows).toInt();
    final tokenValue = token?.trim();

    final userConfig = config?.trim().isNotEmpty == true ? config!.trim() : null;
    final userSplit = split?.trim().isNotEmpty == true ? split!.trim() : null;

    AppLogger.info(
      'preview input="$input" dataset="$dataset" config=${userConfig ?? "-"} split=${userSplit ?? "-"} '
      'offset=$pageOffset length=$pageLength token=${tokenValue != null}',
      tag: 'hf',
    );

    if (userConfig != null && userSplit != null) {
      try {
        final rowsResp = await _getRows(
          dataset,
          userConfig,
          userSplit,
          pageOffset,
          pageLength,
          tokenValue,
        );
        return HfDatasetPreview(
          dataset: dataset,
          config: userConfig,
          split: userSplit,
          configs: const [],
          offset: pageOffset,
          length: pageLength,
          numRowsTotal: rowsResp.numRowsTotal,
          partial: rowsResp.partial,
          features: rowsResp.features,
          rows: rowsResp.rows,
        );
      } on _HfException catch (err) {
        if (!err.isNotFound) rethrow;
        final canonical = await _resolveCanonicalDatasetId(dataset, tokenValue);
        if (canonical != null) {
          dataset = canonical;
        }
        final splitsResult = await _getSplits(dataset, tokenValue);
        dataset = splitsResult.$1;
        final configsMap = splitsResult.$2;
        if (configsMap.isEmpty) {
          throw FormatException('No supported splits found for dataset $dataset.');
        }
        final selectedConfig =
            configsMap.containsKey(userConfig) ? userConfig : configsMap.keys.first;
        final splitsForConfig = configsMap[selectedConfig]!;
        final selectedSplit = splitsForConfig.contains(userSplit)
            ? userSplit
            : _pickDefaultSplit(splitsForConfig);

        final rowsResp = await _getRows(
          dataset,
          selectedConfig,
          selectedSplit,
          pageOffset,
          pageLength,
          tokenValue,
        );

        final configs = configsMap.entries
            .map((entry) => HfConfigSummary(config: entry.key, splits: entry.value.toList()))
            .toList();

        return HfDatasetPreview(
          dataset: dataset,
          config: selectedConfig,
          split: selectedSplit,
          configs: configs,
          offset: pageOffset,
          length: pageLength,
          numRowsTotal: rowsResp.numRowsTotal,
          partial: rowsResp.partial,
          features: rowsResp.features,
          rows: rowsResp.rows,
        );
      }
    }

    final splitsResult = await _getSplits(dataset, tokenValue);
    dataset = splitsResult.$1;
    var splitsResp = splitsResult.$2;
    if (splitsResp.isEmpty) {
      throw FormatException('No supported splits found for dataset $dataset.');
    }

    final configsMap = splitsResp;
    var selectedConfig = userConfig != null && configsMap.containsKey(userConfig)
        ? userConfig
        : configsMap.keys.first;
    var selectedSplit = _pickDefaultSplit(configsMap[selectedConfig]!);

    if (userSplit != null) {
      if (configsMap[selectedConfig]!.contains(userSplit)) {
        selectedSplit = userSplit;
      } else {
        for (final entry in configsMap.entries) {
          if (entry.value.contains(userSplit)) {
            selectedConfig = entry.key;
            selectedSplit = userSplit;
            break;
          }
        }
      }
    }

    final candidates = <(String, String)>[(selectedConfig, selectedSplit)];
    for (final splitName in configsMap[selectedConfig]!) {
      if (splitName != selectedSplit) {
        candidates.add((selectedConfig, splitName));
      }
    }
    for (final entry in configsMap.entries) {
      if (entry.key == selectedConfig) continue;
      candidates.add((entry.key, _pickDefaultSplit(entry.value)));
    }

    _RowsResponse? chosen;
    String? lastNotFound;
    for (final candidate in candidates) {
      try {
        chosen = await _getRows(dataset, candidate.$1, candidate.$2, pageOffset, pageLength, tokenValue);
        selectedConfig = candidate.$1;
        selectedSplit = candidate.$2;
        break;
      } on _HfException catch (err) {
        if (err.isNotFound) {
          lastNotFound = err.message;
          continue;
        }
        rethrow;
      }
    }

    if (chosen == null) {
      throw FormatException(lastNotFound ?? 'Not found.');
    }

    final configs = configsMap.entries
        .map((entry) => HfConfigSummary(config: entry.key, splits: entry.value.toList()))
        .toList();

    return HfDatasetPreview(
      dataset: dataset,
      config: selectedConfig,
      split: selectedSplit,
      configs: configs,
      offset: pageOffset,
      length: pageLength,
      numRowsTotal: chosen.numRowsTotal,
      partial: chosen.partial,
      features: chosen.features,
      rows: chosen.rows,
    );
  }

  Future<OpenLeafResponse> openField({
    required String input,
    required String config,
    required String split,
    required int rowIndex,
    required String fieldName,
    String? openerAppPath,
    String? token,
  }) async {
    final dataset = _extractRepoId(input);
    final configValue = config.trim();
    final splitValue = split.trim();
    final field = fieldName.trim();
    final tokenValue = token?.trim();

    if (configValue.isEmpty || splitValue.isEmpty || field.isEmpty) {
      throw const FormatException('Missing required field selection.');
    }

    AppLogger.info(
      'open field dataset="$dataset" config="$configValue" split="$splitValue" row=$rowIndex field="$field"',
      tag: 'hf',
    );

    final url = Uri.parse(_datasetsServerBase).replace(
      path: 'rows',
      queryParameters: {
        'dataset': dataset,
        'config': configValue,
        'split': splitValue,
        'offset': rowIndex.toString(),
        'length': '1',
      },
    );

    final rowsResp = await _getJson(url, tokenValue);
    final rows = rowsResp['rows'] as List<dynamic>? ?? [];
    if (rows.isEmpty) {
      throw const FormatException('No row returned for the requested offset.');
    }
    final row = rows.first['row'] as Map<String, dynamic>?;
    if (row == null) {
      throw const FormatException('Row is not a JSON object.');
    }
    final value = row[field];
    if (value == null) {
      throw FormatException("Field '$field' not found in the requested row.");
    }

    final asset = _extractAsset(value);
    if (asset != null) {
      final bytes = await _downloadBytes(asset.url, tokenValue);
      final ext = _extFromUrl(asset.url) ??
          _extFromMime(asset.mime) ??
          _inferBasicExt(bytes) ??
          'bin';
      final size = bytes.length;
      final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector/huggingface');
      await tempDir.create(recursive: true);
      final baseName = _sanitize('$dataset-$configValue-$splitValue-r$rowIndex-$field');
      final out = File('${tempDir.path}/$baseName.$ext');
      await out.writeAsBytes(bytes, flush: true);

      final result = await _openWith.openFile(out.path, appPath: openerAppPath);
      final base = '${out.path} ($size bytes)';
      final needsOpener = !result.opened && result.error != null;
      var message = base;
      if (needsOpener) {
        message = '$base · no default app found, choose an app to open it';
      }
      return OpenLeafResponse(
        path: out.path,
        size: size,
        ext: ext,
        opened: result.opened,
        needsOpener: needsOpener,
        message: message,
      );
    }

    final (bytes, ext) = _serializeValue(value);
    final size = bytes.length;
    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector/huggingface');
    await tempDir.create(recursive: true);
    final baseName = _sanitize('$dataset-$configValue-$splitValue-r$rowIndex-$field');
    final out = File('${tempDir.path}/$baseName.$ext');
    await out.writeAsBytes(bytes, flush: true);

    final result = await _openWith.openFile(out.path, appPath: openerAppPath);
    final base = '${out.path} ($size bytes)';
    final needsOpener = !result.opened && result.error != null;
    var message = base;
    if (needsOpener) {
      message = '$base · no default app found, choose an app to open it';
    }
    return OpenLeafResponse(
      path: out.path,
      size: size,
      ext: ext,
      opened: result.opened,
      needsOpener: needsOpener,
      message: message,
    );
  }

  Future<(String, Map<String, List<String>>)> _getSplits(String dataset, String? token) async {
    final url = Uri.parse(_datasetsServerBase).replace(
      path: 'splits',
      queryParameters: {'dataset': dataset},
    );

    Map<String, dynamic> body;
    try {
      body = await _getJson(url, token);
    } on _HfException catch (err) {
      if (err.isCanonicalCandidate) {
        final canonical = await _resolveCanonicalDatasetId(dataset, token);
        if (canonical != null && canonical != dataset) {
          body = await _getJson(url.replace(queryParameters: {'dataset': canonical}), token);
          dataset = canonical;
        } else {
          rethrow;
        }
      } else {
        rethrow;
      }
    }

    final splits = body['splits'] as List<dynamic>? ?? [];
    final configs = <String, Set<String>>{};
    for (final entry in splits) {
      final config = entry['config']?.toString();
      final split = entry['split']?.toString();
      if (config == null || split == null) continue;
      configs.putIfAbsent(config, () => <String>{}).add(split);
    }

    return (dataset, configs.map((key, value) => MapEntry(key, value.toList())));
  }

  Future<_RowsResponse> _getRows(
    String dataset,
    String config,
    String split,
    int offset,
    int length,
    String? token,
  ) async {
    final url = Uri.parse(_datasetsServerBase).replace(
      path: 'rows',
      queryParameters: {
        'dataset': dataset,
        'config': config,
        'split': split,
        'offset': offset.toString(),
        'length': length.toString(),
      },
    );

    final body = await _getJson(url, token);
    final featuresRaw = body['features'] as List<dynamic>? ?? [];
    final rowsRaw = body['rows'] as List<dynamic>? ?? [];
    final numRowsTotal = (body['num_rows_total'] as num?)?.toInt() ?? 0;
    final partial = body['partial'] == true;

    final features = featuresRaw.map((feature) {
      final name = feature['name']?.toString() ?? '';
      final rawType = feature['type'];
      final dtype = _featureDtypeLabel(rawType);
      return HfFeature(name: name, dtype: dtype, rawType: rawType);
    }).toList();

    final rows = rowsRaw.map((row) => row['row']).toList();

    return _RowsResponse(
      features: features,
      rows: rows,
      numRowsTotal: numRowsTotal,
      partial: partial,
    );
  }

  Future<Map<String, dynamic>> _getJson(Uri url, String? token) async {
    final headers = <String, String>{};
    if (token != null && token.isNotEmpty) {
      headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    }
    AppLogger.info('GET $url', tag: 'hf-http');
    final response = await _client.get(url, headers: headers);
    final status = response.statusCode;
    final text = response.body;
    AppLogger.info('GET $url -> $status (${text.length} bytes)', tag: 'hf-http');

    Map<String, dynamic> value;
    try {
      value = jsonDecode(text) as Map<String, dynamic>;
    } catch (err) {
      AppLogger.error(
        'Invalid JSON from $url',
        tag: 'hf-http',
        error: err,
      );
      AppLogger.warn(
        'Body: ${_truncateForLog(text, _logBodyLimit)}',
        tag: 'hf-http',
      );
      throw _HfException('invalid JSON from $url: $err');
    }

    final serverError = value['error']?.toString().trim();
    if (status < 200 || status >= 300) {
      if (serverError != null && serverError.isNotEmpty) {
        AppLogger.warn(
          'HTTP $status from $url: ${_truncateForLog(serverError, _logBodyLimit)}',
          tag: 'hf-http',
        );
        if (status >= 400 && status < 500 && status != 429) {
          throw _HfException('HTTP $status from $url: $serverError', isNotFound: _isNotFound(serverError));
        }
        throw _HfException('HTTP $status from $url: $serverError');
      }
      AppLogger.warn('HTTP $status from $url', tag: 'hf-http');
      throw _HfException('HTTP $status from $url');
    }

    if (serverError != null && serverError.isNotEmpty) {
      AppLogger.warn(
        'HTTP $status from $url: ${_truncateForLog(serverError, _logBodyLimit)}',
        tag: 'hf-http',
      );
      throw _HfException('HTTP $status from $url: $serverError', isNotFound: _isNotFound(serverError));
    }

    return value;
  }

  String _extractRepoId(String input) {
    final trimmed = input.trim();
    if (trimmed.isEmpty) {
      throw const FormatException(
        'Provide a Hugging Face dataset URL, or hf://datasets/<namespace>/<dataset-name>/<path>.',
      );
    }

    final uri = Uri.tryParse(trimmed);
    if (uri == null) {
      throw const FormatException('Unsupported input. Provide a dataset URL or hf://datasets/...');
    }

    final repo = _extractRepoIdFromUrl(uri);
    if (repo != null) return repo;

    throw const FormatException(
      'Unsupported Hugging Face URL. Expected https://huggingface.co/datasets/<namespace>/<dataset-name> or https://hf.co/datasets/<namespace>/<dataset-name>.',
    );
  }

  String? _extractRepoIdFromUrl(Uri url) {
    final segments = url.pathSegments.where((s) => s.isNotEmpty).toList();
    if (segments.isEmpty) return null;

    if (url.scheme == 'hf' && url.host == 'datasets') {
      if (segments.length < 2) return null;
      final org = segments[0];
      var name = segments[1];
      if (name.contains('@')) {
        name = name.split('@').first;
      }
      if (_validateRepoSegment(org) && _validateRepoSegment(name)) {
        return '$org/$name';
      }
      return null;
    }

    if (url.host == 'huggingface.co' || url.host == 'hf.co') {
      final idx = segments.indexOf('datasets');
      if (idx >= 0 && segments.length > idx + 2) {
        final org = segments[idx + 1];
        final name = segments[idx + 2];
        if (_validateRepoSegment(org) && _validateRepoSegment(name)) {
          return '$org/$name';
        }
      }
    }

    return null;
  }

  bool _validateRepoSegment(String segment) {
    if (segment.isEmpty) return false;
    return segment.runes.every((r) {
      final c = String.fromCharCode(r);
      return RegExp(r'[A-Za-z0-9._-]').hasMatch(c);
    });
  }

  String _pickDefaultSplit(List<String> splits) {
    if (splits.contains('train')) return 'train';
    final candidate = splits.firstWhere((s) => s.startsWith('train'), orElse: () => splits.first);
    return candidate;
  }

  bool _isNotFound(String message) {
    return message.toLowerCase().contains('not found');
  }

  Future<String?> _resolveCanonicalDatasetId(String dataset, String? token) async {
    final base = Uri.parse('https://huggingface.co/api/datasets/$dataset');
    final headers = <String, String>{};
    if (token != null && token.isNotEmpty) {
      headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    }
    for (var i = 0; i < 3; i += 1) {
      final response = await _client.get(base, headers: headers);
      if (response.statusCode < 200 || response.statusCode >= 300) {
        return null;
      }
      final value = jsonDecode(response.body) as Map<String, dynamic>;
      final id = value['id']?.toString().trim();
      if (id == null || id.isEmpty) return null;
      return id;
    }
    return null;
  }

  String? _featureDtypeLabel(dynamic value) {
    if (value is Map<String, dynamic>) {
      final dtype = value['dtype']?.toString();
      if (dtype != null) return dtype;
      return value['_type']?.toString();
    }
    return null;
  }

  String _sanitize(String value) {
    final buffer = StringBuffer();
    for (final rune in value.runes) {
      final c = String.fromCharCode(rune);
      if (RegExp(r'[A-Za-z0-9_-]').hasMatch(c)) {
        buffer.write(c);
      } else {
        buffer.write('_');
      }
    }
    return buffer.toString();
  }

  _Asset? _extractAsset(dynamic value) {
    if (value is Map<String, dynamic>) {
      final src = value['src']?.toString().trim();
      if (src == null || src.isEmpty) return null;
      final url = Uri.tryParse(src);
      if (url == null) return null;
      if (!_allowedAssetUrl(url)) return null;
      final mime = value['type']?.toString().trim();
      return _Asset(url: url, mime: mime);
    }
    if (value is List) {
      for (final entry in value) {
        final asset = _extractAsset(entry);
        if (asset != null) return asset;
      }
    }
    return null;
  }

  bool _allowedAssetUrl(Uri url) {
    if (url.scheme != 'https' && url.scheme != 'http') return false;
    final host = url.host;
    return host == 'datasets-server.huggingface.co' ||
        host == 'huggingface.co' ||
        host == 'hf.co' ||
        host == 'cdn-lfs.huggingface.co';
  }

  Future<Uint8List> _downloadBytes(Uri url, String? token) async {
    if (!_allowedAssetUrl(url)) {
      throw const FormatException('Blocked asset URL host/scheme.');
    }
    final headers = <String, String>{};
    if (token != null && token.isNotEmpty) {
      headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    }
    AppLogger.info('GET $url (asset)', tag: 'hf-http');
    final res = await _client.get(url, headers: headers);
    AppLogger.info('GET $url -> ${res.statusCode} (${res.bodyBytes.length} bytes)', tag: 'hf-http');
    if (res.statusCode < 200 || res.statusCode >= 300) {
      AppLogger.error('Asset download failed', tag: 'hf-http', error: res.statusCode);
      throw Exception('asset HTTP ${res.statusCode} from $url');
    }
    return res.bodyBytes;
  }

  String? _extFromUrl(Uri url) {
    final segment = url.pathSegments.isNotEmpty ? url.pathSegments.last : '';
    if (!segment.contains('.')) return null;
    final ext = segment.split('.').last.trim().toLowerCase();
    return ext.isEmpty ? null : ext;
  }

  String? _extFromMime(String? mime) {
    if (mime == null) return null;
    final m = mime.toLowerCase();
    if (m == 'audio/wav' || m == 'audio/x-wav') return 'wav';
    if (m == 'audio/mpeg' || m == 'audio/mp3') return 'mp3';
    if (m == 'audio/flac' || m == 'audio/x-flac') return 'flac';
    if (m == 'audio/ogg') return 'ogg';
    if (m == 'audio/opus') return 'opus';
    if (m == 'audio/aac') return 'aac';
    if (m == 'audio/mp4') return 'm4a';
    if (m == 'image/jpeg') return 'jpg';
    if (m == 'image/png') return 'png';
    return null;
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
    return null;
  }

  (Uint8List, String) _serializeValue(dynamic value) {
    if (value is String) {
      if (value.length > _maxInlineText) {
        throw const FormatException('Text field is too large to open.');
      }
      return (Uint8List.fromList(utf8.encode(value)), 'txt');
    }
    final jsonText = const JsonEncoder.withIndent('  ').convert(value);
    return (Uint8List.fromList(utf8.encode(jsonText)), 'json');
  }
}

class _RowsResponse {
  _RowsResponse({
    required this.features,
    required this.rows,
    required this.numRowsTotal,
    required this.partial,
  });

  final List<HfFeature> features;
  final List<dynamic> rows;
  final int numRowsTotal;
  final bool partial;
}

class _Asset {
  _Asset({required this.url, required this.mime});

  final Uri url;
  final String? mime;
}

class _HfException implements Exception {
  _HfException(this.message, {this.isNotFound = false});

  final String message;
  final bool isNotFound;

  bool get isCanonicalCandidate =>
      message.toLowerCase().contains('renamed') || message.toLowerCase().contains('current dataset name');

  @override
  String toString() => message;
}

String _truncateForLog(String value, int limit) {
  if (value.length <= limit) return value;
  return '${value.substring(0, limit)}...';
}
