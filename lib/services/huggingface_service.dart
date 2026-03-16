import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:http/http.dart' as http;

import '../models/common.dart';
import '../models/huggingface.dart';
import '../utils/audio.dart';
import 'app_logger.dart';
import 'duckdb_parquet_service.dart';
import 'hf_parquet_api.dart';
import 'open_with_service.dart';

const _datasetsServerBase = 'https://datasets-server.huggingface.co/';
const _defaultRows = 50;
const _maxRows = 100;
const _maxInlineText = 10 * 1024 * 1024;
const _logBodyLimit = 1024;
const _datasetsServerFailureTtl = Duration(minutes: 10);
const _hubJsonlReadAheadMinRows = 200;

class HuggingfaceService {
  HuggingfaceService({OpenWithService? openWith, http.Client? client})
      : _openWith = openWith ?? OpenWithService(),
        _client = client ?? http.Client();

  final OpenWithService _openWith;
  final http.Client _client;

  // Lazy-initialized components
  HfParquetApi? _parquetApi;
  DuckDbParquetService? _duckDb;
  final Map<String, Map<String, Map<String, List<String>>>> _hubDataFilesCache =
      <String, Map<String, Map<String, List<String>>>>{};
  final Map<String, DateTime> _parquetUnavailableUntil = <String, DateTime>{};
  final Map<String, DateTime> _rowsUnavailableUntil = <String, DateTime>{};
  final Map<String, DateTime> _firstRowsUnavailableUntil = <String, DateTime>{};
  final Map<String, DateTime> _parquetConfigUnavailableUntil =
      <String, DateTime>{};
  final Map<String, DateTime> _rowsConfigUnavailableUntil =
      <String, DateTime>{};
  final Map<String, DateTime> _firstRowsConfigUnavailableUntil =
      <String, DateTime>{};
  final Map<String, Future<Map<String, Map<String, List<String>>>>>
      _hubDataFilesInFlight =
      <String, Future<Map<String, Map<String, List<String>>>>>{};
  final Map<String, List<Map<String, dynamic>>> _hubJsonlRowsCache =
      <String, List<Map<String, dynamic>>>{};
  final Set<String> _hubJsonlRowsComplete = <String>{};
  final Map<String, Future<void>> _hubJsonlReadInFlight =
      <String, Future<void>>{};
  final Map<String, _FirstRowsPayload> _firstRowsCache =
      <String, _FirstRowsPayload>{};
  final _HttpRequestLimiter _httpLimiter =
      _HttpRequestLimiter(maxConcurrent: 16);

  HfParquetApi get _getParquetApi =>
      _parquetApi ??= HfParquetApi(client: _client);
  DuckDbParquetService get _getDuckDb => _duckDb ??= DuckDbParquetService();

  /// Pre-initialize DuckDB in background (call on app start)
  Future<void> warmup() async {
    try {
      await _getDuckDb.ensureInitialized();
      AppLogger.info('HuggingFace service warmed up', tag: 'hf');
    } catch (e) {
      AppLogger.warn('Warmup failed: $e', tag: 'hf');
    }
  }

  Future<HfDatasetPreview> datasetPreview({
    required String input,
    String? config,
    String? split,
    int? offset,
    int? length,
    String? token,
    int? maxFeatureCount,
    int? featureOffset,
    bool useStreamingApi = false,
  }) async {
    var dataset = _extractRepoId(input);
    final pageOffset = offset ?? 0;
    final pageLength = (length ?? _defaultRows).clamp(1, _maxRows).toInt();
    final tokenValue = token?.trim();

    final userConfig =
        config?.trim().isNotEmpty == true ? config!.trim() : null;
    final userSplit = split?.trim().isNotEmpty == true ? split!.trim() : null;
    final requestedFeatureCount =
        maxFeatureCount == null || maxFeatureCount <= 0
            ? null
            : maxFeatureCount;

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
          featureOffset: featureOffset,
          maxFeatureCount: requestedFeatureCount,
          useStreamingApi: useStreamingApi,
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
          featureOffset: rowsResp.featureOffset,
          featureCount: rowsResp.featureCount,
          totalFeatureCount: rowsResp.totalFeatureCount,
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
          throw FormatException(
              'No supported splits found for dataset $dataset.');
        }
        final selectedConfig = configsMap.containsKey(userConfig)
            ? userConfig
            : configsMap.keys.first;
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
          featureOffset: featureOffset,
          maxFeatureCount: requestedFeatureCount,
          useStreamingApi: useStreamingApi,
        );

        final configs = configsMap.entries
            .map((entry) => HfConfigSummary(
                config: entry.key, splits: entry.value.toList()))
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
          featureOffset: rowsResp.featureOffset,
          featureCount: rowsResp.featureCount,
          totalFeatureCount: rowsResp.totalFeatureCount,
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
    var selectedConfig =
        userConfig != null && configsMap.containsKey(userConfig)
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
        chosen = await _getRows(
          dataset,
          candidate.$1,
          candidate.$2,
          pageOffset,
          pageLength,
          tokenValue,
          featureOffset: featureOffset,
          maxFeatureCount: requestedFeatureCount,
          useStreamingApi: useStreamingApi,
        );
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
        .map((entry) =>
            HfConfigSummary(config: entry.key, splits: entry.value.toList()))
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
      featureOffset: chosen.featureOffset,
      featureCount: chosen.featureCount,
      totalFeatureCount: chosen.totalFeatureCount,
    );
  }

  Future<List<String>> listParquetFiles({
    required String input,
    String? config,
    String? split,
    String? token,
  }) async {
    var dataset = _extractRepoId(input);
    final tokenValue = token?.trim();
    final requestedConfig =
        config?.trim().isNotEmpty == true ? config!.trim() : null;
    final requestedSplit =
        split?.trim().isNotEmpty == true ? split!.trim() : null;

    final (resolvedDataset, configs) = await _getSplits(dataset, tokenValue);
    dataset = resolvedDataset;
    if (configs.isEmpty) {
      throw FormatException('No supported splits found for dataset $dataset.');
    }

    if (requestedConfig != null && !configs.containsKey(requestedConfig)) {
      throw FormatException(
          'Config "$requestedConfig" not found for dataset $dataset.');
    }

    final resolvedConfig = requestedConfig ?? configs.keys.first;
    final availableSplits = configs[resolvedConfig]!;
    if (availableSplits.isEmpty) {
      throw FormatException(
          'No splits available for config "$resolvedConfig".');
    }

    final resolvedSplit =
        requestedSplit != null && availableSplits.contains(requestedSplit)
            ? requestedSplit
            : _pickDefaultSplit(availableSplits);

    final files = await _getParquetApi.getParquetFilesForSplit(
      dataset: dataset,
      config: resolvedConfig,
      split: resolvedSplit,
      token: tokenValue,
    );

    if (files.isEmpty) {
      throw FormatException(
        'No Parquet files found for $dataset/$resolvedConfig/$resolvedSplit.',
      );
    }

    return files.map((file) => file.url).toList(growable: false);
  }

  Future<OpenLeafResponse> openField({
    required String input,
    required String config,
    required String split,
    required int rowIndex,
    required String fieldName,
    bool openWithSystem = true,
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

    final rowsResp = await _getRows(
      dataset,
      configValue,
      splitValue,
      rowIndex,
      1,
      tokenValue,
    );
    final rows = rowsResp.rows;
    if (rows.isEmpty) {
      throw const FormatException('No row returned for the requested offset.');
    }
    final rawRow = rows.first;
    if (rawRow is! Map) {
      throw const FormatException('Row is not a JSON object.');
    }
    final row = Map<String, dynamic>.from(
      rawRow.map((key, value) => MapEntry(key.toString(), value)),
    );
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
      final virtualPath =
          'hf://$dataset/$configValue/$splitValue/r$rowIndex/$field.$ext';
      final base = '$virtualPath ($size bytes)';
      if (!openWithSystem) {
        return OpenLeafResponse(
          path: virtualPath,
          size: size,
          ext: ext,
          opened: false,
          needsOpener: false,
          message: base,
        );
      }
      final tempDir = Directory(
          '${Directory.systemTemp.path}/dataset-inspector/huggingface');
      await tempDir.create(recursive: true);
      final uniqueSuffix = DateTime.now().microsecondsSinceEpoch;
      final baseName =
          _sanitize('$dataset-$configValue-$splitValue-r$rowIndex-$field-$uniqueSuffix');
      final out = File('${tempDir.path}/$baseName.$ext');
      await out.writeAsBytes(bytes, flush: true);

      final localBase = '${out.path} ($size bytes)';
      final result = await _openWith.openFile(out.path, appPath: openerAppPath);
      final needsOpener = !result.opened && result.error != null;
      var message = localBase;
      if (needsOpener) {
        message = '$localBase · no default app found, choose an app to open it';
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
    final virtualPath =
        'hf://$dataset/$configValue/$splitValue/r$rowIndex/$field.$ext';
    final base = '$virtualPath ($size bytes)';
    if (!openWithSystem) {
      return OpenLeafResponse(
        path: virtualPath,
        size: size,
        ext: ext,
        opened: false,
        needsOpener: false,
        message: base,
      );
    }
    final tempDir =
        Directory('${Directory.systemTemp.path}/dataset-inspector/huggingface');
    await tempDir.create(recursive: true);
    final uniqueSuffix = DateTime.now().microsecondsSinceEpoch;
    final baseName =
        _sanitize('$dataset-$configValue-$splitValue-r$rowIndex-$field-$uniqueSuffix');
    final out = File('${tempDir.path}/$baseName.$ext');
    await out.writeAsBytes(bytes, flush: true);

    final localBase = '${out.path} ($size bytes)';
    final result = await _openWith.openFile(out.path, appPath: openerAppPath);
    final needsOpener = !result.opened && result.error != null;
    var message = localBase;
    if (needsOpener) {
      message = '$localBase · no default app found, choose an app to open it';
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

  Future<(String, Map<String, List<String>>)> _getSplits(
      String dataset, String? token) async {
    final url = Uri.parse(_datasetsServerBase).replace(
      path: 'splits',
      queryParameters: {'dataset': dataset},
    );

    Map<String, dynamic> body;
    try {
      body = await _getJson(url, token);
    } on _HfException catch (err) {
      // Fallback to Parquet API for 501 (Not Implemented) errors
      if (err.is501) {
        AppLogger.info(
            'Datasets server returned 501, falling back to Parquet API',
            tag: 'hf');
        return _getSplitsViaParquet(dataset, token);
      }
      if (err.isCanonicalCandidate) {
        final canonical = await _resolveCanonicalDatasetId(dataset, token);
        if (canonical != null && canonical != dataset) {
          try {
            body = await _getJson(
                url.replace(queryParameters: {'dataset': canonical}), token);
            dataset = canonical;
          } on _HfException catch (canonicalErr) {
            AppLogger.warn(
              'Canonical splits lookup failed, fallback to hub metadata: ${canonicalErr.message}',
              tag: 'hf',
            );
            return _getSplitsViaHubMetadata(canonical, token);
          }
        } else {
          AppLogger.warn(
            'Splits API failed, fallback to hub metadata: ${err.message}',
            tag: 'hf',
          );
          return _getSplitsViaHubMetadata(dataset, token);
        }
      } else {
        AppLogger.warn(
          'Splits API failed, fallback to hub metadata: ${err.message}',
          tag: 'hf',
        );
        return _getSplitsViaHubMetadata(dataset, token);
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

    return (
      dataset,
      configs.map((key, value) => MapEntry(key, value.toList()))
    );
  }

  Future<(String, Map<String, List<String>>)> _getSplitsViaParquet(
      String dataset, String? token) async {
    final parquetResp =
        await _getParquetApi.getParquetFiles(dataset: dataset, token: token);
    final configs = <String, Set<String>>{};
    for (final file in parquetResp.parquetFiles) {
      configs.putIfAbsent(file.config, () => <String>{}).add(file.split);
    }
    return (
      dataset,
      configs.map((key, value) => MapEntry(key, value.toList()))
    );
  }

  Future<(String, Map<String, List<String>>)> _getSplitsViaHubMetadata(
      String dataset, String? token) async {
    final configSplitPaths = await _getHubConfigSplitDataFiles(dataset, token);
    final configs = <String, List<String>>{};
    for (final entry in configSplitPaths.entries) {
      if (entry.value.isEmpty) continue;
      configs[entry.key] = entry.value.keys.toList(growable: false);
    }
    if (configs.isEmpty) {
      throw FormatException('No supported splits found for dataset $dataset.');
    }
    return (dataset, configs);
  }

  Future<_RowsResponse> _getRows(
    String dataset,
    String config,
    String split,
    int offset,
    int length,
    String? token, {
    int? featureOffset,
    int? maxFeatureCount,
    bool useStreamingApi = false,
  }) async {
    final splitKey = _splitCacheKey(dataset, config, split);
    final configKey = _configCacheKey(dataset, config);
    final parquetUnavailable =
        _isTemporarilyUnavailable(_parquetUnavailableUntil, splitKey) ||
            _isTemporarilyUnavailable(_parquetConfigUnavailableUntil, configKey);
    final rowsUnavailable =
        _isTemporarilyUnavailable(_rowsUnavailableUntil, splitKey) ||
            _isTemporarilyUnavailable(_rowsConfigUnavailableUntil, configKey);
    final firstRowsUnavailable =
        _isTemporarilyUnavailable(_firstRowsUnavailableUntil, splitKey) ||
            _isTemporarilyUnavailable(
                _firstRowsConfigUnavailableUntil, configKey);

    if (useStreamingApi) {
      if (!rowsUnavailable) {
        try {
          final rowsResp = await _getRowsViaRowsApi(
            dataset,
            config,
            split,
            offset,
            length,
            token,
            featureOffset: featureOffset,
            maxFeatureCount: maxFeatureCount,
          );
          _clearUnavailable(_rowsUnavailableUntil, splitKey);
          _clearUnavailable(_rowsConfigUnavailableUntil, configKey);
          return rowsResp;
        } catch (err) {
          if (_isServerGenerationFailure(err)) {
            _markTemporarilyUnavailable(_rowsUnavailableUntil, splitKey);
            _markTemporarilyUnavailable(_rowsConfigUnavailableUntil, configKey);
          }
          AppLogger.warn('HF rows API failed, fallback to Parquet: $err',
              tag: 'hf');
        }
      }
      if (!parquetUnavailable) {
        try {
          final rowsResp = await _getRowsViaParquet(
            dataset,
            config,
            split,
            offset,
            length,
            token,
            featureOffset: featureOffset,
            maxFeatureCount: maxFeatureCount,
          );
          _clearUnavailable(_parquetUnavailableUntil, splitKey);
          _clearUnavailable(_parquetConfigUnavailableUntil, configKey);
          return rowsResp;
        } catch (err) {
          if (_isServerGenerationFailure(err)) {
            _markTemporarilyUnavailable(_parquetUnavailableUntil, splitKey);
            _markTemporarilyUnavailable(
                _parquetConfigUnavailableUntil, configKey);
          }
          AppLogger.warn(
            'HF parquet fallback failed, fallback to Hub files: $err',
            tag: 'hf',
          );
        }
      }
      if (!firstRowsUnavailable) {
        try {
          final rowsResp = await _getRowsViaFirstRowsApi(
            dataset,
            config,
            split,
            offset,
            length,
            token,
            featureOffset: featureOffset,
            maxFeatureCount: maxFeatureCount,
          );
          _clearUnavailable(_firstRowsUnavailableUntil, splitKey);
          _clearUnavailable(_firstRowsConfigUnavailableUntil, configKey);
          return rowsResp;
        } catch (err) {
          if (_isServerGenerationFailure(err)) {
            _markTemporarilyUnavailable(_firstRowsUnavailableUntil, splitKey);
            _markTemporarilyUnavailable(
                _firstRowsConfigUnavailableUntil, configKey);
          } else if (_isFirstRowsWindowExhausted(err)) {
            _markTemporarilyUnavailable(_firstRowsUnavailableUntil, splitKey);
          }
          AppLogger.warn(
            'HF first-rows API failed, fallback to Hub files: $err',
            tag: 'hf',
          );
        }
      }
      return _getRowsViaHubDataFiles(
        dataset,
        config,
        split,
        offset,
        length,
        token,
        featureOffset: featureOffset,
        maxFeatureCount: maxFeatureCount,
      );
    }

    // Default path: prefer Parquet for stable access, but fallback to rows API.
    if (!parquetUnavailable) {
      try {
        final rowsResp = await _getRowsViaParquet(
          dataset,
          config,
          split,
          offset,
          length,
          token,
          featureOffset: featureOffset,
          maxFeatureCount: maxFeatureCount,
        );
        _clearUnavailable(_parquetUnavailableUntil, splitKey);
        _clearUnavailable(_parquetConfigUnavailableUntil, configKey);
        return rowsResp;
      } catch (err) {
        if (_isServerGenerationFailure(err)) {
          _markTemporarilyUnavailable(_parquetUnavailableUntil, splitKey);
          _markTemporarilyUnavailable(_parquetConfigUnavailableUntil, configKey);
        }
        AppLogger.warn(
          'HF parquet API failed, fallback to rows API: $err',
          tag: 'hf',
        );
      }
    }
    if (!rowsUnavailable) {
      try {
        final rowsResp = await _getRowsViaRowsApi(
          dataset,
          config,
          split,
          offset,
          length,
          token,
          featureOffset: featureOffset,
          maxFeatureCount: maxFeatureCount,
        );
        _clearUnavailable(_rowsUnavailableUntil, splitKey);
        _clearUnavailable(_rowsConfigUnavailableUntil, configKey);
        return rowsResp;
      } catch (err) {
        if (_isServerGenerationFailure(err)) {
          _markTemporarilyUnavailable(_rowsUnavailableUntil, splitKey);
          _markTemporarilyUnavailable(_rowsConfigUnavailableUntil, configKey);
        }
        AppLogger.warn(
          'HF rows API failed, fallback to Hub files: $err',
          tag: 'hf',
        );
      }
    }
    if (!firstRowsUnavailable) {
      try {
        final rowsResp = await _getRowsViaFirstRowsApi(
          dataset,
          config,
          split,
          offset,
          length,
          token,
          featureOffset: featureOffset,
          maxFeatureCount: maxFeatureCount,
        );
        _clearUnavailable(_firstRowsUnavailableUntil, splitKey);
        _clearUnavailable(_firstRowsConfigUnavailableUntil, configKey);
        return rowsResp;
      } catch (err) {
        if (_isServerGenerationFailure(err)) {
          _markTemporarilyUnavailable(_firstRowsUnavailableUntil, splitKey);
          _markTemporarilyUnavailable(
              _firstRowsConfigUnavailableUntil, configKey);
        } else if (_isFirstRowsWindowExhausted(err)) {
          _markTemporarilyUnavailable(_firstRowsUnavailableUntil, splitKey);
        }
        AppLogger.warn(
          'HF first-rows API failed, fallback to Hub files: $err',
          tag: 'hf',
        );
      }
    }
    return _getRowsViaHubDataFiles(
      dataset,
      config,
      split,
      offset,
      length,
      token,
      featureOffset: featureOffset,
      maxFeatureCount: maxFeatureCount,
    );
  }

  Future<_RowsResponse> _getRowsViaParquet(
    String dataset,
    String config,
    String split,
    int offset,
    int length,
    String? token, {
    int? featureOffset,
    int? maxFeatureCount,
  }) async {
    // 1. Get parquet files for this specific config/split
    final files = await _getParquetApi.getParquetFilesForSplit(
      dataset: dataset,
      config: config,
      split: split,
      token: token,
    );

    if (files.isEmpty) {
      throw FormatException(
          'No Parquet files found for $dataset/$config/$split');
    }

    // 2. Get accurate total row count from size API
    int totalRows;
    try {
      totalRows = await _getSplitRowCount(dataset, config, split, token);
    } catch (e) {
      AppLogger.warn('Size API failed: $e', tag: 'hf');
      totalRows = 0;
    }

    AppLogger.info(
      'Reading $dataset/$config/$split via DuckDB: offset=$offset, length=$length, files=${files.length}',
      tag: 'hf',
    );

    final parquetUrls = files.map((file) => file.url).toList(growable: false);
    final result = await _getDuckDb.readParquetFilesRows(
      urls: parquetUrls,
      offset: offset,
      length: length,
      token: token,
      knownTotalRows: totalRows > 0 ? totalRows : null,
      featureOffset: featureOffset,
      maxFeatureCount: maxFeatureCount,
    );

    // Use size API total if available
    if (totalRows <= 0) {
      totalRows = result.totalRows;
    }

    // Prefetch next page in background for faster navigation
    if (result.rows.length >= length) {
      final nextOffset = offset + length;
      if (nextOffset < totalRows || totalRows == 0) {
        _getDuckDb.prefetchNextFiles(
          urls: parquetUrls,
          nextOffset: nextOffset,
          length: length,
          token: token,
          knownTotalRows: totalRows,
          featureOffset: featureOffset,
          maxFeatureCount: maxFeatureCount,
        );
      }
    }

    return _RowsResponse(
      features: result.features,
      rows: result.rows,
      numRowsTotal: totalRows,
      partial: result.partial,
      featureOffset: result.featureOffset,
      featureCount: result.featureCount,
      totalFeatureCount: result.totalFeatureCount,
    );
  }

  Future<_RowsResponse> _getRowsViaRowsApi(
    String dataset,
    String config,
    String split,
    int offset,
    int length,
    String? token, {
    int? featureOffset,
    int? maxFeatureCount,
  }) async {
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

    final rowsResp = await _getJson(url, token);
    final rowsPayload = rowsResp['rows'] as List<dynamic>? ?? [];
    final rows = <Map<String, dynamic>>[];

    for (final entry in rowsPayload) {
      if (entry is Map) {
        final rowValue = entry['row'];
        if (rowValue is Map) {
          rows.add(Map<String, dynamic>.from(
            rowValue.map((key, value) => MapEntry(key.toString(), value)),
          ));
        } else {
          rows.add({'value': rowValue});
        }
      } else {
        rows.add({'value': entry});
      }
    }

    final parsedFeatures = _parseRowsApiFeatures(
      rowsResp['features'],
      rows,
      maxFeatureCount: maxFeatureCount,
      featureOffset: featureOffset,
    );
    final total = _toOptionalInt(rowsResp['num_rows_total']) ??
        _toOptionalInt(rowsResp['num_rows']) ??
        0;
    final partial = rowsResp['partial'] == true ||
        (length > 0 && rows.length >= length && total <= 0);

    final featureCount = parsedFeatures.length;
    return _RowsResponse(
      features: parsedFeatures,
      rows: rows,
      numRowsTotal: total,
      partial: partial,
      featureOffset: featureOffset ?? 0,
      featureCount: featureCount,
      totalFeatureCount:
          _toOptionalInt(rowsResp['num_features']) ?? featureCount,
    );
  }

  Future<_RowsResponse> _getRowsViaFirstRowsApi(
    String dataset,
    String config,
    String split,
    int offset,
    int length,
    String? token, {
    int? featureOffset,
    int? maxFeatureCount,
  }) async {
    final splitKey = _splitCacheKey(dataset, config, split);
    var payload = _firstRowsCache[splitKey];
    if (payload == null) {
      final url = Uri.parse(_datasetsServerBase).replace(
        path: 'first-rows',
        queryParameters: {
          'dataset': dataset,
          'config': config,
          'split': split,
        },
      );
      final body = await _getJson(url, token);
      final rowsPayload = body['rows'] as List<dynamic>? ?? const <dynamic>[];
      final rows = <Map<String, dynamic>>[];
      for (final entry in rowsPayload) {
        if (entry is Map) {
          final rowValue = entry['row'];
          if (rowValue is Map) {
            rows.add(Map<String, dynamic>.from(
              rowValue.map((key, value) => MapEntry(key.toString(), value)),
            ));
          }
        }
      }
      final featuresPayload =
          body['features'] as List<dynamic>? ?? const <dynamic>[];
      final features = <HfFeature>[];
      for (final entry in featuresPayload) {
        if (entry is Map<String, dynamic>) {
          final name = entry['name']?.toString();
          if (name == null || name.isEmpty) continue;
          final rawType = entry['type'] ?? entry['dtype'] ?? entry['_type'];
          features.add(HfFeature(
            name: name,
            dtype: rawType?.toString(),
            rawType: rawType,
          ));
        }
      }
      payload = _FirstRowsPayload(
        features: features,
        rows: rows,
        truncated: body['truncated'] == true,
      );
      _firstRowsCache[splitKey] = payload;
    }

    final available = payload.rows.length;
    if (offset >= available && payload.truncated) {
      throw _HfException(
        'first-rows window exhausted for $dataset/$config/$split',
      );
    }

    final start = offset.clamp(0, available).toInt();
    if (payload.truncated && start + length > available) {
      throw _HfException(
        'first-rows window exhausted for $dataset/$config/$split',
      );
    }
    final end = (start + length).clamp(start, available).toInt();
    final rows = start >= end
        ? const <Map<String, dynamic>>[]
        : List<Map<String, dynamic>>.from(payload.rows.sublist(start, end));

    final allFeatures = payload.features;
    final featureStart =
        featureOffset != null && featureOffset > 0 ? featureOffset : 0;
    final featureTake = maxFeatureCount != null && maxFeatureCount > 0
        ? maxFeatureCount
        : allFeatures.length;
    final featureEnd =
        (featureStart + featureTake).clamp(0, allFeatures.length);
    final features = featureStart >= featureEnd
        ? const <HfFeature>[]
        : allFeatures.sublist(featureStart, featureEnd);
    final knownTotal = payload.truncated ? 0 : available;
    final partial =
        payload.truncated || (knownTotal > 0 && end < knownTotal);

    return _RowsResponse(
      features: features,
      rows: rows,
      numRowsTotal: knownTotal,
      partial: partial,
      featureOffset: featureOffset ?? 0,
      featureCount: features.length,
      totalFeatureCount: allFeatures.length,
    );
  }

  Future<_RowsResponse> _getRowsViaHubDataFiles(
    String dataset,
    String config,
    String split,
    int offset,
    int length,
    String? token, {
    int? featureOffset,
    int? maxFeatureCount,
  }) async {
    AppLogger.info(
      'Fallback to Hub files for $dataset/$config/$split (offset=$offset length=$length)',
      tag: 'hf',
    );
    final configSplitPaths = await _getHubConfigSplitDataFiles(dataset, token);
    final splitPaths = configSplitPaths[config]?[split];
    if (splitPaths == null || splitPaths.isEmpty) {
      throw _HfException(
        'No hub data files found for $dataset/$config/$split',
        isNotFound: true,
      );
    }

    final parquetPaths = <String>[];
    final jsonlPaths = <String>[];
    for (final path in splitPaths) {
      final ext = _pathExt(path);
      if (ext == 'parquet') {
        parquetPaths.add(path);
        continue;
      }
      if (ext == 'jsonl' || ext == 'ndjson') {
        jsonlPaths.add(path);
      }
    }

    if (parquetPaths.isNotEmpty) {
      final parquetUrls = parquetPaths
          .map((path) => _buildHubResolveUri(dataset, path).toString())
          .toList(growable: false);
      final result = await _getDuckDb.readParquetFilesRows(
        urls: parquetUrls,
        offset: offset,
        length: length,
        token: token,
        knownTotalRows: null,
        featureOffset: featureOffset,
        maxFeatureCount: maxFeatureCount,
      );
      return _RowsResponse(
        features: result.features,
        rows: result.rows,
        numRowsTotal: result.totalRows,
        partial: result.partial,
        featureOffset: result.featureOffset,
        featureCount: result.featureCount,
        totalFeatureCount: result.totalFeatureCount,
      );
    }

    if (jsonlPaths.isNotEmpty) {
      final splitKey = _splitCacheKey(dataset, config, split);
      final rows = await _readRowsFromHubJsonlFiles(
        splitKey: splitKey,
        dataset: dataset,
        paths: jsonlPaths,
        offset: offset,
        length: length,
        token: token,
      );
      final cachedRows = _hubJsonlRowsCache[splitKey] ?? const [];
      final isComplete = _hubJsonlRowsComplete.contains(splitKey);
      final parsedFeatures = _parseRowsApiFeatures(
        const <dynamic>[],
        rows,
        featureOffset: featureOffset,
        maxFeatureCount: maxFeatureCount,
      );
      final totalFeatureCount = cachedRows.isNotEmpty
          ? cachedRows.first.length
          : (rows.isNotEmpty ? rows.first.length : parsedFeatures.length);
      final knownTotalRows = isComplete ? cachedRows.length : 0;
      final partial = isComplete
          ? (offset + rows.length) < knownTotalRows
          : (length > 0 && rows.length >= length);
      return _RowsResponse(
        features: parsedFeatures,
        rows: rows,
        numRowsTotal: knownTotalRows,
        partial: partial,
        featureOffset: featureOffset ?? 0,
        featureCount: parsedFeatures.length,
        totalFeatureCount: totalFeatureCount,
      );
    }

    throw _HfException(
      'Hub fallback supports parquet/jsonl/ndjson only for now.',
    );
  }

  Future<List<Map<String, dynamic>>> _readRowsFromHubJsonlFiles({
    required String splitKey,
    required String dataset,
    required List<String> paths,
    required int offset,
    required int length,
    required String? token,
  }) async {
    final cachedRows = _hubJsonlRowsCache[splitKey];
    final needed = offset + length;
    if (cachedRows != null && cachedRows.length >= needed) {
      return _sliceRows(cachedRows, offset, length);
    }
    if (_hubJsonlRowsComplete.contains(splitKey)) {
      return _sliceRows(cachedRows ?? const [], offset, length);
    }

    final targetRows = offset +
        (length > _hubJsonlReadAheadMinRows
            ? length
            : _hubJsonlReadAheadMinRows);
    await _ensureHubJsonlRows(
      splitKey: splitKey,
      dataset: dataset,
      paths: paths,
      token: token,
      targetRows: targetRows,
    );
    return _sliceRows(_hubJsonlRowsCache[splitKey] ?? const [], offset, length);
  }

  Future<void> _ensureHubJsonlRows({
    required String splitKey,
    required String dataset,
    required List<String> paths,
    required String? token,
    required int targetRows,
  }) async {
    while (true) {
      if (_hubJsonlRowsComplete.contains(splitKey)) return;
      final current = _hubJsonlRowsCache[splitKey]?.length ?? 0;
      if (current >= targetRows) return;

      final inFlight = _hubJsonlReadInFlight[splitKey];
      if (inFlight != null) {
        await inFlight;
        continue;
      }

      final future = _fetchHubJsonlRowsToTarget(
        splitKey: splitKey,
        dataset: dataset,
        paths: paths,
        token: token,
        targetRows: targetRows,
      );
      _hubJsonlReadInFlight[splitKey] = future;
      try {
        await future;
      } finally {
        _hubJsonlReadInFlight.remove(splitKey);
      }
    }
  }

  Future<void> _fetchHubJsonlRowsToTarget({
    required String splitKey,
    required String dataset,
    required List<String> paths,
    required String? token,
    required int targetRows,
  }) async {
    final existing = _hubJsonlRowsCache[splitKey] == null
        ? <Map<String, dynamic>>[]
        : List<Map<String, dynamic>>.from(_hubJsonlRowsCache[splitKey]!);
    final existingCount = existing.length;
    var validRowIndex = 0;
    var reachedTarget = false;

    for (final path in paths) {
      if (reachedTarget) break;
      final url = _buildHubResolveUri(dataset, path);
      final request = http.Request('GET', url);
      if (token != null && token.isNotEmpty) {
        request.headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
      }
      AppLogger.info('GET $url (hub stream)', tag: 'hf-http');
      final response = await _httpLimiter.run(
        () => _client.send(request),
      );
      if (response.statusCode < 200 || response.statusCode >= 300) {
        final body = await response.stream.bytesToString();
        throw _HfException(
          'HTTP ${response.statusCode} from $url: ${body.trim().isEmpty ? 'failed to stream hub data file' : body.trim()}',
        );
      }
      final lines =
          response.stream.transform(utf8.decoder).transform(const LineSplitter());
      await for (final line in lines) {
        final trimmed = line.trim();
        if (trimmed.isEmpty) continue;
        dynamic value;
        try {
          value = jsonDecode(trimmed);
        } catch (_) {
          continue;
        }
        validRowIndex += 1;
        if (validRowIndex <= existingCount) {
          continue;
        }
        if (value is Map) {
          existing.add(
            Map<String, dynamic>.from(
              value.map((key, val) => MapEntry(key.toString(), val)),
            ),
          );
        } else {
          existing.add(<String, dynamic>{'value': value});
        }
        if (existing.length >= targetRows) {
          reachedTarget = true;
          break;
        }
      }
    }

    _hubJsonlRowsCache[splitKey] = existing;
    if (!reachedTarget) {
      _hubJsonlRowsComplete.add(splitKey);
    }
  }

  List<HfFeature> _parseRowsApiFeatures(
    dynamic rawFeatures,
    List<Map<String, dynamic>> rows, {
    int? featureOffset,
    int? maxFeatureCount,
  }) {
    final allFeatures = <HfFeature>[];
    final declared = rawFeatures as List<dynamic>? ?? const <dynamic>[];
    for (final feature in declared) {
      if (feature is Map<String, dynamic>) {
        final name = feature['name']?.toString();
        if (name == null || name.isEmpty) continue;
        final rawType = feature['type'] ?? feature['dtype'] ?? feature['_type'];
        final label = rawType?.toString();
        allFeatures.add(HfFeature(
          name: name,
          dtype: label,
          rawType: rawType,
        ));
      }
    }

    if (allFeatures.isNotEmpty) {
      final start =
          featureOffset != null && featureOffset > 0 ? featureOffset : 0;
      final take = maxFeatureCount != null && maxFeatureCount > 0
          ? maxFeatureCount
          : allFeatures.length;
      final end = (start + take).clamp(0, allFeatures.length);
      return allFeatures.sublist(start, end);
    }

    if (rows.isEmpty) return const <HfFeature>[];
    final first = rows.first;
    for (final entry in first.entries) {
      allFeatures.add(HfFeature(
        name: entry.key,
        dtype: _featureDtypeLabel(entry.value),
        rawType: entry.value,
      ));
    }

    final start =
        featureOffset != null && featureOffset > 0 ? featureOffset : 0;
    final take = maxFeatureCount != null && maxFeatureCount > 0
        ? maxFeatureCount
        : allFeatures.length;
    final end = (start + take).clamp(0, allFeatures.length);
    return allFeatures.sublist(start, end);
  }

  int? _toOptionalInt(dynamic value) {
    if (value == null) return null;
    if (value is int) return value;
    if (value is num) return value.toInt();
    if (value is String) return int.tryParse(value);
    return null;
  }

  String _splitCacheKey(String dataset, String config, String split) {
    return '$dataset/$config/$split';
  }

  String _configCacheKey(String dataset, String config) {
    return '$dataset/$config';
  }

  bool _isTemporarilyUnavailable(
      Map<String, DateTime> cache, String splitKey) {
    final expires = cache[splitKey];
    if (expires == null) return false;
    if (DateTime.now().isBefore(expires)) return true;
    cache.remove(splitKey);
    return false;
  }

  void _markTemporarilyUnavailable(
      Map<String, DateTime> cache, String splitKey) {
    cache[splitKey] = DateTime.now().add(_datasetsServerFailureTtl);
  }

  void _clearUnavailable(Map<String, DateTime> cache, String splitKey) {
    cache.remove(splitKey);
  }

  bool _isServerGenerationFailure(Object err) {
    if (err is HfParquetException) {
      final status = err.statusCode ?? 0;
      if (status >= 500) return true;
      final message = (err.serverError ?? err.message).toLowerCase();
      return message.contains('dataset generation failed');
    }
    if (err is _HfException) {
      final message = err.message.toLowerCase();
      return message.contains('dataset generation failed') ||
          message.contains('http 5');
    }
    return false;
  }

  bool _isFirstRowsWindowExhausted(Object err) {
    if (err is _HfException) {
      return err.message.toLowerCase().contains('first-rows window exhausted');
    }
    return false;
  }

  List<Map<String, dynamic>> _sliceRows(
      List<Map<String, dynamic>> rows, int offset, int length) {
    if (offset < 0 || length <= 0) return const <Map<String, dynamic>>[];
    if (offset >= rows.length) return const <Map<String, dynamic>>[];
    final end = (offset + length).clamp(offset, rows.length);
    return List<Map<String, dynamic>>.from(rows.sublist(offset, end));
  }

  Future<Map<String, Map<String, List<String>>>> _getHubConfigSplitDataFiles(
      String dataset, String? token) async {
    final cached = _hubDataFilesCache[dataset];
    if (cached != null) return cached;

    final inFlight = _hubDataFilesInFlight[dataset];
    if (inFlight != null) {
      return inFlight;
    }

    final future = _fetchHubConfigSplitDataFiles(dataset, token);
    _hubDataFilesInFlight[dataset] = future;
    try {
      final parsed = await future;
      _hubDataFilesCache[dataset] = parsed;
      return parsed;
    } finally {
      _hubDataFilesInFlight.remove(dataset);
    }
  }

  Future<Map<String, Map<String, List<String>>>> _fetchHubConfigSplitDataFiles(
      String dataset, String? token) async {
    final url = Uri.parse('https://huggingface.co/api/datasets/$dataset');
    final headers = <String, String>{};
    if (token != null && token.isNotEmpty) {
      headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    }
    AppLogger.info('GET $url', tag: 'hf-http');
    final response = await _httpLimiter.run(
      () => _client.get(url, headers: headers),
    );
    final status = response.statusCode;
    final text = response.body;
    AppLogger.info('GET $url -> $status (${text.length} bytes)',
        tag: 'hf-http');
    if (status < 200 || status >= 300) {
      throw _HfException('HTTP $status from $url');
    }

    Map<String, dynamic> body;
    try {
      body = jsonDecode(text) as Map<String, dynamic>;
    } catch (err) {
      throw _HfException('Invalid JSON from $url: $err');
    }

    final parsed = _extractHubConfigSplitDataFiles(body);
    if (parsed.isEmpty) {
      throw _HfException(
        'Hub metadata has no usable cardData.data_files for dataset $dataset',
      );
    }
    return parsed;
  }

  Map<String, Map<String, List<String>>> _extractHubConfigSplitDataFiles(
      Map<String, dynamic> body) {
    final result = <String, Map<String, List<String>>>{};
    final cardData = body['cardData'];
    if (cardData is! Map) return result;
    final card = Map<String, dynamic>.from(
      cardData.map((key, value) => MapEntry(key.toString(), value)),
    );

    final configs = card['configs'];
    if (configs is List) {
      for (final entry in configs) {
        if (entry is! Map) continue;
        final configMap = Map<String, dynamic>.from(
          entry.map((key, value) => MapEntry(key.toString(), value)),
        );
        final rawConfig = configMap['config_name']?.toString().trim();
        final config =
            (rawConfig == null || rawConfig.isEmpty) ? 'default' : rawConfig;
        final splitMap =
            result.putIfAbsent(config, () => <String, List<String>>{});
        _collectHubDataFiles(splitMap, configMap['data_files']);
      }
    }

    if (result.isEmpty) {
      final splitMap = <String, List<String>>{};
      _collectHubDataFiles(splitMap, card['data_files']);
      if (splitMap.isNotEmpty) {
        result['default'] = splitMap;
      }
    }

    return result;
  }

  void _collectHubDataFiles(Map<String, List<String>> splitMap, dynamic value) {
    if (value is List) {
      for (final entry in value) {
        if (entry is String) {
          _addHubDataFilePath(splitMap, 'train', entry);
          continue;
        }
        if (entry is Map) {
          final map = Map<String, dynamic>.from(
            entry.map((key, val) => MapEntry(key.toString(), val)),
          );
          final rawSplit = map['split']?.toString().trim();
          final split =
              (rawSplit == null || rawSplit.isEmpty) ? 'train' : rawSplit;
          final rawPath = map['path'] ?? map['paths'] ?? map['files'];
          _collectHubDataFilePaths(splitMap, split, rawPath);
        }
      }
      return;
    }

    if (value is Map) {
      for (final entry in value.entries) {
        final rawSplit = entry.key.toString().trim();
        final split = rawSplit.isEmpty ? 'train' : rawSplit;
        _collectHubDataFilePaths(splitMap, split, entry.value);
      }
    }
  }

  void _collectHubDataFilePaths(
    Map<String, List<String>> splitMap,
    String split,
    dynamic value,
  ) {
    if (value is String) {
      _addHubDataFilePath(splitMap, split, value);
      return;
    }
    if (value is List) {
      for (final entry in value) {
        if (entry is String) {
          _addHubDataFilePath(splitMap, split, entry);
        }
      }
    }
  }

  void _addHubDataFilePath(
      Map<String, List<String>> splitMap, String split, String rawPath) {
    final path = rawPath.trim();
    if (path.isEmpty) return;
    final paths = splitMap.putIfAbsent(split, () => <String>[]);
    if (!paths.contains(path)) {
      paths.add(path);
    }
  }

  String _pathExt(String path) {
    final slash = path.lastIndexOf('/');
    final fileName = slash >= 0 ? path.substring(slash + 1) : path;
    final dot = fileName.lastIndexOf('.');
    if (dot < 0) return '';
    return fileName.substring(dot + 1).toLowerCase();
  }

  Uri _buildHubResolveUri(String dataset, String path) {
    final datasetSegments =
        dataset.split('/').where((segment) => segment.isNotEmpty).toList();
    final pathSegments =
        path.split('/').where((segment) => segment.isNotEmpty).toList();
    return Uri(
      scheme: 'https',
      host: 'huggingface.co',
      pathSegments: <String>[
        'datasets',
        ...datasetSegments,
        'resolve',
        'main',
        ...pathSegments,
      ],
      queryParameters: const <String, String>{'download': 'true'},
    );
  }

  Future<Map<String, dynamic>> _getJson(Uri url, String? token) async {
    final headers = <String, String>{};
    if (token != null && token.isNotEmpty) {
      headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    }
    AppLogger.info('GET $url', tag: 'hf-http');
    final response = await _httpLimiter.run(
      () => _client.get(url, headers: headers),
    );
    final status = response.statusCode;
    final text = response.body;
    AppLogger.info('GET $url -> $status (${text.length} bytes)',
        tag: 'hf-http');

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
      final is501 = status == 501;
      if (serverError != null && serverError.isNotEmpty) {
        AppLogger.warn(
          'HTTP $status from $url: ${_truncateForLog(serverError, _logBodyLimit)}',
          tag: 'hf-http',
        );
        if (status >= 400 && status < 500 && status != 429) {
          throw _HfException('HTTP $status from $url: $serverError',
              isNotFound: _isNotFound(serverError));
        }
        throw _HfException('HTTP $status from $url: $serverError',
            is501: is501);
      }
      AppLogger.warn('HTTP $status from $url', tag: 'hf-http');
      throw _HfException('HTTP $status from $url', is501: is501);
    }

    if (serverError != null && serverError.isNotEmpty) {
      AppLogger.warn(
        'HTTP $status from $url: ${_truncateForLog(serverError, _logBodyLimit)}',
        tag: 'hf-http',
      );
      throw _HfException('HTTP $status from $url: $serverError',
          isNotFound: _isNotFound(serverError));
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
      throw const FormatException(
          'Unsupported input. Provide a dataset URL or hf://datasets/...');
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
    final candidate = splits.firstWhere((s) => s.startsWith('train'),
        orElse: () => splits.first);
    return candidate;
  }

  bool _isNotFound(String message) {
    return message.toLowerCase().contains('not found');
  }

  Future<String?> _resolveCanonicalDatasetId(
      String dataset, String? token) async {
    final base = Uri.parse('https://huggingface.co/api/datasets/$dataset');
    final headers = <String, String>{};
    if (token != null && token.isNotEmpty) {
      headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    }
    final response = await _httpLimiter.run(
      () => _client.get(base, headers: headers),
    );
    if (response.statusCode < 200 || response.statusCode >= 300) {
      return null;
    }
    final value = jsonDecode(response.body) as Map<String, dynamic>;
    final id = value['id']?.toString().trim();
    if (id == null || id.isEmpty) return null;
    return id;
  }

  String? _featureDtypeLabel(dynamic value) {
    if (value is Map<String, dynamic>) {
      final dtype = value['dtype']?.toString();
      if (dtype != null) return dtype;
      return value['_type']?.toString();
    }
    return null;
  }

  /// Get accurate row count for a split from the /size API
  Future<int> _getSplitRowCount(
      String dataset, String config, String split, String? token) async {
    final url = Uri.parse(_datasetsServerBase).replace(
      path: 'size',
      queryParameters: {'dataset': dataset},
    );

    final body = await _getJson(url, token);
    final size = body['size'] as Map<String, dynamic>?;
    if (size == null) throw const FormatException('No size data');

    final splits = size['splits'] as List<dynamic>? ?? [];
    for (final s in splits) {
      if (s['config'] == config && s['split'] == split) {
        return (s['num_rows'] as num?)?.toInt() ?? 0;
      }
    }

    // Fallback to dataset total
    final datasetInfo = size['dataset'] as Map<String, dynamic>?;
    return (datasetInfo?['num_rows'] as num?)?.toInt() ?? 0;
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
    final res = await _httpLimiter.run(
      () => _client.get(url, headers: headers),
    );
    AppLogger.info(
        'GET $url -> ${res.statusCode} (${res.bodyBytes.length} bytes)',
        tag: 'hf-http');
    if (res.statusCode < 200 || res.statusCode >= 300) {
      AppLogger.error('Asset download failed',
          tag: 'hf-http', error: res.statusCode);
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
    if (data.length >= 3 &&
        data[0] == 0xff &&
        data[1] == 0xd8 &&
        data[2] == 0xff) {
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
    required this.featureOffset,
    required this.featureCount,
    required this.totalFeatureCount,
  });

  final List<HfFeature> features;
  final List<dynamic> rows;
  final int numRowsTotal;
  final bool partial;
  final int featureOffset;
  final int featureCount;
  final int totalFeatureCount;
}

class _HttpRequestLimiter {
  _HttpRequestLimiter({required int maxConcurrent})
      : _maxConcurrent = maxConcurrent <= 0 ? 1 : maxConcurrent;

  final int _maxConcurrent;
  int _active = 0;
  final Queue<Completer<void>> _waiters = Queue<Completer<void>>();

  Future<T> run<T>(Future<T> Function() task) async {
    if (_active >= _maxConcurrent) {
      final waiter = Completer<void>();
      _waiters.add(waiter);
      await waiter.future;
    }
    _active += 1;
    try {
      return await task();
    } finally {
      _active -= 1;
      if (_waiters.isNotEmpty) {
        final waiter = _waiters.removeFirst();
        if (!waiter.isCompleted) {
          waiter.complete();
        }
      }
    }
  }
}

class _FirstRowsPayload {
  _FirstRowsPayload({
    required this.features,
    required this.rows,
    required this.truncated,
  });

  final List<HfFeature> features;
  final List<Map<String, dynamic>> rows;
  final bool truncated;
}

class _Asset {
  _Asset({required this.url, required this.mime});

  final Uri url;
  final String? mime;
}

class _HfException implements Exception {
  _HfException(this.message, {this.isNotFound = false, this.is501 = false});

  final String message;
  final bool isNotFound;
  final bool is501;

  bool get isCanonicalCandidate =>
      message.toLowerCase().contains('renamed') ||
      message.toLowerCase().contains('current dataset name');

  @override
  String toString() => message;
}

String _truncateForLog(String value, int limit) {
  if (value.length <= limit) return value;
  return '${value.substring(0, limit)}...';
}
