import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;

import 'app_logger.dart';

const _datasetsServerBase = 'https://datasets-server.huggingface.co/';

/// Information about a Parquet file in a HuggingFace dataset.
class HfParquetFile {
  const HfParquetFile({
    required this.dataset,
    required this.config,
    required this.split,
    required this.url,
    required this.filename,
    required this.size,
  });

  final String dataset;
  final String config;
  final String split;
  final String url;
  final String filename;
  final int size;

  factory HfParquetFile.fromJson(Map<String, dynamic> json) {
    return HfParquetFile(
      dataset: json['dataset']?.toString() ?? '',
      config: json['config']?.toString() ?? '',
      split: json['split']?.toString() ?? '',
      url: json['url']?.toString() ?? '',
      filename: json['filename']?.toString() ?? '',
      size: (json['size'] as num?)?.toInt() ?? 0,
    );
  }
}

/// Response from the /parquet API endpoint.
class HfParquetResponse {
  const HfParquetResponse({
    required this.parquetFiles,
    required this.partial,
  });

  final List<HfParquetFile> parquetFiles;
  final bool partial;

  /// Get all unique configs available.
  Set<String> get configs => parquetFiles.map((f) => f.config).toSet();

  /// Get all unique splits for a given config.
  Set<String> splitsForConfig(String config) {
    return parquetFiles.where((f) => f.config == config).map((f) => f.split).toSet();
  }

  /// Get parquet files for a specific config and split.
  List<HfParquetFile> filesForSplit(String config, String split) {
    return parquetFiles.where((f) => f.config == config && f.split == split).toList();
  }
}

/// Client for fetching Parquet file information from HuggingFace datasets-server.
class HfParquetApi {
  HfParquetApi({http.Client? client}) : _client = client ?? http.Client();

  final http.Client _client;

  /// Cache for parquet file lists (keyed by "dataset/config/split")
  final Map<String, List<HfParquetFile>> _filesCache = {};

  /// Fetches the list of Parquet files for a specific config/split.
  /// This uses the more efficient endpoint that filters server-side.
  Future<List<HfParquetFile>> getParquetFilesForSplit({
    required String dataset,
    required String config,
    required String split,
    String? token,
  }) async {
    final cacheKey = '$dataset/$config/$split';
    if (_filesCache.containsKey(cacheKey)) {
      return _filesCache[cacheKey]!;
    }

    final url = Uri.parse(_datasetsServerBase).replace(
      path: 'parquet',
      queryParameters: {
        'dataset': dataset,
        'config': config,
        'split': split,
      },
    );

    final headers = <String, String>{};
    if (token != null && token.isNotEmpty) {
      headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    }

    AppLogger.info('GET $url', tag: 'hf-parquet');
    final response = await _client.get(url, headers: headers);
    final status = response.statusCode;
    final text = response.body;
    AppLogger.info('GET $url -> $status (${text.length} bytes)', tag: 'hf-parquet');

    if (status < 200 || status >= 300) {
      String? errorMessage;
      try {
        final json = jsonDecode(text) as Map<String, dynamic>;
        errorMessage = json['error']?.toString();
      } catch (_) {}
      throw HfParquetException(
        'HTTP $status from parquet API',
        statusCode: status,
        serverError: errorMessage,
      );
    }

    Map<String, dynamic> body;
    try {
      body = jsonDecode(text) as Map<String, dynamic>;
    } catch (e) {
      throw HfParquetException('Invalid JSON from parquet API: $e');
    }

    final parquetFilesRaw = body['parquet_files'] as List<dynamic>? ?? [];
    final parquetFiles = parquetFilesRaw
        .whereType<Map<String, dynamic>>()
        .map((json) => HfParquetFile.fromJson(json))
        .toList();

    _filesCache[cacheKey] = parquetFiles;
    return parquetFiles;
  }

  /// Fetches the list of Parquet files for a dataset (all configs/splits).
  /// Note: This can return a very large response for big datasets.
  ///
  /// Throws [HfParquetException] on HTTP errors.
  Future<HfParquetResponse> getParquetFiles({
    required String dataset,
    String? token,
  }) async {
    final url = Uri.parse(_datasetsServerBase).replace(
      path: 'parquet',
      queryParameters: {'dataset': dataset},
    );

    final headers = <String, String>{};
    if (token != null && token.isNotEmpty) {
      headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    }

    AppLogger.info('GET $url', tag: 'hf-parquet');
    final response = await _client.get(url, headers: headers);
    final status = response.statusCode;
    final text = response.body;
    AppLogger.info('GET $url -> $status (${text.length} bytes)', tag: 'hf-parquet');

    if (status < 200 || status >= 300) {
      String? errorMessage;
      try {
        final json = jsonDecode(text) as Map<String, dynamic>;
        errorMessage = json['error']?.toString();
      } catch (_) {}
      throw HfParquetException(
        'HTTP $status from parquet API',
        statusCode: status,
        serverError: errorMessage,
      );
    }

    Map<String, dynamic> body;
    try {
      body = jsonDecode(text) as Map<String, dynamic>;
    } catch (e) {
      throw HfParquetException('Invalid JSON from parquet API: $e');
    }

    final parquetFilesRaw = body['parquet_files'] as List<dynamic>? ?? [];
    final parquetFiles = parquetFilesRaw
        .whereType<Map<String, dynamic>>()
        .map((json) => HfParquetFile.fromJson(json))
        .toList();

    final partial = body['partial'] == true;

    return HfParquetResponse(
      parquetFiles: parquetFiles,
      partial: partial,
    );
  }
}

class HfParquetException implements Exception {
  HfParquetException(this.message, {this.statusCode, this.serverError});

  final String message;
  final int? statusCode;
  final String? serverError;

  bool get is501 => statusCode == 501;

  @override
  String toString() => serverError ?? message;
}
