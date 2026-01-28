import 'dart:io';

import 'package:dart_duckdb/dart_duckdb.dart';
import 'package:dart_duckdb/src/ffi/load_library.dart' as load_library;
import 'package:path/path.dart' as p;

import '../models/huggingface.dart';
import 'app_logger.dart';

/// Parquet reading service using DuckDB for reliable nested structure support.
class DuckDbParquetService {
  DuckDbParquetService();

  Database? _db;
  Connection? _conn;
  static bool _libraryPathConfigured = false;
  String? _cachedToken; // Cache token to avoid re-setting on every request

  // Prefetch cache: key = "url:offset:length", value = result
  final Map<String, DuckDbParquetResult> _prefetchCache = {};
  static const int _maxCacheSize = 5;

  /// Configure DuckDB library path for macOS Flutter desktop
  static void _configureLibraryPath() {
    if (_libraryPathConfigured) return;
    _libraryPathConfigured = true;

    if (!Platform.isMacOS) return;

    // Get the app bundle path
    final executablePath = Platform.resolvedExecutable;
    final appBundlePath = p.dirname(p.dirname(executablePath));

    // Check common locations for the bundled library
    final possiblePaths = [
      p.join(appBundlePath, 'Frameworks', 'libduckdb.dylib'),
      p.join(appBundlePath, 'Resources', 'libduckdb.dylib'),
    ];

    for (final libPath in possiblePaths) {
      if (File(libPath).existsSync()) {
        AppLogger.info('DuckDB library found at: $libPath', tag: 'duckdb');
        (load_library.open as load_library.OpenDynamicLibrary)
            .overrideFor(OperatingSystem.macOS, libPath);
        return;
      }
    }

    AppLogger.warn('DuckDB library not found in expected locations', tag: 'duckdb');
  }

  /// Initialize DuckDB connection (can be called early to warm up)
  Future<void> ensureInitialized() async {
    if (_db != null && _conn != null) return;

    // Configure library path before first use
    _configureLibraryPath();

    _db = await duckdb.open(':memory:');
    _conn = await duckdb.connect(_db!);

    // Disable extension auto-install/load; httpfs is statically linked.
    try {
      await _conn!.execute("SET autoload_known_extensions = false;");
      await _conn!.execute("SET autoinstall_known_extensions = false;");
    } catch (e) {
      AppLogger.warn('Failed to disable extension autoload/autoinstall: $e', tag: 'duckdb');
    }

    // Enable HTTP support for remote Parquet files (statically linked).
    await _loadHttpfsExtension();

    // Configure for performance
    await _conn!.execute("SET enable_progress_bar = false;");
    await _conn!.execute("SET enable_http_metadata_cache = true;");
    await _conn!.execute("SET http_keep_alive = true;");

    AppLogger.info('DuckDB initialized with httpfs extension', tag: 'duckdb');
  }

  Future<void> _loadHttpfsExtension() async {
    try {
      await _conn!.execute("LOAD httpfs;");
      AppLogger.info('Loaded httpfs extension (statically linked)', tag: 'duckdb');
    } catch (e) {
      AppLogger.error('httpfs extension not available in bundled libduckdb: $e', tag: 'duckdb');
      rethrow;
    }
  }

  /// Set HTTP bearer token for HuggingFace authentication (cached)
  Future<void> _setHttpToken(String token) async {
    // Skip if token hasn't changed
    if (_cachedToken == token) return;
    _cachedToken = token;

    // Use DuckDB's secret manager for bearer token auth
    try {
      await _conn!.execute("DROP SECRET IF EXISTS hf_token;");
      await _conn!.execute("""
        CREATE SECRET hf_token (
          TYPE HTTP,
          EXTRA_HTTP_HEADERS MAP {
            'Authorization': 'Bearer $token'
          }
        );
      """);
    } catch (e) {
      // Fallback: try older syntax or just log warning
      AppLogger.warn('Could not set HTTP token via secret: $e', tag: 'duckdb');
    }
  }

  /// Read rows from a remote Parquet file (optimized - single query)
  Future<DuckDbParquetResult> readParquetRows({
    required String url,
    required int offset,
    required int length,
    String? token,
    int? knownTotalRows, // Skip COUNT if we already know the total
  }) async {
    await ensureInitialized();

    // Set authorization via secret if token provided
    if (token != null && token.isNotEmpty) {
      await _setHttpToken(token);
    }

    try {
      // Single query - fetch rows with offset and limit
      // DuckDB will only download the needed row groups via HTTP range requests
      final dataResult = await _conn!.query(
        "SELECT * FROM '$url' LIMIT $length OFFSET $offset",
      );
      final dataColNames = dataResult.columnNames;
      final dataRows = dataResult.fetchAll();

      // Build features from column names and first row types
      final features = <HfFeature>[];
      for (final colName in dataColNames) {
        features.add(HfFeature(
          name: colName,
          dtype: 'unknown', // We'll infer from values
          rawType: {'_type': 'unknown'},
        ));
      }

      // Convert to list of maps with proper nested structure
      final rows = <Map<String, dynamic>>[];
      for (final row in dataRows) {
        final rowMap = <String, dynamic>{};
        for (var i = 0; i < dataColNames.length && i < row.length; i++) {
          final value = _convertValue(row[i]);
          rowMap[dataColNames[i]] = value;

          // Update feature dtype from first non-null value
          if (rows.isEmpty && value != null && features[i].dtype == 'unknown') {
            features[i] = HfFeature(
              name: dataColNames[i],
              dtype: _inferType(value),
              rawType: {'_type': _inferType(value)},
            );
          }
        }
        rows.add(rowMap);
      }

      AppLogger.info('DuckDB fetched ${rows.length} rows (offset=$offset)', tag: 'duckdb');

      final result = DuckDbParquetResult(
        features: features,
        rows: rows,
        totalRows: knownTotalRows ?? 0,
      );

      // Cache the result
      _cacheResult(url, offset, length, result);

      return result;
    } catch (e) {
      AppLogger.error('DuckDB Parquet read failed: $e', tag: 'duckdb');
      rethrow;
    }
  }

  /// Get cache key
  String _cacheKey(String url, int offset, int length) => '$url:$offset:$length';

  /// Cache a result
  void _cacheResult(String url, int offset, int length, DuckDbParquetResult result) {
    final key = _cacheKey(url, offset, length);
    _prefetchCache[key] = result;

    // Limit cache size
    while (_prefetchCache.length > _maxCacheSize) {
      _prefetchCache.remove(_prefetchCache.keys.first);
    }
  }

  /// Check if result is cached
  DuckDbParquetResult? getCached(String url, int offset, int length) {
    return _prefetchCache[_cacheKey(url, offset, length)];
  }

  /// Prefetch next page in background (fire and forget)
  void prefetchNext({
    required String url,
    required int nextOffset,
    required int length,
    String? token,
    int? knownTotalRows,
  }) {
    final key = _cacheKey(url, nextOffset, length);
    if (_prefetchCache.containsKey(key)) return; // Already cached

    // Fire and forget - don't await
    readParquetRows(
      url: url,
      offset: nextOffset,
      length: length,
      token: token,
      knownTotalRows: knownTotalRows,
    ).then((_) {
      AppLogger.info('Prefetched offset=$nextOffset', tag: 'duckdb');
    }).catchError((e) {
      AppLogger.warn('Prefetch failed: $e', tag: 'duckdb');
    });
  }

  /// Infer type from a Dart value
  String _inferType(dynamic value) {
    if (value is String) return 'string';
    if (value is int) return 'int64';
    if (value is double) return 'float';
    if (value is bool) return 'bool';
    if (value is Map) return 'struct';
    if (value is List) return 'list';
    return 'unknown';
  }

  /// Read rows from multiple Parquet files (for split across files)
  Future<DuckDbParquetResult> readParquetFilesRows({
    required List<String> urls,
    required int offset,
    required int length,
    String? token,
  }) async {
    await ensureInitialized();

    if (urls.isEmpty) {
      throw ArgumentError('No Parquet URLs provided');
    }

    // Set authorization via secret if token provided
    if (token != null && token.isNotEmpty) {
      await _setHttpToken(token);
    }

    try {
      // Create a list of URLs for DuckDB
      final urlList = urls.map((u) => "'$u'").join(', ');
      final unionQuery = "SELECT * FROM read_parquet([$urlList])";

      // Get total row count
      final countResult = await _conn!.query(
        "SELECT COUNT(*) as cnt FROM ($unionQuery)",
      );
      final countRows = countResult.fetchAll();
      final totalRows = countRows.isNotEmpty && countRows.first.isNotEmpty
          ? (countRows.first[0] as num?)?.toInt() ?? 0
          : 0;

      AppLogger.info('Parquet total rows (${urls.length} files): $totalRows', tag: 'duckdb');

      // Get schema/columns info from first file
      final schemaResult = await _conn!.query(
        "DESCRIBE SELECT * FROM '${urls.first}' LIMIT 1",
      );
      final schemaNames = schemaResult.columnNames;
      final schemaRows = schemaResult.fetchAll();

      // Find column_name and column_type indices in DESCRIBE output
      final colNameIdx = schemaNames.indexOf('column_name');
      final colTypeIdx = schemaNames.indexOf('column_type');

      final features = <HfFeature>[];
      for (final row in schemaRows) {
        final name = colNameIdx >= 0 && colNameIdx < row.length
            ? row[colNameIdx]?.toString() ?? ''
            : '';
        final dtype = colTypeIdx >= 0 && colTypeIdx < row.length
            ? row[colTypeIdx]?.toString() ?? ''
            : '';
        features.add(HfFeature(
          name: name,
          dtype: _mapDuckDbType(dtype),
          rawType: {'_type': dtype},
        ));
      }

      // Fetch rows with offset and limit
      final dataResult = await _conn!.query(
        "SELECT * FROM ($unionQuery) LIMIT $length OFFSET $offset",
      );
      final dataColNames = dataResult.columnNames;
      final dataRows = dataResult.fetchAll();

      // Convert to list of maps
      final rows = <Map<String, dynamic>>[];
      for (final row in dataRows) {
        final rowMap = <String, dynamic>{};
        for (var i = 0; i < dataColNames.length && i < row.length; i++) {
          rowMap[dataColNames[i]] = _convertValue(row[i]);
        }
        rows.add(rowMap);
      }

      AppLogger.info('Fetched ${rows.length} rows (offset=$offset)', tag: 'duckdb');

      return DuckDbParquetResult(
        features: features,
        rows: rows,
        totalRows: totalRows,
      );
    } catch (e) {
      AppLogger.error('DuckDB Parquet read failed: $e', tag: 'duckdb');
      rethrow;
    }
  }

  /// Convert DuckDB value to Dart value (handles nested structures)
  dynamic _convertValue(dynamic value) {
    if (value == null) return null;

    if (value is Map) {
      // Nested struct
      final result = <String, dynamic>{};
      for (final key in value.keys) {
        result[key.toString()] = _convertValue(value[key]);
      }
      return result;
    }

    if (value is List) {
      // Array/List type
      return value.map(_convertValue).toList();
    }

    // Primitive types
    return value;
  }

  /// Map DuckDB type to display type
  String _mapDuckDbType(String duckDbType) {
    final lower = duckDbType.toLowerCase();

    if (lower.contains('struct')) return 'struct';
    if (lower.contains('list') || lower.contains('[]')) return 'list';
    if (lower.contains('map')) return 'map';
    if (lower.contains('varchar') || lower.contains('string')) return 'string';
    if (lower.contains('bigint') || lower.contains('int64')) return 'int64';
    if (lower.contains('integer') || lower.contains('int32')) return 'int32';
    if (lower.contains('double') || lower.contains('float')) return 'float';
    if (lower.contains('boolean') || lower.contains('bool')) return 'bool';
    if (lower.contains('timestamp')) return 'timestamp';
    if (lower.contains('date')) return 'date';
    if (lower.contains('blob') || lower.contains('binary')) return 'bytes';

    return duckDbType;
  }

  /// Close DuckDB connection
  Future<void> dispose() async {
    await _conn?.dispose();
    await _db?.dispose();
    _conn = null;
    _db = null;
  }
}

/// Result from DuckDB Parquet read
class DuckDbParquetResult {
  DuckDbParquetResult({
    required this.features,
    required this.rows,
    required this.totalRows,
  });

  final List<HfFeature> features;
  final List<Map<String, dynamic>> rows;
  final int totalRows;
}
