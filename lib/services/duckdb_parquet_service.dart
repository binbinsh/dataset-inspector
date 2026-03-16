import 'dart:io';

import 'package:dart_duckdb/dart_duckdb.dart';
import 'package:dart_duckdb/open.dart';
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

  // In-memory prefetch cache: key = "url:offset:length", value = result.
  // Do not persist this cache to disk.
  final Map<String, DuckDbParquetResult> _prefetchCache = {};
  final Map<String, List<String>> _columnNamesCache = {};
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
        open.overrideFor(OperatingSystem.macOS, libPath);
        return;
      }
    }

    AppLogger.warn('DuckDB library not found in expected locations',
        tag: 'duckdb');
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
      AppLogger.warn('Failed to disable extension autoload/autoinstall: $e',
          tag: 'duckdb');
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
      AppLogger.info('Loaded httpfs extension (statically linked)',
          tag: 'duckdb');
    } catch (e) {
      AppLogger.error('httpfs extension not available in bundled libduckdb: $e',
          tag: 'duckdb');
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
    int? featureOffset,
    int? maxFeatureCount,
  }) async {
    await ensureInitialized();

    // Set authorization via secret if token provided
    if (token != null && token.isNotEmpty) {
      await _setHttpToken(token);
    }

    try {
      // Single query - fetch rows with offset and limit
      // DuckDB will only download the needed row groups via HTTP range requests
      final allColumnNames = await _getColumnNames(url);
      final start =
          featureOffset == null || featureOffset < 0 ? 0 : featureOffset;
      final limit = maxFeatureCount == null || maxFeatureCount <= 0
          ? allColumnNames.length
          : maxFeatureCount;
      final end = start >= allColumnNames.length
          ? allColumnNames.length
          : (start + limit).clamp(start, allColumnNames.length);

      final selectedColumns =
          end <= start ? const <String>[] : allColumnNames.sublist(start, end);
      if (selectedColumns.isEmpty) {
        final emptyResult = DuckDbParquetResult(
          features: const <HfFeature>[],
          rows: const <Map<String, dynamic>>[],
          totalRows: knownTotalRows ?? 0,
          featureOffset: start,
          featureCount: 0,
          totalFeatureCount: allColumnNames.length,
          partial: false,
        );
        return emptyResult;
      }

      final selectClause = selectedColumns.map(_quoteIdentifier).join(', ');
      final dataResult = await _conn!.query(
          "SELECT $selectClause FROM '$url' LIMIT $length OFFSET $offset");
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

      AppLogger.info('DuckDB fetched ${rows.length} rows (offset=$offset)',
          tag: 'duckdb');

      final projectedCount = selectedColumns.length;
      final partial = featureOffset != null &&
              maxFeatureCount != null &&
              maxFeatureCount > 0
          ? (start + projectedCount) < allColumnNames.length
          : false;
      final result = DuckDbParquetResult(
        features: features,
        rows: rows,
        totalRows: knownTotalRows ?? 0,
        featureOffset: start,
        featureCount: projectedCount,
        totalFeatureCount: allColumnNames.length,
        partial: partial,
      );
      _cacheResult(
        url: url,
        offset: offset,
        length: length,
        result: result,
        featureOffset: featureOffset,
        maxFeatureCount: maxFeatureCount,
      );
      return result;
    } catch (e) {
      AppLogger.error('DuckDB Parquet read failed: $e', tag: 'duckdb');
      rethrow;
    }
  }

  /// Get cache key
  String _cacheKey(
    String url,
    int offset,
    int length, {
    int? featureOffset,
    int? maxFeatureCount,
  }) {
    return '$url:$offset:$length:fo=$featureOffset:mc=$maxFeatureCount';
  }

  /// Cache a result
  void _cacheResult({
    required String url,
    required int offset,
    required int length,
    required DuckDbParquetResult result,
    int? featureOffset,
    int? maxFeatureCount,
  }) {
    final key = _cacheKey(
      url,
      offset,
      length,
      featureOffset: featureOffset,
      maxFeatureCount: maxFeatureCount,
    );
    _prefetchCache[key] = result;

    // Limit cache size
    while (_prefetchCache.length > _maxCacheSize) {
      _prefetchCache.remove(_prefetchCache.keys.first);
    }
  }

  /// Check if result is cached
  DuckDbParquetResult? getCached(
    String url,
    int offset,
    int length, {
    int? featureOffset,
    int? maxFeatureCount,
  }) {
    return _prefetchCache[_cacheKey(
      url,
      offset,
      length,
      featureOffset: featureOffset,
      maxFeatureCount: maxFeatureCount,
    )];
  }

  /// Export a JSONL file directly into a Parquet file.
  Future<void> exportJsonlToParquet({
    required String jsonlPath,
    required String parquetPath,
  }) async {
    await ensureInitialized();

    final escapedJsonl = _escapeSqlLiteral(jsonlPath);
    final escapedParquet = _escapeSqlLiteral(parquetPath);

    try {
      await _conn!
          .execute('DROP TABLE IF EXISTS __dataset_inspector_snapshot;');
      await _conn!.execute(
        "CREATE TEMP TABLE __dataset_inspector_snapshot AS "
        "SELECT * FROM read_json_auto('$escapedJsonl')",
      );
      await _conn!.execute(
        "COPY (SELECT * FROM __dataset_inspector_snapshot) "
        "TO '$escapedParquet' (FORMAT PARQUET)",
      );
    } catch (error) {
      AppLogger.error(
        'DuckDB JSONL export failed: $error',
        tag: 'duckdb',
      );
      rethrow;
    }
  }

  /// Query rows from a local Parquet file with optional SQL predicates.
  Future<DuckDbParquetResult> queryLocalParquet({
    required String parquetPath,
    required int offset,
    required int length,
    String? whereClause,
    String? orderBy,
    String? selectClause,
  }) async {
    await ensureInitialized();

    if (length <= 0) {
      throw const FormatException('length must be > 0');
    }

    final escapedPath = _escapeSqlLiteral(parquetPath);
    final source = "read_parquet('$escapedPath')";
    final selected =
        (selectClause?.trim().isNotEmpty == true) ? selectClause! : '*';
    final trimmedWhere = whereClause?.trim();
    final where = (trimmedWhere == null || trimmedWhere.isEmpty)
        ? ''
        : ' WHERE $trimmedWhere';
    final order = (orderBy?.trim().isNotEmpty == true)
        ? ' ORDER BY ${orderBy!.trim()}'
        : '';

    try {
      final countResult = await _conn!.query(
        "SELECT COUNT(*) as cnt FROM ($source$where)",
      );
      final countRows = countResult.fetchAll();
      final totalRows = countRows.isNotEmpty && countRows.first.isNotEmpty
          ? (countRows.first[0] as num?)?.toInt() ?? 0
          : 0;

      final dataResult = await _conn!.query(
        "SELECT $selected FROM ($source$where$order) LIMIT $length OFFSET $offset",
      );
      final names = dataResult.columnNames;
      final rowsRaw = dataResult.fetchAll();

      final rows = <Map<String, dynamic>>[];
      for (final row in rowsRaw) {
        final rowMap = <String, dynamic>{};
        for (var index = 0;
            index < names.length && index < row.length;
            index++) {
          rowMap[names[index]] = _convertValue(row[index]);
        }
        rows.add(rowMap);
      }

      AppLogger.info(
        'DuckDB local parquet query returned ${rows.length} rows from offset=$offset',
        tag: 'duckdb',
      );

      return DuckDbParquetResult(
        features: names
            .map((name) => HfFeature(
                name: name, dtype: 'unknown', rawType: {'_type': 'unknown'}))
            .toList(),
        rows: rows,
        totalRows: totalRows,
      );
    } catch (error) {
      AppLogger.error('DuckDB local parquet query failed: $error',
          tag: 'duckdb');
      rethrow;
    }
  }

  /// Prefetch next page in background (fire and forget)
  void prefetchNext({
    required String url,
    required int nextOffset,
    required int length,
    String? token,
    int? knownTotalRows,
    int? featureOffset,
    int? maxFeatureCount,
  }) {
    final key = _cacheKey(
      url,
      nextOffset,
      length,
      featureOffset: featureOffset,
      maxFeatureCount: maxFeatureCount,
    );
    if (_prefetchCache.containsKey(key)) return; // Already cached

    // Fire and forget - don't await
    readParquetRows(
      url: url,
      offset: nextOffset,
      length: length,
      token: token,
      knownTotalRows: knownTotalRows,
      featureOffset: featureOffset,
      maxFeatureCount: maxFeatureCount,
    ).then((_) {
      AppLogger.info('Prefetched offset=$nextOffset', tag: 'duckdb');
    }).catchError((e) {
      AppLogger.warn('Prefetch failed: $e', tag: 'duckdb');
    });
  }

  /// Prefetch next page for multi-file parquet reads.
  void prefetchNextFiles({
    required List<String> urls,
    required int nextOffset,
    required int length,
    String? token,
    int? knownTotalRows,
    int? featureOffset,
    int? maxFeatureCount,
  }) {
    if (urls.isEmpty) return;

    final key = _cacheKey(
      urls.join('|'),
      nextOffset,
      length,
      featureOffset: featureOffset,
      maxFeatureCount: maxFeatureCount,
    );
    if (_prefetchCache.containsKey(key)) return; // Already cached

    // Fire and forget - don't await
    readParquetFilesRows(
      urls: urls,
      offset: nextOffset,
      length: length,
      token: token,
      knownTotalRows: knownTotalRows,
      featureOffset: featureOffset,
      maxFeatureCount: maxFeatureCount,
    ).then((_) {
      AppLogger.info('Prefetched offset=$nextOffset', tag: 'duckdb');
    }).catchError((e) {
      AppLogger.warn('Prefetch failed: $e', tag: 'duckdb');
    });
  }

  Future<List<String>> _getColumnNames(String url) async {
    final cached = _columnNamesCache[url];
    if (cached != null) return cached;

    final schemaResult = await _conn!.query("DESCRIBE SELECT * FROM '$url'");
    final columns = <String>[];
    final names = schemaResult.columnNames;
    final rows = schemaResult.fetchAll();
    final colNameIdx = names.indexOf('column_name');
    if (colNameIdx >= 0) {
      for (final row in rows) {
        if (colNameIdx < row.length) {
          final rawName = row[colNameIdx]?.toString() ?? '';
          if (rawName.isNotEmpty) columns.add(rawName);
        }
      }
    }
    _columnNamesCache[url] = columns;
    return columns;
  }

  String _buildSourceSql(List<String> urls) {
    if (urls.isEmpty) {
      throw ArgumentError('No Parquet URLs provided.');
    }

    final normalized = urls
        .map((value) => value.trim())
        .where((value) => value.isNotEmpty)
        .toList(growable: false);
    if (normalized.isEmpty) {
      throw ArgumentError('No valid Parquet URLs provided.');
    }

    if (normalized.length == 1) {
      final escapedPath = _escapeSqlLiteral(normalized.first);
      return "read_parquet('$escapedPath')";
    }

    final urlList =
        normalized.map((entry) => "'${_escapeSqlLiteral(entry)}'").join(', ');
    return 'read_parquet([$urlList])';
  }

  String _replaceSourcePlaceholder(String query, String sourceSql) {
    if (query.contains('{{source}}')) {
      return query.replaceAll('{{source}}', sourceSql);
    }
    if (query.contains('{source}')) {
      return query.replaceAll('{source}', sourceSql);
    }
    return query;
  }

  void _ensureSafeReadOnlyQuery(String query) {
    final normalized = query.trim().toLowerCase();
    if (!(normalized.startsWith('select') || normalized.startsWith('with'))) {
      throw const FormatException('Only SELECT/WITH queries are supported.');
    }
    if (query.contains(';')) {
      throw const FormatException('Multiple SQL statements are not supported.');
    }
  }

  /// Execute a paged SQL query against one or more Parquet inputs.
  Future<DuckDbParquetResult> queryParquetSql({
    required List<String> parquetSources,
    required String query,
    required int offset,
    required int length,
    String? token,
    bool includeTotalRows = false,
  }) async {
    await ensureInitialized();

    if (length <= 0) {
      throw const FormatException('length must be > 0');
    }
    if (offset < 0) {
      throw const FormatException('offset must be >= 0');
    }

    final sourceSql = _buildSourceSql(parquetSources);
    _ensureSafeReadOnlyQuery(query);
    final resolvedQuery = _replaceSourcePlaceholder(query, sourceSql);
    final pagedQuery =
        'SELECT * FROM ($resolvedQuery) t LIMIT $length OFFSET $offset';

    try {
      final totalRows = includeTotalRows
          ? (await _conn!.query("SELECT COUNT(*) as cnt FROM ($resolvedQuery)"))
              .fetchAll()
          : const <List<dynamic>>[];

      final countRows = await _conn!.query(
        "DESCRIBE ($resolvedQuery)",
      );
      final describeNames = countRows.columnNames;
      final describeRows = countRows.fetchAll();
      final colNameIdx = describeNames.indexOf('column_name');
      final colTypeIdx = describeNames.indexOf('column_type');
      final features = <HfFeature>[];
      for (final row in describeRows) {
        final name = colNameIdx >= 0 && colNameIdx < row.length
            ? row[colNameIdx]?.toString() ?? ''
            : '';
        final dtype = colTypeIdx >= 0 && colTypeIdx < row.length
            ? row[colTypeIdx]?.toString() ?? ''
            : '';
        if (name.isEmpty) continue;
        features.add(
          HfFeature(
            name: name,
            dtype: _mapDuckDbType(dtype),
            rawType: {'_type': dtype},
          ),
        );
      }

      final dataResult = await _conn!.query(pagedQuery);
      final dataColNames = dataResult.columnNames;
      final dataRows = dataResult.fetchAll();
      final rows = <Map<String, dynamic>>[];
      for (final dataRow in dataRows) {
        final rowMap = <String, dynamic>{};
        for (var index = 0;
            index < dataColNames.length && index < dataRow.length;
            index++) {
          rowMap[dataColNames[index]] = _convertValue(dataRow[index]);
        }
        rows.add(rowMap);
      }

      final resolvedTotalRows = includeTotalRows && totalRows.isNotEmpty
          ? (totalRows.first.isNotEmpty
              ? (totalRows.first[0] as num?)?.toInt() ?? 0
              : 0)
          : 0;
      final hasMoreRows = includeTotalRows
          ? (offset + rows.length) < resolvedTotalRows
          : rows.length >= length;

      AppLogger.info(
        'DuckDB custom query returned ${rows.length} rows from source=${parquetSources.length} offset=$offset',
        tag: 'duckdb',
      );

      return DuckDbParquetResult(
        features: features,
        rows: rows,
        totalRows: resolvedTotalRows,
        featureOffset: 0,
        featureCount: features.length,
        totalFeatureCount: features.length,
        partial: hasMoreRows,
      );
    } catch (error) {
      AppLogger.error('DuckDB custom SQL query failed: $error', tag: 'duckdb');
      rethrow;
    }
  }

  String _quoteIdentifier(String name) {
    final escaped = name.replaceAll('"', '""');
    return '"$escaped"';
  }

  String _escapeSqlLiteral(String value) => value.replaceAll('\'', '\'\'');

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

  int? _extractFirstValueAsInt(List<List<dynamic>> rows) {
    if (rows.isEmpty) return null;
    final firstRow = rows.first;
    if (firstRow.isEmpty) return null;
    return (firstRow[0] as num?)?.toInt();
  }

  /// Read rows from multiple Parquet files (for split across files)
  Future<DuckDbParquetResult> readParquetFilesRows({
    required List<String> urls,
    required int offset,
    required int length,
    String? token,
    int? knownTotalRows,
    int? featureOffset,
    int? maxFeatureCount,
  }) async {
    await ensureInitialized();

    if (urls.isEmpty) {
      throw ArgumentError('No Parquet URLs provided');
    }
    final cacheKey = urls.join('|');

    final cached = getCached(
      cacheKey,
      offset,
      length,
      featureOffset: featureOffset,
      maxFeatureCount: maxFeatureCount,
    );
    if (cached != null) return cached;

    // Set authorization via secret if token provided
    if (token != null && token.isNotEmpty) {
      await _setHttpToken(token);
    }

    try {
      // Create a list of URLs for DuckDB
      final urlList = urls.map((u) {
        final escaped = _escapeSqlLiteral(u);
        return "'$escaped'";
      }).join(', ');
      final unionQuery = "SELECT * FROM read_parquet([$urlList])";
      final startOffset =
          featureOffset != null && featureOffset > 0 ? featureOffset : 0;

      // Get total row count (from known value if provided).
      final effectiveTotalRows = knownTotalRows != null && knownTotalRows > 0
          ? knownTotalRows
          : _extractFirstValueAsInt(
                (await _conn!
                        .query("SELECT COUNT(*) as cnt FROM ($unionQuery)"))
                    .fetchAll(),
              ) ??
              0;

      AppLogger.info(
        'Parquet total rows (${urls.length} files): $effectiveTotalRows',
        tag: 'duckdb',
      );

      // Get schema/columns info from the combined parquet set.
      final schemaResult = await _conn!.query(
        "DESCRIBE SELECT * FROM ($unionQuery) LIMIT 1",
      );
      final schemaNames = schemaResult.columnNames;
      final schemaRows = schemaResult.fetchAll();

      // Find column_name and column_type indices in DESCRIBE output
      final colNameIdx = schemaNames.indexOf('column_name');
      final colTypeIdx = schemaNames.indexOf('column_type');

      final allColumns = <HfFeature>[];
      final features = <HfFeature>[];
      for (final row in schemaRows) {
        final name = colNameIdx >= 0 && colNameIdx < row.length
            ? row[colNameIdx]?.toString() ?? ''
            : '';
        final dtype = colTypeIdx >= 0 && colTypeIdx < row.length
            ? row[colTypeIdx]?.toString() ?? ''
            : '';
        final feature = HfFeature(
          name: name,
          dtype: _mapDuckDbType(dtype),
          rawType: {'_type': dtype},
        );
        allColumns.add(feature);
      }
      final take = maxFeatureCount != null && maxFeatureCount > 0
          ? maxFeatureCount
          : allColumns.length;
      final end = (startOffset + take).clamp(0, allColumns.length);
      final selectedColumns =
          allColumns.sublist(startOffset.clamp(0, allColumns.length), end);
      final projectedCount = selectedColumns.length;
      features
        ..clear()
        ..addAll(selectedColumns);

      final selectClause = features.isNotEmpty
          ? features.map((column) => _quoteIdentifier(column.name)).join(', ')
          : '';
      if (selectClause.isEmpty) {
        final result = DuckDbParquetResult(
          features: features,
          rows: const <Map<String, dynamic>>[],
          totalRows: effectiveTotalRows,
          totalFeatureCount: allColumns.length,
          featureOffset: startOffset,
          featureCount: projectedCount,
          partial: false,
        );
        _cacheResult(
          url: cacheKey,
          offset: offset,
          length: length,
          result: result,
          featureOffset: featureOffset,
          maxFeatureCount: maxFeatureCount,
        );
        return result;
      }

      // Fetch rows with offset and limit
      final dataResult = await _conn!.query(
        "SELECT $selectClause FROM ($unionQuery) LIMIT $length OFFSET $offset",
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

      AppLogger.info('Fetched ${rows.length} rows (offset=$offset)',
          tag: 'duckdb');

      final hasMoreRows = effectiveTotalRows > 0
          ? (offset + rows.length) < effectiveTotalRows
          : rows.length >= length;

      final result = DuckDbParquetResult(
        features: features,
        rows: rows,
        totalRows: effectiveTotalRows,
        totalFeatureCount: allColumns.length,
        featureOffset: startOffset,
        featureCount: projectedCount,
        partial: hasMoreRows,
      );
      _cacheResult(
        url: cacheKey,
        offset: offset,
        length: length,
        result: result,
        featureOffset: featureOffset,
        maxFeatureCount: maxFeatureCount,
      );
      return result;
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
    this.totalFeatureCount = 0,
    this.featureOffset = 0,
    this.featureCount = 0,
    this.partial = false,
  });

  final List<HfFeature> features;
  final List<Map<String, dynamic>> rows;
  final int totalRows;
  final int totalFeatureCount;
  final int featureOffset;
  final int featureCount;
  final bool partial;
}
