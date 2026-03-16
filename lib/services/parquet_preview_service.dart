import 'dart:convert';
import 'dart:io';

import 'package:path/path.dart' as p;

import '../models/remote_host.dart';
import 'duckdb_parquet_service.dart';
import 'remote_dataset_service.dart';

class ParquetPreviewTable {
  const ParquetPreviewTable({
    required this.headers,
    required this.rows,
  });

  final List<String> headers;
  final List<List<String>> rows;
}

class ParquetPreviewService {
  ParquetPreviewService({
    DuckDbParquetService? duckdb,
  }) : _duckdb = duckdb ?? DuckDbParquetService();

  final DuckDbParquetService _duckdb;

  Future<ParquetPreviewTable> previewLocal({
    required String parquetPath,
    int offset = 0,
    int length = 2000,
  }) async {
    final source = parquetPath.trim();
    final result = source.startsWith('http://') || source.startsWith('https://')
        ? await _duckdb.readParquetRows(
            url: source,
            offset: offset,
            length: length,
          )
        : await _duckdb.queryLocalParquet(
            parquetPath: source,
            offset: offset,
            length: length,
          );
    return _toTable(result);
  }

  Future<ParquetPreviewTable> previewRemote({
    required RemoteDatasetService remoteDatasets,
    required RemoteHostConfig host,
    required String remotePath,
    int offset = 0,
    int length = 2000,
    RemoteStatusCallback? onStatus,
  }) async {
    final tempDir =
        await Directory.systemTemp.createTemp('dataset_inspector_parquet_');
    final leaf = p.basename(remotePath).trim();
    final fileName = leaf.isEmpty
        ? 'remote.parquet'
        : (leaf.toLowerCase().endsWith('.parquet') ? leaf : '$leaf.parquet');
    final stagedFile = File(p.join(tempDir.path, fileName));

    IOSink? sink;
    try {
      sink = stagedFile.openWrite();
      await sink.addStream(
        remoteDatasets.openReadFile(
          host: host,
          remotePath: remotePath,
          onStatus: onStatus,
        ),
      );
      await sink.flush();
      await sink.close();
      sink = null;
      return previewLocal(
        parquetPath: stagedFile.path,
        offset: offset,
        length: length,
      );
    } finally {
      if (sink != null) {
        await sink.close();
      }
      try {
        if (await stagedFile.exists()) {
          await stagedFile.delete();
        }
      } catch (_) {}
      try {
        if (await tempDir.exists()) {
          await tempDir.delete(recursive: true);
        }
      } catch (_) {}
    }
  }

  ParquetPreviewTable _toTable(DuckDbParquetResult result) {
    final headers = result.features.map((feature) => feature.name).toList();
    final rows = result.rows.map((row) {
      final cells = <String>[];
      if (headers.isEmpty) {
        for (final value in row.values) {
          cells.add(_toCell(value));
        }
        return cells;
      }
      for (final header in headers) {
        cells.add(_toCell(row[header]));
      }
      return cells;
    }).toList(growable: false);
    return ParquetPreviewTable(headers: headers, rows: rows);
  }

  String _toCell(dynamic value) {
    if (value == null) return '';
    if (value is String) return value;
    if (value is num || value is bool) return value.toString();
    try {
      return jsonEncode(value);
    } catch (_) {
      return value.toString();
    }
  }
}
