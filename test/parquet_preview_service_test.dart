import 'dart:typed_data';

import 'package:dataset_inspector/models/huggingface.dart';
import 'package:dataset_inspector/models/remote_host.dart';
import 'package:dataset_inspector/services/duckdb_parquet_service.dart';
import 'package:dataset_inspector/services/parquet_preview_service.dart';
import 'package:dataset_inspector/services/remote_dataset_service.dart';
import 'package:flutter_test/flutter_test.dart';

class _FakeDuckDbParquetService extends DuckDbParquetService {
  String? lastQueryLocalPath;
  String? lastReadRemoteUrl;
  int queryLocalCalls = 0;
  int readUrlCalls = 0;

  DuckDbParquetResult _result() {
    return DuckDbParquetResult(
      features: const <HfFeature>[
        HfFeature(name: 'name', dtype: 'string', rawType: <String, dynamic>{}),
        HfFeature(name: 'id', dtype: 'int64', rawType: <String, dynamic>{}),
      ],
      rows: const <Map<String, dynamic>>[
        <String, dynamic>{'name': 'alice', 'id': 1},
        <String, dynamic>{'name': 'bob', 'id': 2},
      ],
      totalRows: 2,
    );
  }

  @override
  Future<DuckDbParquetResult> queryLocalParquet({
    required String parquetPath,
    required int offset,
    required int length,
    String? whereClause,
    String? orderBy,
    String? selectClause,
  }) async {
    queryLocalCalls += 1;
    lastQueryLocalPath = parquetPath;
    return _result();
  }

  @override
  Future<DuckDbParquetResult> readParquetRows({
    required String url,
    required int offset,
    required int length,
    String? token,
    int? knownTotalRows,
    int? featureOffset,
    int? maxFeatureCount,
  }) async {
    readUrlCalls += 1;
    lastReadRemoteUrl = url;
    return _result();
  }
}

class _FakeRemoteDatasetService extends RemoteDatasetService {
  _FakeRemoteDatasetService(this.payload);

  final Uint8List payload;
  int openReadCalls = 0;
  String? lastRemotePath;

  @override
  Stream<List<int>> openReadFile({
    required RemoteHostConfig host,
    required String remotePath,
    int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) {
    openReadCalls += 1;
    lastRemotePath = remotePath;
    return Stream<List<int>>.value(payload);
  }
}

RemoteHostConfig _buildSshHost() {
  return const RemoteHostConfig(
    id: 'ssh',
    label: 'SSH',
    type: RemoteHostType.ssh,
    ssh: SshRemoteHostConfig(
      host: '10.0.0.2',
      username: 'tester',
    ),
  );
}

void main() {
  test('previewLocal uses local parquet query for file path', () async {
    final fakeDuckDb = _FakeDuckDbParquetService();
    final service = ParquetPreviewService(duckdb: fakeDuckDb);

    final preview =
        await service.previewLocal(parquetPath: '/tmp/data.parquet');

    expect(fakeDuckDb.queryLocalCalls, equals(1));
    expect(fakeDuckDb.readUrlCalls, equals(0));
    expect(fakeDuckDb.lastQueryLocalPath, equals('/tmp/data.parquet'));
    expect(preview.headers, equals(<String>['name', 'id']));
    expect(
        preview.rows,
        equals(<List<String>>[
          <String>['alice', '1'],
          <String>['bob', '2'],
        ]));
  });

  test('previewLocal uses httpfs branch for HTTP URLs', () async {
    final fakeDuckDb = _FakeDuckDbParquetService();
    final service = ParquetPreviewService(duckdb: fakeDuckDb);

    await service.previewLocal(parquetPath: 'https://example.com/data.parquet');

    expect(fakeDuckDb.queryLocalCalls, equals(0));
    expect(fakeDuckDb.readUrlCalls, equals(1));
    expect(
      fakeDuckDb.lastReadRemoteUrl,
      equals('https://example.com/data.parquet'),
    );
  });

  test('previewRemote stages stream and routes through local parquet query',
      () async {
    final fakeDuckDb = _FakeDuckDbParquetService();
    final service = ParquetPreviewService(duckdb: fakeDuckDb);
    final fakeRemote = _FakeRemoteDatasetService(
      Uint8List.fromList(<int>[0x50, 0x41, 0x52, 0x31]),
    );

    await service.previewRemote(
      remoteDatasets: fakeRemote,
      host: _buildSshHost(),
      remotePath: 'train/part-000.parquet',
    );

    expect(fakeRemote.openReadCalls, equals(1));
    expect(fakeRemote.lastRemotePath, equals('train/part-000.parquet'));
    expect(fakeDuckDb.queryLocalCalls, equals(1));
    expect(
        fakeDuckDb.lastQueryLocalPath, contains('dataset_inspector_parquet_'));
  });
}
