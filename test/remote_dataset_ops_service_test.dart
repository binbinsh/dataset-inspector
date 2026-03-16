import 'dart:typed_data';

import 'package:dataset_inspector/models/common.dart';
import 'package:dataset_inspector/services/litdata_service.dart';
import 'package:dataset_inspector/services/mosaicml_service.dart';
import 'package:dataset_inspector/services/remote_dataset_ops_service.dart';
import 'package:flutter_test/flutter_test.dart';

class _FakeMosaicmlService extends MosaicmlService {
  _FakeMosaicmlService({
    required this.failTimes,
    this.errorMessage = 'malformed shard',
    this.retryable = true,
  });

  final int failTimes;
  final String errorMessage;
  final bool retryable;
  int calls = 0;
  int compressedCalls = 0;
  String? lastDecodedShardCacheKey;

  @override
  Future<List<ItemMeta>> listSamplesFromRawStream({
    Uint8List? indexBytes,
    String indexName = 'index.json',
    String? indexPath,
    required String shardFilename,
    required Stream<List<int>> rawStream,
  }) async {
    calls += 1;
    await rawStream.drain<void>();
    if (calls <= failTimes) {
      if (retryable) {
        throw FormatException(errorMessage);
      }
      throw StateError(errorMessage);
    }
    return const <ItemMeta>[
      ItemMeta(itemIndex: 0, totalBytes: 4, fields: <FieldMeta>[]),
    ];
  }

  @override
  Future<List<ItemMeta>> listSamplesFromZstdCompressedStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String shardFilename,
    required Stream<List<int>> Function(int? maxBytes) openCompressedStream,
    String? decodedShardCacheKey,
  }) async {
    compressedCalls += 1;
    calls += 1;
    lastDecodedShardCacheKey = decodedShardCacheKey;
    await openCompressedStream(null).drain<void>();
    if (calls <= failTimes) {
      if (retryable) {
        throw FormatException(errorMessage);
      }
      throw StateError(errorMessage);
    }
    return const <ItemMeta>[
      ItemMeta(itemIndex: 0, totalBytes: 4, fields: <FieldMeta>[]),
    ];
  }
}

class _FakeLitdataService extends LitDataService {
  _FakeLitdataService({
    required this.failTimes,
  });

  final int failTimes;
  int calls = 0;

  @override
  Future<FieldPreview> peekFieldFromStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> chunkStream,
  }) async {
    calls += 1;
    await chunkStream.drain<void>();
    if (calls <= failTimes) {
      throw const FormatException('malformed chunk');
    }
    return const FieldPreview(
      previewText: 'ok',
      hexSnippet: '',
      guessedExt: 'txt',
      isBinary: false,
      size: 2,
    );
  }
}

void main() {
  test('MDS raw stream retry succeeds after transient failures', () async {
    final service = RemoteDatasetOpsService();
    final mosaicml = _FakeMosaicmlService(failTimes: 2);
    var openCalls = 0;

    final items = await service.listMdsItemsFromRawStreamWithRetry(
      mosaicml: mosaicml,
      indexBytes: Uint8List(4),
      indexName: 'index.json',
      shardFilename: 'shard.mds',
      maxAttempts: 4,
      openRawStream: () {
        openCalls += 1;
        return Stream<List<int>>.value(const <int>[1, 2, 3]);
      },
    );

    expect(items, hasLength(1));
    expect(mosaicml.calls, equals(3));
    expect(openCalls, equals(3));
  });

  test('MDS raw stream does not retry on non-retryable failure', () async {
    final service = RemoteDatasetOpsService();
    final mosaicml = _FakeMosaicmlService(
        failTimes: 1, retryable: false, errorMessage: 'fatal');
    var openCalls = 0;

    await expectLater(
      () => service.listMdsItemsFromRawStreamWithRetry(
        mosaicml: mosaicml,
        indexBytes: Uint8List(4),
        indexName: 'index.json',
        shardFilename: 'shard.mds',
        maxAttempts: 4,
        openRawStream: () {
          openCalls += 1;
          return Stream<List<int>>.value(const <int>[1, 2, 3]);
        },
      ),
      throwsA(isA<StateError>()),
    );

    expect(mosaicml.calls, equals(1));
    expect(openCalls, equals(1));
  });

  test('LitData preview retries on malformed chunk errors', () async {
    final service = RemoteDatasetOpsService();
    final litdata = _FakeLitdataService(failTimes: 1);
    var openCalls = 0;

    final preview = await service.peekLitdataFieldFromStreamWithRetry(
      litdata: litdata,
      indexBytes: Uint8List(8),
      indexName: 'index.json',
      chunkFilename: 'chunk-0.bin',
      itemIndex: 0,
      fieldIndex: 0,
      maxAttempts: 3,
      openChunkStream: () {
        openCalls += 1;
        return Stream<List<int>>.value(const <int>[9, 8, 7]);
      },
    );

    expect(preview.previewText, equals('ok'));
    expect(litdata.calls, equals(2));
    expect(openCalls, equals(2));
  });

  test('MDS compressed stream forwards cache key with index fingerprint',
      () async {
    final service = RemoteDatasetOpsService();
    final mosaicml = _FakeMosaicmlService(failTimes: 0);
    var openCalls = 0;
    final indexBytes = Uint8List.fromList(const <int>[3, 1, 4, 1, 5, 9]);

    await service.listMdsItemsFromCompressedStreamWithRetry(
      mosaicml: mosaicml,
      indexBytes: indexBytes,
      indexName: 'index.json',
      shardFilename: 'shard-000.mds.zstd',
      compressedShardCacheKey:
          'remote-mds-zstd:ssh:host:train/shard-000.mds.zstd',
      maxAttempts: 2,
      openCompressedStream: (maxBytes) {
        openCalls += 1;
        expect(maxBytes == null || maxBytes > 0, isTrue);
        return Stream<List<int>>.value(const <int>[1, 2, 3]);
      },
    );

    expect(mosaicml.compressedCalls, equals(1));
    expect(openCalls, equals(1));
    expect(
      mosaicml.lastDecodedShardCacheKey,
      startsWith(
        'remote-mds-zstd:ssh:host:train/shard-000.mds.zstd'
        '|index.json|shard-000.mds.zstd|6|',
      ),
    );
  });

  test('MDS compressed stream still builds cache key when base key is null',
      () async {
    final service = RemoteDatasetOpsService();
    final mosaicml = _FakeMosaicmlService(failTimes: 0);
    final indexBytes = Uint8List.fromList(const <int>[3, 1, 4, 1, 5, 9]);

    await service.listMdsItemsFromCompressedStreamWithRetry(
      mosaicml: mosaicml,
      indexBytes: indexBytes,
      indexName: 'index.json',
      shardFilename: 'shard-000.mds.zstd',
      maxAttempts: 2,
      openCompressedStream: (maxBytes) {
        return Stream<List<int>>.value(const <int>[1, 2, 3]);
      },
    );

    expect(mosaicml.compressedCalls, equals(1));
    expect(
      mosaicml.lastDecodedShardCacheKey,
      startsWith('index-only|index.json|shard-000.mds.zstd|6|'),
    );
  });

  test('MDS compressed stream list retries with increasing scan bytes',
      () async {
    final service = RemoteDatasetOpsService();
    final mosaicml = _FakeMosaicmlService(failTimes: 2);
    final scanBytesHistory = <int?>[];
    final indexBytes = Uint8List.fromList(const <int>[3, 1, 4, 1, 5, 9]);

    await service.listMdsItemsFromCompressedStreamWithRetry(
      mosaicml: mosaicml,
      indexBytes: indexBytes,
      indexName: 'index.json',
      shardFilename: 'shard-000.mds.zstd',
      maxAttempts: 4,
      initialCompressedBytes: 1024,
      openCompressedStream: (maxBytes) {
        scanBytesHistory.add(maxBytes);
        return Stream<List<int>>.value(const <int>[1, 2, 3]);
      },
    );

    expect(mosaicml.compressedCalls, equals(3));
    expect(scanBytesHistory, equals(<int?>[1024, 2048, 4096]));
  });
}
