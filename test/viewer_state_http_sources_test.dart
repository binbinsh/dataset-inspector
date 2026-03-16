import 'dart:convert';
import 'dart:typed_data';

import 'package:dataset_inspector/models/common.dart';
import 'package:dataset_inspector/models/webdataset.dart';
import 'package:dataset_inspector/services/http_dataset_service.dart';
import 'package:dataset_inspector/services/mosaicml_service.dart';
import 'package:dataset_inspector/services/webdataset_service.dart';
import 'package:dataset_inspector/state/viewer_state.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shared_preferences/shared_preferences.dart';

class _FakeHttpDatasetService extends HttpDatasetService {
  _FakeHttpDatasetService(this.bytesByUrl);

  final Map<String, Uint8List> bytesByUrl;
  int readBytesCalls = 0;
  int openReadCalls = 0;
  final List<String> openedUrls = <String>[];

  @override
  Future<Uint8List> readBytes({
    required Uri url,
    int? maxBytes,
    HttpStatusCallback? onStatus,
  }) async {
    readBytesCalls += 1;
    final bytes = bytesByUrl[url.toString()];
    if (bytes == null) {
      throw FormatException('Missing HTTP bytes for $url');
    }
    if (maxBytes != null && maxBytes > 0 && bytes.length > maxBytes) {
      return Uint8List.sublistView(bytes, 0, maxBytes);
    }
    return bytes;
  }

  @override
  Stream<List<int>> openRead({
    required Uri url,
    int? maxBytes,
    HttpStatusCallback? onStatus,
  }) {
    openReadCalls += 1;
    openedUrls.add(url.toString());
    final bytes = bytesByUrl[url.toString()];
    if (bytes == null) {
      return Stream<List<int>>.error(
        FormatException('Missing HTTP stream for $url'),
      );
    }
    return Stream<List<int>>.value(bytes);
  }
}

class _FakeWebdatasetService extends WebdatasetService {
  _FakeWebdatasetService() : super();

  int listSamplesFromStreamCalls = 0;
  final List<String> streamedShards = <String>[];

  @override
  Future<WdsSampleListResponse> listSamplesFromStream({
    required Stream<List<int>> shardStream,
    required String shardFilename,
    int? offset,
    int? length,
    bool? computeTotal,
  }) async {
    listSamplesFromStreamCalls += 1;
    streamedShards.add(shardFilename);
    return WdsSampleListResponse(
      offset: offset ?? 0,
      length: length ?? 50,
      numSamplesTotal: null,
      partial: true,
      samples: const <WdsSampleInfo>[],
    );
  }
}

class _FakeMosaicmlService extends MosaicmlService {
  _FakeMosaicmlService() : super();

  int loadIndexFromBytesCalls = 0;
  int listFromRawCalls = 0;
  int peekFromRawCalls = 0;

  @override
  Future<IndexSummary> loadIndexFromBytes(
    Uint8List indexBytes, {
    String indexName = 'index.json',
  }) async {
    loadIndexFromBytesCalls += 1;
    return const IndexSummary(
      indexPath: 'index.json',
      rootDir: '',
      dataFormat: <String>['bytes'],
      compression: null,
      chunkSize: null,
      chunkBytes: null,
      configRaw: <String, dynamic>{},
      chunks: <ChunkSummary>[
        ChunkSummary(
          filename: 'shard-000.mds',
          path: 'shard-000.mds',
          chunkSize: 1,
          chunkBytes: 4,
          dim: null,
          exists: true,
        ),
      ],
    );
  }

  @override
  Future<List<ItemMeta>> listSamplesFromRawStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String shardFilename,
    required Stream<List<int>> rawStream,
  }) async {
    listFromRawCalls += 1;
    await rawStream.drain<void>();
    return const <ItemMeta>[
      ItemMeta(
        itemIndex: 0,
        totalBytes: 8,
        fields: <FieldMeta>[
          FieldMeta(fieldIndex: 0, size: 8),
        ],
      ),
    ];
  }

  @override
  Future<FieldPreview> peekFieldFromRawStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> rawStream,
  }) async {
    peekFromRawCalls += 1;
    await rawStream.drain<void>();
    return const FieldPreview(
      previewText: 'http mds preview',
      hexSnippet: '',
      guessedExt: 'txt',
      isBinary: false,
      size: 16,
    );
  }
}

List<int> _u32le(int value) {
  final data = ByteData(4)..setUint32(0, value, Endian.little);
  return data.buffer.asUint8List();
}

Uint8List _buildSingleFieldChunk(String text) {
  final payload = utf8.encode(text);
  final start = 4 + 8; // num_items(4) + offsets(2 * 4)
  final end = start + 4 + payload.length; // field_sizes(4) + payload
  return Uint8List.fromList(<int>[
    ..._u32le(1),
    ..._u32le(start),
    ..._u32le(end),
    ..._u32le(payload.length),
    ...payload,
  ]);
}

Uint8List _buildIndexBytes({
  required String chunkFilename,
  required int chunkBytes,
}) {
  final json = <String, dynamic>{
    'chunks': <Map<String, dynamic>>[
      <String, dynamic>{
        'filename': chunkFilename,
        'chunk_bytes': chunkBytes,
        'chunk_size': 1,
        'dim': null,
      },
    ],
    'config': <String, dynamic>{
      'compression': null,
      'chunk_size': 1,
      'chunk_bytes': chunkBytes,
      'data_format': <String>['bytes'],
      'data_spec': null,
    },
  };
  return Uint8List.fromList(utf8.encode(jsonEncode(json)));
}

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  setUp(() async {
    SharedPreferences.setMockInitialValues(<String, Object>{});
  });

  test('routes HTTP parquet URL to local directory virtual file', () async {
    const parquetUrl = 'https://example.com/data.parquet';
    final fakeHttp = _FakeHttpDatasetService(
      <String, Uint8List>{
        parquetUrl: Uint8List.fromList(const <int>[0x50, 0x41, 0x52, 0x31]),
      },
    );
    final state = ViewerState(httpDatasets: fakeHttp);
    addTearDown(state.dispose);

    final added = await state.addSource(parquetUrl, recordRecent: false);

    expect(added, isTrue);
    expect(state.mode, ViewerMode.localDirectory);
    await state.localDirectoryItemsFuture!;
    expect(state.localDirectoryItems, hasLength(1));
    expect(state.localDirectoryItems.single.path, parquetUrl);
  });

  test('opens HTTP WebDataset shard with streaming', () async {
    const shardUrl = 'https://example.com/wds/00000.tar.gz';
    final fakeHttp = _FakeHttpDatasetService(
      <String, Uint8List>{
        shardUrl: Uint8List.fromList(const <int>[1]),
      },
    );
    final fakeWebdataset = _FakeWebdatasetService();
    final state = ViewerState(
      httpDatasets: fakeHttp,
      webdataset: fakeWebdataset,
    );
    addTearDown(state.dispose);

    final added = await state.addSource(shardUrl, recordRecent: false);

    expect(added, isTrue);
    expect(state.mode, ViewerMode.webdatasetDir);
    await state.wdsDirFuture!;
    await state.wdsSamplesFuture!;
    expect(fakeWebdataset.listSamplesFromStreamCalls, equals(1));
    expect(fakeWebdataset.streamedShards, contains('00000.tar.gz'));
    expect(fakeHttp.openReadCalls, greaterThanOrEqualTo(1));
    expect(fakeHttp.openedUrls, contains(shardUrl));
  });

  test('opens HTTP WebDataset directory from manifest with streaming',
      () async {
    const sourceUrl = 'https://example.com/wds/';
    const manifestUrl = 'https://example.com/wds/shards.json';
    const firstShardUrl = 'https://example.com/wds/00000.tar';
    final fakeHttp = _FakeHttpDatasetService(
      <String, Uint8List>{
        manifestUrl: Uint8List.fromList(
          utf8.encode(
            jsonEncode(<String, dynamic>{
              'shards': <String>['00000.tar', '00001.tar.gz'],
            }),
          ),
        ),
        firstShardUrl: Uint8List.fromList(const <int>[1, 2, 3]),
      },
    );
    final fakeWebdataset = _FakeWebdatasetService();
    final state = ViewerState(
      httpDatasets: fakeHttp,
      webdataset: fakeWebdataset,
    );
    addTearDown(state.dispose);

    final added = await state.addSource(sourceUrl, recordRecent: false);

    expect(added, isTrue);
    expect(state.mode, ViewerMode.webdatasetDir);
    await state.wdsDirFuture!;
    await state.wdsSamplesFuture!;
    expect(state.wdsDirSummary?.shards, hasLength(2));
    expect(state.wdsDirSummary?.shards.first.filename, '00000.tar');
    expect(fakeWebdataset.listSamplesFromStreamCalls, equals(1));
    expect(fakeHttp.openReadCalls, greaterThanOrEqualTo(1));
    expect(fakeHttp.openedUrls, contains(firstShardUrl));
  });

  test('opens HTTP LitData index and streams chunk for preview', () async {
    const indexUrl = 'https://example.com/lit/index.json';
    const chunkFilename = 'chunk-000.bin';
    const chunkUrl = 'https://example.com/lit/chunk-000.bin';
    final chunkBytes = _buildSingleFieldChunk('hello http litdata');
    final indexBytes = _buildIndexBytes(
      chunkFilename: chunkFilename,
      chunkBytes: chunkBytes.length,
    );
    final fakeHttp = _FakeHttpDatasetService(
      <String, Uint8List>{
        indexUrl: indexBytes,
        chunkUrl: chunkBytes,
      },
    );
    final state = ViewerState(httpDatasets: fakeHttp);
    addTearDown(state.dispose);

    final added = await state.addSource(indexUrl, recordRecent: false);

    expect(added, isTrue);
    expect(state.mode, ViewerMode.litdataIndex);
    await state.indexFuture!;

    final items = await state.litdataItemsFuture!;
    expect(items, hasLength(1));
    state.selectItem(0, fieldCount: items.first.fields.length);
    state.selectField(0);
    final preview = await state.fieldPreviewFuture!;
    expect(preview.previewText, contains('hello http litdata'));
    expect(fakeHttp.readBytesCalls, greaterThanOrEqualTo(1));
    expect(fakeHttp.openReadCalls, greaterThanOrEqualTo(2));
    expect(fakeHttp.openedUrls, contains(chunkUrl));
  });

  test('opens HTTP LitData directory and streams chunk for preview', () async {
    const sourceUrl = 'https://example.com/lit/';
    const indexUrl = 'https://example.com/lit/index.json';
    const chunkFilename = 'chunk-001.bin';
    const chunkUrl = 'https://example.com/lit/chunk-001.bin';
    final chunkBytes = _buildSingleFieldChunk('hello litdata directory');
    final indexBytes = _buildIndexBytes(
      chunkFilename: chunkFilename,
      chunkBytes: chunkBytes.length,
    );
    final fakeHttp = _FakeHttpDatasetService(
      <String, Uint8List>{
        indexUrl: indexBytes,
        chunkUrl: chunkBytes,
      },
    );
    final state = ViewerState(httpDatasets: fakeHttp);
    addTearDown(state.dispose);

    final added = await state.addSource(sourceUrl, recordRecent: false);

    expect(added, isTrue);
    expect(state.mode, ViewerMode.litdataIndex);
    await state.indexFuture!;

    final items = await state.litdataItemsFuture!;
    expect(items, hasLength(1));
    state.selectItem(0, fieldCount: items.first.fields.length);
    state.selectField(0);
    final preview = await state.fieldPreviewFuture!;
    expect(preview.previewText, contains('hello litdata directory'));
    expect(fakeHttp.openedUrls, contains(chunkUrl));
  });

  test('opens HTTP MDS shard URL in mdsIndex mode with streamed raw shard',
      () async {
    const shardUrl = 'https://example.com/mds/shard-000.mds';
    const indexUrl = 'https://example.com/mds/index.json';
    final fakeHttp = _FakeHttpDatasetService(
      <String, Uint8List>{
        shardUrl: Uint8List.fromList(const <int>[1, 2, 3, 4]),
        indexUrl: Uint8List.fromList(utf8.encode('{}')),
      },
    );
    final fakeMosaic = _FakeMosaicmlService();
    final state = ViewerState(
      httpDatasets: fakeHttp,
      mosaicml: fakeMosaic,
    );
    addTearDown(state.dispose);

    final added = await state.addSource(shardUrl, recordRecent: false);

    expect(added, isTrue);
    expect(state.mode, ViewerMode.mdsIndex);
    await state.indexFuture!;
    final items = await state.mdsItemsFuture!;
    expect(items, hasLength(1));
    expect(items.first.fields, hasLength(1));

    state.selectItem(0, fieldCount: items.first.fields.length);
    state.selectField(0);
    final preview = await state.mdsFieldPreviewFuture!;
    expect(preview.previewText, contains('http mds preview'));

    expect(fakeMosaic.loadIndexFromBytesCalls, greaterThanOrEqualTo(1));
    expect(fakeMosaic.listFromRawCalls, greaterThanOrEqualTo(1));
    expect(fakeMosaic.peekFromRawCalls, greaterThanOrEqualTo(1));
    expect(fakeHttp.readBytesCalls, greaterThanOrEqualTo(1));
    expect(fakeHttp.openReadCalls, greaterThanOrEqualTo(2));
    expect(fakeHttp.openedUrls, contains(shardUrl));
  });

  test('opens HTTP MDS index URL in mdsIndex mode with streaming', () async {
    const indexUrl = 'https://example.com/mds/index.json';
    const shardUrl = 'https://example.com/mds/shard-000.mds';
    final fakeHttp = _FakeHttpDatasetService(
      <String, Uint8List>{
        indexUrl: Uint8List.fromList(const <int>[1, 2, 3]),
        shardUrl: Uint8List.fromList(const <int>[9, 8, 7, 6]),
      },
    );
    final fakeMosaic = _FakeMosaicmlService();
    final state = ViewerState(
      httpDatasets: fakeHttp,
      mosaicml: fakeMosaic,
    );
    addTearDown(state.dispose);

    final added = await state.addSource(indexUrl, recordRecent: false);

    expect(added, isTrue);
    expect(state.mode, ViewerMode.mdsIndex);
    await state.indexFuture!;
    final items = await state.mdsItemsFuture!;
    expect(items, hasLength(1));
    state.selectItem(0, fieldCount: items.first.fields.length);
    state.selectField(0);
    final preview = await state.mdsFieldPreviewFuture!;
    expect(preview.previewText, contains('http mds preview'));

    expect(fakeMosaic.loadIndexFromBytesCalls, greaterThanOrEqualTo(1));
    expect(fakeMosaic.listFromRawCalls, greaterThanOrEqualTo(1));
    expect(fakeMosaic.peekFromRawCalls, greaterThanOrEqualTo(1));
    expect(fakeHttp.openedUrls, contains(shardUrl));
  });
}
