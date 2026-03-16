import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dataset_inspector/models/common.dart';
import 'package:dataset_inspector/models/webdataset.dart';
import 'package:dataset_inspector/models/zenodo.dart';
import 'package:dataset_inspector/services/dataset_inspector_api_server.dart';
import 'package:dataset_inspector/services/http_dataset_service.dart';
import 'package:dataset_inspector/services/mosaicml_service.dart';
import 'package:dataset_inspector/services/webdataset_service.dart';
import 'package:dataset_inspector/services/zenodo_service.dart';
import 'package:dataset_inspector/state/viewer_state.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shared_preferences/shared_preferences.dart';

class _ApiCall {
  _ApiCall(this.statusCode, this.payload, this.body, this.contentType);

  final int statusCode;
  final Map<String, dynamic>? payload;
  final String body;
  final String? contentType;
}

class _ApiClient {
  _ApiClient({
    required this.host,
    required this.port,
  });

  final String host;
  final int port;
  final HttpClient _httpClient = HttpClient();

  Uri _uri(String path, [Map<String, String>? queryParameters]) {
    return Uri(
      scheme: 'http',
      host: host,
      port: port,
      path: path,
      queryParameters: queryParameters,
    );
  }

  Future<_ApiCall> request(
    String method,
    String path, {
    Map<String, String>? queryParameters,
    Object? body,
    Map<String, String>? headers,
  }) async {
    final response = await requestRaw(
      method,
      path,
      queryParameters: queryParameters,
      body: body,
      headers: headers,
    );
    final rawBody = response.body;
    final decoded = rawBody.trim().isEmpty
        ? <String, dynamic>{}
        : Map<String, dynamic>.from(
            jsonDecode(rawBody) as Map<String, dynamic>,
          );
    return _ApiCall(
      response.statusCode,
      decoded,
      rawBody,
      response.contentType,
    );
  }

  Future<_ApiCall> requestRaw(
    String method,
    String path, {
    Map<String, String>? queryParameters,
    Object? body,
    Map<String, String>? headers,
  }) async {
    final request = await _httpClient.openUrl(
      method,
      _uri(path, queryParameters),
    );
    request.headers.set(HttpHeaders.acceptHeader, 'application/json');
    if (headers != null) {
      headers.forEach(request.headers.set);
    }
    if (body != null) {
      request.headers.set(HttpHeaders.contentTypeHeader, 'application/json');
      request.write(body is String ? body : jsonEncode(body));
    }
    final response = await request.close();
    final rawBody = await utf8.decoder.bind(response).join();
    return _ApiCall(
      response.statusCode,
      null,
      rawBody,
      response.headers.contentType?.mimeType,
    );
  }

  void close() {
    _httpClient.close(force: true);
  }
}

class _FakeHttpDatasetService extends HttpDatasetService {
  _FakeHttpDatasetService(this.bytesByUrl);

  final Map<String, Uint8List> bytesByUrl;

  @override
  Future<Uint8List> readBytes({
    required Uri url,
    int? maxBytes,
    HttpStatusCallback? onStatus,
  }) async {
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
    final bytes = bytesByUrl[url.toString()];
    if (bytes == null) {
      return Stream<List<int>>.error(
        FormatException('Missing HTTP stream for $url'),
      );
    }
    if (maxBytes != null && maxBytes > 0 && bytes.length > maxBytes) {
      return Stream<List<int>>.value(bytes.sublist(0, maxBytes));
    }
    return Stream<List<int>>.value(bytes);
  }
}

class _FakeMosaicmlService extends MosaicmlService {
  _FakeMosaicmlService() : super();

  @override
  Future<IndexSummary> loadIndexFromBytes(
    Uint8List indexBytes, {
    String indexName = 'index.json',
  }) async {
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
          chunkSize: 4,
          chunkBytes: 16,
          dim: null,
          exists: true,
        ),
      ],
    );
  }

  @override
  Future<ItemPage> listSamplesPagedFromRawStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String shardFilename,
    required Stream<List<int>> rawStream,
    int offset = 0,
    int length = 200,
  }) async {
    await rawStream.drain<void>();
    final safeOffset = offset < 0 ? 0 : offset;
    final safeLength = length < 1 ? 1 : length;
    const total = 4;
    final start = safeOffset.clamp(0, total).toInt();
    final end = (start + safeLength).clamp(0, total).toInt();
    final items = <ItemMeta>[];
    for (var index = start; index < end; index += 1) {
      items.add(
        const ItemMeta(
          itemIndex: 0,
          totalBytes: 4,
          fields: <FieldMeta>[
            FieldMeta(fieldIndex: 0, size: 4),
          ],
        ),
      );
    }
    for (var i = 0; i < items.length; i += 1) {
      items[i] = ItemMeta(
        itemIndex: start + i,
        totalBytes: 4,
        fields: const <FieldMeta>[
          FieldMeta(fieldIndex: 0, size: 4),
        ],
      );
    }
    return ItemPage(
      offset: start,
      length: safeLength,
      items: items,
      partial: end < total,
      numItemsTotal: total,
    );
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
    await rawStream.drain<void>();
    return FieldPreview(
      previewText: 'mds-$itemIndex-$fieldIndex',
      hexSnippet: '00 01',
      guessedExt: 'txt',
      isBinary: false,
      size: 4,
    );
  }

  @override
  Future<PreparedFileResponse> prepareFieldFileFromRawStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> rawStream,
    bool convertSphereToWav = false,
  }) async {
    await rawStream.drain<void>();
    final dir = await Directory.systemTemp.createTemp('dataset-inspector-api-');
    final file =
        File('${dir.path}/$shardFilename-i$itemIndex-f$fieldIndex.txt');
    const contents = 'mds-file';
    await file.writeAsString(contents, flush: true);
    return PreparedFileResponse(
      path: file.path,
      size: contents.length,
      ext: 'txt',
    );
  }

  @override
  Future<PreparedMediaResponse> prepareAudioPreviewFromRawStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> rawStream,
  }) async {
    await rawStream.drain<void>();
    return PreparedMediaResponse(
      bytes: Uint8List.fromList(<int>[itemIndex, fieldIndex, 2, 3]),
      size: 4,
      ext: 'wav',
    );
  }
}

class _FakeWebdatasetService extends WebdatasetService {
  _FakeWebdatasetService() : super();

  @override
  Future<WdsSampleListResponse> listSamplesFromStream({
    required Stream<List<int>> shardStream,
    required String shardFilename,
    int? offset,
    int? length,
    bool? computeTotal,
  }) async {
    await shardStream.drain<void>();
    final safeOffset = (offset ?? 0).clamp(0, 2).toInt();
    final safeLength = (length ?? 50).clamp(1, 50).toInt();
    const samples = <WdsSampleInfo>[
      WdsSampleInfo(
        sampleIndex: 0,
        key: 'sample-0',
        totalBytes: 10,
        fields: <WdsFieldInfo>[
          WdsFieldInfo(name: 'txt', memberPath: 'sample-0.txt', size: 5),
          WdsFieldInfo(name: 'json', memberPath: 'sample-0.json', size: 5),
        ],
      ),
      WdsSampleInfo(
        sampleIndex: 1,
        key: 'sample-1',
        totalBytes: 10,
        fields: <WdsFieldInfo>[
          WdsFieldInfo(name: 'txt', memberPath: 'sample-1.txt', size: 5),
          WdsFieldInfo(name: 'json', memberPath: 'sample-1.json', size: 5),
        ],
      ),
    ];
    final end = (safeOffset + safeLength).clamp(0, samples.length).toInt();
    final page = safeOffset >= samples.length
        ? const <WdsSampleInfo>[]
        : samples.sublist(safeOffset, end);
    return WdsSampleListResponse(
      offset: safeOffset,
      length: safeLength,
      numSamplesTotal: 2,
      partial: end < samples.length,
      samples: page,
    );
  }

  @override
  Future<FieldPreview> peekMemberFromStream({
    required Stream<List<int>> shardStream,
    required String shardFilename,
    required String memberPath,
  }) async {
    await shardStream.drain<void>();
    return FieldPreview(
      previewText: 'wds:$memberPath',
      hexSnippet: '',
      guessedExt: memberPath.endsWith('.json') ? 'json' : 'txt',
      isBinary: false,
      size: 12,
    );
  }
}

class _FakeZenodoService extends ZenodoService {
  _FakeZenodoService() : super();

  @override
  Future<ZenodoRecordSummary> recordSummary(String input) async {
    return const ZenodoRecordSummary(
      recordId: 123,
      title: 'Zenodo Dataset',
      doi: null,
      doiUrl: null,
      publicationDate: null,
      version: null,
      accessRight: null,
      recordUrl: null,
      creators: <ZenodoCreator>[],
      files: <ZenodoFileSummary>[
        ZenodoFileSummary(
          key: 'notes.txt',
          size: 9,
          checksum: null,
          contentUrl: 'https://example.com/zenodo/notes.txt',
        ),
        ZenodoFileSummary(
          key: 'readme.md',
          size: 12,
          checksum: null,
          contentUrl: 'https://example.com/zenodo/readme.md',
        ),
      ],
    );
  }

  @override
  Future<FieldPreview> peekFile(String contentUrl) async {
    return FieldPreview(
      previewText: 'zenodo:$contentUrl',
      hexSnippet: '',
      guessedExt: contentUrl.endsWith('.md') ? 'md' : 'txt',
      isBinary: false,
      size: 16,
    );
  }
}

Future<int> _pickUnusedPort() async {
  final socket = await ServerSocket.bind('127.0.0.1', 0);
  final port = socket.port;
  await socket.close();
  return port;
}

Map<String, dynamic> _json(String raw) {
  final decoded = raw.trim().isEmpty
      ? <String, dynamic>{}
      : (jsonDecode(raw) as Map<dynamic, dynamic>);
  return Map<String, dynamic>.from(decoded);
}

List<Map<String, dynamic>> _ndjson(String raw) {
  return raw
      .split('\n')
      .map((line) => line.trim())
      .where((line) => line.isNotEmpty)
      .map((line) => Map<String, dynamic>.from(jsonDecode(line) as Map))
      .toList(growable: false);
}

List<int> _u32le(int value) {
  final data = ByteData(4)..setUint32(0, value, Endian.little);
  return data.buffer.asUint8List();
}

Uint8List _buildSingleFieldChunk(String text) {
  final payload = utf8.encode(text);
  final start = 4 + 8;
  final end = start + 4 + payload.length;
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
        'chunk_size': 2,
        'dim': null,
      },
    ],
    'config': <String, dynamic>{
      'compression': null,
      'chunk_size': 2,
      'chunk_bytes': chunkBytes,
      'data_format': <String>['bytes'],
      'data_spec': null,
    },
  };
  return Uint8List.fromList(utf8.encode(jsonEncode(json)));
}

void main() {
  late ViewerState state;
  late DatasetInspectorApiServer apiServer;
  late _ApiClient apiClient;
  var stateInitialized = false;
  var apiServerStarted = false;
  var apiClientCreated = false;
  String? litdataId;
  String? mdsId;
  String? wdsId;
  String? zenodoId;

  Future<void> assertSuccess(_ApiCall response) async {
    expect(response.statusCode, equals(HttpStatus.ok));
    expect(response.payload?['ok'], isTrue);
  }

  setUpAll(() async {
    SharedPreferences.setMockInitialValues(<String, Object>{});

    const litIndexUrl = 'https://example.com/lit/index.json';
    const litChunkName = 'chunk-000.bin';
    const litChunkUrl = 'https://example.com/lit/chunk-000.bin';
    const mdsIndexUrl = 'https://example.com/mds/index.json';
    const mdsShardUrl = 'https://example.com/mds/shard-000.mds';
    const wdsShardUrl = 'https://example.com/wds/00000.tar';

    final litChunkBytes = _buildSingleFieldChunk('hello litdata');
    final litIndexBytes = _buildIndexBytes(
      chunkFilename: litChunkName,
      chunkBytes: litChunkBytes.length,
    );

    final fakeHttp = _FakeHttpDatasetService(
      <String, Uint8List>{
        litIndexUrl: litIndexBytes,
        litChunkUrl: litChunkBytes,
        mdsIndexUrl: Uint8List.fromList(const <int>[1, 2, 3]),
        mdsShardUrl: Uint8List.fromList(const <int>[9, 8, 7, 6]),
        wdsShardUrl: Uint8List.fromList(const <int>[1, 2, 3]),
      },
    );

    state = ViewerState(
      httpDatasets: fakeHttp,
      mosaicml: _FakeMosaicmlService(),
      webdataset: _FakeWebdatasetService(),
      zenodo: _FakeZenodoService(),
    );
    stateInitialized = true;

    expect(
      await state.addSource(litIndexUrl, recordRecent: false),
      isTrue,
    );
    expect(
      await state.addSource(mdsIndexUrl, recordRecent: false),
      isTrue,
    );
    expect(
      await state.addSource(wdsShardUrl, recordRecent: false),
      isTrue,
    );
    expect(
      await state.addSource('https://zenodo.org/records/123',
          recordRecent: false),
      isTrue,
    );
    expect(state.openedDatasets.length, greaterThanOrEqualTo(4));

    for (final ds in state.openedDatasets) {
      if (ds.mode == ViewerMode.litdataIndex && litdataId == null) {
        litdataId = ds.id;
      }
      if (ds.mode == ViewerMode.mdsIndex && mdsId == null) {
        mdsId = ds.id;
      }
      if (ds.mode == ViewerMode.webdatasetDir && wdsId == null) {
        wdsId = ds.id;
      }
      if (ds.mode == ViewerMode.zenodo && zenodoId == null) {
        zenodoId = ds.id;
      }
    }
    expect(litdataId, isNotNull);
    expect(mdsId, isNotNull);
    expect(wdsId, isNotNull);
    expect(zenodoId, isNotNull);

    final port = await _pickUnusedPort();
    apiServer = DatasetInspectorApiServer(
      state: state,
      host: '127.0.0.1',
      port: port,
      defaultConcurrency: 32,
    );
    final actualPort = await apiServer.start();
    apiServerStarted = true;
    apiClient = _ApiClient(host: '127.0.0.1', port: actualPort);
    apiClientCreated = true;
  });

  tearDownAll(() async {
    if (apiServerStarted) {
      await apiServer.stop();
    }
    if (apiClientCreated) {
      apiClient.close();
    }
    if (stateInitialized) {
      state.dispose();
    }
  });

  test('LitData field endpoint supports direct and traversal paging', () async {
    final direct = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(litdataId!)}'
          '/field',
      queryParameters: <String, String>{
        'chunkName': 'chunk-000.bin',
        'itemIndex': '0',
        'fieldIndex': '0',
      },
    );
    await assertSuccess(direct);
    final directData = _json(direct.body);
    expect(directData['data']['mode'], equals('litdataIndex'));
    expect(directData['data']['preview']['previewText'],
        contains('hello litdata'));

    final traversed = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(litdataId!)}'
          '/field',
      queryParameters: <String, String>{
        'chunkName': 'chunk-000.bin',
        'itemIndex': '-1',
        'fieldIndex': '0',
        'traverse': 'true',
        'traverseOffset': '0',
        'traverseLimit': '1',
      },
    );
    await assertSuccess(traversed);
    final traversedData = _json(traversed.body);
    expect(traversedData['data']['traversed'], isTrue);
    expect(traversedData['data']['itemCount'], equals(1));
    expect(traversedData['data']['nextOffset'], isNotNull);
  });

  test('MDS field endpoint supports direct and traversal paging', () async {
    final direct = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(mdsId!)}'
          '/field',
      queryParameters: <String, String>{
        'shardName': 'shard-000.mds',
        'itemIndex': '1',
        'fieldIndex': '0',
      },
    );
    await assertSuccess(direct);
    final directData = _json(direct.body);
    expect(directData['data']['mode'], equals('mdsIndex'));
    expect(directData['data']['preview']['previewText'], equals('mds-1-0'));

    final traversed = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(mdsId!)}'
          '/field',
      queryParameters: <String, String>{
        'shardName': 'shard-000.mds',
        'itemIndex': '-1',
        'fieldIndex': '0',
        'traverse': 'true',
        'traverseOffset': '1',
        'traverseLimit': '2',
      },
    );
    await assertSuccess(traversed);
    final traversedData = _json(traversed.body);
    expect(traversedData['data']['traversed'], isTrue);
    expect(traversedData['data']['offset'], equals(1));
    expect(traversedData['data']['itemCount'], equals(2));
  });

  test('MDS field endpoint supports responseMode=file for direct field access',
      () async {
    final direct = await apiClient.request(
      'POST',
      '/api/v1/opened/${Uri.encodeComponent(mdsId!)}'
          '/field',
      body: <String, dynamic>{
        'shardName': 'shard-000.mds',
        'itemIndex': 1,
        'fieldIndex': 0,
        'responseMode': 'file',
      },
    );
    await assertSuccess(direct);
    final directData = _json(direct.body);
    expect(directData['data']['mode'], equals('mdsIndex'));
    expect(directData['data']['ext'], equals('txt'));
    expect(directData['data']['size'], equals(8));
    expect(
      (directData['data']['path'] as String),
      contains('shard-000.mds-i1-f0'),
    );
  });

  test('MDS extract endpoint streams bounded records as NDJSON', () async {
    final response = await apiClient.requestRaw(
      'POST',
      '/api/v1/opened/${Uri.encodeComponent(mdsId!)}'
          '/extract',
      headers: <String, String>{
        HttpHeaders.acceptHeader: 'application/x-ndjson',
      },
      body: <String, dynamic>{
        'shardName': 'shard-000.mds',
        'offset': 0,
        'limit': 2,
        'audioFieldIndex': 0,
        'textFieldIndex': 0,
        'audioEncoding': 'base64',
        'responseMode': 'stream',
      },
    );

    expect(response.statusCode, equals(HttpStatus.ok));
    expect(response.contentType, equals('application/x-ndjson'));
    final lines = _ndjson(response.body);
    expect(lines, hasLength(4));
    expect(lines.first['type'], equals('meta'));
    expect(lines[1]['type'], equals('record'));
    expect(lines[1]['transcript'], equals('mds-0-0'));
    expect(lines[1]['audio_base64'], equals(base64Encode(<int>[0, 0, 2, 3])));
    expect(lines[2]['type'], equals('record'));
    expect(lines.last['type'], equals('summary'));
    expect(lines.last['record_count'], equals(2));
  });

  test('MDS extract endpoint materializes a bounded manifest and audio files',
      () async {
    final outputDir = await Directory.systemTemp.createTemp(
      'dataset-inspector-extract-',
    );
    addTearDown(() async {
      if (await outputDir.exists()) {
        await outputDir.delete(recursive: true);
      }
    });

    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/${Uri.encodeComponent(mdsId!)}'
          '/extract',
      body: <String, dynamic>{
        'shardName': 'shard-000.mds',
        'offset': 0,
        'limit': 2,
        'audioFieldIndex': 0,
        'textFieldIndex': 0,
        'responseMode': 'materialize',
        'outputDir': outputDir.path,
        'overwrite': true,
      },
    );

    await assertSuccess(response);
    final data = _json(response.body);
    final result = Map<String, dynamic>.from(data['data'] as Map);
    expect(result['recordCount'], equals(2));
    expect(result['manifestPath'], isA<String>());
    expect(File(result['manifestPath'] as String).existsSync(), isTrue);
    expect(Directory(result['audioDir'] as String).existsSync(), isTrue);
    final records = List<Map<String, dynamic>>.from(
      (result['records'] as List)
          .map((item) => Map<String, dynamic>.from(item as Map)),
    );
    expect(records, hasLength(2));
    expect(records.first['transcript'], equals('mds-0-0'));
    expect(File(records.first['audio_path'] as String).existsSync(), isTrue);
  });

  test('WebDataset field endpoint supports traversal and random item field',
      () async {
    final traversed = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(wdsId!)}'
          '/field',
      queryParameters: <String, String>{
        'shardName': '00000.tar',
        'itemIndex': '-1',
        'fieldIndex': '0',
        'traverse': 'true',
        'traverseLimit': '2',
      },
    );
    await assertSuccess(traversed);
    final traversedData = _json(traversed.body);
    expect(traversedData['data']['traversed'], isTrue);
    expect(traversedData['data']['itemCount'], equals(2));

    final direct = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(wdsId!)}'
          '/field',
      queryParameters: <String, String>{
        'shardName': '00000.tar',
        'itemIndex': '0',
        'fieldIndex': '1',
      },
    );
    await assertSuccess(direct);
    final directData = _json(direct.body);
    expect(directData['data']['mode'], equals('webdatasetDir'));
    expect(directData['data']['memberPath'], equals('sample-0.json'));
    expect(directData['data']['preview']['previewText'],
        contains('sample-0.json'));
  });

  test('Zenodo field endpoint supports traversal and random file preview',
      () async {
    final traversed = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(zenodoId!)}'
          '/field',
      queryParameters: <String, String>{
        'itemIndex': '-1',
        'fieldIndex': '0',
        'traverse': 'true',
        'traverseOffset': '0',
        'traverseLimit': '1',
      },
    );
    await assertSuccess(traversed);
    final traversedData = _json(traversed.body);
    expect(traversedData['data']['traversed'], isTrue);
    expect(traversedData['data']['itemCount'], equals(1));
    expect(traversedData['data']['total'], equals(2));

    final direct = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(zenodoId!)}'
          '/field',
      queryParameters: <String, String>{
        'itemIndex': '1',
        'fieldIndex': '0',
      },
    );
    await assertSuccess(direct);
    final directData = _json(direct.body);
    expect(directData['data']['mode'], equals('zenodo'));
    expect(directData['data']['fileKey'], equals('readme.md'));
    expect(directData['data']['preview']['previewText'], contains('readme.md'));
  });

  test('batch field endpoint supports mixed modes with concurrency=64',
      () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      queryParameters: <String, String>{
        'concurrency': '64',
      },
      body: <String, dynamic>{
        'requests': <dynamic>[
          <String, dynamic>{
            'datasetId': litdataId,
            'chunkName': 'chunk-000.bin',
            'itemIndex': 0,
            'fieldIndex': 0,
          },
          <String, dynamic>{
            'datasetId': mdsId,
            'shardName': 'shard-000.mds',
            'itemIndex': 0,
            'fieldIndex': 0,
          },
          <String, dynamic>{
            'datasetId': wdsId,
            'shardName': '00000.tar',
            'itemIndex': 0,
            'fieldIndex': 0,
          },
          <String, dynamic>{
            'datasetId': zenodoId,
            'itemIndex': 0,
            'fieldIndex': 0,
          },
        ],
      },
    );
    await assertSuccess(response);
    final data = _json(response.body);
    expect(data['meta']['concurrency'], equals(64));
    final requests = List<Map<String, dynamic>>.from(
      (data['data']['requests'] as List)
          .map((item) => Map<String, dynamic>.from(item as Map)),
    );
    expect(requests, hasLength(4));
    expect(requests.where((item) => item['ok'] == true), hasLength(4));
    final modes = requests
        .map((item) => Map<String, dynamic>.from(item['data'] as Map)['mode'])
        .toSet();
    expect(
      modes,
      containsAll(
          <String>['litdataIndex', 'mdsIndex', 'webdatasetDir', 'zenodo']),
    );
  });

  test('batch field endpoint supports mds traversal requests at concurrency=64',
      () async {
    final batchRequests = List<Map<String, dynamic>>.generate(
      64,
      (index) => <String, dynamic>{
        'datasetId': mdsId,
        'shardName': 'shard-000.mds',
        'itemIndex': -1,
        'fieldIndex': 0,
        'traverse': true,
        'traverseOffset': 0,
        'traverseLimit': 1,
      },
      growable: false,
    );
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      queryParameters: <String, String>{
        'concurrency': '64',
      },
      body: <String, dynamic>{
        'requests': batchRequests,
      },
    );
    await assertSuccess(response);
    final data = _json(response.body);
    expect(data['meta']['concurrency'], equals(64));
    expect(data['meta']['count'], equals(64));
    final requests = List<Map<String, dynamic>>.from(
      (data['data']['requests'] as List)
          .map((item) => Map<String, dynamic>.from(item as Map)),
    );
    expect(requests, hasLength(64));
    expect(requests.where((item) => item['ok'] == true), hasLength(64));

    final payloads = requests
        .map((item) => Map<String, dynamic>.from(item['data'] as Map))
        .toList(growable: false);
    expect(payloads.every((item) => item['traversed'] == true), isTrue);
    expect(payloads.every((item) => item['itemCount'] == 1), isTrue);
    expect(
      payloads.every((item) => item['mode'] == 'mdsIndex'),
      isTrue,
    );
  });
}
