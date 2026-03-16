import 'dart:convert';
import 'dart:io';

import 'package:dataset_inspector/services/dataset_inspector_api_server.dart';
import 'package:dataset_inspector/state/viewer_state.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:path/path.dart' as p;

class _ApiCall {
  _ApiCall(
    this.statusCode,
    this.payload,
    this.body,
    this.headers,
  );

  final int statusCode;
  final Map<String, dynamic> payload;
  final String body;
  final Map<String, List<String>> headers;

  String? header(String name) {
    final values = headers[name.toLowerCase()];
    if (values == null || values.isEmpty) return null;
    return values.first;
  }
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
    String? contentType,
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
      request.headers.set(
        HttpHeaders.contentTypeHeader,
        contentType ??
            request.headers.value(HttpHeaders.contentTypeHeader) ??
            'application/json',
      );
      request.write(body is String ? body : jsonEncode(body));
    }

    final response = await request.close();
    final rawBody = await utf8.decoder.bind(response).join();
    final responseHeaders = <String, List<String>>{};
    response.headers.forEach((name, values) {
      responseHeaders[name.toLowerCase()] = List<String>.from(values);
    });
    final decoded = rawBody.trim().isEmpty
        ? <String, dynamic>{}
        : Map<String, dynamic>.from(
            jsonDecode(rawBody) as Map<String, dynamic>);
    return _ApiCall(response.statusCode, decoded, rawBody, responseHeaders);
  }

  void close() {
    _httpClient.close(force: true);
  }
}

Map<String, dynamic> _normalizeJsonBody(String raw) {
  final decoded = raw.trim().isEmpty
      ? <String, dynamic>{}
      : (jsonDecode(raw) as Map<dynamic, dynamic>);
  return Map<String, dynamic>.from(decoded);
}

Future<Directory> _createLocalDirectory(
  String prefix,
  Map<String, String> files,
) async {
  final directory = await Directory.systemTemp.createTemp(prefix);
  for (final entry in files.entries) {
    final file = File(p.join(directory.path, entry.key));
    await file.create(recursive: true);
    await file.writeAsString(entry.value);
  }
  return directory;
}

Future<int> _pickUnusedPort() async {
  final socket = await ServerSocket.bind('127.0.0.1', 0);
  final port = socket.port;
  await socket.close();
  return port;
}

void main() {
  late ViewerState state;
  late Directory datasetDirA;
  late Directory datasetDirB;
  late String datasetAId;
  late String datasetBId;
  late DatasetInspectorApiServer apiServer;
  late _ApiClient apiClient;
  late List<String> openedIds;

  Future<void> assertSuccess(
    _ApiCall response, {
    int statusCode = HttpStatus.ok,
  }) async {
    expect(response.statusCode, equals(statusCode));
    expect(response.payload['ok'], isTrue);
  }

  Future<void> assertError(
    _ApiCall response,
    String expectedCode,
  ) async {
    expect(response.statusCode, equals(HttpStatus.badRequest));
    expect(response.payload['ok'], isFalse);
    expect(response.payload['error'], isA<Map>());
    expect(response.payload['error']['code'], equals(expectedCode));
  }

  setUpAll(() async {
    SharedPreferences.setMockInitialValues(<String, Object>{});
    state = ViewerState();

    datasetDirA = await _createLocalDirectory('api-ds-a-', <String, String>{
      'readme.txt': 'alpha',
      'metadata.json': '{"foo": 1}',
      p.join('nested', 'payload.bin'): 'binary',
      'raw.mystery': 'payload with unknown extension',
    });

    datasetDirB = await _createLocalDirectory('api-ds-b-', <String, String>{
      'config.json': '{"ok": true}',
      'notes.md': 'hello world',
      p.join('samples', 'shard.bin'): 's',
    });

    final addedA = await state.addSource(datasetDirA.path, recordRecent: false);
    final addedB = await state.addSource(datasetDirB.path, recordRecent: false);
    expect(addedA, isTrue);
    expect(addedB, isTrue);
    expect(state.openedDatasets, hasLength(2));

    datasetAId = state.openedDatasets[0].id;
    datasetBId = state.openedDatasets[1].id;
    openedIds = <String>[datasetAId, datasetBId];

    final port = await _pickUnusedPort();
    apiServer = DatasetInspectorApiServer(
      state: state,
      host: '127.0.0.1',
      port: port,
      defaultConcurrency: 32,
    );
    final actualPort = await apiServer.start();
    apiClient = _ApiClient(host: '127.0.0.1', port: actualPort);
  });

  tearDownAll(() async {
    await apiServer.stop();
    state.dispose();
    apiClient.close();
    if (await datasetDirA.exists()) {
      await datasetDirA.delete(recursive: true);
    }
    if (await datasetDirB.exists()) {
      await datasetDirB.delete(recursive: true);
    }
  });

  test('GET / returns service routes and API metadata', () async {
    final response = await apiClient.request('GET', '/');

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    expect(data['data']['routes'], isNotEmpty);
    expect(
      data['data']['routes'],
      containsAll(<String>[
        'GET /health',
        'GET /api/v1/opened',
        'POST /api/v1/opened',
        'GET /api/v1/opened/{datasetId}',
        'POST /api/v1/opened/batch',
      ]),
    );
    expect(data['meta']['path'], '/');
    expect(data['meta']['method'], 'GET');
    expect(data['apiVersion'], isNotEmpty);
  });

  test('GET /health returns ready status', () async {
    final response = await apiClient.request('GET', '/health');

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    expect(data['data']['status'], equals('ready'));
    expect(data['data']['service'], equals('dataset-inspector'));
  });

  test('OPTIONS /api/v1/opened returns allowed methods', () async {
    final response = await apiClient.request('OPTIONS', '/api/v1/opened');

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    expect(data['meta']['allowedMethods'], equals(['GET', 'POST', 'OPTIONS']));
  });

  test('OPTIONS / returns generic options contract for unknown path', () async {
    final response = await apiClient.request('OPTIONS', '/');

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    expect(data['meta']['path'], equals('/'));
    expect(data['meta']['allowedMethods'], isNotEmpty);
    expect(data['meta']['method'], equals('OPTIONS'));
    expect(data['meta']['service'], equals('dataset-inspector'));
  });

  test('GET /api/v1/opened supports projection and list metadata', () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'includeDetails': 'false',
        'concurrency': '4',
        'fields': 'id,label',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final meta = Map<String, dynamic>.from(data['meta'] as Map);
    final items = List<Map<String, dynamic>>.from(
      (data['data']['datasets'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );

    expect(meta['count'], equals(2));
    expect(meta['requested'], equals(2));
    expect(meta['returned'], equals(2));
    expect(meta['concurrency'], equals(4));
    expect(meta['includeDetails'], isFalse);
    expect(items, hasLength(2));
    for (final item in items) {
      expect(item.keys.toSet(), equals({'id', 'label'}));
      expect(item['id'], isNotEmpty);
    }
  });

  test('GET /api/v1/opened defaults concurrency to server default', () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'includeDetails': 'false',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    expect(data['meta']['concurrency'], equals(32));
    expect(data['meta']['includeDetails'], isFalse);
  });

  test('GET /api/v1/opened with missing includeMissing flag on batch ids',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'ids': '${openedIds[0]},ghost-dataset-id',
        'includeMissing': 'true',
        'fields': 'id,details',
        'concurrency': '2',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final meta = Map<String, dynamic>.from(data['meta'] as Map);
    final payload = Map<String, dynamic>.from(data['data'] as Map);

    expect(meta['requested'], equals(2));
    expect(meta['returned'], equals(1));
    expect(payload['missing'], equals(<String>['ghost-dataset-id']));
    final datasets = List<Map<String, dynamic>>.from(
      (payload['datasets'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );
    expect(datasets, hasLength(1));
    expect(datasets.first['id'], equals(openedIds[0]));
    final details = Map<String, dynamic>.from(datasets.first['details'] as Map);
    expect(details['kind'], equals('localDirectory'));
  });

  test('GET /api/v1/opened accepts datasetIds and idList aliases', () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'datasetIds': openedIds[0],
        'idList': 'ghost-id',
        'fields': 'id,details',
        'includeMissing': 'true',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final payload = Map<String, dynamic>.from(data['data'] as Map);
    final meta = Map<String, dynamic>.from(data['meta'] as Map);
    final datasets = List<Map<String, dynamic>>.from(
      (payload['datasets'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );

    expect(meta['requested'], equals(2));
    expect(meta['returned'], equals(1));
    expect(payload['missing'], equals(<String>['ghost-id']));
    expect(datasets, hasLength(1));
    expect(datasets.first['id'], equals(openedIds[0]));
  });

  test('GET /api/v1/opened deduplicates duplicated ids in batch mode',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'ids': '${openedIds[0]},${openedIds[0]},${openedIds[0]}',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final payload = Map<String, dynamic>.from(data['data'] as Map);
    final meta = Map<String, dynamic>.from(data['meta'] as Map);
    final datasets = List<Map<String, dynamic>>.from(
      (payload['datasets'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );

    expect(meta['requested'], equals(1));
    expect(meta['returned'], equals(1));
    expect(datasets, hasLength(1));
    expect(datasets.first['id'], equals(openedIds[0]));
  });

  test('GET /api/v1/opened with includeMissing=false omits missing payload',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'ids': '${openedIds[0]},ghost-id',
        'includeMissing': 'false',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final payload = Map<String, dynamic>.from(data['data'] as Map);
    final meta = Map<String, dynamic>.from(data['meta'] as Map);

    expect(meta['requested'], equals(2));
    expect(meta['returned'], equals(1));
    expect(payload['missing'], isNull);
    expect(meta.containsKey('missing'), isFalse);
    expect(payload['datasets'], hasLength(1));
  });

  test('POST /api/v1/opened without ids behaves as list API', () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'includeDetails': false,
        'fields': 'id,label',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final meta = Map<String, dynamic>.from(data['meta'] as Map);
    final payload = Map<String, dynamic>.from(data['data'] as Map);
    final datasets = List<Map<String, dynamic>>.from(
      (payload['datasets'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );

    expect(meta['count'], equals(2));
    expect(datasets, hasLength(2));
    for (final item in datasets) {
      expect(item.keys.toSet(), equals({'id', 'label'}));
    }
  });

  test(
      'POST /api/v1/opened with ids queries body and missing IDs returns batch semantics',
      () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'ids': '${openedIds[0]},${openedIds[1]}',
      },
      body: <String, dynamic>{
        'ids': <String>[openedIds[0]],
        'datasetIds': <String>['ghost-dataset-id'],
        'fields': <String>['id', 'status', 'uniform'],
        'includeMissing': false,
        'concurrency': 2,
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final payload = Map<String, dynamic>.from(data['data'] as Map);
    final meta = Map<String, dynamic>.from(data['meta'] as Map);
    final datasets = List<Map<String, dynamic>>.from(
      (payload['datasets'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );

    expect(meta['requested'], equals(3));
    expect(meta['returned'], equals(2));
    expect(payload['missing'], isNull);
    expect(datasets.map((item) => item['id']), containsAll(openedIds));
  });

  test('POST /api/v1/opened parses semicolon and comma mixed ids', () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'ids': '${openedIds[0]};${openedIds[1]},ghost-id',
        'fields': <String>['id'],
        'includeMissing': true,
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final payload = Map<String, dynamic>.from(data['data'] as Map);
    final meta = Map<String, dynamic>.from(data['meta'] as Map);
    final datasets = List<Map<String, dynamic>>.from(
      (payload['datasets'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );

    expect(meta['requested'], equals(3));
    expect(meta['returned'], equals(2));
    expect(payload['missing'], equals(<String>['ghost-id']));
    expect(datasets.map((item) => item['id']), containsAll(openedIds));
  });

  test(
      'POST /api/v1/opened/batch supports includeMissing and fields projection',
      () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'datasetIds': <String>[openedIds[0], openedIds[0], 'ghost-id'],
        'fields': <String>['id'],
        'includeMissing': true,
        'concurrency': 8,
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final payload = Map<String, dynamic>.from(data['data'] as Map);
    final meta = Map<String, dynamic>.from(data['meta'] as Map);
    final datasets = List<Map<String, dynamic>>.from(
      (payload['datasets'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );

    expect(meta['requested'], equals(2));
    expect(meta['returned'], equals(1));
    expect(meta['count'], equals(1));
    expect(payload['missing'], equals(<String>['ghost-id']));
    expect(meta['missing'], equals(1));
    expect(datasets, hasLength(1));
    for (final item in datasets) {
      expect(item.keys.toSet(), equals({'id'}));
    }
  });

  test('POST /api/v1/opened/_batch validates malformed JSON body', () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/_batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: '{"ids":["',
    );

    await assertError(response, 'INVALID_JSON');
  });

  test('POST /api/v1/opened/batch enforces max request body size', () async {
    final hugePayload = 'x' * 1100000;
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: '{"payload":"$hugePayload"}',
    );

    await assertError(response, 'INVALID_JSON');
  });

  test('POST /api/v1/opened/_batch aliases /api/v1/opened/batch', () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/_batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'ids': <String>[openedIds[1]],
        'includeMissing': true,
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final payload = Map<String, dynamic>.from(data['data'] as Map);
    expect(payload['datasets'], hasLength(1));
    expect(payload['datasets'].first['id'], equals(openedIds[1]));
  });

  test(
      'GET /api/v1/opened/{datasetId} returns dataset details for existing dataset',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}',
    );

    await assertSuccess(response);
    final payload = _normalizeJsonBody(response.body);
    final dataset = Map<String, dynamic>.from(payload['data'] as Map);
    final details = Map<String, dynamic>.from(dataset['details'] as Map);

    expect(dataset['id'], equals(openedIds[0]));
    expect(dataset['sourceInput'], equals(datasetDirA.path));
    expect(details['kind'], equals('localDirectory'));
    expect(details['itemCount'], equals(4));
    expect(details['fileCount'], equals(3));
    expect(details['directoryCount'], equals(1));
  });

  test(
      'GET /api/v1/opened/{datasetId} supports includeDetails=false projection',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}',
      queryParameters: <String, String>{
        'includeDetails': 'false',
        'fields': 'id,details',
      },
    );

    await assertSuccess(response);
    final payload = _normalizeJsonBody(response.body);
    final dataset = Map<String, dynamic>.from(payload['data'] as Map);
    final details = Map<String, dynamic>.from(dataset['details'] as Map);

    expect(dataset.keys.toSet(), equals({'id', 'details'}));
    expect(details['kind'], equals('localDirectory'));
    expect(details.containsKey('itemCount'), isFalse);
    expect(details.containsKey('fileCount'), isFalse);
    expect(details.containsKey('directoryCount'), isFalse);
  });

  test('GET /api/v1/opened/{datasetId} for missing dataset returns 404',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent('missing-dataset-id')}',
    );

    expect(response.statusCode, equals(HttpStatus.notFound));
    expect(response.payload['ok'], isFalse);
    expect(response.payload['error']['code'], equals('DATASET_NOT_FOUND'));
  });

  test('POST /api/v1/opened/batch validates empty ids as invalid request',
      () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: const <String, dynamic>{},
    );

    await assertError(response, 'INVALID_REQUEST');
  });

  test('POST /api/v1/opened/batch rejects too many ids', () async {
    final ids = List<String>.generate(257, (index) => 'id-$index');
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'ids': ids,
      },
    );

    await assertError(response, 'TOO_MANY_IDS');
  });

  test('POST /api/v1/opened rejects unsupported content-type', () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'text/plain',
      },
      body: '{"ids":["a"]}',
    );

    expect(response.statusCode, equals(HttpStatus.unsupportedMediaType));
    expect(response.payload['error']['code'], equals('UNSUPPORTED_MEDIA_TYPE'));
  });

  test('POST /api/v1/opened handles JSON body without explicit content-type',
      () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened',
      body: <String, dynamic>{
        'ids': <String>[openedIds[0]],
        'fields': <String>['id'],
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final payload = Map<String, dynamic>.from(data['data'] as Map);
    final datasets = List<Map<String, dynamic>>.from(
      (payload['datasets'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );

    expect(datasets, hasLength(1));
    expect(datasets.first['id'], equals(openedIds[0]));
  });

  test('GET /api/v1/opened validates boolean and concurrency params', () async {
    final boolResponse = await apiClient.request(
      'GET',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'includeDetails': 'maybe',
      },
    );

    await assertError(boolResponse, 'INVALID_BOOLEAN');

    final concurrencyResponse = await apiClient.request(
      'GET',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'concurrency': '999',
      },
    );

    await assertError(concurrencyResponse, 'INVALID_CONCURRENCY');

    final rangeResponse = await apiClient.request(
      'GET',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'concurrency': '0',
      },
    );

    await assertError(rangeResponse, 'INVALID_CONCURRENCY');
  });

  test('POST /api/v1/opened validates ids / fields payload types', () async {
    final idsResponse = await apiClient.request(
      'POST',
      '/api/v1/opened',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'ids': 1,
      },
    );

    await assertError(idsResponse, 'INVALID_IDS');

    final fieldsResponse = await apiClient.request(
      'POST',
      '/api/v1/opened',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'ids': <String>[openedIds[0]],
        'fields': 1,
      },
    );

    await assertError(fieldsResponse, 'INVALID_FIELDS');
  });

  test('OPTIONS/CORS and request-id are wired through responses', () async {
    final response = await apiClient.request(
      'GET',
      '/health',
      headers: <String, String>{
        'x-request-id': 'api-test-request-id',
      },
    );

    await assertSuccess(response);
    expect(response.payload['requestId'], equals('api-test-request-id'));
    expect(response.header('x-request-id'), equals('api-test-request-id'));
    expect(response.header('access-control-allow-origin'), equals('*'));
    expect(response.header('cache-control'), contains('no-store'));
  });

  test('GET /api/v1/opened accepts maximum concurrency', () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened',
      queryParameters: <String, String>{
        'concurrency': '64',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    expect(data['meta']['concurrency'], equals(64));
  });

  test('GET / exposes all current API routes', () async {
    final response = await apiClient.request('GET', '/');

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final routes = List<String>.from(data['data']['routes'] as List);
    expect(routes, hasLength(9));
    expect(
      routes,
      containsAll(
        <String>[
          'GET /health',
          'GET /api/v1/opened',
          'POST /api/v1/opened',
          'GET /api/v1/opened/{datasetId}',
          'GET /api/v1/opened/{datasetId}/field',
          'POST /api/v1/opened/{datasetId}/field',
          'POST /api/v1/opened/field/batch',
          'POST /api/v1/opened/batch',
          'POST /api/v1/opened/_batch',
        ],
      ),
    );
  });

  test(
      'OPTIONS on field routes returns generic allowed methods contract',
      () async {
    final firstResponse = await apiClient.request(
      'OPTIONS',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
    );
    await assertSuccess(firstResponse);
    final firstData = _normalizeJsonBody(firstResponse.body);
    expect(
      firstData['meta']['allowedMethods'],
      equals(<String>['GET', 'POST', 'OPTIONS']),
    );
    expect(
      firstData['meta']['path'],
      equals('/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field'),
    );

    final batchResponse = await apiClient.request(
      'OPTIONS',
      '/api/v1/opened/field/batch',
    );
    await assertSuccess(batchResponse);
    final batchData = _normalizeJsonBody(batchResponse.body);
    expect(
      batchData['meta']['allowedMethods'],
      equals(<String>['GET', 'POST', 'OPTIONS']),
    );
    expect(batchData['meta']['path'], equals('/api/v1/opened/field/batch'));
  });

  test('GET /api/v1/opened/{datasetId}/field validates missing required params',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
    );

    expect(response.statusCode, equals(HttpStatus.badRequest));
    expect(response.payload['error']['code'], equals('INVALID_REQUEST'));
    expect(
      response.payload['error']['details']['field'],
      equals('itemIndex'),
    );
  });

  test('GET /api/v1/opened/{datasetId}/field validates numeric values', () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      queryParameters: <String, String>{
        'itemIndex': 'not-a-number',
        'fieldIndex': '0',
      },
    );

    expect(response.statusCode, equals(HttpStatus.badRequest));
    expect(response.payload['error']['code'], equals('INVALID_INTEGER'));
    expect(
      response.payload['error']['details']['field'],
      equals('itemIndex'),
    );
  });

  test(
      'GET /api/v1/opened/{datasetId}/field validates optional numeric traversal values',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      queryParameters: <String, String>{
        'itemIndex': '-1',
        'fieldIndex': '0',
        'shardName': 'nested',
        'traverse': 'true',
        'traverseOffset': 'bad-offset',
      },
    );

    expect(response.statusCode, equals(HttpStatus.badRequest));
    expect(response.payload['error']['code'], equals('INVALID_INTEGER'));
    expect(
      response.payload['error']['details']['field'],
      equals('traverseOffset'),
    );
  });

  test('GET /api/v1/opened/{datasetId}/field reads localDirectory file by field',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      queryParameters: <String, String>{
        'itemIndex': '0',
        'fieldIndex': '0',
        'shardName': 'readme.txt',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    expect(data['data']['path'], endsWith('readme.txt'));
    expect(data['data']['itemIndex'], equals(0));
    expect(data['data']['fieldIndex'], equals(0));
    expect(data['data']['mode'], equals('localDirectory'));
    expect(data['data']['preview']['previewText'], contains('alpha'));
    expect(data['data']['preview']['guessedExt'], equals('txt'));
  });

  test(
      'GET /api/v1/opened/{datasetId}/field reads nested localDirectory directory items by index',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      queryParameters: <String, String>{
        'itemIndex': '0',
        'fieldIndex': '0',
        'shardName': 'nested',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    expect(data['data']['mode'], equals('localDirectory'));
    expect(data['data']['itemIndex'], equals(0));
    expect(data['data']['fieldIndex'], equals(0));
    expect(data['data']['path'], endsWith('payload.bin'));
    expect(data['data']['preview']['guessedExt'], equals('bin'));
    expect(data['data']['preview']['isBinary'], isTrue);

    final outOfRangeResponse = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      queryParameters: <String, String>{
        'itemIndex': '1',
        'fieldIndex': '0',
        'shardName': 'nested',
      },
    );

    expect(outOfRangeResponse.statusCode, equals(HttpStatus.badRequest));
    expect(outOfRangeResponse.payload['error']['code'], equals('INVALID_REQUEST'));
    expect(
      outOfRangeResponse.payload['error']['details']['itemIndex'],
      equals(1),
    );
  });

  test('GET /api/v1/opened/{datasetId}/field traverses all files in shard when itemIndex=-1',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      queryParameters: <String, String>{
        'itemIndex': '-1',
        'fieldIndex': '0',
        'traverse': 'true',
        'shardName': 'nested',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final traversal = Map<String, dynamic>.from(data['data'] as Map);
    expect(traversal['traversed'], isTrue);
    expect(traversal['ok'], isTrue);
    expect(traversal['itemCount'], equals(1));
    final items = List<Map<String, dynamic>>.from(
      (traversal['items'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );
    expect(items, hasLength(1));
    expect(items.first['preview']['guessedExt'], equals('bin'));
  });

  test(
      'GET /api/v1/opened/{datasetId}/field supports unknown local formats and mixed traversal requests',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      queryParameters: <String, String>{
        'itemIndex': '-1',
        'fieldIndex': '0',
        'traverse': 'true',
        'shardName': '.',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final traversal = Map<String, dynamic>.from(data['data'] as Map);
    expect(traversal['traversed'], isTrue);
    expect(traversal['ok'], isTrue);
    expect(traversal['itemCount'], equals(3));
    final extensions = List<String>.from(
      (traversal['items'] as List)
          .map((item) => Map<String, dynamic>.from(item))
          .map((item) => item['preview']['guessedExt'] as String),
    );
    expect(
      extensions,
      containsAll(<String>['txt', 'json', 'mystery']),
    );
    expect(traversal['errorCode'], equals('SKIPPED_DIRECTORIES'));
  });

  test(
      'POST /api/v1/opened/{datasetId}/field supports allItems alias for non-MDS traversal',
      () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'itemIndex': -1,
        'fieldIndex': 0,
        'allItems': true,
        'shardName': 'nested',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final traversal = Map<String, dynamic>.from(data['data'] as Map);
    expect(traversal['traversed'], isTrue);
    expect(traversal['ok'], isTrue);
    expect(traversal['itemCount'], equals(1));
    expect(
      (traversal['items'] as List),
      hasLength(1),
    );
  });

  test('GET /api/v1/opened/{datasetId}/field supports traversal batch paging',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      queryParameters: <String, String>{
        'itemIndex': '-1',
        'fieldIndex': '0',
        'traverse': 'true',
        'traverseOffset': '1',
        'traverseLimit': '2',
        'shardName': '.',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final traversal = Map<String, dynamic>.from(data['data'] as Map);
    expect(traversal['traversed'], isTrue);
    expect(traversal['offset'], equals(1));
    expect(traversal['limit'], equals(2));
    expect((traversal['items'] as List), hasLength(2));
    expect(traversal['nextOffset'], isNotNull);
    expect(
      (traversal['items'] as List),
      isNotEmpty,
    );
  });

  test(
      'GET /api/v1/opened/{datasetId}/field reads unknown extension file and markdown from second dataset',
      () async {
    final mysteryResponse = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      queryParameters: <String, String>{
        'itemIndex': '0',
        'fieldIndex': '0',
        'shardName': 'raw.mystery',
      },
    );

    await assertSuccess(mysteryResponse);
    final mysteryData = _normalizeJsonBody(mysteryResponse.body);
    expect(mysteryData['data']['preview']['guessedExt'], equals('mystery'));
    expect(
      mysteryData['data']['preview']['previewText'],
      contains('payload with unknown extension'),
    );

    final markdownResponse = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[1])}/field',
      queryParameters: <String, String>{
        'itemIndex': '0',
        'fieldIndex': '0',
        'shardName': 'notes.md',
      },
    );

    await assertSuccess(markdownResponse);
    final markdownData = _normalizeJsonBody(markdownResponse.body);
    expect(markdownData['data']['path'], endsWith('notes.md'));
    expect(markdownData['data']['preview']['guessedExt'], equals('md'));
    expect(
      markdownData['data']['preview']['previewText'],
      contains('hello world'),
    );
  });

  test(
      'POST /api/v1/opened/field/batch traverses multiple shards in parallel with traversal mode',
      () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      queryParameters: <String, String>{
        'concurrency': '64',
      },
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'requests': <dynamic>[
          <String, dynamic>{
            'datasetId': openedIds[0],
            'itemIndex': -1,
            'fieldIndex': 0,
            'traverse': true,
            'shardName': 'nested',
          },
          <String, dynamic>{
            'datasetId': openedIds[1],
            'itemIndex': -1,
            'fieldIndex': 0,
            'traverse': true,
            'shardName': 'samples',
          },
        ],
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    expect(data['meta']['concurrency'], equals(64));
    expect(data['meta']['requested'], equals(2));
    final requests = List<Map<String, dynamic>>.from(
      (data['data']['requests'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );
    expect(requests, hasLength(2));
    final traversalRequests = requests.where((entry) => entry['data'] is Map);
    expect(traversalRequests.length, equals(2));
    for (final entry in requests) {
      expect(entry['ok'], isTrue);
      final traversal = Map<String, dynamic>.from(entry['data'] as Map);
      expect(traversal['traversed'], isTrue);
      expect(traversal['itemCount'], equals(1));
      expect(
        (traversal['items'] as List),
        hasLength(1),
      );
    }
  });

  test(
      'POST /api/v1/opened/field/batch keeps mixed traversal and direct field requests in one batch',
      () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      queryParameters: <String, String>{
        'concurrency': '64',
      },
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'requests': <dynamic>[
          <String, dynamic>{
            'datasetId': openedIds[0],
            'itemIndex': -1,
            'fieldIndex': 0,
            'traverse': true,
            'shardName': 'nested',
          },
          <String, dynamic>{
            'datasetId': openedIds[0],
            'itemIndex': 0,
            'fieldIndex': 0,
            'shardName': 'raw.mystery',
          },
          <String, dynamic>{
            'datasetId': openedIds[1],
            'itemIndex': -1,
            'fieldIndex': 0,
            'traverse': true,
            'shardName': 'samples',
          },
        ],
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final requests = List<Map<String, dynamic>>.from(
      (data['data']['requests'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );

    expect(data['meta']['concurrency'], equals(64));
    expect(data['meta']['requested'], equals(3));
    expect(requests, hasLength(3));
    expect(requests.where((item) => item['ok'] == true), hasLength(3));

    final traversalResults = requests
        .map((item) => item['data'])
        .whereType<Map<String, dynamic>>()
        .where((data) => data['traversed'] == true)
        .toList();
    expect(traversalResults, hasLength(2));
    for (final traversal in traversalResults) {
      expect(traversal['itemCount'], isPositive);
      expect(traversal['items'], isList);
    }

    final directEntries = requests
        .map((item) => item['data'])
        .whereType<Map<String, dynamic>>()
        .where((entry) => entry['traversed'] != true)
        .toList();
    expect(directEntries, hasLength(1));
    expect(directEntries.first['path'], endsWith('raw.mystery'));
  });

  test('GET /api/v1/opened/{datasetId}/field validates non-MDS field constraints',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      queryParameters: <String, String>{
        'itemIndex': '1',
        'fieldIndex': '0',
        'shardName': 'readme.txt',
      },
    );

    expect(response.statusCode, equals(HttpStatus.badRequest));
    expect(response.payload['error']['code'], equals('INVALID_REQUEST'));
    expect(
      response.payload['error']['details']['itemIndex'],
      equals(1),
    );
  });

  test('POST /api/v1/opened/{datasetId}/field rejects unsupported media type', () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'text/plain',
      },
      body: <String, dynamic>{
        'itemIndex': 0,
        'fieldIndex': 0,
        'shardName': 'readme.txt',
      },
    );

    expect(response.statusCode, equals(HttpStatus.unsupportedMediaType));
    expect(response.payload['error']['code'], equals('UNSUPPORTED_MEDIA_TYPE'));
  });

  test('POST /api/v1/opened/{datasetId}/field validates body and size', () async {
    final malformedResponse = await apiClient.request(
      'POST',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: '{"itemIndex":0',
    );
    expect(malformedResponse.statusCode, equals(HttpStatus.badRequest));
    expect(malformedResponse.payload['error']['code'], equals('INVALID_JSON'));

    final hugePayload = 'x' * 1100000;
    final largeBodyResponse = await apiClient.request(
      'POST',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: '{"payload":"$hugePayload"}',
    );
    expect(largeBodyResponse.statusCode, equals(HttpStatus.badRequest));
    expect(largeBodyResponse.payload['error']['code'], equals('INVALID_JSON'));
  });

  test('POST /api/v1/opened/{datasetId}/field validates unsupported request methods',
      () async {
    final response = await apiClient.request(
      'PUT',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
    );

    expect(response.statusCode, equals(HttpStatus.methodNotAllowed));
    expect(response.payload['error']['code'], equals('METHOD_NOT_ALLOWED'));
    expect(
      response.payload['error']['details']['allowed'],
      equals(<String>['GET', 'POST']),
    );
  });

  test('POST /api/v1/opened/field/batch validates required requests payload',
      () async {
    final emptyResponse = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: const <String, dynamic>{},
    );
    await assertError(emptyResponse, 'INVALID_REQUEST');

    final malformedResponse = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: '{"requests":[',
    );
    expect(malformedResponse.statusCode, equals(HttpStatus.badRequest));
    expect(malformedResponse.payload['error']['code'], equals('INVALID_JSON'));
  });

  test('POST /api/v1/opened/field/batch accepts legacy single request payload', () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'datasetId': openedIds[0],
        'itemIndex': 0,
        'fieldIndex': 0,
        'shardName': 'readme.txt',
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    expect(data['meta']['requested'], equals(1));
    expect(data['meta']['returned'], equals(1));
    final requests = List<Map<String, dynamic>>.from(
      (data['data']['requests'] as List).map((item) => Map<String, dynamic>.from(item)),
    );
    expect(requests, hasLength(1));
    expect(requests.first['ok'], isTrue);
    expect(requests.first['datasetId'], equals(openedIds[0]));
    expect(requests.first['data']['preview']['previewText'], contains('alpha'));
  });

  test('POST /api/v1/opened/field/batch returns per-item errors for mixed batch', () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'requests': <dynamic>[
          <String, dynamic>{'datasetId': 'missing-dataset-id'},
          1,
          <String, dynamic>{
            'datasetId': openedIds[0],
            'itemIndex': 'abc',
            'fieldIndex': 0,
            'shardName': 'readme.txt',
          },
          <String, dynamic>{
            'id': openedIds[0],
            'itemIndex': 0,
            'fieldIndex': 1,
            'shardName': 'readme.txt',
          },
          <String, dynamic>{
            'datasetId': openedIds[1],
            'itemIndex': 0,
            'fieldIndex': 0,
            'shardName': 'notes.md',
          },
          <String, dynamic>{
            'datasetId': openedIds[0],
            'itemIndex': 0,
            'fieldIndex': 0,
            'shardName': 'nested',
          },
          <String, dynamic>{
            'datasetId': openedIds[1],
            'itemIndex': 0,
            'fieldIndex': 1,
            'shardName': 'notes.md',
          },
        ],
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final requests = List<Map<String, dynamic>>.from(
      (data['data']['requests'] as List).map((item) => Map<String, dynamic>.from(item)),
    );
    expect(data['meta']['requested'], equals(7));
    expect(requests, hasLength(7));
    expect(requests[0]['error']['code'], equals('DATASET_NOT_FOUND'));
    expect(requests[0]['error']['details']['datasetId'], equals('missing-dataset-id'));
    expect(requests[1]['ok'], isFalse);
    expect(requests[1]['error']['code'], equals('INVALID_REQUEST'));
    final errors = requests.where((item) => item['ok'] == false).toList();
    expect(errors, hasLength(5));
    expect(
      errors.any((item) => item['error']['code'] == 'INVALID_REQUEST'),
      isTrue,
    );
    expect(
      errors.any((item) => item['error']['code'] == 'DATASET_NOT_FOUND'),
      isTrue,
    );
    expect(
      errors.any(
        (item) => item['error']['details'] != null &&
            item['error']['details']['field'] == 'itemIndex',
      ),
      isTrue,
    );
    expect(
      errors.any(
        (item) => item['error']['details'] != null &&
            item['error']['details']['fieldIndex'] == 1,
      ),
      isTrue,
    );
    final successes = requests.where((item) => item['ok'] == true).toList();
    expect(successes, hasLength(2));
    expect(successes[0]['data']['path'], endsWith('notes.md'));
    expect(successes[1]['data']['path'], endsWith('payload.bin'));
  });

  test('POST /api/v1/opened/field/batch supports custom concurrency and returns it in meta',
      () async {
    final requests = List<Map<String, dynamic>>.generate(
      12,
      (index) => <String, dynamic>{
        'datasetId': index.isEven ? openedIds[0] : openedIds[1],
        'itemIndex': 0,
        'fieldIndex': 0,
        'shardName': index % 3 == 0 ? 'readme.txt' :
                      index % 3 == 1 ? 'notes.md' : 'nested',
      },
    );
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      queryParameters: <String, String>{
        'concurrency': '8',
      },
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'requests': requests,
      },
    );

    await assertSuccess(response);
    final data = _normalizeJsonBody(response.body);
    final requestsData = List<Map<String, dynamic>>.from(
      (data['data']['requests'] as List)
          .map((item) => Map<String, dynamic>.from(item)),
    );
    expect(data['meta']['requested'], equals(12));
    expect(data['meta']['returned'], equals(12));
    expect(data['meta']['concurrency'], equals(8));
    expect(requestsData, hasLength(12));
    expect(
      requestsData.map((item) => item['ok']).every((value) => value == true),
      isTrue,
    );
  });

  test('POST /api/v1/opened/field/batch rejects invalid concurrency value',
      () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      queryParameters: <String, String>{
        'concurrency': '65',
      },
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'application/json',
      },
      body: <String, dynamic>{
        'requests': <dynamic>[
          <String, dynamic>{
            'datasetId': openedIds[0],
            'itemIndex': 0,
            'fieldIndex': 0,
            'shardName': 'readme.txt',
          }
        ],
      },
    );

    await assertError(response, 'INVALID_CONCURRENCY');
  });

  test('POST /api/v1/opened/field/batch rejects unsupported media type',
      () async {
    final response = await apiClient.request(
      'POST',
      '/api/v1/opened/field/batch',
      headers: <String, String>{
        HttpHeaders.contentTypeHeader: 'text/plain',
      },
      body: <String, dynamic>{
        'requests': <dynamic>[
          <String, dynamic>{'datasetId': openedIds[0]},
        ],
      },
    );

    expect(response.statusCode, equals(HttpStatus.unsupportedMediaType));
    expect(response.payload['error']['code'], equals('UNSUPPORTED_MEDIA_TYPE'));
  });

  test('Non-field custom path under /api/v1/opened/{datasetId}/ returns not found',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field/extra',
    );

    expect(response.statusCode, equals(HttpStatus.notFound));
    expect(response.payload['error']['code'], equals('NOT_FOUND'));
  });

  test('Method restrictions are returned as METHOD_NOT_ALLOWED', () async {
    final putResponse = await apiClient.request(
      'PUT',
      '/api/v1/opened',
    );
    expect(putResponse.statusCode, equals(HttpStatus.methodNotAllowed));
    expect(putResponse.payload['error']['code'], equals('METHOD_NOT_ALLOWED'));

    final postByIdResponse = await apiClient.request(
      'POST',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}',
    );
    expect(postByIdResponse.statusCode, equals(HttpStatus.methodNotAllowed));
    expect(postByIdResponse.payload['error']['code'],
        equals('METHOD_NOT_ALLOWED'));
  });

  test(
      'GET /api/v1/opened/{datasetId}/field propagates request id from x-request-id header',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/${Uri.encodeComponent(openedIds[0])}/field',
      headers: <String, String>{
        'x-request-id': 'field-request-id',
      },
      queryParameters: <String, String>{
        'itemIndex': '0',
        'fieldIndex': '0',
        'shardName': 'readme.txt',
      },
    );

    expect(response.payload['requestId'], equals('field-request-id'));
    expect(response.header('x-request-id'), equals('field-request-id'));
    await assertSuccess(response);
  });

  test('POST /api/v1/opened/field/batch/ should 405 on GET and alias remains unknown on malformed path',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/field/batch',
    );
    expect(response.statusCode, equals(HttpStatus.methodNotAllowed));
    expect(response.payload['error']['code'], equals('METHOD_NOT_ALLOWED'));

    final malformedResponse = await apiClient.request(
      'GET',
      '/api/v1/opened/field/batch/extra',
    );
    expect(malformedResponse.statusCode, equals(HttpStatus.notFound));
    expect(malformedResponse.payload['error']['code'], equals('NOT_FOUND'));
  });

  test('POST /api/v1/opened/batch/ should 404 and unknown routes fail clearly',
      () async {
    final response = await apiClient.request(
      'GET',
      '/api/v1/opened/batch/extra',
    );

    expect(response.statusCode, equals(HttpStatus.notFound));
    expect(response.payload['ok'], isFalse);
    expect(response.payload['error']['code'], equals('NOT_FOUND'));
  });
}
