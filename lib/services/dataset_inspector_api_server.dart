import 'dart:convert';
import 'dart:io';

import '../models/common.dart';
import '../models/webdataset.dart';
import '../models/zenodo.dart';
import '../services/local_file_preview_flow_service.dart';
import '../state/viewer_state.dart';
import 'app_logger.dart';
import 'package:path/path.dart' as p;

class DatasetInspectorApiServer {
  static const String _apiVersion = '26.0228.1031';
  static const String _serviceName = 'dataset-inspector';
  static const int _factoryDefaultConcurrency = 64;
  static const int _maxConcurrency = 128;
  static const int _maxBatchIds = 256;
  static const int _maxRequestBytes = 1024 * 1024;
  static const int _maxTraversalLimit = 1024;
  static const int _maxExtractLimit = 2048;

  DatasetInspectorApiServer({
    required ViewerState state,
    required String host,
    required int port,
    int defaultConcurrency = _factoryDefaultConcurrency,
  })  : _state = state,
        _host = host.isEmpty ? '127.0.0.1' : host,
        _port = port <= 0 ? 9292 : port,
        _defaultConcurrency = defaultConcurrency <= 0
            ? _factoryDefaultConcurrency
            : (defaultConcurrency > _maxConcurrency
                ? _maxConcurrency
                : defaultConcurrency);

  final ViewerState _state;
  final String _host;
  final int _port;
  final int _defaultConcurrency;
  final LocalFilePreviewFlowService _localFileFlow =
      const LocalFilePreviewFlowService();
  HttpServer? _server;
  final Map<String, Future<Map<String, dynamic>?>> _inflightInspectByQuery = {};
  int _requestCounter = 0;

  Future<int> start() async {
    _server = await HttpServer.bind(_host, _port);
    _server!.listen(_handleRequest);
    AppLogger.info(
      'Dataset Inspector API started on http://$_host:${_server!.port}',
      tag: 'api',
    );
    return _server!.port;
  }

  Future<void> stop() async {
    final server = _server;
    if (server == null) return;
    await server.close(force: true);
    _server = null;
  }

  Future<void> _handleRequest(HttpRequest request) async {
    final startedAt = DateTime.now().toUtc();
    final requestId = _extractRequestId(request);
    final method = request.method.toUpperCase();
    final segments =
        request.uri.pathSegments.where((s) => s.isNotEmpty).toList();

    try {
      if (method == 'OPTIONS') {
        await _handleOptions(
          request,
          requestId: requestId,
          startedAt: startedAt,
        );
        return;
      }

      if (segments.isEmpty) {
        await _writeSuccessResponse(
          request,
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: method,
          data: <String, dynamic>{
            'service': _serviceName,
            'apiVersion': _apiVersion,
            'routes': const <String>[
              'GET /health',
              'GET /api/v1/opened',
              'POST /api/v1/opened',
              'GET /api/v1/opened/{datasetId}',
              'GET /api/v1/opened/{datasetId}/field',
              'POST /api/v1/opened/{datasetId}/field',
              'POST /api/v1/opened/{datasetId}/extract',
              'POST /api/v1/opened/{datasetId}/scan',
              'POST /api/v1/opened/field/batch',
              'POST /api/v1/opened/batch',
              'POST /api/v1/opened/_batch',
            ],
          },
          meta: const <String, dynamic>{},
        );
        return;
      }

      if (segments.length == 1 && segments[0] == 'health') {
        if (method != 'GET') {
          await _writeMethodNotAllowed(
            request,
            requestId: requestId,
            startedAt: startedAt,
            allowed: const ['GET'],
          );
          return;
        }
        await _writeSuccessResponse(
          request,
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: method,
          data: <String, dynamic>{'status': 'ready', 'service': _serviceName},
          meta: const <String, dynamic>{},
        );
        return;
      }

      if (segments.length >= 3 &&
          segments[0] == 'api' &&
          segments[1] == 'v1' &&
          segments[2] == 'opened') {
        if (segments.length == 3) {
          if (method == 'GET') {
            await _handleListOpened(
              request,
              requestId: requestId,
              startedAt: startedAt,
            );
            return;
          }
          if (method == 'POST') {
            await _handleListOrBatchPost(
              request,
              requestId: requestId,
              startedAt: startedAt,
            );
            return;
          }
          await _writeMethodNotAllowed(
            request,
            requestId: requestId,
            startedAt: startedAt,
            allowed: const ['GET', 'POST'],
          );
          return;
        }

        if (segments.length == 5 &&
            segments[3] == 'field' &&
            segments[4] == 'batch') {
          if (method == 'POST') {
            await _handleBatchFieldAccess(
              request,
              requestId: requestId,
              startedAt: startedAt,
            );
            return;
          }
          await _writeMethodNotAllowed(
            request,
            requestId: requestId,
            startedAt: startedAt,
            allowed: const ['POST'],
          );
          return;
        }

        if (segments.length == 5 && segments[4] == 'field') {
          final datasetId = Uri.decodeComponent(segments[3]);
          if (method == 'GET' || method == 'POST') {
            await _handleFieldAccess(
              request,
              requestId: requestId,
              startedAt: startedAt,
              datasetId: datasetId,
            );
            return;
          }
          await _writeMethodNotAllowed(
            request,
            requestId: requestId,
            startedAt: startedAt,
            allowed: const ['GET', 'POST'],
          );
          return;
        }

        if (segments.length == 5 && segments[4] == 'extract') {
          final datasetId = Uri.decodeComponent(segments[3]);
          if (method == 'POST') {
            await _handleExtract(
              request,
              requestId: requestId,
              startedAt: startedAt,
              datasetId: datasetId,
            );
            return;
          }
          await _writeMethodNotAllowed(
            request,
            requestId: requestId,
            startedAt: startedAt,
            allowed: const ['POST'],
          );
          return;
        }

        if (segments.length == 5 && segments[4] == 'scan') {
          final datasetId = Uri.decodeComponent(segments[3]);
          if (method == 'POST') {
            await _handleScan(
              request,
              requestId: requestId,
              startedAt: startedAt,
              datasetId: datasetId,
            );
            return;
          }
          await _writeMethodNotAllowed(
            request,
            requestId: requestId,
            startedAt: startedAt,
            allowed: const ['POST'],
          );
          return;
        }

        final datasetId = Uri.decodeComponent(segments[3]);
        if (segments.length == 4 &&
            (datasetId == '_batch' || datasetId == 'batch')) {
          if (method == 'POST') {
            await _handleBatchOpened(
              request,
              requestId: requestId,
              startedAt: startedAt,
              defaultIncludeMissing: true,
            );
            return;
          }
          await _writeMethodNotAllowed(
            request,
            requestId: requestId,
            startedAt: startedAt,
            allowed: const ['POST'],
          );
          return;
        }

        if (segments.length == 4 && method == 'GET') {
          await _handleInspectDataset(
            request,
            requestId: requestId,
            startedAt: startedAt,
            datasetId: datasetId,
          );
          return;
        }

        if (segments.length == 4) {
          await _writeMethodNotAllowed(
            request,
            requestId: requestId,
            startedAt: startedAt,
            allowed: const ['GET'],
          );
          return;
        }

        await _writeJson(
          request,
          requestId: requestId,
          statusCode: HttpStatus.notFound,
          body: _buildErrorEnvelope(
            requestId: requestId,
            startedAt: startedAt,
            path: request.uri.path,
            method: method,
            statusCode: HttpStatus.notFound,
            code: 'NOT_FOUND',
            message: 'Unknown opened dataset action.',
            details: <String, dynamic>{'path': request.uri.path},
          ),
        );
        return;
      }

      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.notFound,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: method,
          statusCode: HttpStatus.notFound,
          code: 'NOT_FOUND',
          message: 'Not found.',
          details: <String, dynamic>{'path': request.uri.path},
        ),
      );
    } on _ApiValidationError catch (error) {
      final status = error.code == 'UNSUPPORTED_MEDIA_TYPE'
          ? HttpStatus.unsupportedMediaType
          : HttpStatus.badRequest;
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: status,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: method,
          statusCode: status,
          code: error.code,
          message: error.message,
          details: error.details,
        ),
      );
    } catch (error, stack) {
      AppLogger.error(
        'API request failed',
        tag: 'api',
        error: error,
        stackTrace: stack,
      );
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.internalServerError,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: method,
          statusCode: HttpStatus.internalServerError,
          code: 'INTERNAL_ERROR',
          message: 'Internal server error.',
        ),
      );
    }
  }

  Future<void> _handleOptions(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
  }) async {
    await _writeSuccessResponse(
      request,
      requestId: requestId,
      startedAt: startedAt,
      path: request.uri.path,
      method: 'OPTIONS',
      data: <String, dynamic>{},
      meta: <String, dynamic>{
        'allowedMethods': const ['GET', 'POST', 'OPTIONS']
      },
    );
  }

  Future<void> _handleListOpened(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
    Map<String, dynamic> body = const <String, dynamic>{},
    bool defaultIncludeMissing = false,
  }) async {
    final options = _parseOpenedRequestOptions(
      queryParameters: request.uri.queryParametersAll,
      body: body,
      defaultIncludeDetails: true,
      defaultIncludeMissing: defaultIncludeMissing,
    );

    final ids = _parseIdsFromQuery(request.uri.queryParametersAll,
        body: body, includeBody: false);
    if (ids.isNotEmpty) {
      await _handleBatchOpenedFromIds(
        request,
        requestId: requestId,
        startedAt: startedAt,
        ids: ids,
        includeDetails: options.includeDetails,
        concurrency: options.concurrency,
        fields: options.fields,
        includeMissing: options.includeMissing,
      );
      return;
    }

    try {
      final response = await _state.apiListOpenedDatasets(
        includeDetails: options.includeDetails,
        concurrency: options.concurrency,
      );
      final datasets = <Map<String, dynamic>>[];
      final rawDatasets = response['datasets'];
      if (rawDatasets is List) {
        for (final raw in rawDatasets) {
          if (raw is Map<String, dynamic>) {
            datasets.add(_projectFields(raw, options.fields));
          }
        }
      }
      final responseData = <String, dynamic>{
        'activeDatasetId': response['activeDatasetId'],
        'sourceInput': response['sourceInput'],
        'datasets': datasets,
      };
      await _writeSuccessResponse(
        request,
        requestId: requestId,
        startedAt: startedAt,
        path: request.uri.path,
        method: request.method.toUpperCase(),
        data: responseData,
        meta: <String, dynamic>{
          'requested': datasets.length,
          'returned': datasets.length,
          'count': datasets.length,
          'concurrency': options.concurrency,
          'includeDetails': options.includeDetails,
          'projection': options.fields.isEmpty
              ? null
              : options.fields.toList(growable: false),
        },
      );
    } catch (error, stack) {
      AppLogger.error(
        'Failed to list opened datasets',
        tag: 'api',
        error: error,
        stackTrace: stack,
      );
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.internalServerError,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: request.method.toUpperCase(),
          statusCode: HttpStatus.internalServerError,
          code: 'LIST_FAILED',
          message: error.toString(),
        ),
      );
    }
  }

  Future<void> _handleListOrBatchPost(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
  }) async {
    _validateJsonBodyRequest(request);
    final payload = await _readJsonBody(request);
    final fromBody = <String>[
      ..._extractIds(payload['ids']),
      ..._extractIds(payload['datasetIds']),
    ];
    final fromQuery = _parseIdsFromQuery(request.uri.queryParametersAll,
        body: payload, includeBody: false);
    final ids = _normalizeIds(
      <String>[...fromBody, ...fromQuery],
      field: 'ids',
      allowEmpty: true,
    );

    if (ids.isNotEmpty) {
      final options = _parseOpenedRequestOptions(
        queryParameters: request.uri.queryParametersAll,
        body: payload,
        defaultIncludeDetails: true,
        defaultIncludeMissing: true,
      );
      await _handleBatchOpenedFromIds(
        request,
        requestId: requestId,
        startedAt: startedAt,
        ids: ids,
        includeDetails: options.includeDetails,
        concurrency: options.concurrency,
        fields: options.fields,
        includeMissing: options.includeMissing,
      );
      return;
    }

    await _handleListOpened(
      request,
      requestId: requestId,
      startedAt: startedAt,
      body: payload,
      defaultIncludeMissing: false,
    );
  }

  Future<void> _handleInspectDataset(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
    required String datasetId,
  }) async {
    final options = _parseOpenedRequestOptions(
      queryParameters: request.uri.queryParametersAll,
      defaultIncludeDetails: true,
      defaultIncludeMissing: false,
    );
    final response = await _inspectDatasetWithConcurrency(
      datasetId,
      includeDetails: options.includeDetails,
    );
    if (response == null) {
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.notFound,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: request.method.toUpperCase(),
          statusCode: HttpStatus.notFound,
          code: 'DATASET_NOT_FOUND',
          message: 'Dataset not found.',
          details: <String, dynamic>{'datasetId': datasetId},
        ),
      );
      return;
    }

    await _writeSuccessResponse(
      request,
      requestId: requestId,
      startedAt: startedAt,
      path: request.uri.path,
      method: request.method.toUpperCase(),
      data: _projectFields(response, options.fields),
      meta: <String, dynamic>{
        'requested': 1,
        'returned': 1,
        'count': 1,
        'includeDetails': options.includeDetails,
      },
    );
  }

  Future<void> _handleBatchOpened(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
    bool defaultIncludeMissing = true,
  }) async {
    _validateJsonBodyRequest(request);
    final payload = await _readJsonBody(request);
    final ids = _normalizeIds(
      <String>[
        ..._extractIds(payload['ids']),
        ..._extractIds(payload['datasetIds']),
        ..._parseIdsFromQuery(
          request.uri.queryParametersAll,
          body: payload,
          includeBody: false,
        ),
      ],
      field: 'ids',
      allowEmpty: false,
    );

    if (ids.isEmpty) {
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.badRequest,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: request.method.toUpperCase(),
          statusCode: HttpStatus.badRequest,
          code: 'INVALID_REQUEST',
          message: 'POST /api/v1/opened/batch requires ids.',
          details: const <String, dynamic>{'field': 'ids'},
        ),
      );
      return;
    }

    final options = _parseOpenedRequestOptions(
      queryParameters: request.uri.queryParametersAll,
      body: payload,
      defaultIncludeDetails: true,
      defaultIncludeMissing: defaultIncludeMissing,
    );
    await _handleBatchOpenedFromIds(
      request,
      requestId: requestId,
      startedAt: startedAt,
      ids: ids,
      includeDetails: options.includeDetails,
      concurrency: options.concurrency,
      fields: options.fields,
      includeMissing: options.includeMissing,
    );
  }

  Future<void> _handleBatchOpenedFromIds(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
    required List<String> ids,
    required bool includeDetails,
    required int concurrency,
    Set<String> fields = const <String>{},
    bool includeMissing = false,
  }) async {
    final requested = _normalizeIds(ids, field: 'ids', allowEmpty: false);
    final snapshots = await _runConcurrentTasks<String, Map<String, dynamic>?>(
      items: requested,
      maxConcurrency: concurrency,
      mapper: (id) => _inspectDatasetWithConcurrency(
        id,
        includeDetails: includeDetails,
      ),
    );

    final seen = <String>{};
    final datasets = <Map<String, dynamic>>[];
    for (final snapshot in snapshots) {
      if (snapshot == null) {
        continue;
      }
      final id = snapshot['id'];
      if (id is String && id.isNotEmpty) {
        seen.add(id);
      }
      datasets.add(_projectFields(snapshot, fields));
    }

    final missing = <String>[];
    if (includeMissing) {
      for (final id in requested) {
        if (!seen.contains(id)) {
          missing.add(id);
        }
      }
    }

    final data = <String, dynamic>{
      'datasets': datasets,
      'requested': requested.length,
      'returned': datasets.length,
      'count': datasets.length,
      if (includeMissing) 'missing': missing,
    };

    await _writeSuccessResponse(
      request,
      requestId: requestId,
      startedAt: startedAt,
      path: request.uri.path,
      method: request.method.toUpperCase(),
      data: data,
      meta: <String, dynamic>{
        'requested': requested.length,
        'returned': datasets.length,
        'count': datasets.length,
        'concurrency': concurrency,
        'includeDetails': includeDetails,
        if (includeMissing) 'missing': missing.length,
      },
    );
  }

  Future<void> _handleFieldAccess(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
    required String datasetId,
  }) async {
    final isPost = request.method == 'POST';
    final query = request.uri.queryParametersAll;
    final Map<String, dynamic> payload;
    if (isPost) {
      _validateJsonBodyRequest(request);
      payload = await _readJsonBody(request);
    } else {
      payload = const <String, dynamic>{};
    }
    final fieldOptions = _parseOpenedRequestOptions(
      queryParameters: request.uri.queryParametersAll,
      body: payload,
      defaultIncludeDetails: false,
      defaultIncludeMissing: false,
    );

    final dataset = _findOpenedDataset(datasetId);
    if (dataset == null) {
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.notFound,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: request.method.toUpperCase(),
          statusCode: HttpStatus.notFound,
          code: 'DATASET_NOT_FOUND',
          message: 'Dataset not found.',
          details: <String, dynamic>{'datasetId': datasetId},
        ),
      );
      return;
    }

    final response = await _resolveFieldAccessResponse(
      dataset: dataset,
      payload: payload,
      query: query,
      traversalConcurrency: fieldOptions.concurrency,
    );

    await _writeSuccessResponse(
      request,
      requestId: requestId,
      startedAt: startedAt,
      path: request.uri.path,
      method: request.method.toUpperCase(),
      data: response,
      meta: <String, dynamic>{
        'requested': 1,
        'returned': 1,
        'count': 1,
      },
    );
  }

  Future<void> _handleBatchFieldAccess(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
  }) async {
    _validateJsonBodyRequest(request);
    final payload = await _readJsonBody(request);
    final rawRequests = _extractBatchFieldRequests(payload);
    if (rawRequests.isEmpty) {
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.badRequest,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: request.method.toUpperCase(),
          statusCode: HttpStatus.badRequest,
          code: 'INVALID_REQUEST',
          message: 'POST /api/v1/opened/field/batch requires requests.',
          details: const <String, dynamic>{
            'field': 'requests',
          },
        ),
      );
      return;
    }

    final requested = rawRequests.length;
    final normalized = _normalizeBatchCount(requested);
    if (normalized > _maxBatchIds) {
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.badRequest,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: request.method.toUpperCase(),
          statusCode: HttpStatus.badRequest,
          code: 'TOO_MANY_IDS',
          message: 'Too many field requests in one batch.',
          details: <String, dynamic>{
            'field': 'requests',
            'count': normalized,
            'maxAllowed': _maxBatchIds,
          },
        ),
      );
      return;
    }

    final options = _parseOpenedRequestOptions(
      queryParameters: request.uri.queryParametersAll,
      body: payload,
      defaultIncludeDetails: false,
      defaultIncludeMissing: false,
    );
    final traversalConcurrency =
        _deriveBatchTraversalConcurrency(options.concurrency);

    final indexedRequests = List<MapEntry<int, dynamic>>.generate(
      normalized,
      (index) => MapEntry(index, rawRequests[index]),
    );
    final ordered = await _runConcurrentTasks<MapEntry<int, dynamic>,
        Map<String, dynamic>?>(
      items: indexedRequests,
      maxConcurrency: options.concurrency,
      mapper: (entry) => _resolveFieldBatchRequest(
        entry,
        traversalConcurrency: traversalConcurrency,
      ),
    );

    final results = ordered.cast<Map<String, dynamic>>();

    await _writeSuccessResponse(
      request,
      requestId: requestId,
      startedAt: startedAt,
      path: request.uri.path,
      method: request.method.toUpperCase(),
      data: <String, dynamic>{
        'requests': results,
      },
      meta: <String, dynamic>{
        'requested': requested,
        'returned': results.length,
        'count': results.length,
        'concurrency': options.concurrency,
        'traversalConcurrency': traversalConcurrency,
      },
    );
  }

  Future<void> _handleExtract(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
    required String datasetId,
  }) async {
    _validateJsonBodyRequest(request);
    final payload = await _readJsonBody(request);
    final options = _parseOpenedRequestOptions(
      queryParameters: request.uri.queryParametersAll,
      body: payload,
      defaultIncludeDetails: false,
      defaultIncludeMissing: false,
    );

    final dataset = _findOpenedDataset(datasetId);
    if (dataset == null) {
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.notFound,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: request.method.toUpperCase(),
          statusCode: HttpStatus.notFound,
          code: 'DATASET_NOT_FOUND',
          message: 'Dataset not found.',
          details: <String, dynamic>{'datasetId': datasetId},
        ),
      );
      return;
    }

    final extractRequest = _parseExtractRequest(
      dataset: dataset,
      payload: payload,
      query: request.uri.queryParametersAll,
    );
    if (extractRequest.responseMode == 'stream') {
      await _writeExtractStreamResponse(
        request,
        requestId: requestId,
        startedAt: startedAt,
        dataset: dataset,
        extractRequest: extractRequest,
      );
      return;
    }

    final result = await _extractDatasetSample(
      dataset: dataset,
      extractRequest: extractRequest,
      concurrency: options.concurrency,
    );

    await _writeSuccessResponse(
      request,
      requestId: requestId,
      startedAt: startedAt,
      path: request.uri.path,
      method: request.method.toUpperCase(),
      data: result,
      meta: <String, dynamic>{
        'requested': result['recordCount'],
        'returned': result['recordCount'],
        'count': result['recordCount'],
        'concurrency': options.concurrency,
      },
    );
  }

  Future<void> _handleScan(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
    required String datasetId,
  }) async {
    _validateJsonBodyRequest(request);
    final payload = await _readJsonBody(request);

    final dataset = _findOpenedDataset(datasetId);
    if (dataset == null) {
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.notFound,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: request.method.toUpperCase(),
          statusCode: HttpStatus.notFound,
          code: 'DATASET_NOT_FOUND',
          message: 'Dataset not found.',
          details: <String, dynamic>{'datasetId': datasetId},
        ),
      );
      return;
    }

    final shardName = (payload['shardName'] as String?)?.trim() ?? '';
    final textFieldIndex = (payload['textFieldIndex'] as num?)?.toInt() ?? 4;
    final idFieldIndex = (payload['idFieldIndex'] as num?)?.toInt();
    final audioFieldIndex = (payload['audioFieldIndex'] as num?)?.toInt();

    if (shardName.isEmpty) {
      await _writeJson(
        request,
        requestId: requestId,
        statusCode: HttpStatus.badRequest,
        body: _buildErrorEnvelope(
          requestId: requestId,
          startedAt: startedAt,
          path: request.uri.path,
          method: 'POST',
          statusCode: HttpStatus.badRequest,
          code: 'INVALID_REQUEST',
          message: 'shardName is required.',
          details: const <String, dynamic>{},
        ),
      );
      return;
    }

    final result = await _state.apiScanShardTextFields(
      dataset: dataset,
      shardFilename: shardName,
      textFieldIndex: textFieldIndex,
      idFieldIndex: idFieldIndex,
      audioFieldIndex: audioFieldIndex,
    );

    final records = result.records
        .map(
          (r) => <String, dynamic>{
            'item_index': r.itemIndex,
            'transcript': r.transcript,
            'transcript_chars': r.transcriptChars,
            'utt_id': r.uttId,
            'audio_size': r.audioSize,
          },
        )
        .toList(growable: false);

    await _writeSuccessResponse(
      request,
      requestId: requestId,
      startedAt: startedAt,
      path: request.uri.path,
      method: 'POST',
      data: <String, dynamic>{
        'shardName': result.shardName,
        'totalItems': result.totalItems,
        'scannedCount': records.length,
        'records': records,
      },
      meta: <String, dynamic>{
        'count': records.length,
      },
    );
  }

  Future<Map<String, dynamic>> _extractDatasetSample({
    required LoadedDatasetSource dataset,
    required _ExtractRequest extractRequest,
    required int concurrency,
  }) async {
    final outputDir = await _resolveExtractOutputDir(
      datasetId: dataset.id,
      outputDir: extractRequest.outputDir,
      overwrite: extractRequest.overwrite,
    );
    final audioDir = p.join(outputDir.path, extractRequest.audioDirName);
    await Directory(audioDir).create(recursive: true);

    final normalizedShard = extractRequest.shardName;
    final page = await _listExtractableItemsPage(
      dataset: dataset,
      shardName: normalizedShard,
      offset: extractRequest.offset,
      length: extractRequest.limit,
    );

    final rows = await _runConcurrentTasks<ItemMeta, Map<String, dynamic>>(
      items: page.items,
      maxConcurrency: concurrency.clamp(1, _maxConcurrency),
      mapper: (item) async {
        return _buildMaterializedExtractRecord(
          dataset: dataset,
          shardName: normalizedShard,
          itemIndex: item.itemIndex,
          audioFieldIndex: extractRequest.audioFieldIndex,
          textFieldIndex: extractRequest.textFieldIndex,
          idFieldIndex: extractRequest.idFieldIndex,
          audioDir: audioDir,
          overwrite: extractRequest.overwrite,
        );
      },
    );

    final manifestPath = p.join(outputDir.path, extractRequest.manifestName);
    if (!extractRequest.overwrite && await File(manifestPath).exists()) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Manifest already exists. Set overwrite=true or use a new outputDir.',
        details: <String, dynamic>{'manifestPath': manifestPath},
      );
    }
    final sink = File(manifestPath).openWrite();
    try {
      for (final row in rows) {
        sink.writeln(jsonEncode(row));
      }
    } finally {
      await sink.close();
    }

    return <String, dynamic>{
      'ok': true,
      'datasetId': dataset.id,
      'mode': dataset.mode.name,
      'sourceInput': dataset.sourceInput,
      'shardName': normalizedShard,
      'offset': page.offset,
      'limit': page.length,
      'total': page.numItemsTotal,
      'recordCount': rows.length,
      'outputDir': outputDir.path,
      'audioDir': audioDir,
      'manifestPath': manifestPath,
      'records': rows,
    };
  }

  _ExtractRequest _parseExtractRequest({
    required LoadedDatasetSource dataset,
    required Map<String, dynamic> payload,
    required Map<String, List<String>> query,
  }) {
    final shardName = _pickFirstFieldValue(
          names: const <String>['shardName', 'chunkName', 'path'],
          query: query,
          body: payload,
        ) ??
        dataset.selectedChunkName ??
        dataset.selectedShardName;
    if (shardName == null || shardName.trim().isEmpty) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing required `shardName` for extraction.',
        details: <String, dynamic>{'datasetId': dataset.id},
      );
    }

    final audioFieldIndex = _parseIntFieldValue(
      names: const <String>['audioFieldIndex'],
      query: query,
      body: payload,
      requiredName: 'audioFieldIndex',
      required: true,
    );
    final textFieldIndex = _parseIntFieldValue(
      names: const <String>['textFieldIndex'],
      query: query,
      body: payload,
      requiredName: 'textFieldIndex',
      required: true,
    );
    final idFieldIndex = _parseOptionalIntFieldValue(
      names: const <String>['idFieldIndex'],
      query: query,
      body: payload,
      field: 'idFieldIndex',
    );
    final offset = _parseOptionalIntFieldValue(
          names: const <String>['offset', 'itemOffset'],
          query: query,
          body: payload,
          field: 'offset',
        ) ??
        0;
    final limitRaw = _parseOptionalIntFieldValue(
          names: const <String>['limit', 'length'],
          query: query,
          body: payload,
          field: 'limit',
        ) ??
        32;
    final limit = _normalizeExtractLimit(limitRaw, field: 'limit');
    final overwrite = _parseOptionalBoolFieldValue(
      names: const <String>['overwrite'],
      query: query,
      body: payload,
      field: 'overwrite',
      defaultValue: false,
    );
    final requestedOutputDir = _pickFirstFieldValue(
      names: const <String>['outputDir'],
      query: query,
      body: payload,
    );
    final manifestName = _pickFirstFieldValue(
          names: const <String>['manifestName'],
          query: query,
          body: payload,
        ) ??
        'manifest.jsonl';
    final audioDirName = _pickFirstFieldValue(
          names: const <String>['audioDirName'],
          query: query,
          body: payload,
        ) ??
        'audio';
    final responseMode = _parseExtractResponseMode(
      query: query,
      body: payload,
      defaultValue:
          requestedOutputDir != null && requestedOutputDir.trim().isNotEmpty
              ? 'materialize'
              : 'stream',
    );
    final audioEncoding = _parseExtractAudioEncoding(
      query: query,
      body: payload,
    );

    return _ExtractRequest(
      shardName: shardName.trim(),
      audioFieldIndex: audioFieldIndex,
      textFieldIndex: textFieldIndex,
      idFieldIndex: idFieldIndex,
      offset: offset,
      limit: limit,
      overwrite: overwrite,
      outputDir: requestedOutputDir?.trim(),
      manifestName: manifestName,
      audioDirName: audioDirName,
      responseMode: responseMode,
      audioEncoding: audioEncoding,
    );
  }

  Future<void> _writeExtractStreamResponse(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
    required LoadedDatasetSource dataset,
    required _ExtractRequest extractRequest,
  }) async {
    final page = await _listExtractableItemsPage(
      dataset: dataset,
      shardName: extractRequest.shardName,
      offset: extractRequest.offset,
      length: extractRequest.limit,
    );

    _prepareNdjsonResponse(
      request,
      requestId: requestId,
      contentType: 'application/x-ndjson; charset=utf-8',
    );

    var successCount = 0;
    var errorCount = 0;

    await _writeNdjsonLine(
      request,
      <String, dynamic>{
        'type': 'meta',
        'ok': true,
        'request_id': requestId,
        'api_version': _apiVersion,
        'timestamp': startedAt.toIso8601String(),
        'path': request.uri.path,
        'method': request.method.toUpperCase(),
        'service': _serviceName,
        'dataset_id': dataset.id,
        'source_input': dataset.sourceInput,
        'mode': dataset.mode.name,
        'shard_name': extractRequest.shardName,
        'offset': page.offset,
        'limit': page.length,
        'total': page.numItemsTotal,
        'audio_field_index': extractRequest.audioFieldIndex,
        'text_field_index': extractRequest.textFieldIndex,
        'id_field_index': extractRequest.idFieldIndex,
        'audio_encoding': extractRequest.audioEncoding,
      },
    );

    for (final item in page.items) {
      try {
        final row = await _buildStreamExtractRecord(
          dataset: dataset,
          shardName: extractRequest.shardName,
          itemIndex: item.itemIndex,
          audioFieldIndex: extractRequest.audioFieldIndex,
          textFieldIndex: extractRequest.textFieldIndex,
          idFieldIndex: extractRequest.idFieldIndex,
          audioEncoding: extractRequest.audioEncoding,
        );
        successCount += 1;
        await _writeNdjsonLine(
          request,
          <String, dynamic>{
            'type': 'record',
            ...row,
          },
        );
      } on _ApiValidationError catch (error) {
        errorCount += 1;
        await _writeNdjsonLine(
          request,
          <String, dynamic>{
            'type': 'error',
            'item_index': item.itemIndex,
            'error': <String, dynamic>{
              'code': error.code,
              'message': error.message,
              if (error.details.isNotEmpty) 'details': error.details,
            },
          },
        );
      } catch (error) {
        errorCount += 1;
        await _writeNdjsonLine(
          request,
          <String, dynamic>{
            'type': 'error',
            'item_index': item.itemIndex,
            'error': <String, dynamic>{
              'code': 'INTERNAL_ERROR',
              'message': error.toString(),
            },
          },
        );
      }
    }

    await _writeNdjsonLine(
      request,
      <String, dynamic>{
        'type': 'summary',
        'ok': errorCount == 0,
        'request_id': requestId,
        'dataset_id': dataset.id,
        'record_count': successCount,
        'error_count': errorCount,
        'duration_ms':
            DateTime.now().toUtc().difference(startedAt).inMilliseconds,
      },
    );
    await request.response.close();
  }

  Future<Directory> _resolveExtractOutputDir({
    required String datasetId,
    required String? outputDir,
    required bool overwrite,
  }) async {
    if (outputDir == null || outputDir.trim().isEmpty) {
      return Directory.systemTemp
          .createTemp('dataset_inspector_extract_${datasetId}_');
    }
    final dir = Directory(outputDir.trim());
    if (await dir.exists()) {
      if (!overwrite) {
        final hasEntries = await dir.list().isEmpty.then((empty) => !empty);
        if (hasEntries) {
          throw _ApiValidationError(
            'INVALID_REQUEST',
            'outputDir already exists and is not empty. Set overwrite=true or use a new outputDir.',
            details: <String, dynamic>{'outputDir': dir.path},
          );
        }
      }
    } else {
      await dir.create(recursive: true);
    }
    return dir;
  }

  Future<ItemPage> _listExtractableItemsPage({
    required LoadedDatasetSource dataset,
    required String shardName,
    required int offset,
    required int length,
  }) async {
    if (dataset.mode == ViewerMode.mdsIndex) {
      return _state.apiListMdsItemsPage(
        dataset: dataset,
        shardFilename: shardName,
        offset: offset,
        length: length,
      );
    }
    if (dataset.mode == ViewerMode.localDirectory) {
      final resolvedPath = _resolveLocalDirectoryFieldPath(
        datasetSourceInput: dataset.sourceInput,
        shardPath: shardName,
      );
      if (!_state.isLocalDirectoryMdsShardPath(resolvedPath)) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'Extraction currently supports MDS-backed opened datasets only.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'mode': dataset.mode.name,
            'shardName': shardName,
          },
        );
      }
      return _state.listLocalDirectoryMdsItemsPage(
        resolvedPath,
        offset: offset,
        length: length,
      );
    }
    throw _ApiValidationError(
      'INVALID_REQUEST',
      'Extraction currently supports MDS-backed opened datasets only.',
      details: <String, dynamic>{
        'datasetId': dataset.id,
        'mode': dataset.mode.name,
      },
    );
  }

  Future<PreparedFileResponse> _prepareExtractAudioFile({
    required LoadedDatasetSource dataset,
    required String shardName,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    if (dataset.mode == ViewerMode.mdsIndex) {
      return _state.apiPrepareMdsFieldFile(
        dataset: dataset,
        shardFilename: shardName,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }
    final resolvedPath = _resolveLocalDirectoryFieldPath(
      datasetSourceInput: dataset.sourceInput,
      shardPath: shardName,
    );
    return _state.apiPrepareLocalDirectoryFieldFile(
      path: resolvedPath,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<PreparedMediaResponse> _prepareExtractAudioMedia({
    required LoadedDatasetSource dataset,
    required String shardName,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    if (dataset.mode == ViewerMode.mdsIndex) {
      return _state.apiPrepareMdsFieldAudio(
        dataset: dataset,
        shardFilename: shardName,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }
    final resolvedPath = _resolveLocalDirectoryFieldPath(
      datasetSourceInput: dataset.sourceInput,
      shardPath: shardName,
    );
    return _state.apiPrepareLocalDirectoryFieldAudio(
      path: resolvedPath,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<FieldPreview> _peekExtractTextField({
    required LoadedDatasetSource dataset,
    required String shardName,
    required int itemIndex,
    required int fieldIndex,
  }) async {
    if (dataset.mode == ViewerMode.mdsIndex) {
      return _state.apiPeekMdsField(
        dataset: dataset,
        shardFilename: shardName,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    }
    final resolvedPath = _resolveLocalDirectoryFieldPath(
      datasetSourceInput: dataset.sourceInput,
      shardPath: shardName,
    );
    return _state.peekLocalDirectoryMdsField(
      shardPath: resolvedPath,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
  }

  Future<String> _copyPreparedFile({
    required PreparedFileResponse prepared,
    required String destinationDir,
    required String baseName,
    required bool overwrite,
  }) async {
    final safeBase = baseName.replaceAll(RegExp(r'[^A-Za-z0-9._-]+'), '_');
    final ext = prepared.ext.trim().isEmpty ? 'bin' : prepared.ext.trim();
    final targetPath = p.join(destinationDir, '$safeBase.$ext');
    final source = File(prepared.path);
    if (!await source.exists()) {
      throw _ApiValidationError(
        'INTERNAL_ERROR',
        'Prepared file is missing.',
        details: <String, dynamic>{'path': prepared.path},
      );
    }
    final target = File(targetPath);
    if (await target.exists()) {
      if (!overwrite) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'Target audio file already exists. Set overwrite=true or use a new outputDir.',
          details: <String, dynamic>{'path': targetPath},
        );
      }
      await target.delete();
    }
    await source.copy(targetPath);
    return targetPath;
  }

  Future<Map<String, dynamic>> _buildMaterializedExtractRecord({
    required LoadedDatasetSource dataset,
    required String shardName,
    required int itemIndex,
    required int audioFieldIndex,
    required int textFieldIndex,
    required int? idFieldIndex,
    required String audioDir,
    required bool overwrite,
  }) async {
    final audio = await _prepareExtractAudioFile(
      dataset: dataset,
      shardName: shardName,
      itemIndex: itemIndex,
      fieldIndex: audioFieldIndex,
    );
    final copiedAudioPath = await _copyPreparedFile(
      prepared: audio,
      destinationDir: audioDir,
      baseName: '${itemIndex.toString().padLeft(6, '0')}_audio',
      overwrite: overwrite,
    );
    final transcript = await _peekExtractTextField(
      dataset: dataset,
      shardName: shardName,
      itemIndex: itemIndex,
      fieldIndex: textFieldIndex,
    );
    final uttId = await _peekOptionalExtractIdField(
      dataset: dataset,
      shardName: shardName,
      itemIndex: itemIndex,
      fieldIndex: idFieldIndex,
    );
    return <String, dynamic>{
      'dataset_id': dataset.id,
      'source_input': dataset.sourceInput,
      'mode': dataset.mode.name,
      'shard_name': shardName,
      'item_index': itemIndex,
      'audio_path': copiedAudioPath,
      'audio_ext': audio.ext,
      'audio_size': audio.size,
      'text_field_index': textFieldIndex,
      'audio_field_index': audioFieldIndex,
      'id_field_index': idFieldIndex,
      'utt_id': uttId,
      'transcript': transcript.previewText,
      'transcript_chars': transcript.previewText == null
          ? null
          : transcript.previewText!.runes.length,
    };
  }

  Future<Map<String, dynamic>> _buildStreamExtractRecord({
    required LoadedDatasetSource dataset,
    required String shardName,
    required int itemIndex,
    required int audioFieldIndex,
    required int textFieldIndex,
    required int? idFieldIndex,
    required String audioEncoding,
  }) async {
    final audio = await _prepareExtractAudioMedia(
      dataset: dataset,
      shardName: shardName,
      itemIndex: itemIndex,
      fieldIndex: audioFieldIndex,
    );
    final transcript = await _peekExtractTextField(
      dataset: dataset,
      shardName: shardName,
      itemIndex: itemIndex,
      fieldIndex: textFieldIndex,
    );
    final uttId = await _peekOptionalExtractIdField(
      dataset: dataset,
      shardName: shardName,
      itemIndex: itemIndex,
      fieldIndex: idFieldIndex,
    );
    return <String, dynamic>{
      'dataset_id': dataset.id,
      'source_input': dataset.sourceInput,
      'mode': dataset.mode.name,
      'shard_name': shardName,
      'item_index': itemIndex,
      'audio_ext': audio.ext,
      'audio_size': audio.size,
      'audio_encoding': audioEncoding,
      if (audioEncoding == 'base64') 'audio_base64': base64Encode(audio.bytes),
      'text_field_index': textFieldIndex,
      'audio_field_index': audioFieldIndex,
      'id_field_index': idFieldIndex,
      'utt_id': uttId,
      'transcript': transcript.previewText,
      'transcript_chars': transcript.previewText == null
          ? null
          : transcript.previewText!.runes.length,
    };
  }

  Future<String?> _peekOptionalExtractIdField({
    required LoadedDatasetSource dataset,
    required String shardName,
    required int itemIndex,
    required int? fieldIndex,
  }) async {
    if (fieldIndex == null) return null;
    final idPreview = await _peekExtractTextField(
      dataset: dataset,
      shardName: shardName,
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
    return idPreview.previewText;
  }

  void _prepareNdjsonResponse(
    HttpRequest request, {
    required String requestId,
    required String contentType,
    int statusCode = HttpStatus.ok,
  }) {
    request.response.statusCode = statusCode;
    request.response.headers.set(
      'cache-control',
      'no-store, no-cache, must-revalidate, max-age=0',
    );
    request.response.headers.set('x-request-id', requestId);
    request.response.headers.set('content-type', contentType);
    request.response.headers.set('access-control-allow-origin', '*');
    request.response.headers.set('vary', 'Origin');
    request.response.headers
        .set('access-control-allow-methods', 'GET,POST,OPTIONS');
    request.response.headers.set(
      'access-control-allow-headers',
      'Content-Type,Authorization,Accept,X-Request-ID',
    );
  }

  Future<void> _writeNdjsonLine(
    HttpRequest request,
    Map<String, dynamic> body,
  ) async {
    request.response.write(jsonEncode(body));
    request.response.write('\n');
    await request.response.flush();
  }

  Future<Map<String, dynamic>> _resolveFieldAccessResponse({
    required LoadedDatasetSource dataset,
    required Map<String, dynamic> payload,
    required Map<String, List<String>> query,
    int? traversalConcurrency,
  }) async {
    final resolvedTraversalConcurrency =
        traversalConcurrency ?? _defaultConcurrency;
    return switch (dataset.mode) {
      ViewerMode.huggingface => await _handleHuggingFaceFieldAccess(
          dataset: dataset,
          payload: payload,
          query: query,
        ),
      ViewerMode.litdataIndex ||
      ViewerMode.litdataChunks =>
        await _handleLitdataFieldAccess(
          dataset: dataset,
          payload: payload,
          query: query,
        ),
      ViewerMode.mdsIndex => await _handleMdsFieldAccess(
          dataset: dataset,
          payload: payload,
          query: query,
        ),
      ViewerMode.webdatasetDir => await _handleWebdatasetFieldAccess(
          dataset: dataset,
          payload: payload,
          query: query,
        ),
      ViewerMode.zenodo => await _handleZenodoFieldAccess(
          dataset: dataset,
          payload: payload,
          query: query,
        ),
      ViewerMode.localDirectory => await _handleLocalDirectoryFieldAccess(
          dataset: dataset,
          payload: payload,
          query: query,
          traversalConcurrency: resolvedTraversalConcurrency,
        ),
      _ => throw _ApiValidationError(
          'INVALID_REQUEST',
          'Field access is not supported for dataset mode.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'mode': dataset.mode.name,
          },
        ),
    };
  }

  Future<Map<String, dynamic>> _resolveFieldBatchRequest(
    MapEntry<int, dynamic> requestEntry, {
    required int traversalConcurrency,
  }) async {
    final index = requestEntry.key;
    final rawRequest = requestEntry.value;
    if (rawRequest is! Map) {
      return <String, dynamic>{
        'ok': false,
        'error': <String, dynamic>{
          'code': 'INVALID_REQUEST',
          'message': 'Each request must be a JSON object.',
          'details': <String, dynamic>{'index': index},
        },
      };
    }

    final item = Map<String, dynamic>.from(
      rawRequest.map((key, value) => MapEntry(key.toString(), value)),
    );
    final datasetId = _pickFirstFieldValue(
      names: const <String>['datasetId', 'id'],
      query: const <String, List<String>>{},
      body: item,
    );
    if (datasetId == null || datasetId.trim().isEmpty) {
      return <String, dynamic>{
        'ok': false,
        'error': <String, dynamic>{
          'code': 'INVALID_REQUEST',
          'message': 'Missing datasetId in batch request.',
          'details': <String, dynamic>{'index': index},
        },
      };
    }

    final dataset = _findOpenedDataset(datasetId);
    if (dataset == null) {
      return <String, dynamic>{
        'ok': false,
        'datasetId': datasetId,
        'error': <String, dynamic>{
          'code': 'DATASET_NOT_FOUND',
          'message': 'Dataset not found.',
          'details': <String, dynamic>{'datasetId': datasetId},
        },
      };
    }

    try {
      final fieldResponse = await _resolveFieldAccessResponse(
        dataset: dataset,
        payload: item,
        query: const <String, List<String>>{},
        traversalConcurrency: traversalConcurrency,
      );
      return <String, dynamic>{
        'ok': true,
        'datasetId': datasetId,
        'data': fieldResponse,
      };
    } on _ApiValidationError catch (error) {
      return <String, dynamic>{
        'ok': false,
        'datasetId': datasetId,
        'error': <String, dynamic>{
          'code': error.code,
          'message': error.message,
          'details': error.details,
        },
      };
    } catch (error) {
      return <String, dynamic>{
        'ok': false,
        'datasetId': datasetId,
        'error': <String, dynamic>{
          'code': 'INTERNAL_ERROR',
          'message': 'Failed to resolve field request.',
          'details': <String, dynamic>{
            'datasetId': datasetId,
            'error': error.toString(),
            'type': error.runtimeType.toString(),
          },
        },
      };
    }
  }

  Future<Map<String, dynamic>> _handleHuggingFaceFieldAccess({
    required LoadedDatasetSource dataset,
    required Map<String, dynamic> payload,
    required Map<String, List<String>> query,
  }) async {
    final config = _pickFirstFieldValue(
      names: const <String>['config', 'hfConfig'],
      query: query,
      body: payload,
    );
    final split = _pickFirstFieldValue(
      names: const <String>['split'],
      query: query,
      body: payload,
    );
    final rowIndexRaw = _pickFirstFieldValue(
      names: const <String>['rowIndex', 'row'],
      query: query,
      body: payload,
    );
    final fieldName = _pickFirstFieldValue(
      names: const <String>['fieldName', 'field'],
      query: query,
      body: payload,
    );

    final selectedConfig = _pickFirstFieldValue(
      names: const <String>['selectedHfConfig'],
      query: const <String, List<String>>{},
      body: <String, dynamic>{
        if (dataset.selectedHfConfig != null)
          'selectedHfConfig': dataset.selectedHfConfig,
      },
    );
    final selectedSplit = _pickFirstFieldValue(
      names: const <String>['selectedHfSplit'],
      query: const <String, List<String>>{},
      body: <String, dynamic>{
        if (dataset.selectedHfSplit != null)
          'selectedHfSplit': dataset.selectedHfSplit,
      },
    );

    final normalizedConfig = (config ?? selectedConfig)?.trim();
    final normalizedSplit = (split ?? selectedSplit)?.trim();
    if (normalizedConfig == null || normalizedConfig.isEmpty) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing required `config` for Hugging Face field access.',
        details: <String, dynamic>{'datasetId': dataset.id},
      );
    }
    if (normalizedSplit == null || normalizedSplit.isEmpty) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing required `split` for Hugging Face field access.',
        details: <String, dynamic>{'datasetId': dataset.id},
      );
    }
    final row = _parseIntFieldValue(
      names: const <String>['rowIndex', 'row'],
      query: query,
      body: payload,
      requiredName: 'rowIndex',
      required: true,
    );
    if (fieldName == null || fieldName.isEmpty) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing required `fieldName` for Hugging Face field access.',
        details: <String, dynamic>{'datasetId': dataset.id},
      );
    }

    final result = await _state.huggingfaceOpenField(
      input: dataset.sourceInput,
      config: normalizedConfig,
      split: normalizedSplit,
      rowIndex: row,
      fieldName: fieldName,
      openWithSystem: false,
      openerAppPath: _firstValueAsString(
        names: const <String>['openerAppPath', 'opener'],
        query: query,
        body: payload,
      ),
    );
    return _openLeafResponsePayload(datasetId: dataset.id, response: result);
  }

  Future<Map<String, dynamic>> _handleLitdataFieldAccess({
    required LoadedDatasetSource dataset,
    required Map<String, dynamic> payload,
    required Map<String, List<String>> query,
  }) async {
    final responseMode = _parseFieldResponseMode(
      query: query,
      body: payload,
    );
    final traverseAll = _parseOptionalBoolFieldValue(
      names: const <String>['traverse', 'allItems'],
      query: query,
      body: payload,
      field: 'traverse',
      defaultValue: false,
    );
    final traverseOffset = _parseOptionalIntFieldValue(
      names: const <String>['traverseOffset'],
      query: query,
      body: payload,
      field: 'traverseOffset',
    );
    final traverseLimit = _parseOptionalIntFieldValue(
      names: const <String>['traverseLimit'],
      query: query,
      body: payload,
      field: 'traverseLimit',
    );
    final chunkName = _pickFirstFieldValue(
          names: const <String>['chunkName', 'shardName', 'path'],
          query: query,
          body: payload,
        ) ??
        dataset.selectedChunkName;
    if (chunkName == null || chunkName.trim().isEmpty) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing required `chunkName` for LitData field access.',
        details: <String, dynamic>{'datasetId': dataset.id},
      );
    }
    final itemIndex = _parseIntFieldValue(
      names: const <String>['itemIndex', 'rowIndex'],
      query: query,
      body: payload,
      requiredName: 'itemIndex',
      required: !traverseAll,
    );
    final fieldIndex = _parseIntFieldValue(
      names: const <String>['fieldIndex', 'field'],
      query: query,
      body: payload,
      requiredName: 'fieldIndex',
      required: !traverseAll,
    );

    if (traverseAll) {
      if (responseMode != 'preview') {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'Traversal only supports `responseMode=preview`.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'responseMode': responseMode,
          },
        );
      }
      if (itemIndex != -1) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'For LitData traversal, itemIndex must be -1.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'itemIndex': itemIndex,
          },
        );
      }
      final normalizedOffset = traverseOffset ?? 0;
      if (normalizedOffset < 0) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'Invalid `traverseOffset` for LitData traversal.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'traverseOffset': traverseOffset,
          },
        );
      }
      final normalizedLimit = traverseLimit == null
          ? 200
          : _normalizeTraversalLimit(traverseLimit, field: 'traverseLimit');
      final page = await _state.apiListLitdataItemsPage(
        dataset: dataset,
        chunkFilename: chunkName.trim(),
        offset: normalizedOffset,
        length: normalizedLimit,
      );
      final nextOffset = page.partial ? page.offset + page.items.length : null;
      return <String, dynamic>{
        'ok': true,
        'datasetId': dataset.id,
        'mode': dataset.mode.name,
        'sourceInput': dataset.sourceInput,
        'chunkName': chunkName.trim(),
        'traversed': true,
        'offset': page.offset,
        'limit': page.length,
        'nextOffset': nextOffset,
        'total': page.numItemsTotal,
        'itemCount': page.items.length,
        'items': page.items.map(_itemMetaPayload).toList(growable: false),
      };
    }

    if (itemIndex < 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'LitData itemIndex must be >= 0 for direct field access.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'itemIndex': itemIndex,
        },
      );
    }
    if (fieldIndex < 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'LitData fieldIndex must be >= 0 for direct field access.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'fieldIndex': fieldIndex,
        },
      );
    }
    if (responseMode == 'file') {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'responseMode=file is not supported for LitData field access.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'responseMode': responseMode,
        },
      );
    }
    FieldPreview preview;
    try {
      preview = await _state.apiPeekLitdataField(
        dataset: dataset,
        chunkFilename: chunkName.trim(),
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
    } on FormatException catch (error) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'LitData direct field access failed.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'chunkName': chunkName.trim(),
          'itemIndex': itemIndex,
          'fieldIndex': fieldIndex,
          'error': error.toString(),
        },
      );
    }
    return _fieldPreviewPayload(
      datasetId: dataset.id,
      sourceInput: dataset.sourceInput,
      modeName: dataset.mode.name,
      payload: preview,
      details: <String, dynamic>{
        'chunkName': chunkName.trim(),
        'itemIndex': itemIndex,
        'fieldIndex': fieldIndex,
      },
    );
  }

  Future<Map<String, dynamic>> _handleMdsFieldAccess({
    required LoadedDatasetSource dataset,
    required Map<String, dynamic> payload,
    required Map<String, List<String>> query,
  }) async {
    final responseMode = _parseFieldResponseMode(
      query: query,
      body: payload,
    );
    final traverseAll = _parseOptionalBoolFieldValue(
      names: const <String>['traverse', 'allItems'],
      query: query,
      body: payload,
      field: 'traverse',
      defaultValue: false,
    );
    final traverseOffset = _parseOptionalIntFieldValue(
      names: const <String>['traverseOffset'],
      query: query,
      body: payload,
      field: 'traverseOffset',
    );
    final traverseLimit = _parseOptionalIntFieldValue(
      names: const <String>['traverseLimit'],
      query: query,
      body: payload,
      field: 'traverseLimit',
    );
    final shardName = _pickFirstFieldValue(
          names: const <String>['shardName', 'chunkName', 'path'],
          query: query,
          body: payload,
        ) ??
        dataset.selectedChunkName;
    if (shardName == null || shardName.trim().isEmpty) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing required `shardName` for MDS field access.',
        details: <String, dynamic>{'datasetId': dataset.id},
      );
    }
    final itemIndex = _parseIntFieldValue(
      names: const <String>['itemIndex', 'rowIndex'],
      query: query,
      body: payload,
      requiredName: 'itemIndex',
      required: !traverseAll,
    );
    final fieldIndex = _parseIntFieldValue(
      names: const <String>['fieldIndex', 'field'],
      query: query,
      body: payload,
      requiredName: 'fieldIndex',
      required: !traverseAll,
    );

    if (traverseAll) {
      if (responseMode != 'preview') {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'Traversal only supports `responseMode=preview`.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'responseMode': responseMode,
          },
        );
      }
      if (itemIndex != -1) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'For MDS traversal, itemIndex must be -1.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'itemIndex': itemIndex,
          },
        );
      }
      final normalizedOffset = traverseOffset ?? 0;
      if (normalizedOffset < 0) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'Invalid `traverseOffset` for MDS traversal.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'traverseOffset': traverseOffset,
          },
        );
      }
      final normalizedLimit = traverseLimit == null
          ? 200
          : _normalizeTraversalLimit(traverseLimit, field: 'traverseLimit');
      final page = await _state.apiListMdsItemsPage(
        dataset: dataset,
        shardFilename: shardName.trim(),
        offset: normalizedOffset,
        length: normalizedLimit,
      );
      final nextOffset = page.partial ? page.offset + page.items.length : null;
      return <String, dynamic>{
        'ok': true,
        'datasetId': dataset.id,
        'mode': dataset.mode.name,
        'sourceInput': dataset.sourceInput,
        'shardName': shardName.trim(),
        'traversed': true,
        'offset': page.offset,
        'limit': page.length,
        'nextOffset': nextOffset,
        'total': page.numItemsTotal,
        'itemCount': page.items.length,
        'items': page.items.map(_itemMetaPayload).toList(growable: false),
      };
    }

    if (itemIndex < 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'MDS itemIndex must be >= 0 for direct field access.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'itemIndex': itemIndex,
        },
      );
    }
    if (fieldIndex < 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'MDS fieldIndex must be >= 0 for direct field access.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'fieldIndex': fieldIndex,
        },
      );
    }
    if (responseMode == 'file') {
      final prepared = await _state.apiPrepareMdsFieldFile(
        dataset: dataset,
        shardFilename: shardName.trim(),
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
      return _preparedFilePayload(
        datasetId: dataset.id,
        sourceInput: dataset.sourceInput,
        modeName: dataset.mode.name,
        payload: prepared,
        details: <String, dynamic>{
          'shardName': shardName.trim(),
          'itemIndex': itemIndex,
          'fieldIndex': fieldIndex,
        },
      );
    }
    final preview = await _state.apiPeekMdsField(
      dataset: dataset,
      shardFilename: shardName.trim(),
      itemIndex: itemIndex,
      fieldIndex: fieldIndex,
    );
    return _fieldPreviewPayload(
      datasetId: dataset.id,
      sourceInput: dataset.sourceInput,
      modeName: dataset.mode.name,
      payload: preview,
      details: <String, dynamic>{
        'shardName': shardName.trim(),
        'itemIndex': itemIndex,
        'fieldIndex': fieldIndex,
      },
    );
  }

  Future<Map<String, dynamic>> _handleWebdatasetFieldAccess({
    required LoadedDatasetSource dataset,
    required Map<String, dynamic> payload,
    required Map<String, List<String>> query,
  }) async {
    final responseMode = _parseFieldResponseMode(
      query: query,
      body: payload,
    );
    final traverseAll = _parseOptionalBoolFieldValue(
      names: const <String>['traverse', 'allItems'],
      query: query,
      body: payload,
      field: 'traverse',
      defaultValue: false,
    );
    final traverseOffset = _parseOptionalIntFieldValue(
      names: const <String>['traverseOffset'],
      query: query,
      body: payload,
      field: 'traverseOffset',
    );
    final traverseLimit = _parseOptionalIntFieldValue(
      names: const <String>['traverseLimit'],
      query: query,
      body: payload,
      field: 'traverseLimit',
    );
    final computeTotal = _parseOptionalBoolFieldValue(
      names: const <String>['computeTotal', 'total'],
      query: query,
      body: payload,
      field: 'computeTotal',
      defaultValue: false,
    );
    final shardName = _pickFirstFieldValue(
          names: const <String>['shardName', 'chunkName'],
          query: query,
          body: payload,
        ) ??
        dataset.selectedShardName;
    if (shardName == null || shardName.trim().isEmpty) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing required `shardName` for WebDataset field access.',
        details: <String, dynamic>{'datasetId': dataset.id},
      );
    }
    final itemIndex = _parseIntFieldValue(
      names: const <String>['itemIndex', 'sampleIndex', 'rowIndex'],
      query: query,
      body: payload,
      requiredName: 'itemIndex',
      required: !traverseAll,
    );
    final fieldIndex = _parseOptionalIntFieldValue(
      names: const <String>['fieldIndex', 'field'],
      query: query,
      body: payload,
      field: 'fieldIndex',
    );
    final memberPath = _pickFirstFieldValue(
      names: const <String>['memberPath', 'member', 'entryName'],
      query: query,
      body: payload,
    );
    final fieldName = _pickFirstFieldValue(
      names: const <String>['fieldName'],
      query: query,
      body: payload,
    );

    if (traverseAll) {
      if (responseMode != 'preview') {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'Traversal only supports `responseMode=preview`.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'responseMode': responseMode,
          },
        );
      }
      if (itemIndex != -1) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'For WebDataset traversal, itemIndex must be -1.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'itemIndex': itemIndex,
          },
        );
      }
      final normalizedOffset = traverseOffset ?? 0;
      if (normalizedOffset < 0) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'Invalid `traverseOffset` for WebDataset traversal.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'traverseOffset': traverseOffset,
          },
        );
      }
      final normalizedLimit = traverseLimit == null
          ? 200
          : _normalizeTraversalLimit(traverseLimit, field: 'traverseLimit');
      final page = await _state.apiListWebdatasetSamplesPage(
        dataset: dataset,
        shardFilename: shardName.trim(),
        offset: normalizedOffset,
        length: normalizedLimit,
        computeTotal: computeTotal,
      );
      final nextOffset =
          page.partial ? page.offset + page.samples.length : null;
      return <String, dynamic>{
        'ok': true,
        'datasetId': dataset.id,
        'mode': dataset.mode.name,
        'sourceInput': dataset.sourceInput,
        'shardName': shardName.trim(),
        'traversed': true,
        'offset': page.offset,
        'limit': page.length,
        'nextOffset': nextOffset,
        'total': page.numSamplesTotal,
        'itemCount': page.samples.length,
        'items': page.samples.map(_wdsSamplePayload).toList(growable: false),
      };
    }

    if (itemIndex < 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'WebDataset itemIndex must be >= 0 for direct field access.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'itemIndex': itemIndex,
        },
      );
    }
    final single = await _state.apiListWebdatasetSamplesPage(
      dataset: dataset,
      shardFilename: shardName.trim(),
      offset: itemIndex,
      length: 1,
      computeTotal: false,
    );
    if (single.samples.isEmpty) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'WebDataset itemIndex out of range.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'shardName': shardName.trim(),
          'itemIndex': itemIndex,
          'itemCount': single.numSamplesTotal,
        },
      );
    }
    final sample = single.samples.first;
    WdsFieldInfo? selectedField;
    if (memberPath != null && memberPath.trim().isNotEmpty) {
      for (final candidate in sample.fields) {
        if (candidate.memberPath == memberPath.trim()) {
          selectedField = candidate;
          break;
        }
      }
      if (selectedField == null) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'WebDataset memberPath not found in selected sample.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'shardName': shardName.trim(),
            'itemIndex': itemIndex,
            'memberPath': memberPath.trim(),
          },
        );
      }
    }
    if (selectedField == null &&
        fieldName != null &&
        fieldName.trim().isNotEmpty) {
      for (final candidate in sample.fields) {
        if (candidate.name == fieldName.trim()) {
          selectedField = candidate;
          break;
        }
      }
    }
    if (selectedField == null && fieldIndex != null && fieldIndex >= 0) {
      if (fieldIndex < sample.fields.length) {
        selectedField = sample.fields[fieldIndex];
      } else {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'WebDataset fieldIndex out of range for selected sample.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'itemIndex': itemIndex,
            'fieldIndex': fieldIndex,
            'fieldCount': sample.fields.length,
          },
        );
      }
    }
    if (selectedField == null) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing field selector for WebDataset direct field access.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'fields': const <String>[
            'memberPath',
            'fieldName',
            'fieldIndex',
          ],
        },
      );
    }
    final resolvedFieldIndex = sample.fields.indexWhere(
      (field) => field.memberPath == selectedField!.memberPath,
    );
    if (responseMode == 'file') {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'responseMode=file is not supported for WebDataset field access.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'responseMode': responseMode,
        },
      );
    }
    final preview = await _state.apiPeekWebdatasetMember(
      dataset: dataset,
      shardFilename: shardName.trim(),
      memberPath: selectedField.memberPath,
    );
    return _fieldPreviewPayload(
      datasetId: dataset.id,
      sourceInput: dataset.sourceInput,
      modeName: dataset.mode.name,
      payload: preview,
      details: <String, dynamic>{
        'shardName': shardName.trim(),
        'itemIndex': itemIndex,
        'sampleKey': sample.key,
        'memberPath': selectedField.memberPath,
        'memberName': selectedField.name,
        'fieldIndex': resolvedFieldIndex,
      },
    );
  }

  Future<Map<String, dynamic>> _handleZenodoFieldAccess({
    required LoadedDatasetSource dataset,
    required Map<String, dynamic> payload,
    required Map<String, List<String>> query,
  }) async {
    final traverseAll = _parseOptionalBoolFieldValue(
      names: const <String>['traverse', 'allItems'],
      query: query,
      body: payload,
      field: 'traverse',
      defaultValue: false,
    );
    final traverseOffset = _parseOptionalIntFieldValue(
      names: const <String>['traverseOffset'],
      query: query,
      body: payload,
      field: 'traverseOffset',
    );
    final traverseLimit = _parseOptionalIntFieldValue(
      names: const <String>['traverseLimit'],
      query: query,
      body: payload,
      field: 'traverseLimit',
    );
    final itemIndex = _parseOptionalIntFieldValue(
      names: const <String>['itemIndex', 'fileIndex', 'rowIndex'],
      query: query,
      body: payload,
      field: 'itemIndex',
    );
    final fieldIndex = _parseOptionalIntFieldValue(
      names: const <String>['fieldIndex', 'field'],
      query: query,
      body: payload,
      field: 'fieldIndex',
    );
    final fileKey = _pickFirstFieldValue(
      names: const <String>['fileKey', 'shardName'],
      query: query,
      body: payload,
    );
    final entryName = _pickFirstFieldValue(
      names: const <String>['entryName', 'memberPath', 'member'],
      query: query,
      body: payload,
    );

    final record = await _state.apiLoadZenodoRecordSummary(dataset);
    final normalizedOffset = traverseOffset ?? 0;
    if (normalizedOffset < 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Invalid `traverseOffset` for Zenodo traversal.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'traverseOffset': traverseOffset,
        },
      );
    }
    final normalizedLimit = traverseLimit == null
        ? 50
        : _normalizeTraversalLimit(traverseLimit, field: 'traverseLimit');
    final selectedFile = _resolveZenodoFileSelection(
      record: record,
      fileKey: fileKey,
      itemIndex: itemIndex,
    );

    if (traverseAll) {
      final normalizedEntryName = entryName?.trim();
      if (normalizedEntryName != null && normalizedEntryName.isNotEmpty) {
        if (selectedFile == null) {
          throw _ApiValidationError(
            'INVALID_REQUEST',
            'Missing file selection for Zenodo container traversal.',
            details: <String, dynamic>{
              'datasetId': dataset.id,
              'required': const <String>['fileKey', 'itemIndex'],
            },
          );
        }
        if (_isZenodoZipFilename(selectedFile.key)) {
          final allEntries = await _state.apiZenodoZipListEntries(
            contentUrl: selectedFile.contentUrl,
            filename: selectedFile.key,
          );
          final start = normalizedOffset.clamp(0, allEntries.length).toInt();
          final end =
              (start + normalizedLimit).clamp(0, allEntries.length).toInt();
          final entries = start >= allEntries.length
              ? const <ZenodoZipEntrySummary>[]
              : allEntries.sublist(start, end);
          final nextOffset = end < allEntries.length ? end : null;
          return <String, dynamic>{
            'ok': true,
            'datasetId': dataset.id,
            'mode': dataset.mode.name,
            'sourceInput': dataset.sourceInput,
            'fileKey': selectedFile.key,
            'traversed': true,
            'offset': start,
            'limit': normalizedLimit,
            'nextOffset': nextOffset,
            'total': allEntries.length,
            'itemCount': entries.length,
            'items':
                entries.map(_zenodoZipEntryPayload).toList(growable: false),
          };
        }
        if (_isZenodoTarFilename(selectedFile.key)) {
          final tarPage = await _state.apiZenodoTarListEntriesPaged(
            contentUrl: selectedFile.contentUrl,
            filename: selectedFile.key,
            offset: normalizedOffset,
            length: normalizedLimit > 200 ? 200 : normalizedLimit,
          );
          final nextOffset =
              tarPage.partial ? tarPage.offset + tarPage.entries.length : null;
          return <String, dynamic>{
            'ok': true,
            'datasetId': dataset.id,
            'mode': dataset.mode.name,
            'sourceInput': dataset.sourceInput,
            'fileKey': selectedFile.key,
            'traversed': true,
            'offset': tarPage.offset,
            'limit': tarPage.length,
            'nextOffset': nextOffset,
            'total': tarPage.numEntriesTotal,
            'itemCount': tarPage.entries.length,
            'items': tarPage.entries
                .map(_zenodoTarEntryPayload)
                .toList(growable: false),
          };
        }
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'Entry traversal requires a ZIP/TAR Zenodo file.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'fileKey': selectedFile.key,
          },
        );
      }

      final files = record.files;
      final start = normalizedOffset.clamp(0, files.length).toInt();
      final end = (start + normalizedLimit).clamp(0, files.length).toInt();
      final page = start >= files.length
          ? const <ZenodoFileSummary>[]
          : files.sublist(start, end);
      final nextOffset = end < files.length ? end : null;
      return <String, dynamic>{
        'ok': true,
        'datasetId': dataset.id,
        'mode': dataset.mode.name,
        'sourceInput': dataset.sourceInput,
        'recordId': record.recordId,
        'traversed': true,
        'offset': start,
        'limit': normalizedLimit,
        'nextOffset': nextOffset,
        'total': files.length,
        'itemCount': page.length,
        'items': page.map(_zenodoFilePayload).toList(growable: false),
      };
    }

    final effectiveFieldIndex = fieldIndex ?? 0;
    if (effectiveFieldIndex != 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'For Zenodo file preview, fieldIndex must be 0.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'fieldIndex': effectiveFieldIndex,
        },
      );
    }
    if (selectedFile == null) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing Zenodo file selector.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'required': const <String>['fileKey', 'itemIndex'],
        },
      );
    }
    final resolvedItemIndex = record.files.indexWhere(
      (file) => file.key == selectedFile.key,
    );
    final normalizedEntryName = entryName?.trim();
    if (normalizedEntryName != null && normalizedEntryName.isNotEmpty) {
      if (_isZenodoZipFilename(selectedFile.key)) {
        final preview = await _state.apiZenodoZipPeekEntry(
          contentUrl: selectedFile.contentUrl,
          filename: selectedFile.key,
          entryName: normalizedEntryName,
        );
        return _fieldPreviewPayload(
          datasetId: dataset.id,
          sourceInput: dataset.sourceInput,
          modeName: dataset.mode.name,
          payload: preview,
          details: <String, dynamic>{
            'recordId': record.recordId,
            'fileKey': selectedFile.key,
            'itemIndex': resolvedItemIndex,
            'fieldIndex': 0,
            'entryName': normalizedEntryName,
          },
        );
      }
      if (_isZenodoTarFilename(selectedFile.key)) {
        final preview = await _state.apiZenodoTarPeekEntry(
          contentUrl: selectedFile.contentUrl,
          filename: selectedFile.key,
          entryName: normalizedEntryName,
        );
        return _fieldPreviewPayload(
          datasetId: dataset.id,
          sourceInput: dataset.sourceInput,
          modeName: dataset.mode.name,
          payload: preview,
          details: <String, dynamic>{
            'recordId': record.recordId,
            'fileKey': selectedFile.key,
            'itemIndex': resolvedItemIndex,
            'fieldIndex': 0,
            'entryName': normalizedEntryName,
          },
        );
      }
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Entry preview requires a ZIP/TAR Zenodo file.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'fileKey': selectedFile.key,
        },
      );
    }

    final preview = await _state.apiPeekZenodoFile(
      contentUrl: selectedFile.contentUrl,
    );
    return _fieldPreviewPayload(
      datasetId: dataset.id,
      sourceInput: dataset.sourceInput,
      modeName: dataset.mode.name,
      payload: preview,
      details: <String, dynamic>{
        'recordId': record.recordId,
        'fileKey': selectedFile.key,
        'itemIndex': resolvedItemIndex,
        'fieldIndex': 0,
      },
    );
  }

  Future<Map<String, dynamic>> _handleLocalDirectoryFieldAccess({
    required LoadedDatasetSource dataset,
    required Map<String, dynamic> payload,
    required Map<String, List<String>> query,
    int? traversalConcurrency,
  }) async {
    final responseMode = _parseFieldResponseMode(
      query: query,
      body: payload,
    );
    final resolvedTraversalConcurrency =
        traversalConcurrency ?? _defaultConcurrency;
    final traverseAll = _parseOptionalBoolFieldValue(
      names: const <String>['traverse', 'allItems'],
      query: query,
      body: payload,
      field: 'traverse',
      defaultValue: false,
    );
    final traverseOffset = _parseOptionalIntFieldValue(
      names: const <String>['traverseOffset'],
      query: query,
      body: payload,
      field: 'traverseOffset',
    );
    final traverseLimit = _parseOptionalIntFieldValue(
      names: const <String>['traverseLimit'],
      query: query,
      body: payload,
      field: 'traverseLimit',
    );
    final shardName = _pickFirstFieldValue(
      names: const <String>['shardName', 'chunkName', 'path'],
      query: query,
      body: payload,
    );
    final itemIndex = _parseIntFieldValue(
      names: const <String>['itemIndex', 'rowIndex'],
      query: query,
      body: payload,
      requiredName: 'itemIndex',
      required: true,
    );
    final fieldIndex = _parseIntFieldValue(
      names: const <String>['fieldIndex', 'field'],
      query: query,
      body: payload,
      requiredName: 'fieldIndex',
      required: true,
    );

    final selectedChunkName = dataset.selectedChunkName;
    final fallbackPath = shardName?.trim().isNotEmpty == true
        ? shardName!.trim()
        : selectedChunkName;

    if (fallbackPath == null || fallbackPath.trim().isEmpty) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing required `shardName` or `chunkName` for local directory field access.',
        details: <String, dynamic>{'datasetId': dataset.id},
      );
    }

    final resolvedPath = _resolveLocalDirectoryFieldPath(
      datasetSourceInput: dataset.sourceInput,
      shardPath: fallbackPath,
    );

    if (_state.isLocalDirectoryMdsShardPath(resolvedPath)) {
      if (traverseAll) {
        if (responseMode != 'preview') {
          throw _ApiValidationError(
            'INVALID_REQUEST',
            'Traversal only supports `responseMode=preview`.',
            details: <String, dynamic>{
              'datasetId': dataset.id,
              'responseMode': responseMode,
            },
          );
        }
        if (itemIndex != -1) {
          throw _ApiValidationError(
            'INVALID_REQUEST',
            'For MDS traversal, itemIndex must be -1.',
            details: <String, dynamic>{
              'datasetId': dataset.id,
              'itemIndex': itemIndex,
            },
          );
        }
        final normalizedOffset = traverseOffset ?? 0;
        if (normalizedOffset < 0) {
          throw _ApiValidationError(
            'INVALID_REQUEST',
            'Invalid `traverseOffset` for MDS traversal.',
            details: <String, dynamic>{
              'datasetId': dataset.id,
              'traverseOffset': traverseOffset,
            },
          );
        }
        final normalizedLimit = traverseLimit == null
            ? 200
            : _normalizeTraversalLimit(traverseLimit, field: 'traverseLimit');
        final page = await _state.listLocalDirectoryMdsItemsPage(
          resolvedPath,
          offset: normalizedOffset,
          length: normalizedLimit,
        );
        return <String, dynamic>{
          'ok': true,
          'datasetId': dataset.id,
          'mode': dataset.mode.name,
          'sourceInput': dataset.sourceInput,
          'path': resolvedPath,
          'traversed': true,
          'offset': page.offset,
          'limit': page.length,
          'nextOffset': page.partial ? page.offset + page.items.length : null,
          'total': page.numItemsTotal,
          'itemCount': page.items.length,
          'items': page.items.map(_itemMetaPayload).toList(growable: false),
        };
      }
      if (itemIndex < 0) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'MDS itemIndex must be >= 0 for direct field access.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'itemIndex': itemIndex,
          },
        );
      }
      if (fieldIndex < 0) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'MDS fieldIndex must be >= 0 for direct field access.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'fieldIndex': fieldIndex,
          },
        );
      }
      if (responseMode == 'file') {
        final prepared = await _state.apiPrepareLocalDirectoryFieldFile(
          path: resolvedPath,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
        );
        return _preparedFilePayload(
          datasetId: dataset.id,
          sourceInput: dataset.sourceInput,
          payload: prepared,
          details: <String, dynamic>{
            'path': resolvedPath,
            'itemIndex': itemIndex,
            'fieldIndex': fieldIndex,
          },
        );
      }
      final response = await _state.peekLocalDirectoryMdsField(
        shardPath: resolvedPath,
        itemIndex: itemIndex,
        fieldIndex: fieldIndex,
      );
      return _fieldPreviewPayload(
        datasetId: dataset.id,
        sourceInput: dataset.sourceInput,
        payload: response,
        details: <String, dynamic>{
          'path': resolvedPath,
          'itemIndex': itemIndex,
          'fieldIndex': fieldIndex,
        },
      );
    }

    if (fieldIndex != 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'For non-MDS localDirectory fields, fieldIndex must be 0.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'fieldIndex': fieldIndex,
        },
      );
    }

    final localType = await _localDirectoryEntityType(resolvedPath);

    if (traverseAll && fieldIndex != 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'For localDirectory traversal, fieldIndex must be 0.',
        details: <String, dynamic>{
          'datasetId': dataset.id,
          'fieldIndex': fieldIndex,
        },
      );
    }

    if (localType == FileSystemEntityType.directory) {
      if (traverseAll) {
        if (itemIndex != -1) {
          throw _ApiValidationError(
            'INVALID_REQUEST',
            'For localDirectory traversal, itemIndex must be -1.',
            details: <String, dynamic>{
              'datasetId': dataset.id,
              'itemIndex': itemIndex,
            },
          );
        }
        return _handleLocalDirectoryDirectoryTraversal(
          datasetId: dataset.id,
          sourceInput: dataset.sourceInput,
          shardPath: resolvedPath,
          traversalConcurrency: resolvedTraversalConcurrency,
          start: traverseOffset,
          limit: traverseLimit,
        );
      }
      return _handleLocalDirectoryDirectoryFieldAccess(
        datasetId: dataset.id,
        sourceInput: dataset.sourceInput,
        shardPath: resolvedPath,
        itemIndex: itemIndex,
      );
    }

    if (localType == FileSystemEntityType.file) {
      if (itemIndex != 0) {
        throw _ApiValidationError(
          'INVALID_REQUEST',
          'For non-MDS localDirectory file fields, itemIndex must be 0.',
          details: <String, dynamic>{
            'datasetId': dataset.id,
            'itemIndex': itemIndex,
          },
        );
      }
      if (responseMode == 'file') {
        final prepared = await _state.apiPrepareLocalDirectoryFieldFile(
          path: resolvedPath,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
        );
        return _preparedFilePayload(
          datasetId: dataset.id,
          sourceInput: dataset.sourceInput,
          payload: prepared,
          details: <String, dynamic>{
            'path': resolvedPath,
            'itemIndex': 0,
            'fieldIndex': 0,
          },
        );
      }
      final response = await _readLocalDirectoryFilePreview(resolvedPath);
      return _fieldPreviewPayload(
        datasetId: dataset.id,
        sourceInput: dataset.sourceInput,
        payload: response,
        details: <String, dynamic>{
          'path': resolvedPath,
          'itemIndex': 0,
          'fieldIndex': 0,
        },
      );
    }

    final response = await _previewLocalDirectoryPathAsItem(
      datasetId: dataset.id,
      sourceInput: dataset.sourceInput,
      path: resolvedPath,
      itemIndex: itemIndex,
    );
    if (response != null) {
      return response;
    }

    throw _ApiValidationError(
      'INVALID_REQUEST',
      'Unable to resolve localDirectory field target.',
      details: <String, dynamic>{
        'datasetId': dataset.id,
        'path': resolvedPath,
      },
    );
  }

  Future<Map<String, dynamic>?> _previewLocalDirectoryPathAsItem({
    required String datasetId,
    required String sourceInput,
    required String path,
    required int itemIndex,
  }) async {
    if (itemIndex != 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'For non-MDS localDirectory fields, itemIndex must be 0 when target is a file path.',
        details: <String, dynamic>{
          'datasetId': datasetId,
          'itemIndex': itemIndex,
        },
      );
    }

    final payload = await _readLocalDirectoryFilePreview(path);
    return _fieldPreviewPayload(
      datasetId: datasetId,
      sourceInput: sourceInput,
      payload: payload,
      details: <String, dynamic>{
        'path': path,
        'itemIndex': 0,
        'fieldIndex': 0,
      },
    );
  }

  Future<Map<String, dynamic>> _handleLocalDirectoryDirectoryTraversal({
    required String datasetId,
    required String sourceInput,
    required String shardPath,
    required int traversalConcurrency,
    int? start,
    int? limit,
  }) async {
    final items = await _state.listLocalDirectoryItems(shardPath);
    final normalizedStart = start ?? 0;
    if (normalizedStart < 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Invalid `traverseOffset` for traversal.',
        details: <String, dynamic>{
          'datasetId': datasetId,
          'traverseOffset': start,
        },
      );
    }
    final normalizedLimit = limit == null
        ? items.length
        : _normalizeTraversalLimit(limit, field: 'traverseLimit');
    if (normalizedStart >= items.length) {
      return <String, dynamic>{
        'ok': true,
        'datasetId': datasetId,
        'mode': ViewerMode.localDirectory.name,
        'path': shardPath,
        'sourceInput': sourceInput,
        'itemCount': 0,
        'items': const <Map<String, dynamic>>[],
        'traversed': true,
        'offset': normalizedStart,
        'limit': normalizedLimit,
      };
    }
    final nextOffset = normalizedStart + normalizedLimit;
    final indexedItems = List<MapEntry<int, LocalDirectoryItem>>.generate(
      items.length,
      (index) => MapEntry(index, items[index]),
    );
    final selected = indexedItems.skip(normalizedStart).take(normalizedLimit);
    final previewConcurrency = _normalizeConcurrency(traversalConcurrency);
    final previews = await _runConcurrentTasks<
        MapEntry<int, LocalDirectoryItem>, Map<String, dynamic>?>(
      items: selected.toList(),
      maxConcurrency: previewConcurrency,
      mapper: (entry) async {
        final itemIndex = entry.key;
        final item = entry.value;
        if (item.isDirectory) {
          return <String, dynamic>{
            'ok': false,
            'itemIndex': itemIndex,
            'path': item.path,
            'error': <String, dynamic>{
              'code': 'SKIP_DIRECTORY',
              'message': 'Directory entry skipped during traversal.',
              'details': <String, dynamic>{
                'itemIndex': itemIndex,
                'path': item.path,
              },
            },
          };
        }
        try {
          final preview = await _readLocalDirectoryFilePreview(item.path);
          return _fieldPreviewPayload(
            datasetId: datasetId,
            sourceInput: sourceInput,
            payload: preview,
            details: <String, dynamic>{
              'path': item.path,
              'itemIndex': itemIndex,
              'fieldIndex': 0,
              'traversed': true,
            },
          );
        } catch (error) {
          return <String, dynamic>{
            'ok': false,
            'itemIndex': itemIndex,
            'path': item.path,
            'error': <String, dynamic>{
              'code': 'INTERNAL_ERROR',
              'message': 'Failed to read localDirectory item.',
              'details': <String, dynamic>{
                'datasetId': datasetId,
                'path': item.path,
                'itemIndex': itemIndex,
                'error': error.toString(),
                'type': error.runtimeType.toString(),
              },
            },
          };
        }
      },
    );

    final itemPayloads = <Map<String, dynamic>>[];
    final skippedDirectories = <Map<String, dynamic>>[];
    final readErrors = <Map<String, dynamic>>[];

    for (final preview in previews) {
      if (preview == null) continue;
      if (preview['ok'] == true) {
        itemPayloads.add(preview);
      } else {
        final error = preview['error'];
        if (error is Map<String, dynamic> &&
            error['code'] == 'SKIP_DIRECTORY') {
          skippedDirectories.add(preview);
        } else {
          readErrors.add(preview);
        }
      }
    }

    final traversalErrorCode = readErrors.isEmpty
        ? (skippedDirectories.isEmpty ? null : 'SKIPPED_DIRECTORIES')
        : 'PARTIAL_FAILURE';

    return <String, dynamic>{
      'ok': readErrors.isEmpty,
      'datasetId': datasetId,
      'mode': ViewerMode.localDirectory.name,
      'path': shardPath,
      'sourceInput': sourceInput,
      'itemCount': itemPayloads.length,
      'items': itemPayloads,
      'traversed': true,
      'offset': normalizedStart,
      'limit': normalizedLimit,
      'nextOffset': nextOffset < items.length ? nextOffset : null,
      'total': items.length,
      if (traversalErrorCode != null) 'errorCode': traversalErrorCode,
      if (readErrors.isNotEmpty)
        'errors': <Map<String, dynamic>>[...readErrors],
      if (skippedDirectories.isNotEmpty)
        'skipped': <Map<String, dynamic>>[...skippedDirectories],
    };
  }

  Future<Map<String, dynamic>> _handleLocalDirectoryDirectoryFieldAccess({
    required String datasetId,
    required String sourceInput,
    required String shardPath,
    required int itemIndex,
  }) async {
    if (itemIndex < 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Invalid `itemIndex` for directory field access.',
        details: <String, dynamic>{
          'datasetId': datasetId,
          'itemIndex': itemIndex,
        },
      );
    }

    final items = await _state.listLocalDirectoryItems(shardPath);
    if (itemIndex >= items.length) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'itemIndex out of range for directory field access.',
        details: <String, dynamic>{
          'datasetId': datasetId,
          'path': shardPath,
          'itemIndex': itemIndex,
          'itemCount': items.length,
        },
      );
    }

    final item = items[itemIndex];
    if (item.isDirectory) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Directory entries cannot be opened as leaf fields.',
        details: <String, dynamic>{
          'datasetId': datasetId,
          'path': item.path,
          'itemIndex': itemIndex,
        },
      );
    }

    final response = await _readLocalDirectoryFilePreview(item.path);
    return _fieldPreviewPayload(
      datasetId: datasetId,
      sourceInput: sourceInput,
      payload: response,
      details: <String, dynamic>{
        'path': item.path,
        'itemIndex': itemIndex,
        'fieldIndex': 0,
      },
    );
  }

  Future<FieldPreview> _readLocalDirectoryFilePreview(String path) async {
    final remoteLike = _looksLikeRemoteSource(path);
    if (remoteLike) {
      final bytes = await _state.readDirectoryFileBytes(
        path,
        maxBytes: LocalFilePreviewFlowService.defaultPreviewBytes,
      );
      return _localFileFlow.buildRemotePreview(
        path: path,
        bytes: bytes,
        hexSnippetBytes: LocalFilePreviewFlowService.defaultHexSnippetBytes,
      );
    }

    final normalizedPath =
        path.startsWith('file://') ? Uri.parse(path).toFilePath() : path;
    return _localFileFlow.readLocalFilePreview(
      path: normalizedPath,
      emptyPreview: () => const FieldPreview(
        previewText: '',
        hexSnippet: '',
        guessedExt: 'bin',
        isBinary: false,
        size: 0,
      ),
      previewBytes: LocalFilePreviewFlowService.defaultPreviewBytes,
      hexSnippetBytes: LocalFilePreviewFlowService.defaultHexSnippetBytes,
    );
  }

  String _resolveLocalDirectoryFieldPath({
    required String datasetSourceInput,
    required String shardPath,
  }) {
    final requested = shardPath.trim();
    if (requested.isEmpty) return requested;
    final source = datasetSourceInput.trim();
    if (requested == '.') {
      return source.isEmpty ? requested : source;
    }

    if (_looksLikeRemoteSource(requested) || p.isAbsolute(requested)) {
      return requested;
    }

    if (_looksLikeRemoteSource(source)) {
      final sourceUri = Uri.tryParse(source);
      final requestedNormalizedRaw = requested.startsWith('/')
          ? requested.substring(1)
          : requested;
      final requestedNormalized = _normalizeRemoteLocalDirectoryShardRequest(
        requestedNormalizedRaw,
      );
      if (sourceUri != null &&
          sourceUri.scheme == 'remote' &&
          sourceUri.host.trim().isNotEmpty) {
        final sourcePath = sourceUri.pathSegments
            .map(Uri.decodeComponent)
            .join('/')
            .trim()
            .replaceAll('\\', '/')
            .replaceFirst(RegExp(r'^/+'), '');
        final requestedLooksHostRelative = sourcePath.isNotEmpty &&
            (requestedNormalized == sourcePath ||
                requestedNormalized.startsWith('$sourcePath/'));
        if (requestedLooksHostRelative) {
          return Uri(
            scheme: sourceUri.scheme,
            host: sourceUri.host,
            pathSegments: requestedNormalized
                .split('/')
                .where((segment) => segment.isNotEmpty)
                .map(Uri.encodeComponent),
          ).toString();
        }
      }
      return _joinRemotePath(source, requested);
    }
    if (source.isEmpty) return requested;
    return p.join(source, requested);
  }

  String _normalizeRemoteLocalDirectoryShardRequest(String requested) {
    final value = requested.trim();
    if (value.isEmpty) return value;
    final lower = value.toLowerCase();
    if (lower.endsWith('.mds.zst') || lower.endsWith('.mds.zstd')) {
      return value;
    }
    final basename = p.basename(value);
    if (RegExp(r'^shard\.\d+\.mds$', caseSensitive: false).hasMatch(basename)) {
      return '$value.zstd';
    }
    return value;
  }

  String _joinRemotePath(String base, String path) {
    if (base.isEmpty) return path;
    if (path.isEmpty) return base;
    final trimmedBase = base.endsWith('/') ? base : '$base/';
    final child = path.startsWith('/') ? path.substring(1) : path;
    return '$trimmedBase$child';
  }

  bool _looksLikeRemoteSource(String raw) {
    final value = raw.trim().toLowerCase();
    return value.startsWith('remote://') ||
        value.startsWith('http://') ||
        value.startsWith('https://') ||
        value.startsWith('ftp://') ||
        value.startsWith('s3://') ||
        value.startsWith('file://');
  }

  Future<FileSystemEntityType> _localDirectoryEntityType(String path) async {
    if (_looksLikeRemoteSource(path)) {
      try {
        await _state.listLocalDirectoryItems(path);
        return FileSystemEntityType.directory;
      } catch (_) {
        return FileSystemEntityType.notFound;
      }
    }
    return FileSystemEntity.type(
      path,
      followLinks: true,
    ).onError((_, __) => FileSystemEntityType.notFound);
  }

  List<dynamic> _extractBatchFieldRequests(Map<String, dynamic> payload) {
    final requested = payload['requests'];
    if (requested != null && requested is List) {
      return requested;
    }
    if (payload.isEmpty) return const <dynamic>[];
    final fallbackSingle = payload['datasetId'] ?? payload['id'];
    if (fallbackSingle != null) {
      return <dynamic>[payload];
    }
    return const <dynamic>[];
  }

  int _normalizeBatchCount(int count) => count;

  LoadedDatasetSource? _findOpenedDataset(String datasetId) {
    for (final dataset in _state.openedDatasets) {
      if (dataset.id == datasetId) {
        return dataset;
      }
    }
    return null;
  }

  String? _pickFirstFieldValue({
    required List<String> names,
    required Map<String, List<String>> query,
    required Map<String, dynamic> body,
  }) {
    for (final name in names) {
      if (body.containsKey(name)) {
        final raw = body[name];
        if (raw == null) continue;
        final value = raw.toString().trim();
        if (value.isNotEmpty) return value;
      }
    }
    for (final name in names) {
      final values = query[name];
      if (values != null && values.isNotEmpty) {
        final value = values.first.trim();
        if (value.isNotEmpty) return value;
      }
    }
    return null;
  }

  int _parseIntFieldValue({
    required List<String> names,
    required Map<String, List<String>> query,
    required Map<String, dynamic> body,
    required String requiredName,
    bool required = false,
  }) {
    final raw = _pickFirstFieldValue(
      names: names,
      query: query,
      body: body,
    );
    if (raw == null || raw.isEmpty) {
      if (!required) return -1;
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Missing required integer `$requiredName`.',
        details: <String, dynamic>{'field': requiredName},
      );
    }
    final parsed = int.tryParse(raw);
    if (parsed == null) {
      throw _ApiValidationError(
        'INVALID_INTEGER',
        'Invalid integer `$requiredName`.',
        details: <String, dynamic>{
          'field': requiredName,
          'value': raw,
        },
      );
    }
    return parsed;
  }

  String? _firstValueAsString({
    required List<String> names,
    required Map<String, List<String>> query,
    required Map<String, dynamic> body,
  }) {
    return _pickFirstFieldValue(names: names, query: query, body: body);
  }

  Map<String, dynamic> _openLeafResponsePayload({
    required String datasetId,
    required OpenLeafResponse response,
  }) {
    return <String, dynamic>{
      'ok': true,
      'datasetId': datasetId,
      'mode': ViewerMode.huggingface.name,
      'path': response.path,
      'size': response.size,
      'ext': response.ext,
      'opened': response.opened,
      'needsOpener': response.needsOpener,
      'message': response.message,
    };
  }

  Map<String, dynamic> _fieldPreviewPayload({
    required String datasetId,
    required String sourceInput,
    String modeName = 'localDirectory',
    required FieldPreview payload,
    required Map<String, dynamic> details,
  }) {
    return <String, dynamic>{
      'ok': true,
      'datasetId': datasetId,
      'mode': modeName,
      'sourceInput': sourceInput,
      'preview': <String, dynamic>{
        'previewText': payload.previewText,
        'hexSnippet': payload.hexSnippet,
        'guessedExt': payload.guessedExt,
        'isBinary': payload.isBinary,
        'size': payload.size,
      },
      ...details,
    };
  }

  Map<String, dynamic> _preparedFilePayload({
    required String datasetId,
    required String sourceInput,
    String modeName = 'localDirectory',
    required PreparedFileResponse payload,
    required Map<String, dynamic> details,
  }) {
    return <String, dynamic>{
      'ok': true,
      'datasetId': datasetId,
      'mode': modeName,
      'sourceInput': sourceInput,
      'path': payload.path,
      'size': payload.size,
      'ext': payload.ext,
      ...details,
    };
  }

  Map<String, dynamic> _fieldMetaPayload(FieldMeta field) {
    return <String, dynamic>{
      'fieldIndex': field.fieldIndex,
      'size': field.size,
    };
  }

  Map<String, dynamic> _itemMetaPayload(ItemMeta item) {
    return <String, dynamic>{
      'itemIndex': item.itemIndex,
      'totalBytes': item.totalBytes,
      'fields': item.fields.map(_fieldMetaPayload).toList(growable: false),
    };
  }

  Map<String, dynamic> _wdsFieldPayload(WdsFieldInfo field) {
    return <String, dynamic>{
      'name': field.name,
      'memberPath': field.memberPath,
      'size': field.size,
    };
  }

  Map<String, dynamic> _wdsSamplePayload(WdsSampleInfo sample) {
    return <String, dynamic>{
      'sampleIndex': sample.sampleIndex,
      'key': sample.key,
      'totalBytes': sample.totalBytes,
      'fields': sample.fields.map(_wdsFieldPayload).toList(growable: false),
    };
  }

  Map<String, dynamic> _zenodoFilePayload(ZenodoFileSummary file) {
    return <String, dynamic>{
      'key': file.key,
      'size': file.size,
      'checksum': file.checksum,
      'contentUrl': file.contentUrl,
    };
  }

  Map<String, dynamic> _zenodoZipEntryPayload(ZenodoZipEntrySummary entry) {
    return <String, dynamic>{
      'name': entry.name,
      'method': entry.method,
      'compressedSize': entry.compressedSize,
      'uncompressedSize': entry.uncompressedSize,
      'isDir': entry.isDir,
    };
  }

  Map<String, dynamic> _zenodoTarEntryPayload(ZenodoTarEntrySummary entry) {
    return <String, dynamic>{
      'name': entry.name,
      'size': entry.size,
      'isDir': entry.isDir,
    };
  }

  ZenodoFileSummary? _resolveZenodoFileSelection({
    required ZenodoRecordSummary record,
    String? fileKey,
    int? itemIndex,
  }) {
    if (record.files.isEmpty) return null;
    final normalizedKey = fileKey?.trim();
    if (normalizedKey != null && normalizedKey.isNotEmpty) {
      for (final file in record.files) {
        if (file.key == normalizedKey) {
          return file;
        }
      }
      return null;
    }
    if (itemIndex != null &&
        itemIndex >= 0 &&
        itemIndex < record.files.length) {
      return record.files[itemIndex];
    }
    if (record.files.length == 1) {
      return record.files.first;
    }
    return null;
  }

  bool _isZenodoZipFilename(String filename) {
    final lower = filename.trim().toLowerCase();
    return lower.endsWith('.zip');
  }

  bool _isZenodoTarFilename(String filename) {
    final lower = filename.trim().toLowerCase();
    return lower.endsWith('.tar') ||
        lower.endsWith('.tar.gz') ||
        lower.endsWith('.tgz') ||
        lower.endsWith('.tar.zst') ||
        lower.endsWith('.tar.zstd');
  }

  _OpenedRequestOptions _parseOpenedRequestOptions({
    required Map<String, List<String>> queryParameters,
    Map<String, dynamic>? body,
    bool defaultIncludeDetails = true,
    bool defaultIncludeMissing = false,
  }) {
    final includeDetails = _parseBoolValue(
      _pickFirstValue(
        names: const ['includeDetails', 'details'],
        queryParameters: queryParameters,
        body: body,
      ),
      name: 'includeDetails',
      defaultValue: defaultIncludeDetails,
    );
    final includeMissing = _parseBoolValue(
      _pickFirstValue(
        names: const ['includeMissing'],
        queryParameters: queryParameters,
        body: body,
      ),
      name: 'includeMissing',
      defaultValue: defaultIncludeMissing,
    );
    final concurrency = _normalizeConcurrency(_parseConcurrencyValue(
      _pickFirstValue(
        names: const ['concurrency'],
        queryParameters: queryParameters,
        body: body,
      ),
      field: 'concurrency',
    ));
    return _OpenedRequestOptions(
      includeDetails: includeDetails,
      includeMissing: includeMissing,
      concurrency: concurrency,
      fields: _parseFields(
        queryParameters: queryParameters,
        body: body,
      ),
    );
  }

  String? _pickFirstValue({
    required List<String> names,
    required Map<String, List<String>> queryParameters,
    Map<String, dynamic>? body,
  }) {
    if (body != null) {
      for (final name in names) {
        if (body.containsKey(name)) {
          final raw = body[name];
          if (raw == null) return null;
          return raw.toString();
        }
      }
    }
    for (final name in names) {
      final values = queryParameters[name];
      if (values != null && values.isNotEmpty) {
        return values.first;
      }
    }
    return null;
  }

  Set<String> _parseFields({
    required Map<String, List<String>> queryParameters,
    Map<String, dynamic>? body,
  }) {
    if (body != null) {
      for (final name in const ['fields', 'projection']) {
        if (!body.containsKey(name)) continue;
        final raw = body[name];
        return _parseFieldTokens(raw, field: name);
      }
    }

    final values = <String>[];
    for (final name in const ['fields', 'projection']) {
      final rawValues = queryParameters[name];
      if (rawValues != null && rawValues.isNotEmpty) {
        values.addAll(rawValues);
      }
    }
    if (values.isEmpty) return const <String>{};
    return _parseFieldTokens(values.join(','), field: 'fields');
  }

  Set<String> _parseFieldTokens(dynamic raw, {required String field}) {
    if (raw == null) return const <String>{};
    final tokens = <String>{};
    if (raw is Iterable) {
      for (final item in raw) {
        if (item == null) continue;
        _addTokens(item.toString(), tokens);
      }
      return tokens;
    }
    if (raw is String) {
      _addTokens(raw, tokens);
      return tokens;
    }
    throw _ApiValidationError(
      'INVALID_FIELDS',
      'Invalid `$field` value. Use array or comma/semicolon separated string.',
      details: <String, dynamic>{
        'field': field,
        'value': raw.runtimeType.toString(),
      },
    );
  }

  void _addTokens(String raw, Set<String> out) {
    for (final token in raw
        .split(',')
        .expand((part) => part.split(';'))
        .map((part) => part.trim())) {
      if (token.isNotEmpty) {
        out.add(token);
      }
    }
  }

  Map<String, dynamic> _projectFields(
      Map<String, dynamic> source, Set<String> fields) {
    if (fields.isEmpty) return source;
    if (fields.contains('*')) return source;
    final projected = <String, dynamic>{};
    for (final key in fields) {
      if (source.containsKey(key)) {
        projected[key] = source[key];
      }
    }
    return projected;
  }

  bool _parseBoolValue(
    String? rawValue, {
    required String name,
    required bool defaultValue,
  }) {
    if (rawValue == null || rawValue.isEmpty) {
      return defaultValue;
    }
    final value = rawValue.trim().toLowerCase();
    if (value == 'true' || value == '1' || value == 'yes' || value == 'y') {
      return true;
    }
    if (value == 'false' || value == '0' || value == 'no' || value == 'n') {
      return false;
    }
    throw _ApiValidationError(
      'INVALID_BOOLEAN',
      'Invalid boolean for `$name`.',
      details: <String, dynamic>{'field': name, 'value': rawValue},
    );
  }

  bool _parseOptionalBoolFieldValue({
    required List<String> names,
    required Map<String, List<String>> query,
    required Map<String, dynamic> body,
    required String field,
    required bool defaultValue,
  }) {
    final raw = _pickFirstFieldValue(
      names: names,
      query: query,
      body: body,
    );
    return _parseBoolValue(
      raw,
      name: field,
      defaultValue: defaultValue,
    );
  }

  int? _parseOptionalIntFieldValue({
    required List<String> names,
    required Map<String, List<String>> query,
    required Map<String, dynamic> body,
    required String field,
  }) {
    final raw = _pickFirstFieldValue(
      names: names,
      query: query,
      body: body,
    );
    if (raw == null || raw.isEmpty) return null;
    final value = int.tryParse(raw);
    if (value == null) {
      throw _ApiValidationError(
        'INVALID_INTEGER',
        'Invalid integer for `$field`.',
        details: <String, dynamic>{'field': field, 'value': raw},
      );
    }
    return value;
  }

  String _parseFieldResponseMode({
    required Map<String, List<String>> query,
    required Map<String, dynamic> body,
  }) {
    final raw = _pickFirstFieldValue(
      names: const <String>['responseMode', 'response'],
      query: query,
      body: body,
    );
    if (raw == null || raw.isEmpty) return 'preview';
    final value = raw.trim().toLowerCase();
    if (value == 'preview' || value == 'file') {
      return value;
    }
    throw _ApiValidationError(
      'INVALID_REQUEST',
      'Unsupported `responseMode`.',
      details: <String, dynamic>{
        'field': 'responseMode',
        'value': raw,
        'allowed': const <String>['preview', 'file'],
      },
    );
  }

  String _parseExtractResponseMode({
    required Map<String, List<String>> query,
    required Map<String, dynamic> body,
    required String defaultValue,
  }) {
    final raw = _pickFirstFieldValue(
      names: const <String>['responseMode', 'response'],
      query: query,
      body: body,
    );
    if (raw == null || raw.isEmpty) return defaultValue;
    final value = raw.trim().toLowerCase();
    if (value == 'stream' || value == 'materialize') {
      return value;
    }
    throw _ApiValidationError(
      'INVALID_REQUEST',
      'Unsupported extract `responseMode`.',
      details: <String, dynamic>{
        'field': 'responseMode',
        'value': raw,
        'allowed': const <String>['stream', 'materialize'],
      },
    );
  }

  String _parseExtractAudioEncoding({
    required Map<String, List<String>> query,
    required Map<String, dynamic> body,
  }) {
    final raw = _pickFirstFieldValue(
      names: const <String>['audioEncoding'],
      query: query,
      body: body,
    );
    if (raw == null || raw.isEmpty) return 'base64';
    final value = raw.trim().toLowerCase();
    if (value == 'base64' || value == 'none') {
      return value;
    }
    throw _ApiValidationError(
      'INVALID_REQUEST',
      'Unsupported `audioEncoding`.',
      details: <String, dynamic>{
        'field': 'audioEncoding',
        'value': raw,
        'allowed': const <String>['base64', 'none'],
      },
    );
  }

  int _parseConcurrencyValue(String? value, {required String field}) {
    if (value == null || value.isEmpty) return _defaultConcurrency;
    final parsed = int.tryParse(value);
    if (parsed == null) {
      throw _ApiValidationError(
        'INVALID_CONCURRENCY',
        'Invalid integer for `$field`.',
        details: <String, dynamic>{'field': field, 'value': value},
      );
    }
    return parsed;
  }

  int _normalizeConcurrency(int value) {
    if (value < 1 || value > _maxConcurrency) {
      throw _ApiValidationError(
        'INVALID_CONCURRENCY',
        'concurrency must be in range [1, $_maxConcurrency].',
        details: <String, dynamic>{'field': 'concurrency', 'value': value},
      );
    }
    return value;
  }

  int _normalizeTraversalLimit(int value, {required String field}) {
    if (value <= 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Invalid `$field` value.',
        details: <String, dynamic>{
          'field': field,
          'value': value,
          'min': 1,
        },
      );
    }
    if (value > _maxTraversalLimit) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Invalid `$field` value.',
        details: <String, dynamic>{
          'field': field,
          'value': value,
          'max': _maxTraversalLimit,
        },
      );
    }
    return value;
  }

  int _normalizeExtractLimit(int value, {required String field}) {
    if (value <= 0) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Invalid `$field` value.',
        details: <String, dynamic>{
          'field': field,
          'value': value,
          'min': 1,
        },
      );
    }
    if (value > _maxExtractLimit) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'Invalid `$field` value.',
        details: <String, dynamic>{
          'field': field,
          'value': value,
          'max': _maxExtractLimit,
        },
      );
    }
    return value;
  }

  List<String> _parseIdsFromQuery(
    Map<String, List<String>> queryParameters, {
    required Map<String, dynamic>? body,
    required bool includeBody,
  }) {
    final raw = <String>[];
    if (includeBody && body != null) {
      raw.addAll(_extractIds(body['ids']));
      raw.addAll(_extractIds(body['datasetIds']));
    }

    final ids = queryParameters['ids'];
    if (ids != null) {
      raw.addAll(ids);
    }
    final idList = queryParameters['idList'];
    if (idList != null) {
      raw.addAll(idList);
    }
    final datasetIds = queryParameters['datasetIds'];
    if (datasetIds != null) {
      raw.addAll(datasetIds);
    }
    return raw
        .expand((value) => value.split(',').expand((item) => item.split(';')))
        .map((item) => item.trim())
        .where((item) => item.isNotEmpty)
        .toList(growable: false);
  }

  List<String> _extractIds(dynamic rawIds) {
    final values = <String>[];
    if (rawIds == null) return values;
    if (rawIds is String) {
      values.addAll(
        rawIds
            .split(',')
            .expand((item) => item.split(';'))
            .map((item) => item.trim())
            .where((item) => item.isNotEmpty),
      );
      return values;
    }
    if (rawIds is Iterable) {
      for (final item in rawIds) {
        if (item == null) continue;
        final text = item.toString().trim();
        if (text.isNotEmpty) values.add(text);
      }
      return values;
    }
    throw _ApiValidationError(
      'INVALID_IDS',
      'The `ids` field must be a list or comma/semicolon separated string.',
      details: <String, dynamic>{
        'field': 'ids',
        'value': rawIds.toString(),
      },
    );
  }

  List<String> _normalizeIds(
    List<String> rawIds, {
    required String field,
    bool allowEmpty = false,
  }) {
    final ids = <String>[];
    final seen = <String>{};
    for (final raw in rawIds) {
      final id = raw.trim();
      if (id.isEmpty) continue;
      if (seen.add(id)) {
        ids.add(id);
      }
    }
    if (ids.length > _maxBatchIds) {
      throw _ApiValidationError(
        'TOO_MANY_IDS',
        'Too many dataset ids in one request.',
        details: <String, dynamic>{
          'field': field,
          'count': ids.length,
          'maxAllowed': _maxBatchIds,
        },
      );
    }
    if (!allowEmpty && ids.isEmpty) {
      throw _ApiValidationError(
        'INVALID_REQUEST',
        'ids is required and cannot be empty.',
        details: <String, dynamic>{'field': field},
      );
    }
    return ids;
  }

  Future<Map<String, dynamic>?> _inspectDatasetWithConcurrency(
    String datasetId, {
    bool includeDetails = true,
  }) async {
    final key = '$datasetId:${includeDetails ? 'full' : 'meta'}';
    final existing = _inflightInspectByQuery[key];
    if (existing != null) return existing;
    final task = _state
        .apiInspectDataset(datasetId, includeDetails: includeDetails)
        .whenComplete(() {
      _inflightInspectByQuery.remove(key);
    }).catchError((error, stack) {
      AppLogger.error(
        'Dataset inspection failed',
        tag: 'api',
        error: error,
        stackTrace: stack,
      );
      return <String, dynamic>{
        'id': datasetId,
        'ok': false,
        'status': 'error',
        'error': error.toString(),
      };
    });
    _inflightInspectByQuery[key] = task;
    return task;
  }

  int _deriveBatchTraversalConcurrency(int batchConcurrency) {
    final normalizedBatch = _normalizeConcurrency(batchConcurrency);
    final budget = _maxConcurrency;
    final derived = budget ~/ normalizedBatch;
    if (derived <= 0) {
      return 1;
    }
    if (derived > _maxConcurrency) {
      return _maxConcurrency;
    }
    return derived;
  }

  Future<List<R?>> _runConcurrentTasks<T, R>({
    required List<T> items,
    required int maxConcurrency,
    required Future<R?> Function(T item) mapper,
  }) async {
    if (items.isEmpty) return <R?>[];
    final results = List<R?>.filled(
      items.length,
      null,
      growable: false,
    );
    var nextIndex = 0;
    final workerCount = maxConcurrency.clamp(1, items.length);

    Future<void> worker() async {
      while (true) {
        final index = nextIndex;
        if (index >= items.length) return;
        nextIndex += 1;
        results[index] = await mapper(items[index]);
      }
    }

    await Future.wait<void>(List<Future<void>>.generate(
      workerCount,
      (_) => worker(),
    ));
    return results;
  }

  Future<void> _writeSuccessResponse(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
    required String path,
    required String method,
    required Object? data,
    required Map<String, dynamic> meta,
    int statusCode = HttpStatus.ok,
  }) async {
    await _writeJson(
      request,
      requestId: requestId,
      statusCode: statusCode,
      body: _buildSuccessEnvelope(
        requestId: requestId,
        startedAt: startedAt,
        path: path,
        method: method,
        data: data,
        meta: meta,
      ),
    );
  }

  Future<void> _writeMethodNotAllowed(
    HttpRequest request, {
    required String requestId,
    required DateTime startedAt,
    required List<String> allowed,
  }) async {
    request.response.headers.set('allow', allowed.join(', '));
    await _writeJson(
      request,
      requestId: requestId,
      statusCode: HttpStatus.methodNotAllowed,
      body: _buildErrorEnvelope(
        requestId: requestId,
        startedAt: startedAt,
        path: request.uri.path,
        method: request.method.toUpperCase(),
        statusCode: HttpStatus.methodNotAllowed,
        code: 'METHOD_NOT_ALLOWED',
        message: 'Method not allowed.',
        details: <String, dynamic>{
          'allowed': allowed,
        },
      ),
    );
  }

  Map<String, dynamic> _buildSuccessEnvelope({
    required String requestId,
    required DateTime startedAt,
    required String path,
    required String method,
    required Object? data,
    required Map<String, dynamic> meta,
  }) {
    final stableMeta = Map<String, dynamic>.from(meta);
    if (stableMeta.containsKey('projection') &&
        stableMeta['projection'] == null) {
      stableMeta.remove('projection');
    }
    return <String, dynamic>{
      'ok': true,
      'requestId': requestId,
      'apiVersion': _apiVersion,
      'timestamp': startedAt.toIso8601String(),
      'meta': <String, dynamic>{
        'path': path,
        'method': method,
        'service': _serviceName,
        'durationMs':
            DateTime.now().toUtc().difference(startedAt).inMilliseconds,
        ...stableMeta,
      },
      'data': data,
    };
  }

  Map<String, dynamic> _buildErrorEnvelope({
    required String requestId,
    required DateTime startedAt,
    required String path,
    required String method,
    required int statusCode,
    required String code,
    required String message,
    Map<String, dynamic> details = const {},
  }) {
    return <String, dynamic>{
      'ok': false,
      'requestId': requestId,
      'apiVersion': _apiVersion,
      'timestamp': startedAt.toIso8601String(),
      'meta': <String, dynamic>{
        'path': path,
        'method': method,
        'service': _serviceName,
        'status': statusCode,
        'durationMs':
            DateTime.now().toUtc().difference(startedAt).inMilliseconds,
      },
      'error': <String, dynamic>{
        'code': code,
        'message': message,
        if (details.isNotEmpty) 'details': details,
      },
    };
  }

  Future<Map<String, dynamic>> _readJsonBody(HttpRequest request) async {
    final body = await utf8.decoder.bind(request).join();
    if (body.length > _maxRequestBytes) {
      throw _ApiValidationError(
        'INVALID_JSON',
        'Request body is too large.',
        details: <String, dynamic>{
          'size': body.length,
          'maxAllowed': _maxRequestBytes,
        },
      );
    }
    if (body.trim().isEmpty) return const <String, dynamic>{};
    final decoded = (() {
      try {
        return jsonDecode(body);
      } on FormatException catch (error) {
        throw _ApiValidationError(
          'INVALID_JSON',
          'Request body must be a valid JSON object.',
          details: <String, dynamic>{
            'type': 'malformed_json',
            'error': error.message,
          },
        );
      }
    })();
    if (decoded is Map<String, dynamic>) return decoded;
    if (decoded is Map) return Map<String, dynamic>.from(decoded);
    throw _ApiValidationError(
      'INVALID_JSON',
      'Request body must be a JSON object.',
      details: <String, dynamic>{'type': decoded.runtimeType.toString()},
    );
  }

  Future<void> _writeJson(
    HttpRequest request, {
    required Map<String, dynamic> body,
    required String requestId,
    int statusCode = HttpStatus.ok,
  }) async {
    request.response.statusCode = statusCode;
    request.response.headers.set(
      'cache-control',
      'no-store, no-cache, must-revalidate, max-age=0',
    );
    request.response.headers.set('x-request-id', requestId);
    request.response.headers
        .set('content-type', 'application/json; charset=utf-8');
    request.response.headers.set('access-control-allow-origin', '*');
    request.response.headers.set('vary', 'Origin');
    request.response.headers
        .set('access-control-allow-methods', 'GET,POST,OPTIONS');
    request.response.headers.set(
      'access-control-allow-headers',
      'Content-Type,Authorization,Accept,X-Request-ID',
    );
    request.response.write(jsonEncode(body));
    await request.response.close();
  }

  String _nextRequestId() {
    _requestCounter += 1;
    final counter = _requestCounter.toString().padLeft(6, '0');
    return '${DateTime.now().toUtc().millisecondsSinceEpoch}-$counter';
  }

  String _extractRequestId(HttpRequest request) {
    final headerValue = request.headers.value('x-request-id');
    if (headerValue == null) return _nextRequestId();
    final candidate = headerValue.trim();
    if (candidate.isEmpty || candidate.length > 128) return _nextRequestId();
    if (candidate.contains('\n') || candidate.contains('\r'))
      return _nextRequestId();
    return candidate;
  }

  void _validateJsonBodyRequest(HttpRequest request) {
    final contentType = request.headers.contentType;
    if (contentType == null) return;
    final mimeType = contentType.mimeType.toLowerCase();
    if (mimeType == 'application/json' || mimeType.endsWith('+json')) {
      return;
    }
    throw _ApiValidationError(
      'UNSUPPORTED_MEDIA_TYPE',
      'Unsupported Content-Type.',
      details: <String, dynamic>{
        'received': contentType.toString(),
        'supported': const <String>['application/json', 'application/*+json'],
      },
    );
  }
}

class _OpenedRequestOptions {
  const _OpenedRequestOptions({
    required this.includeDetails,
    required this.includeMissing,
    required this.concurrency,
    required this.fields,
  });

  final bool includeDetails;
  final bool includeMissing;
  final int concurrency;
  final Set<String> fields;
}

class _ExtractRequest {
  const _ExtractRequest({
    required this.shardName,
    required this.audioFieldIndex,
    required this.textFieldIndex,
    required this.idFieldIndex,
    required this.offset,
    required this.limit,
    required this.overwrite,
    required this.outputDir,
    required this.manifestName,
    required this.audioDirName,
    required this.responseMode,
    required this.audioEncoding,
  });

  final String shardName;
  final int audioFieldIndex;
  final int textFieldIndex;
  final int? idFieldIndex;
  final int offset;
  final int limit;
  final bool overwrite;
  final String? outputDir;
  final String manifestName;
  final String audioDirName;
  final String responseMode;
  final String audioEncoding;
}

class _ApiValidationError implements Exception {
  const _ApiValidationError(
    this.code,
    this.message, {
    this.details = const <String, dynamic>{},
  });

  final String code;
  final String message;
  final Map<String, dynamic> details;
}
