import 'dart:convert';

import 'package:dataset_inspector/services/huggingface_service.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';

void main() {
  test('datasetPreview falls back to rows API when parquet API fails', () async {
    final calls = <Uri>[];
    final client = MockClient((request) async {
      calls.add(request.url);
      if (request.url.path == '/parquet') {
        expect(request.url.queryParameters['dataset'], 'facebook/DigiData');
        expect(request.url.queryParameters['config'], 'default');
        expect(request.url.queryParameters['split'], 'train');
        return http.Response(
          jsonEncode(<String, dynamic>{
            'error': 'The dataset generation failed',
          }),
          500,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      if (request.url.path == '/rows') {
        expect(request.url.queryParameters['dataset'], 'facebook/DigiData');
        expect(request.url.queryParameters['config'], 'default');
        expect(request.url.queryParameters['split'], 'train');
        expect(request.url.queryParameters['offset'], '0');
        expect(request.url.queryParameters['length'], '50');
        return http.Response(
          jsonEncode(<String, dynamic>{
            'features': <Map<String, dynamic>>[
              <String, dynamic>{'name': 'id', 'type': 'int64'},
              <String, dynamic>{'name': 'text', 'type': 'string'},
            ],
            'rows': <Map<String, dynamic>>[
              <String, dynamic>{
                'row': <String, dynamic>{'id': 1, 'text': 'hello'},
              },
            ],
            'num_rows_total': 1,
            'num_features': 2,
            'partial': false,
          }),
          200,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      fail('Unexpected request: ${request.url}');
    });

    final service = HuggingfaceService(client: client);
    final preview = await service.datasetPreview(
      input: 'https://huggingface.co/datasets/facebook/DigiData',
      config: 'default',
      split: 'train',
    );

    expect(preview.dataset, 'facebook/DigiData');
    expect(preview.config, 'default');
    expect(preview.split, 'train');
    expect(preview.rows.length, 1);
    final row = preview.rows.first as Map<String, dynamic>;
    expect(row['id'], 1);
    expect(row['text'], 'hello');
    expect(preview.features.map((f) => f.name).toList(growable: false),
        <String>['id', 'text']);
    expect(preview.numRowsTotal, 1);
    expect(calls.map((u) => u.path).toList(growable: false),
        <String>['/parquet', '/rows']);
  });

  test('datasetPreview falls back to hub jsonl when parquet and rows fail',
      () async {
    final calls = <Uri>[];
    final client = MockClient((request) async {
      calls.add(request.url);
      if (request.url.path == '/parquet') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'error': 'The dataset generation failed',
          }),
          500,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      if (request.url.path == '/rows') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'error': 'The dataset generation failed',
          }),
          500,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      if (request.url.path == '/first-rows') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'error': 'The dataset generation failed',
          }),
          500,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      if (request.url.path == '/api/datasets/facebook/DigiData') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'id': 'facebook/DigiData',
            'cardData': <String, dynamic>{
              'configs': <Map<String, dynamic>>[
                <String, dynamic>{
                  'config_name': 'default',
                  'data_files': <Map<String, dynamic>>[
                    <String, dynamic>{
                      'split': 'train',
                      'path': 'digidata_train.jsonl',
                    },
                  ],
                },
              ],
            },
          }),
          200,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      if (request.url.path ==
          '/datasets/facebook/DigiData/resolve/main/digidata_train.jsonl') {
        return http.Response(
          '{"id":1,"text":"hello"}\n{"id":2,"text":"world"}\n',
          200,
          headers: const <String, String>{
            'content-type': 'application/jsonl',
          },
        );
      }
      fail('Unexpected request: ${request.url}');
    });

    final service = HuggingfaceService(client: client);
    final preview = await service.datasetPreview(
      input: 'https://huggingface.co/datasets/facebook/DigiData',
      config: 'default',
      split: 'train',
    );

    expect(preview.rows.length, 2);
    final row = preview.rows.first as Map<String, dynamic>;
    expect(row['id'], 1);
    expect(row['text'], 'hello');
    expect(preview.features.map((f) => f.name).toList(growable: false),
        <String>['id', 'text']);
    expect(calls.map((u) => u.path).toList(growable: false), <String>[
      '/parquet',
      '/rows',
      '/first-rows',
      '/api/datasets/facebook/DigiData',
      '/datasets/facebook/DigiData/resolve/main/digidata_train.jsonl',
    ]);
  });

  test('datasetPreview prefers rows API when useStreamingApi is enabled',
      () async {
    final calls = <Uri>[];
    final client = MockClient((request) async {
      calls.add(request.url);
      if (request.url.path == '/rows') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'features': <Map<String, dynamic>>[
              <String, dynamic>{'name': 'value', 'type': 'int64'},
            ],
            'rows': <Map<String, dynamic>>[
              <String, dynamic>{
                'row': <String, dynamic>{'value': 42},
              },
            ],
            'num_rows_total': 1,
            'num_features': 1,
            'partial': false,
          }),
          200,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      fail('Unexpected request: ${request.url}');
    });

    final service = HuggingfaceService(client: client);
    final preview = await service.datasetPreview(
      input: 'https://huggingface.co/datasets/facebook/DigiData',
      config: 'default',
      split: 'train',
      useStreamingApi: true,
    );

    expect(preview.rows.length, 1);
    final row = preview.rows.first as Map<String, dynamic>;
    expect(row['value'], 42);
    expect(calls.map((u) => u.path).toList(growable: false), <String>['/rows']);
  });

  test('datasetPreview caches hub jsonl pages and skips repeated server failures',
      () async {
    final calls = <Uri>[];
    final jsonlBody = List<String>.generate(
      220,
      (index) => jsonEncode(<String, dynamic>{
        'id': index,
        'text': 'row-$index',
      }),
    ).join('\n');

    final client = MockClient((request) async {
      calls.add(request.url);
      if (request.url.path == '/parquet') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'error': 'The dataset generation failed',
          }),
          500,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      if (request.url.path == '/rows') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'error': 'The dataset generation failed',
          }),
          500,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      if (request.url.path == '/first-rows') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'error': 'The dataset generation failed',
          }),
          500,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      if (request.url.path == '/api/datasets/facebook/DigiData') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'id': 'facebook/DigiData',
            'cardData': <String, dynamic>{
              'configs': <Map<String, dynamic>>[
                <String, dynamic>{
                  'config_name': 'default',
                  'data_files': <Map<String, dynamic>>[
                    <String, dynamic>{
                      'split': 'train',
                      'path': 'digidata_train.jsonl',
                    },
                  ],
                },
              ],
            },
          }),
          200,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      if (request.url.path ==
          '/datasets/facebook/DigiData/resolve/main/digidata_train.jsonl') {
        return http.Response(
          '$jsonlBody\n',
          200,
          headers: const <String, String>{
            'content-type': 'application/jsonl',
          },
        );
      }
      fail('Unexpected request: ${request.url}');
    });

    final service = HuggingfaceService(client: client);
    final page1 = await service.datasetPreview(
      input: 'https://huggingface.co/datasets/facebook/DigiData',
      config: 'default',
      split: 'train',
      offset: 0,
      length: 50,
    );
    final page2 = await service.datasetPreview(
      input: 'https://huggingface.co/datasets/facebook/DigiData',
      config: 'default',
      split: 'train',
      offset: 50,
      length: 50,
    );

    expect(page1.rows.length, 50);
    expect(page2.rows.length, 50);
    final firstRowPage2 = page2.rows.first as Map<String, dynamic>;
    expect(firstRowPage2['id'], 50);

    final parquetCalls =
        calls.where((uri) => uri.path == '/parquet').toList(growable: false);
    final rowsCalls =
        calls.where((uri) => uri.path == '/rows').toList(growable: false);
    final firstRowsCalls =
        calls.where((uri) => uri.path == '/first-rows').toList(growable: false);
    final hubFileCalls = calls
        .where((uri) =>
            uri.path ==
            '/datasets/facebook/DigiData/resolve/main/digidata_train.jsonl')
        .toList(growable: false);

    expect(parquetCalls.length, 1);
    expect(rowsCalls.length, 1);
    expect(firstRowsCalls.length, 1);
    expect(hubFileCalls.length, 1);
  });

  test('datasetPreview uses first-rows when parquet and rows fail', () async {
    final calls = <Uri>[];
    final client = MockClient((request) async {
      calls.add(request.url);
      if (request.url.path == '/parquet' ||
          request.url.path == '/rows') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'error': 'The dataset generation failed',
          }),
          500,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      if (request.url.path == '/first-rows') {
        return http.Response(
          jsonEncode(<String, dynamic>{
            'features': <Map<String, dynamic>>[
              <String, dynamic>{'name': 'id', 'type': <String, dynamic>{'dtype': 'int64'}},
              <String, dynamic>{'name': 'text', 'type': <String, dynamic>{'dtype': 'string'}},
            ],
            'rows': <Map<String, dynamic>>[
              <String, dynamic>{'row_idx': 0, 'row': <String, dynamic>{'id': 1, 'text': 'a'}},
              <String, dynamic>{'row_idx': 1, 'row': <String, dynamic>{'id': 2, 'text': 'b'}},
            ],
            'truncated': true,
          }),
          200,
          headers: const <String, String>{
            'content-type': 'application/json',
          },
        );
      }
      fail('Unexpected request: ${request.url}');
    });

    final service = HuggingfaceService(client: client);
    final preview = await service.datasetPreview(
      input: 'https://huggingface.co/datasets/facebook/DigiData',
      config: 'default',
      split: 'train',
      offset: 0,
      length: 2,
    );

    expect(preview.rows.length, 2);
    final row = preview.rows.first as Map<String, dynamic>;
    expect(row['id'], 1);
    expect(preview.partial, isTrue);
    expect(preview.numRowsTotal, 0);
    expect(calls.map((u) => u.path).toList(growable: false),
        <String>['/parquet', '/rows', '/first-rows']);
  });
}
