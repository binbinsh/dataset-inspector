import 'dart:convert';
import 'dart:typed_data';

import 'package:dataset_inspector/services/litdata_service.dart';
import 'package:flutter_test/flutter_test.dart';

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

Uint8List _buildTokensChunk(String text) {
  final payload = utf8.encode(text);
  final start = 4 + 8;
  final end = start + payload.length;
  return Uint8List.fromList(<int>[
    ..._u32le(1),
    ..._u32le(start),
    ..._u32le(end),
    ...payload,
  ]);
}

Uint8List _buildIndexBytes({
  required String chunkFilename,
  required int chunkBytes,
  required List<String> dataFormat,
  String? itemLoader,
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
      'data_format': dataFormat,
      'data_spec': null,
      'item_loader': itemLoader,
    },
  };
  return Uint8List.fromList(utf8.encode(jsonEncode(json)));
}

void main() {
  test('parses TokensLoader LitData chunks without per-field headers',
      () async {
    const chunkFilename = 'chunk-000.bin';
    final chunkBytes = _buildTokensChunk('abcd');
    final indexBytes = _buildIndexBytes(
      chunkFilename: chunkFilename,
      chunkBytes: chunkBytes.length,
      dataFormat: const <String>['no_header_numpy:3'],
      itemLoader: 'TokensLoader',
    );
    final service = LitDataService();

    final items = await service.listChunkItemsFromStream(
      indexBytes: indexBytes,
      indexName: 'index.json',
      chunkFilename: chunkFilename,
      chunkStream: Stream<List<int>>.value(chunkBytes),
    );

    expect(items, hasLength(1));
    expect(items.single.fields, hasLength(1));
    expect(items.single.fields.single.size, equals(4));

    final preview = await service.peekFieldFromStream(
      indexBytes: indexBytes,
      indexName: 'index.json',
      chunkFilename: chunkFilename,
      itemIndex: 0,
      fieldIndex: 0,
      chunkStream: Stream<List<int>>.value(chunkBytes),
    );

    expect(preview.previewText, contains('abcd'));
    expect(preview.size, equals(4));
  });

  test('rejects unsupported LitData item loaders before parsing chunks',
      () async {
    const chunkFilename = 'chunk-000.parquet';
    final indexBytes = _buildIndexBytes(
      chunkFilename: chunkFilename,
      chunkBytes: 4,
      dataFormat: const <String>['str'],
      itemLoader: 'ParquetLoader',
    );
    final service = LitDataService();

    await expectLater(
      service.listChunkItemsFromStream(
        indexBytes: indexBytes,
        indexName: 'index.json',
        chunkFilename: chunkFilename,
        chunkStream: Stream<List<int>>.value(
          Uint8List.fromList(const <int>[0x50, 0x41, 0x52, 0x31]),
        ),
      ),
      throwsA(
        isA<FormatException>().having(
          (error) => error.message.toString(),
          'message',
          contains('Unsupported LitData item loader: ParquetLoader'),
        ),
      ),
    );
  });

  test('validates streamed LitData chunk length against index metadata',
      () async {
    const chunkFilename = 'chunk-000.bin';
    final chunkBytes = _buildSingleFieldChunk('short');
    final indexBytes = _buildIndexBytes(
      chunkFilename: chunkFilename,
      chunkBytes: chunkBytes.length + 3,
      dataFormat: const <String>['bytes'],
    );
    final service = LitDataService();

    await expectLater(
      service.listChunkItemsFromStream(
        indexBytes: indexBytes,
        indexName: 'index.json',
        chunkFilename: chunkFilename,
        chunkStream: Stream<List<int>>.value(chunkBytes),
      ),
      throwsA(
        isA<FormatException>().having(
          (error) => error.message.toString(),
          'message',
          contains('Chunk length mismatch'),
        ),
      ),
    );
  });
}
