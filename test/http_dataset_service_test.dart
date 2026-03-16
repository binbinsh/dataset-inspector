import 'dart:typed_data';

import 'package:dataset_inspector/services/http_dataset_service.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';

void main() {
  test('readBytes applies range header and truncates to maxBytes', () async {
    final client = MockClient((request) async {
      expect(request.headers['Range'], 'bytes=0-3');
      return http.Response.bytes(
        const <int>[1, 2, 3, 4, 5, 6],
        206,
      );
    });
    final service = HttpDatasetService(client: client);

    final bytes = await service.readBytes(
      url: Uri.parse('https://example.com/file.bin'),
      maxBytes: 4,
    );

    expect(bytes, Uint8List.fromList(const <int>[1, 2, 3, 4]));
  });

  test('openRead streams response bytes', () async {
    final client = MockClient((request) async {
      return http.Response.bytes(const <int>[9, 8, 7], 200);
    });
    final service = HttpDatasetService(client: client);

    final chunks = await service
        .openRead(url: Uri.parse('https://example.com/file.bin'))
        .toList();
    final merged = chunks.expand((chunk) => chunk).toList(growable: false);

    expect(merged, equals(const <int>[9, 8, 7]));
  });

  test('readBytes throws on non-2xx status', () async {
    final client = MockClient((request) async {
      return http.Response('not found', 404);
    });
    final service = HttpDatasetService(client: client);

    expect(
      () => service.readBytes(url: Uri.parse('https://example.com/missing')),
      throwsA(isA<FormatException>()),
    );
  });

  test('parentDirectoryUri and resolveFromDirectory build correct URLs', () {
    final service = HttpDatasetService();
    final parent = service.parentDirectoryUri(
      Uri.parse('https://example.com/datasets/train/index.json'),
    );
    final chunk = service.resolveFromDirectory(parent, 'chunk-000.bin');

    expect(parent.toString(), 'https://example.com/datasets/train/');
    expect(
        chunk.toString(), 'https://example.com/datasets/train/chunk-000.bin');
  });
}
