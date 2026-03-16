import 'dart:io';
import 'dart:typed_data';

import 'package:dataset_inspector/models/common.dart';
import 'package:dataset_inspector/services/local_file_preview_flow_service.dart';
import 'package:flutter_test/flutter_test.dart';

FieldPreview _emptyPreview() {
  return const FieldPreview(
    previewText: '',
    hexSnippet: '',
    guessedExt: null,
    isBinary: false,
    size: 0,
  );
}

void main() {
  test('isBinary respects known extensions and byte heuristics', () {
    const service = LocalFilePreviewFlowService();

    expect(service.isBinary('txt', const <int>[0x00, 0x01, 0x02]), isFalse);
    expect(service.isBinary('png', const <int>[0x61, 0x62]), isTrue);
    expect(service.isBinary('custom', const <int>[0x00, 0x62]), isTrue);
  });

  test('buildRemotePreview returns binary hex preview', () {
    const service = LocalFilePreviewFlowService();
    final preview = service.buildRemotePreview(
      path: '/tmp/sample.bin',
      bytes: Uint8List.fromList(const <int>[0x00, 0x01, 0x02, 0x03]),
      hexSnippetBytes: 3,
    );

    expect(preview.isBinary, isTrue);
    expect(preview.guessedExt, 'bin');
    expect(preview.hexSnippet, 'Read 4 bytes\n00 01 02');
  });

  test('buildRemotePreview returns text preview', () {
    const service = LocalFilePreviewFlowService();
    final preview = service.buildRemotePreview(
      path: '/tmp/readme.md',
      bytes: Uint8List.fromList('hello'.codeUnits),
    );

    expect(preview.isBinary, isFalse);
    expect(preview.previewText, 'hello');
    expect(preview.guessedExt, 'md');
  });

  test('readLocalFilePreview returns empty preview for missing file', () async {
    const service = LocalFilePreviewFlowService();
    final preview = await service.readLocalFilePreview(
      path: '/tmp/does-not-exist-${DateTime.now().microsecondsSinceEpoch}',
      emptyPreview: _emptyPreview,
    );

    expect(preview.size, 0);
    expect(preview.previewText, '');
  });

  test('readLocalFilePreview truncates text preview and keeps full size',
      () async {
    const service = LocalFilePreviewFlowService();
    final root = await Directory.systemTemp.createTemp('local-preview-');
    addTearDown(() async {
      if (await root.exists()) {
        await root.delete(recursive: true);
      }
    });
    final file = File('${root.path}/notes.txt');
    await file.writeAsString('abcdefghijklmnop');

    final preview = await service.readLocalFilePreview(
      path: file.path,
      emptyPreview: _emptyPreview,
      previewBytes: 8,
      hexSnippetBytes: 8,
    );

    expect(preview.isBinary, isFalse);
    expect(preview.size, 16);
    expect(preview.previewText, 'abcdefgh\n\n(first 8 bytes)');
  });

  test('readLocalFilePreview emits binary size/hex metadata', () async {
    const service = LocalFilePreviewFlowService();
    final root = await Directory.systemTemp.createTemp('local-preview-');
    addTearDown(() async {
      if (await root.exists()) {
        await root.delete(recursive: true);
      }
    });
    final file = File('${root.path}/payload.bin');
    final bytes =
        Uint8List.fromList(List<int>.generate(32, (index) => index % 256));
    await file.writeAsBytes(bytes, flush: true);

    final preview = await service.readLocalFilePreview(
      path: file.path,
      emptyPreview: _emptyPreview,
      previewBytes: 16,
      hexSnippetBytes: 8,
    );

    expect(preview.isBinary, isTrue);
    expect(preview.size, 32);
    expect(preview.hexSnippet, contains('Size: 32 bytes'));
    expect(preview.hexSnippet, contains('(truncated 24 bytes)'));
    expect(preview.hexSnippet, contains('00 01 02 03 04 05 06 07'));
  });
}
