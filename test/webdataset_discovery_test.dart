import 'dart:io';

import 'package:dataset_inspector/models/webdataset.dart';
import 'package:dataset_inspector/services/webdataset_service.dart';
import 'package:flutter_test/flutter_test.dart';

const _validLitdataIndex = '''
{
  "chunks": [
    {
      "filename": "chunk-00000.bin",
      "chunk_bytes": 128,
      "chunk_size": 1
    }
  ],
  "config": {
    "data_format": ["bytes"]
  }
}
''';

void main() {
  group('WebdatasetService.discoverLocalDatasets', () {
    late Directory root;
    late WebdatasetService service;

    setUp(() async {
      final suffix = DateTime.now().microsecondsSinceEpoch;
      root = await Directory(
              '${Directory.systemTemp.path}/dataset-inspector-discovery-$suffix')
          .create(recursive: true);
      service = WebdatasetService();
    });

    tearDown(() async {
      if (await root.exists()) {
        await root.delete(recursive: true);
      }
    });

    test('discovers litdata and webdataset folders recursively', () async {
      final litdataDir =
          await Directory('${root.path}/litdata').create(recursive: true);
      await File('${litdataDir.path}/index.json')
          .writeAsString(_validLitdataIndex);

      final wdsDir =
          await Directory('${root.path}/wds').create(recursive: true);
      await File('${wdsDir.path}/00000.tar').writeAsBytes(const [0]);

      final discovered = await service.discoverLocalDatasets(root.path);

      expect(
        discovered.any(
          (dataset) =>
              dataset.kind == LocalDatasetKind.litdataIndex &&
              dataset.path == Directory(litdataDir.path).absolute.path,
        ),
        isTrue,
      );
      expect(
        discovered.any(
          (dataset) =>
              dataset.kind == LocalDatasetKind.webdatasetDir &&
              dataset.path == Directory(wdsDir.path).absolute.path,
        ),
        isTrue,
      );
    });

    test('accepts a shard file path as the scan root', () async {
      final wdsDir =
          await Directory('${root.path}/single').create(recursive: true);
      final shardPath = '${wdsDir.path}/00001.tar';
      await File(shardPath).writeAsBytes(const [1, 2, 3]);

      final discovered = await service.discoverLocalDatasets(shardPath);

      expect(discovered, hasLength(1));
      expect(discovered.first.kind, LocalDatasetKind.webdatasetDir);
      expect(discovered.first.path, Directory(wdsDir.path).absolute.path);
    });

    test(
        'does not traverse child directories once a strongly detected dataset root is found',
        () async {
      final litdataRoot = await Directory('${root.path}/litdata_parent')
          .create(recursive: true);
      await File('${litdataRoot.path}/index.json')
          .writeAsString(_validLitdataIndex);

      final nestedWds = await Directory('${litdataRoot.path}/nested_wds')
          .create(recursive: true);
      await File('${nestedWds.path}/00000.tar').writeAsBytes(const [9, 9, 9]);

      final discovered = await service.discoverLocalDatasets(root.path);

      expect(
        discovered.any(
          (dataset) =>
              dataset.kind == LocalDatasetKind.litdataIndex &&
              dataset.path == Directory(litdataRoot.path).absolute.path,
        ),
        isTrue,
      );
      expect(
        discovered.any(
          (dataset) => dataset.path == Directory(nestedWds.path).absolute.path,
        ),
        isFalse,
      );
    });

    test('continues traversing child directories when root match is weak',
        () async {
      final weakRoot =
          await Directory('${root.path}/weak_root').create(recursive: true);
      await File('${weakRoot.path}/index.json').writeAsString('{}');

      final nestedWds = await Directory('${weakRoot.path}/nested_wds')
          .create(recursive: true);
      await File('${nestedWds.path}/00000.tar').writeAsBytes(const [7, 7, 7]);

      final discovered = await service.discoverLocalDatasets(root.path);

      expect(
        discovered.any(
          (dataset) => dataset.path == Directory(nestedWds.path).absolute.path,
        ),
        isTrue,
      );
    });
  });
}
