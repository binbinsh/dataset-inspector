import 'dart:io';

import 'package:flutter_test/flutter_test.dart';

void main() {
  test('remote dataset policy: no on-disk cache subsystem markers in lib/', () {
    const forbiddenMarkers = <String>[
      'remote_cache',
      '_storeRemoteCacheQuotaMb',
      'Remote Cache Manager',
      'dataset-inspector/wds-cache',
      'dataset-inspector/mds-cache',
      'cacheRoot',
    ];

    final libRoot = Directory('lib');
    expect(libRoot.existsSync(), isTrue);

    final violations = <String>[];
    for (final entity in libRoot.listSync(recursive: true)) {
      if (entity is! File || !entity.path.endsWith('.dart')) {
        continue;
      }
      final content = entity.readAsStringSync();
      for (final marker in forbiddenMarkers) {
        if (content.contains(marker)) {
          violations.add('${entity.path}: "$marker"');
        }
      }
    }

    expect(
      violations,
      isEmpty,
      reason:
          'Found forbidden on-disk cache markers:\n${violations.join('\n')}',
    );
  });

  test('remote dataset path resolution stays direct (no local sync path)', () {
    final file = File('lib/services/remote_dataset_service.dart');
    expect(file.existsSync(), isTrue);
    final content = file.readAsStringSync();

    expect(
      content.contains("scheme: 'remote'"),
      isTrue,
      reason: 'resolveDatasetPath should keep returning remote:// URIs.',
    );
    expect(
      content.contains('without local cache'),
      isTrue,
      reason: 'status text should make direct non-persistent mode explicit.',
    );
  });
}
