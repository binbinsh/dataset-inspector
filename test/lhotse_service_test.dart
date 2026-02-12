import 'dart:io';

import 'package:dataset_inspector/services/lhotse_service.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('LhotseService', () {
    test('detects, writes, appends and lists manifest entries', () async {
      final dir = await Directory.systemTemp.createTemp('lhotse-service-');
      addTearDown(() async {
        if (await dir.exists()) {
          await dir.delete(recursive: true);
        }
      });

      final service = LhotseService();
      expect(await service.detectLocalSource(dir.path), isFalse);

      final write = await service.writeEntries(
        input: dir.path,
        manifest: 'recordings',
        entries: [
          {
            'id': 'rec-1',
            'sources': [
              {
                'type': 'file',
                'channels': [0],
                'source': '/tmp/a.wav',
              }
            ],
            'sampling_rate': 16000,
            'num_samples': 16000,
            'duration': 1.0,
          }
        ],
      );
      expect(write['rows'], 1);
      expect(await service.detectLocalSource(dir.path), isTrue);

      final append = await service.appendEntries(
        input: dir.path,
        manifest: 'recordings',
        entries: [
          {
            'id': 'rec-2',
            'sources': [
              {
                'type': 'file',
                'channels': [0],
                'source': '/tmp/b.wav',
              }
            ],
            'sampling_rate': 16000,
            'num_samples': 32000,
            'duration': 2.0,
          }
        ],
      );
      expect(append['rows'], 2);

      final listed = await service.listEntries(
        input: dir.path,
        manifest: 'recordings',
        offset: 0,
        length: 10,
      );
      expect(listed['total'], 2);

      final loaded = await service.loadSource(dir.path);
      expect(loaded['manifest_count'], 1);
    });

    test('supports compressed manifest writes', () async {
      final dir = await Directory.systemTemp.createTemp('lhotse-service-');
      addTearDown(() async {
        if (await dir.exists()) {
          await dir.delete(recursive: true);
        }
      });

      final service = LhotseService();
      final write = await service.writeEntries(
        input: dir.path,
        manifest: 'cuts',
        compressed: true,
        entries: [
          {
            'id': 'cut-1',
            'start': 0.0,
            'duration': 1.0,
            'channel': 0,
            'recording_id': 'rec-1',
            'supervisions': [],
          }
        ],
      );
      expect((write['path'] as String).endsWith('.jsonl.gz'), isTrue);

      final listed = await service.listEntries(
        input: dir.path,
        manifest: 'cuts',
      );
      expect(listed['total'], 1);
    });
  });
}
