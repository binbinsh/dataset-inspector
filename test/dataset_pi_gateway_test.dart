import 'dart:io';

import 'package:dataset_inspector/services/agent/dataset_pi_gateway.dart';
import 'package:dataset_inspector/services/agent/dataset_pi_tools.dart';
import 'package:dataset_inspector/services/dataset_workspace_store.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('DatasetPiGateway', () {
    late DatasetPiGateway gateway;

    setUp(() {
      gateway = DatasetPiGateway();
    });

    test('describes PI-native tool surface', () {
      final described = gateway.describeTools();
      expect(described['ok'], isTrue);
      final tools = (described['tools'] as List<dynamic>)
          .whereType<Map<String, dynamic>>()
          .toList();
      final names =
          tools.map((tool) => tool['name']).whereType<String>().toSet();
      expect(names.contains(DatasetPiTools.detectAndLoad), isTrue);
      expect(names.contains(DatasetPiTools.profileDistribution), isTrue);
      expect(names.contains(DatasetPiTools.lhotseWriteManifest), isTrue);
      expect(names.contains(DatasetPiTools.workspaceCreate), isTrue);
      expect(names.contains(DatasetPiTools.optimizationApply), isTrue);
    });

    test('writes and reads lhotse manifest through PI gateway', () async {
      final dir = await Directory.systemTemp.createTemp('pi-gateway-lhotse-');
      addTearDown(() async {
        if (await dir.exists()) {
          await dir.delete(recursive: true);
        }
      });

      final write = await gateway.call(
        tool: DatasetPiTools.lhotseWriteManifest,
        arguments: {
          'source': dir.path,
          'manifest': 'cuts',
          'entries': [
            {
              'id': 'cut-1',
              'start': 0.0,
              'duration': 1.0,
              'channel': 0,
              'recording_id': 'rec-1',
              'supervisions': [
                {
                  'id': 'sup-1',
                  'recording_id': 'rec-1',
                  'start': 0.0,
                  'duration': 1.0,
                  'channel': 0,
                  'text': 'hello',
                }
              ],
            }
          ],
        },
      );
      expect(write['ok'], isTrue);

      final detect = await gateway.call(
        tool: DatasetPiTools.detectAndLoad,
        arguments: {
          'input': dir.path,
          'provider_hint': 'lhotse',
        },
      );
      expect(detect['ok'], isTrue);
      final data = detect['data'] as Map<String, dynamic>;
      expect(data['provider'], 'lhotse');

      final read = await gateway.call(
        tool: DatasetPiTools.lhotseLoadManifest,
        arguments: {
          'source': dir.path,
          'manifest': 'cuts',
        },
      );
      expect(read['ok'], isTrue);
      final listed = read['data'] as Map<String, dynamic>;
      expect(listed['total'], 1);
    });

    test('workspace snapshot syncs lhotse cuts and supports profiling',
        () async {
      final workspaceRoot =
          await Directory.systemTemp.createTemp('pi-gateway-store-');
      addTearDown(() async {
        if (await workspaceRoot.exists()) {
          await workspaceRoot.delete(recursive: true);
        }
      });
      final workspaceGateway = DatasetPiGateway(
        workspaceStore:
            DatasetWorkspaceStore(rootDirectoryPath: workspaceRoot.path),
      );

      final create = await workspaceGateway.call(
        tool: DatasetPiTools.workspaceCreate,
        arguments: {
          'label': 'lhotse-sync',
        },
      );
      expect(create['ok'], isTrue);
      final workspaceId =
          (create['data'] as Map<String, dynamic>)['workspace_id'] as String;

      final saveSnapshot = await workspaceGateway.call(
        tool: DatasetPiTools.workspaceSaveSnapshot,
        arguments: {
          'workspace_id': workspaceId,
          'name': 'sample',
          'rows': [
            {
              'record_key': 'row-1',
              'start': 0.0,
              'duration': 1.2,
              'recording_id': 'rec-a',
              'text': 'alpha',
            },
            {
              'record_key': 'row-2',
              'start': 1.2,
              'duration': 0.8,
              'recording_id': 'rec-a',
              'text': 'beta',
            },
          ],
        },
      );
      expect(saveSnapshot['ok'], isTrue);

      final readCuts = await workspaceGateway.call(
        tool: DatasetPiTools.lhotseLoadManifest,
        arguments: {
          'workspace_id': workspaceId,
          'manifest': 'cuts',
        },
      );
      expect(readCuts['ok'], isTrue);
      final entries = ((readCuts['data'] as Map<String, dynamic>)['entries']
              as List<dynamic>)
          .whereType<Map<String, dynamic>>()
          .toList();
      expect(entries.length, 2);

      final schema = await gateway.call(
        tool: DatasetPiTools.profileSchema,
        arguments: {
          'records': entries,
        },
      );
      expect(schema['ok'], isTrue);
      final quality = await gateway.call(
        tool: DatasetPiTools.qualityReport,
        arguments: {
          'records': entries,
          'key_field': 'id',
        },
      );
      expect(quality['ok'], isTrue);
      final plan = await gateway.call(
        tool: DatasetPiTools.optimizationPlan,
        arguments: {
          'quality_report': quality['data'],
          'target': 'storage',
        },
      );
      expect(plan['ok'], isTrue);
    });

    test('profiles field distributions', () async {
      final distribution = await gateway.call(
        tool: DatasetPiTools.profileDistribution,
        arguments: {
          'records': [
            {
              'id': 'a',
              'duration': 1.0,
              'speaker': 'spk-1',
              'created_at': '2026-01-01T00:00:00Z',
            },
            {
              'id': 'b',
              'duration': 2.5,
              'speaker': 'spk-1',
              'created_at': '2026-01-02T00:00:00Z',
            },
            {
              'id': 'c',
              'duration': 3.0,
              'speaker': 'spk-2',
              'created_at': '2026-01-03T00:00:00Z',
            },
          ],
        },
      );
      expect(distribution['ok'], isTrue);
      final data = distribution['data'] as Map<String, dynamic>;
      final fields = (data['fields'] as List<dynamic>)
          .whereType<Map<String, dynamic>>()
          .toList(growable: false);
      expect(fields.isNotEmpty, isTrue);

      final durationField =
          fields.firstWhere((field) => field['name'] == 'duration');
      expect(durationField['kind'], 'numeric');
      final numeric = durationField['numeric'] as Map<String, dynamic>;
      expect(numeric['count'], 3);
      expect((numeric['histogram'] as List<dynamic>).isNotEmpty, isTrue);
    });

    test('applies optimization actions and persists workspace snapshot',
        () async {
      final workspaceRoot =
          await Directory.systemTemp.createTemp('pi-gateway-opt-');
      addTearDown(() async {
        if (await workspaceRoot.exists()) {
          await workspaceRoot.delete(recursive: true);
        }
      });
      final workspaceGateway = DatasetPiGateway(
        workspaceStore:
            DatasetWorkspaceStore(rootDirectoryPath: workspaceRoot.path),
      );

      final create = await workspaceGateway.call(
        tool: DatasetPiTools.workspaceCreate,
        arguments: {
          'label': 'optimization',
        },
      );
      expect(create['ok'], isTrue);
      final workspaceId =
          (create['data'] as Map<String, dynamic>)['workspace_id'] as String;

      final applied = await workspaceGateway.call(
        tool: DatasetPiTools.optimizationApply,
        arguments: {
          'workspace_id': workspaceId,
          'key_field': 'record_key',
          'records': [
            {
              'record_key': 'k1',
              'text': ' hello ',
              'lang': '',
            },
            {
              'record_key': 'k1',
              'text': 'duplicate',
              'lang': 'en',
            },
          ],
          'plan': {
            'actions': [
              {'action': 'deduplicate_by_key'},
              {'action': 'normalize_missing_values'},
              {'action': 'materialize_lhotse_manifests'},
            ],
          },
        },
      );
      expect(applied['ok'], isTrue);
      final data = applied['data'] as Map<String, dynamic>;
      expect(data['output_count'], 1);
      expect(data['workspace_snapshot'], isA<Map<String, dynamic>>());

      final records = (data['records'] as List<dynamic>)
          .whereType<Map<String, dynamic>>()
          .toList(growable: false);
      expect(records.first['text'], 'hello');
      expect(records.first['lang'], isNull);
    });
  });
}
