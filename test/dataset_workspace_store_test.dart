import 'dart:io';

import 'package:dataset_inspector/services/dataset_workspace_store.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('DatasetWorkspaceStore', () {
    test('create/list/get workspace manifest', () async {
      final root = await Directory.systemTemp.createTemp('workspace-store-');
      addTearDown(() async {
        if (await root.exists()) {
          await root.delete(recursive: true);
        }
      });

      final store = DatasetWorkspaceStore(rootDirectoryPath: root.path);
      final created = await store.createWorkspace(
        label: 'sample',
        source: {'provider': 'litdata', 'source': '/tmp/a'},
        tags: ['analysis'],
      );
      final workspaceId = created['workspace_id'] as String;

      final listed = await store.listWorkspaces();
      expect(listed['total'], 1);
      final items =
          (listed['items'] as List<dynamic>).whereType<Map<String, dynamic>>();
      expect(items.single['workspace_id'], workspaceId);

      final loaded = await store.getWorkspace(workspaceId: workspaceId);
      expect(loaded['label'], 'sample');
      expect((loaded['tags'] as List<dynamic>), contains('analysis'));
    });

    test('append/apply/save/list workspace derivatives', () async {
      final root = await Directory.systemTemp.createTemp('workspace-store-');
      addTearDown(() async {
        if (await root.exists()) {
          await root.delete(recursive: true);
        }
      });

      final store = DatasetWorkspaceStore(rootDirectoryPath: root.path);
      final created = await store.createWorkspace(label: 'ops');
      final workspaceId = created['workspace_id'] as String;

      final appended = await store.appendOperations(
        workspaceId: workspaceId,
        operations: [
          {
            'type': 'set_field',
            'record_key': 'r1',
            'field': 'score',
            'value': 100,
          },
          {
            'type': 'delete_record',
            'record_key': 'r2',
          },
        ],
      );
      expect(appended['appended_count'], 2);

      final applied = await store.applyOperations(
        workspaceId: workspaceId,
        records: [
          {'record_key': 'r1', 'name': 'alpha'},
          {'record_key': 'r2', 'name': 'beta'},
        ],
      );
      final rows = (applied['records'] as List<dynamic>)
          .whereType<Map<String, dynamic>>()
          .toList();
      expect(rows.length, 1);
      expect(rows.single['record_key'], 'r1');
      expect(rows.single['score'], 100);

      final artifact = await store.saveArtifact(
        workspaceId: workspaceId,
        name: 'report',
        data: {'rows': rows.length},
      );
      expect(artifact['artifact_count'], 1);

      final snapshot = await store.saveSnapshot(
        workspaceId: workspaceId,
        name: 'rows',
        rows: rows,
      );
      expect(snapshot['snapshot_count'], 1);
      expect(snapshot['row_count'], 1);

      final artifacts = await store.listArtifacts(workspaceId: workspaceId);
      final snapshots = await store.listSnapshots(workspaceId: workspaceId);
      expect(artifacts['total'], 1);
      expect(snapshots['total'], 1);
    });
  });
}
