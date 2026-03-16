import 'dart:typed_data';

import 'package:dataset_inspector/models/remote_host.dart';
import 'package:dataset_inspector/models/webdataset.dart';
import 'package:dataset_inspector/services/remote_dataset_service.dart';
import 'package:dataset_inspector/services/webdataset_service.dart';
import 'package:dataset_inspector/state/viewer_state.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shared_preferences/shared_preferences.dart';

class _FakeRemoteDatasetService extends RemoteDatasetService {
  _FakeRemoteDatasetService({
    required this.entries,
    required this.resolvedPath,
    this.bytesByPath = const <String, List<int>>{},
  });

  final List<RemotePathEntry> entries;
  final String resolvedPath;
  final Map<String, List<int>> bytesByPath;

  int listEntriesCalls = 0;
  int resolveDatasetPathCalls = 0;
  int openReadFileCalls = 0;
  int readBytesFileCalls = 0;

  @override
  Future<List<RemotePathEntry>> listEntries({
    required RemoteHostConfig host,
    required String directoryPath,
    RemoteStatusCallback? onStatus,
  }) async {
    listEntriesCalls += 1;
    return entries;
  }

  @override
  Future<String> resolveDatasetPath({
    required RemoteHostConfig host,
    required String datasetPath,
    RemoteStatusCallback? onStatus,
  }) async {
    resolveDatasetPathCalls += 1;
    return resolvedPath;
  }

  @override
  Stream<List<int>> openReadFile({
    required RemoteHostConfig host,
    required String remotePath,
    int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) {
    openReadFileCalls += 1;
    return const Stream<List<int>>.empty();
  }

  @override
  Future<Uint8List> readBytesFile({
    required RemoteHostConfig host,
    required String remotePath,
    int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) async {
    readBytesFileCalls += 1;
    final normalized = remotePath.trim().replaceAll('\\', '/');
    final bytes = bytesByPath[normalized] ?? const <int>[0x61];
    if (maxBytes != null && maxBytes > 0 && bytes.length > maxBytes) {
      return Uint8List.fromList(bytes.sublist(0, maxBytes));
    }
    return Uint8List.fromList(bytes);
  }
}

class _FakeWebdatasetService extends WebdatasetService {
  _FakeWebdatasetService({
    required this.shards,
  }) : super();

  final List<WdsShardSummary> shards;
  final List<String> loadDirPaths = <String>[];
  final List<String> listSampleDirPaths = <String>[];
  final List<String> listSampleStreamShards = <String>[];

  @override
  Future<WdsDirSummary> loadDir(String dirPath) async {
    loadDirPaths.add(dirPath);
    return WdsDirSummary(dirPath: dirPath, shards: shards);
  }

  @override
  Future<WdsSampleListResponse> listSamples({
    required String dirPath,
    required String shardFilename,
    int? offset,
    int? length,
    bool? computeTotal,
  }) async {
    listSampleDirPaths.add(dirPath);
    return WdsSampleListResponse(
      offset: offset ?? 0,
      length: length ?? 50,
      numSamplesTotal: 0,
      partial: false,
      samples: const <WdsSampleInfo>[],
    );
  }

  @override
  Future<WdsSampleListResponse> listSamplesFromStream({
    required Stream<List<int>> shardStream,
    required String shardFilename,
    int? offset,
    int? length,
    bool? computeTotal,
  }) async {
    listSampleStreamShards.add(shardFilename);
    return WdsSampleListResponse(
      offset: offset ?? 0,
      length: length ?? 50,
      numSamplesTotal: null,
      partial: true,
      samples: const <WdsSampleInfo>[],
    );
  }
}

RemoteHostConfig _buildSambaHost() {
  return const RemoteHostConfig(
    id: 'nas',
    label: 'NAS',
    type: RemoteHostType.samba,
    samba: SambaRemoteHostConfig(
      host: '192.168.0.2',
      share: 'datasets',
    ),
  );
}

RemoteHostConfig _buildSshHost() {
  return const RemoteHostConfig(
    id: 'ssh',
    label: 'SSH',
    type: RemoteHostType.ssh,
    ssh: SshRemoteHostConfig(
      host: '10.0.0.2',
      username: 'tester',
    ),
  );
}

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  setUp(() async {
    SharedPreferences.setMockInitialValues(<String, Object>{});
  });

  test('opens Samba tar.gz directory as WebDataset', () async {
    final fakeWebdataset = _FakeWebdatasetService(
      shards: const <WdsShardSummary>[
        WdsShardSummary(
          filename: '00000.tar.gz',
          path: '/cache/wds/00000.tar.gz',
          bytes: 123,
          exists: true,
        ),
      ],
    );
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[
        RemotePathEntry(
          path: 'train/00000.tar.gz',
          name: '00000.tar.gz',
          isDirectory: false,
          sizeBytes: 123,
        ),
      ],
      resolvedPath: '/cache/wds',
      bytesByPath: const <String, List<int>>{},
    );
    final state = ViewerState(
      webdataset: fakeWebdataset,
      remoteDatasets: fakeRemote,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSambaHost()];
    addTearDown(state.dispose);

    final added = await state.addSourceFromRemoteHost(
      hostId: 'nas',
      datasetPath: 'train',
      recordRecent: false,
    );

    expect(added, isTrue);
    expect(state.mode, ViewerMode.webdatasetDir);
    final loadFuture = state.wdsDirFuture;
    expect(loadFuture, isNotNull);
    await loadFuture;
    final samplesFuture = state.wdsSamplesFuture;
    expect(samplesFuture, isNotNull);
    await samplesFuture;
    expect(state.wdsDirSummary?.dirPath, 'remote://nas/train');
    expect(state.isRemoteDirectoryMode, isFalse);
    expect(fakeRemote.resolveDatasetPathCalls, equals(0));
    expect(fakeRemote.openReadFileCalls, equals(1));
    expect(fakeWebdataset.listSampleStreamShards, contains('00000.tar.gz'));
  });

  test('keeps remote directory mode when Samba folder has no WebDataset shard',
      () async {
    final fakeWebdataset = _FakeWebdatasetService(shards: const []);
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[
        RemotePathEntry(
          path: 'train/readme.txt',
          name: 'readme.txt',
          isDirectory: false,
          sizeBytes: 10,
        ),
      ],
      resolvedPath: '/cache/unused',
    );
    final state = ViewerState(
      webdataset: fakeWebdataset,
      remoteDatasets: fakeRemote,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSambaHost()];
    addTearDown(state.dispose);

    final added = await state.addSourceFromRemoteHost(
      hostId: 'nas',
      datasetPath: 'train',
      recordRecent: false,
    );

    expect(added, isTrue);
    expect(state.mode, ViewerMode.localDirectory);
    expect(state.isRemoteDirectoryMode, isTrue);
    expect(fakeRemote.resolveDatasetPathCalls, equals(0));
  });

  test('opening same Samba source again still reports success', () async {
    final fakeWebdataset = _FakeWebdatasetService(shards: const []);
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[
        RemotePathEntry(
          path: 'train/readme.txt',
          name: 'readme.txt',
          isDirectory: false,
          sizeBytes: 10,
        ),
      ],
      resolvedPath: '/cache/unused',
    );
    final state = ViewerState(
      webdataset: fakeWebdataset,
      remoteDatasets: fakeRemote,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSambaHost()];
    addTearDown(state.dispose);

    final first = await state.addSourceFromRemoteHost(
      hostId: 'nas',
      datasetPath: 'train',
      recordRecent: false,
    );
    final second = await state.addSourceFromRemoteHost(
      hostId: 'nas',
      datasetPath: 'train',
      recordRecent: false,
    );

    expect(first, isTrue);
    expect(second, isTrue);
  });

  test('opens SSH tar.gz directory as WebDataset', () async {
    final fakeWebdataset = _FakeWebdatasetService(
      shards: const <WdsShardSummary>[
        WdsShardSummary(
          filename: '00000.tar.gz',
          path: '/cache/wds/00000.tar.gz',
          bytes: 123,
          exists: true,
        ),
      ],
    );
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[
        RemotePathEntry(
          path: 'train/00000.tar.gz',
          name: '00000.tar.gz',
          isDirectory: false,
          sizeBytes: 123,
        ),
      ],
      resolvedPath: '/cache/wds',
    );
    final state = ViewerState(
      webdataset: fakeWebdataset,
      remoteDatasets: fakeRemote,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSshHost()];
    addTearDown(state.dispose);

    final added = await state.addSourceFromRemoteHost(
      hostId: 'ssh',
      datasetPath: 'train',
      recordRecent: false,
    );

    expect(added, isTrue);
    expect(state.mode, ViewerMode.webdatasetDir);
    await state.wdsDirFuture;
    await state.wdsSamplesFuture;
    expect(fakeRemote.openReadFileCalls, equals(1));
    expect(fakeWebdataset.listSampleStreamShards, contains('00000.tar.gz'));
  });

  test('opens remote single WebDataset shard path directly', () async {
    final fakeWebdataset = _FakeWebdatasetService(shards: const []);
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[],
      resolvedPath: '/cache/unused',
    );
    final state = ViewerState(
      webdataset: fakeWebdataset,
      remoteDatasets: fakeRemote,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSambaHost()];
    addTearDown(state.dispose);

    final added = await state.addSourceFromRemoteHost(
      hostId: 'nas',
      datasetPath: 'train/00042.tar.gz',
      recordRecent: false,
    );

    expect(added, isTrue);
    expect(state.mode, ViewerMode.webdatasetDir);
    await state.wdsDirFuture;
    await state.wdsSamplesFuture;
    expect(state.wdsDirSummary?.shards, hasLength(1));
    expect(state.wdsDirSummary?.shards.first.filename, '00042.tar.gz');
    expect(fakeRemote.listEntriesCalls, equals(0));
    expect(fakeRemote.openReadFileCalls, equals(1));
    expect(fakeWebdataset.listSampleStreamShards, contains('00042.tar.gz'));
  });

  test('routes remote parquet file path into local directory file item',
      () async {
    final fakeWebdataset = _FakeWebdatasetService(shards: const []);
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[],
      resolvedPath: '/cache/unused',
    );
    final state = ViewerState(
      webdataset: fakeWebdataset,
      remoteDatasets: fakeRemote,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSambaHost()];
    addTearDown(state.dispose);

    final added = await state.addSourceFromRemoteHost(
      hostId: 'nas',
      datasetPath: 'train/part-000.parquet',
      recordRecent: false,
    );

    expect(added, isTrue);
    expect(state.mode, ViewerMode.localDirectory);
    await state.localDirectoryItemsFuture;
    expect(state.localDirectoryItems, hasLength(1));
    expect(state.localDirectoryItems.first.isDirectory, isFalse);
    expect(state.localDirectoryItems.first.name, 'part-000.parquet');
    expect(state.localDirectoryItems.first.path, 'train/part-000.parquet');
    expect(fakeRemote.listEntriesCalls, equals(0));
  });

  test('falls back to localDirectory for remote MDS shard without index',
      () async {
    final fakeWebdataset = _FakeWebdatasetService(shards: const []);
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[],
      resolvedPath: '/cache/unused',
    );
    final state = ViewerState(
      webdataset: fakeWebdataset,
      remoteDatasets: fakeRemote,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSambaHost()];
    addTearDown(state.dispose);

    final added = await state.addSourceFromRemoteHost(
      hostId: 'nas',
      datasetPath: 'train/shard-000.mds',
      recordRecent: false,
    );

    expect(added, isTrue);
    expect(state.mode, ViewerMode.localDirectory);
  });
}
