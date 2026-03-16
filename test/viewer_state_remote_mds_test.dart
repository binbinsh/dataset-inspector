import 'dart:typed_data';

import 'package:dataset_inspector/models/common.dart';
import 'package:dataset_inspector/models/remote_host.dart';
import 'package:dataset_inspector/services/mosaicml_service.dart';
import 'package:dataset_inspector/services/remote_dataset_service.dart';
import 'package:dataset_inspector/state/viewer_state.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shared_preferences/shared_preferences.dart';

class _FakeRemoteDatasetService extends RemoteDatasetService {
  _FakeRemoteDatasetService({
    required this.entries,
    required this.bytesByPath,
  });

  final List<RemotePathEntry> entries;
  final Map<String, Uint8List> bytesByPath;

  int listEntriesCalls = 0;
  int readBytesFileCalls = 0;
  int openReadFileCalls = 0;
  String? lastOpenReadRemotePath;

  String _key(String path) {
    return path.trim().replaceAll('\\', '/').replaceFirst(RegExp(r'^/+'), '');
  }

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
  Future<Uint8List> readBytesFile({
    required RemoteHostConfig host,
    required String remotePath,
    int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) async {
    readBytesFileCalls += 1;
    final key = _key(remotePath);
    final bytes = bytesByPath[key];
    if (bytes == null) {
      throw FormatException('Missing remote bytes for $remotePath');
    }
    if (maxBytes != null && maxBytes > 0 && bytes.length > maxBytes) {
      return Uint8List.sublistView(bytes, 0, maxBytes);
    }
    return bytes;
  }

  @override
  Stream<List<int>> openReadFile({
    required RemoteHostConfig host,
    required String remotePath,
    int? maxBytes,
    RemoteStatusCallback? onStatus,
  }) {
    openReadFileCalls += 1;
    lastOpenReadRemotePath = remotePath;
    final key = _key(remotePath);
    final bytes = bytesByPath[key];
    if (bytes == null) {
      return Stream<List<int>>.error(
        FormatException('Missing remote stream for $remotePath'),
      );
    }
    return Stream<List<int>>.value(bytes);
  }
}

class _FakeMosaicmlService extends MosaicmlService {
  _FakeMosaicmlService() : super();

  int loadIndexFromBytesCalls = 0;
  int listRawCalls = 0;
  int listCompressedCalls = 0;
  int peekRawCalls = 0;
  int prepareCompressedCalls = 0;

  @override
  Future<IndexSummary> loadIndexFromBytes(
    Uint8List indexBytes, {
    String indexName = 'index.json',
  }) async {
    loadIndexFromBytesCalls += 1;
    return const IndexSummary(
      indexPath: 'index.json',
      rootDir: '',
      dataFormat: <String>['bytes'],
      compression: null,
      chunkSize: null,
      chunkBytes: null,
      configRaw: <String, dynamic>{},
      chunks: <ChunkSummary>[
        ChunkSummary(
          filename: 'shard-000.mds',
          path: 'shard-000.mds',
          chunkSize: 1,
          chunkBytes: 4,
          dim: null,
          exists: true,
        ),
      ],
    );
  }

  @override
  Future<List<ItemMeta>> listSamplesFromRawStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String shardFilename,
    required Stream<List<int>> rawStream,
  }) async {
    listRawCalls += 1;
    await rawStream.drain<void>();
    return const <ItemMeta>[
      ItemMeta(
        itemIndex: 0,
        totalBytes: 4,
        fields: <FieldMeta>[FieldMeta(fieldIndex: 0, size: 4)],
      ),
    ];
  }

  @override
  Future<List<ItemMeta>> listSamplesFromZstdCompressedStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String shardFilename,
    required Stream<List<int>> Function(int? maxBytes) openCompressedStream,
    String? decodedShardCacheKey,
  }) async {
    listCompressedCalls += 1;
    await openCompressedStream(null).drain<void>();
    return const <ItemMeta>[
      ItemMeta(
        itemIndex: 0,
        totalBytes: 4,
        fields: <FieldMeta>[FieldMeta(fieldIndex: 0, size: 4)],
      ),
    ];
  }

  @override
  Future<FieldPreview> peekFieldFromRawStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> rawStream,
  }) async {
    peekRawCalls += 1;
    await rawStream.drain<void>();
    return const FieldPreview(
      previewText: 'remote mds preview',
      hexSnippet: '00 01',
      guessedExt: 'txt',
      isBinary: false,
      size: 4,
    );
  }

  @override
  Future<PreparedFileResponse> prepareFieldFileFromZstdCompressedStream({
    String? indexPath,
    Uint8List? indexBytes,
    String indexName = 'index.json',
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> Function(int? maxBytes) openCompressedStream,
    String? decodedShardCacheKey,
    bool convertSphereToWav = false,
  }) async {
    prepareCompressedCalls += 1;
    await openCompressedStream(null).drain<void>();
    return const PreparedFileResponse(
      path: '/tmp/remote-mds-field.wav',
      size: 1234,
      ext: 'wav',
    );
  }
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

  test('opens remote MDS directory in mdsIndex mode with streaming', () async {
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[
        RemotePathEntry(
          path: 'train/index.json',
          name: 'index.json',
          isDirectory: false,
          sizeBytes: 32,
        ),
        RemotePathEntry(
          path: 'train/shard-000.mds',
          name: 'shard-000.mds',
          isDirectory: false,
          sizeBytes: 4,
        ),
      ],
      bytesByPath: <String, Uint8List>{
        'train/index.json': Uint8List.fromList(const <int>[1, 2, 3]),
        'train/shard-000.mds': Uint8List.fromList(const <int>[4, 5, 6, 7]),
      },
    );
    final fakeMosaic = _FakeMosaicmlService();
    final state = ViewerState(
      remoteDatasets: fakeRemote,
      mosaicml: fakeMosaic,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSshHost()];
    addTearDown(state.dispose);

    final added = await state.addSourceFromRemoteHost(
      hostId: 'ssh',
      datasetPath: 'train',
      recordRecent: false,
    );

    expect(added, isTrue);
    expect(state.mode, ViewerMode.mdsIndex);
    await state.indexFuture!;

    final items = await state.mdsItemsFuture!;
    expect(items, hasLength(1));
    state.selectItem(0, fieldCount: items.first.fields.length);
    state.selectField(0);
    final preview = await state.mdsFieldPreviewFuture!;
    expect(preview.previewText, contains('remote mds preview'));

    expect(fakeRemote.listEntriesCalls, greaterThanOrEqualTo(1));
    expect(fakeRemote.readBytesFileCalls, greaterThanOrEqualTo(1));
    expect(fakeRemote.openReadFileCalls, greaterThanOrEqualTo(2));
    expect(fakeMosaic.loadIndexFromBytesCalls, greaterThanOrEqualTo(1));
    expect(fakeMosaic.listRawCalls, greaterThanOrEqualTo(1));
    expect(fakeMosaic.peekRawCalls, greaterThanOrEqualTo(1));
  });

  test('routes remote index.json file to mdsIndex when MDS parser matches',
      () async {
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[],
      bytesByPath: <String, Uint8List>{
        'train/index.json': Uint8List.fromList(const <int>[1, 2, 3]),
        'train/shard-000.mds': Uint8List.fromList(const <int>[4, 5, 6, 7]),
      },
    );
    final fakeMosaic = _FakeMosaicmlService();
    final state = ViewerState(
      remoteDatasets: fakeRemote,
      mosaicml: fakeMosaic,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSshHost()];
    addTearDown(state.dispose);

    final added = await state.addSourceFromRemoteHost(
      hostId: 'ssh',
      datasetPath: 'train/index.json',
      recordRecent: false,
    );

    expect(added, isTrue);
    expect(state.mode, ViewerMode.mdsIndex);
    await state.indexFuture!;
    expect(state.selectedChunkName, equals('shard-000.mds'));
    expect(fakeMosaic.loadIndexFromBytesCalls, greaterThanOrEqualTo(2));
  });

  test(
      'prepares remote localDirectory compressed MDS field file by explicit path',
      () async {
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[],
      bytesByPath: <String, Uint8List>{
        'train/index.json': Uint8List.fromList(const <int>[1, 2, 3]),
        'train/shard-000.mds.zstd': Uint8List.fromList(const <int>[4, 5, 6, 7]),
      },
    );
    final fakeMosaic = _FakeMosaicmlService();
    final state = ViewerState(
      remoteDatasets: fakeRemote,
      mosaicml: fakeMosaic,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSshHost()];
    addTearDown(state.dispose);

    final prepared = await state.apiPrepareLocalDirectoryFieldFile(
      path: 'remote://ssh/train/shard-000.mds.zstd',
      itemIndex: 0,
      fieldIndex: 0,
    );

    expect(prepared.path, '/tmp/remote-mds-field.wav');
    expect(prepared.ext, 'wav');
    expect(prepared.size, 1234);
    expect(fakeRemote.readBytesFileCalls, greaterThanOrEqualTo(1));
    expect(fakeRemote.openReadFileCalls, greaterThanOrEqualTo(1));
    expect(fakeMosaic.prepareCompressedCalls, equals(1));
  });

  test('loads remote compressed MDS shard when index filename omits .zstd',
      () async {
    final fakeRemote = _FakeRemoteDatasetService(
      entries: const <RemotePathEntry>[
        RemotePathEntry(
          path: 'train/index.json',
          name: 'index.json',
          isDirectory: false,
          sizeBytes: 32,
        ),
        RemotePathEntry(
          path: 'train/shard-000.mds.zstd',
          name: 'shard-000.mds.zstd',
          isDirectory: false,
          sizeBytes: 8,
        ),
      ],
      bytesByPath: <String, Uint8List>{
        'train/index.json': Uint8List.fromList(const <int>[1, 2, 3]),
        'train/shard-000.mds.zstd': Uint8List.fromList(const <int>[4, 5, 6, 7]),
      },
    );
    final fakeMosaic = _FakeMosaicmlService();
    final state = ViewerState(
      remoteDatasets: fakeRemote,
      mosaicml: fakeMosaic,
    );
    state.remoteHosts = <RemoteHostConfig>[_buildSshHost()];
    addTearDown(state.dispose);

    final added = await state.addSourceFromRemoteHost(
      hostId: 'ssh',
      datasetPath: 'train',
      recordRecent: false,
    );

    expect(added, isTrue);
    expect(state.mode, ViewerMode.mdsIndex);
    await state.indexFuture!;
    final items = await state.mdsItemsFuture!;
    expect(items, hasLength(1));
    expect(fakeMosaic.listCompressedCalls, equals(1));
    expect(
        fakeRemote.lastOpenReadRemotePath, equals('train/shard-000.mds.zstd'));
  });
}
