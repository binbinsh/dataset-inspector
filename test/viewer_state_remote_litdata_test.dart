import 'dart:convert';
import 'dart:typed_data';

import 'package:dataset_inspector/models/remote_host.dart';
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
      throw FormatException('Missing remote file: $remotePath');
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
    final key = _key(remotePath);
    final bytes = bytesByPath[key];
    if (bytes == null) {
      return Stream<List<int>>.error(
        FormatException('Missing remote stream: $remotePath'),
      );
    }
    return Stream<List<int>>.value(bytes);
  }
}

RemoteHostConfig _buildR2Host() {
  return const RemoteHostConfig(
    id: 'r2',
    label: 'R2',
    type: RemoteHostType.r2,
    r2: R2RemoteHostConfig(
      endpoint: 'example.r2.cloudflarestorage.com',
      bucket: 'datasets',
      accessKeyId: 'ak',
      secretAccessKey: 'sk',
    ),
  );
}

List<int> _u32le(int value) {
  final data = ByteData(4)..setUint32(0, value, Endian.little);
  return data.buffer.asUint8List();
}

Uint8List _buildSingleFieldChunk(String text) {
  final payload = utf8.encode(text);
  final start = 4 + 8; // num_items(4) + offsets(2 * 4)
  final end = start + 4 + payload.length; // field_sizes(4) + payload
  return Uint8List.fromList(<int>[
    ..._u32le(1),
    ..._u32le(start),
    ..._u32le(end),
    ..._u32le(payload.length),
    ...payload,
  ]);
}

Uint8List _buildIndexBytes({
  required String chunkFilename,
  required int chunkBytes,
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
      'data_format': <String>['bytes'],
      'data_spec': null,
    },
  };
  return Uint8List.fromList(utf8.encode(jsonEncode(json)));
}

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  setUp(() async {
    SharedPreferences.setMockInitialValues(<String, Object>{});
  });

  test('opens R2 LitData directory with remote streaming', () async {
    const chunkFilename = 'chunk-000.bin';
    final chunkBytes = _buildSingleFieldChunk('hello remote litdata');
    final indexBytes = _buildIndexBytes(
      chunkFilename: chunkFilename,
      chunkBytes: chunkBytes.length,
    );
    final fakeRemote = _FakeRemoteDatasetService(
      entries: <RemotePathEntry>[
        RemotePathEntry(
          path: 'train/index.json',
          name: 'index.json',
          isDirectory: false,
          sizeBytes: indexBytes.length,
        ),
        RemotePathEntry(
          path: 'train/$chunkFilename',
          name: chunkFilename,
          isDirectory: false,
          sizeBytes: chunkBytes.length,
        ),
      ],
      bytesByPath: <String, Uint8List>{
        'train/index.json': indexBytes,
        'train/$chunkFilename': chunkBytes,
      },
    );
    final state = ViewerState(remoteDatasets: fakeRemote);
    state.remoteHosts = <RemoteHostConfig>[_buildR2Host()];
    addTearDown(state.dispose);

    final added = await state.addSourceFromRemoteHost(
      hostId: 'r2',
      datasetPath: 'train',
      recordRecent: false,
    );

    expect(added, isTrue);
    expect(state.mode, ViewerMode.litdataIndex);
    await state.indexFuture!;

    final items = await state.litdataItemsFuture!;
    expect(items, hasLength(1));
    expect(items.first.fields, hasLength(1));

    state.selectItem(0, fieldCount: items.first.fields.length);
    state.selectField(0);
    final preview = await state.fieldPreviewFuture!;
    expect(preview.previewText, contains('hello remote litdata'));

    expect(fakeRemote.listEntriesCalls, greaterThanOrEqualTo(1));
    expect(fakeRemote.readBytesFileCalls, greaterThanOrEqualTo(1));
    expect(fakeRemote.openReadFileCalls, greaterThanOrEqualTo(2));
  });
}
