import 'dart:convert';

import 'package:dataset_inspector/models/remote_host.dart';
import 'package:dataset_inspector/services/dataset_source_routing_service.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('detectFormatFromPath recognizes key dataset file types', () {
    const service = DatasetSourceRoutingService();

    expect(
      service.detectFormatFromPath('/tmp/shard.tar.gz'),
      DatasetSourceFormat.webdatasetShard,
    );
    expect(
      service.detectFormatFromPath('/tmp/index.json.zst'),
      DatasetSourceFormat.litdataIndex,
    );
    expect(
      service.detectFormatFromPath('/tmp/train.mds'),
      DatasetSourceFormat.mdsShard,
    );
    expect(
      service.detectFormatFromPath('/tmp/data.parquet'),
      DatasetSourceFormat.parquetFile,
    );
  });

  test('detectFormatFromEntries prioritizes webdataset then litdata', () {
    const service = DatasetSourceRoutingService();

    expect(
      service.detectFormatFromEntries(const <String>[
        '00000.tar',
        'index.json',
      ]),
      DatasetSourceFormat.webdatasetShard,
    );

    expect(
      service.detectFormatFromEntries(const <String>[
        'index.json',
        'chunk-000.bin',
      ]),
      DatasetSourceFormat.litdataIndex,
    );
  });

  test('capabilityFor returns expected HTTP and Samba matrix entries', () {
    const service = DatasetSourceRoutingService();

    final httpParquet = service.capabilityFor(
      backend: DatasetAccessBackend.httpfs,
      format: DatasetSourceFormat.parquetFile,
    );
    expect(httpParquet.supportsListing, isFalse);
    expect(httpParquet.supportsStreaming, isTrue);
    expect(httpParquet.supportsRandomRead, isTrue);
    expect(httpParquet.supportsWrite, isFalse);

    final sambaMds = service.capabilityFor(
      backend: DatasetAccessBackend.samba,
      format: DatasetSourceFormat.mdsShard,
    );
    expect(sambaMds.supportsListing, isTrue);
    expect(sambaMds.supportsStreaming, isTrue);
    expect(sambaMds.supportsRandomRead, isTrue);
    expect(sambaMds.supportsWrite, isTrue);
  });

  test('backendForRemoteHost maps remote host types', () {
    const service = DatasetSourceRoutingService();

    expect(
      service.backendForRemoteHost(RemoteHostType.ssh),
      DatasetAccessBackend.sshfs,
    );
    expect(
      service.backendForRemoteHost(RemoteHostType.samba),
      DatasetAccessBackend.samba,
    );
    expect(
      service.backendForRemoteHost(RemoteHostType.r2),
      DatasetAccessBackend.r2,
    );
  });

  test('tryParseHttpUrl accepts only HTTP/HTTPS URLs', () {
    const service = DatasetSourceRoutingService();

    expect(service.tryParseHttpUrl('https://example.com/a.parquet'), isNotNull);
    expect(service.tryParseHttpUrl('http://example.com/a.mds'), isNotNull);
    expect(service.tryParseHttpUrl('ftp://example.com/a.mds'), isNull);
    expect(service.tryParseHttpUrl('/tmp/local.txt'), isNull);
  });

  test('parseWebdatasetShardsFromManifest supports JSON and TXT manifests', () {
    const service = DatasetSourceRoutingService();

    final jsonManifest = utf8.encode(jsonEncode(<String, dynamic>{
      'shards': <dynamic>[
        '00000.tar',
        <String, dynamic>{'filename': '00001.tar.gz'},
      ],
    }));
    final txtManifest = utf8.encode('00002.tar\n# comment\n00003.tar.zst\n');

    final fromJson = service.parseWebdatasetShardsFromManifest(
      manifestName: 'shards.json',
      bytes: jsonManifest,
    );
    final fromTxt = service.parseWebdatasetShardsFromManifest(
      manifestName: 'shards.txt',
      bytes: txtManifest,
    );

    expect(fromJson, equals(const <String>['00000.tar', '00001.tar.gz']));
    expect(fromTxt, equals(const <String>['00002.tar', '00003.tar.zst']));
  });
}
