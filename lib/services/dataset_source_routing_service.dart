import 'dart:convert';

import '../models/remote_host.dart';

enum DatasetSourceFormat {
  webdatasetShard,
  litdataIndex,
  litdataChunk,
  mdsShard,
  parquetFile,
  directory,
  unknown,
}

enum DatasetAccessBackend {
  localFs,
  httpfs,
  sshfs,
  samba,
  r2,
}

class DatasetAccessCapability {
  const DatasetAccessCapability({
    required this.supportsListing,
    required this.supportsStreaming,
    required this.supportsRandomRead,
    required this.supportsWrite,
  });

  final bool supportsListing;
  final bool supportsStreaming;
  final bool supportsRandomRead;
  final bool supportsWrite;
}

class DatasetSourceRoutingService {
  const DatasetSourceRoutingService();

  static const List<String> litdataIndexCandidates = <String>[
    'index.json',
    'index.json.zstd',
    'index.json.zst',
    '0.index.json',
    '0.index.json.zstd',
    '0.index.json.zst',
  ];

  static const List<String> mdsIndexCandidates = <String>[
    'index.json',
    'index.json.zstd',
    'index.json.zst',
  ];

  static const List<String> webdatasetManifestCandidates = <String>[
    'shards.json',
    'shards.txt',
    'manifest.json',
    'wds.json',
    'wds-manifest.json',
  ];

  Uri? tryParseHttpUrl(String input) {
    final trimmed = input.trim();
    if (trimmed.isEmpty) return null;
    final uri = Uri.tryParse(trimmed);
    if (uri == null) return null;
    final scheme = uri.scheme.toLowerCase();
    if (scheme != 'http' && scheme != 'https') return null;
    if (uri.host.trim().isEmpty) return null;
    return uri;
  }

  DatasetAccessBackend backendForRemoteHost(RemoteHostType type) {
    switch (type) {
      case RemoteHostType.samba:
        return DatasetAccessBackend.samba;
      case RemoteHostType.ssh:
        return DatasetAccessBackend.sshfs;
      case RemoteHostType.r2:
        return DatasetAccessBackend.r2;
    }
  }

  DatasetSourceFormat detectFormatFromPath(String path) {
    final name = _basename(path).toLowerCase();
    if (name.isEmpty) return DatasetSourceFormat.unknown;
    if (isWebdatasetShardName(name)) return DatasetSourceFormat.webdatasetShard;
    if (isLitdataIndexName(name)) return DatasetSourceFormat.litdataIndex;
    if (isMdsShardName(name)) return DatasetSourceFormat.mdsShard;
    if (isParquetName(name)) return DatasetSourceFormat.parquetFile;
    if (name.endsWith('/')) return DatasetSourceFormat.directory;
    return DatasetSourceFormat.unknown;
  }

  DatasetSourceFormat detectFormatFromEntries(Iterable<String> fileNames) {
    final names = fileNames
        .map((name) => _basename(name).toLowerCase().trim())
        .where((name) => name.isNotEmpty)
        .toList(growable: false);
    if (names.isEmpty) return DatasetSourceFormat.directory;
    if (names.any(isWebdatasetShardName)) {
      return DatasetSourceFormat.webdatasetShard;
    }
    final hasLitdataIndex = names.any(isLitdataIndexName);
    final hasLitdataChunk = names.any(isLitdataChunkName);
    if (hasLitdataIndex && hasLitdataChunk) {
      return DatasetSourceFormat.litdataIndex;
    }
    if (names.any(isMdsShardName)) {
      return DatasetSourceFormat.mdsShard;
    }
    if (names.any(isParquetName)) {
      return DatasetSourceFormat.parquetFile;
    }
    return DatasetSourceFormat.directory;
  }

  DatasetAccessCapability capabilityFor({
    required DatasetAccessBackend backend,
    required DatasetSourceFormat format,
  }) {
    if (format == DatasetSourceFormat.unknown) {
      return const DatasetAccessCapability(
        supportsListing: false,
        supportsStreaming: false,
        supportsRandomRead: false,
        supportsWrite: false,
      );
    }

    switch (backend) {
      case DatasetAccessBackend.localFs:
        return const DatasetAccessCapability(
          supportsListing: true,
          supportsStreaming: true,
          supportsRandomRead: true,
          supportsWrite: true,
        );
      case DatasetAccessBackend.sshfs:
      case DatasetAccessBackend.samba:
      case DatasetAccessBackend.r2:
        return const DatasetAccessCapability(
          supportsListing: true,
          supportsStreaming: true,
          supportsRandomRead: true,
          supportsWrite: true,
        );
      case DatasetAccessBackend.httpfs:
        if (format == DatasetSourceFormat.directory) {
          return const DatasetAccessCapability(
            supportsListing: false,
            supportsStreaming: false,
            supportsRandomRead: false,
            supportsWrite: false,
          );
        }
        return const DatasetAccessCapability(
          supportsListing: false,
          supportsStreaming: true,
          supportsRandomRead: true,
          supportsWrite: false,
        );
    }
  }

  bool isWebdatasetShardName(String filename) {
    final name = filename.toLowerCase();
    return name.endsWith('.tar') ||
        name.endsWith('.tar.gz') ||
        name.endsWith('.tgz') ||
        name.endsWith('.tar.zst') ||
        name.endsWith('.tar.zstd');
  }

  bool isLitdataIndexName(String filename) {
    final name = filename.toLowerCase().trim();
    return name == 'index.json' ||
        name == 'index.json.zstd' ||
        name == 'index.json.zst' ||
        name == '0.index.json' ||
        name == '0.index.json.zstd' ||
        name == '0.index.json.zst' ||
        name.endsWith('.index.json') ||
        name.contains('.index.json.');
  }

  bool isLitdataChunkName(String filename) {
    final name = filename.toLowerCase().trim();
    if (name.isEmpty || isLitdataIndexName(name)) return false;
    if (isWebdatasetShardName(name)) return false;
    if (isMdsShardName(name)) return false;
    if (name.endsWith('.zst') || name.endsWith('.zstd')) return true;
    return name.endsWith('.bin') || name.contains('.bin.');
  }

  bool isMdsShardName(String filename) {
    final name = filename.toLowerCase().trim();
    return name.endsWith('.mds') ||
        name.endsWith('.mds.zst') ||
        name.endsWith('.mds.zstd') ||
        name == 'mds.zst' ||
        name == 'mds.zstd';
  }

  bool isParquetName(String filename) {
    return filename.toLowerCase().trim().endsWith('.parquet');
  }

  List<String> parseWebdatasetShardsFromManifest({
    required String manifestName,
    required List<int> bytes,
  }) {
    final name = manifestName.toLowerCase().trim();
    if (name.endsWith('.txt')) {
      final text = utf8.decode(bytes, allowMalformed: true);
      final result = <String>[];
      for (final raw in const LineSplitter().convert(text)) {
        final value = raw.trim();
        if (value.isEmpty || value.startsWith('#')) continue;
        if (isWebdatasetShardName(value)) {
          result.add(_basename(value));
        }
      }
      return result;
    }

    final decoded = jsonDecode(utf8.decode(bytes, allowMalformed: true));
    final result = <String>[];
    void addIfShard(String? value) {
      if (value == null) return;
      final trimmed = value.trim();
      if (trimmed.isEmpty) return;
      if (!isWebdatasetShardName(trimmed)) return;
      result.add(_basename(trimmed));
    }

    if (decoded is List) {
      for (final entry in decoded) {
        if (entry is String) {
          addIfShard(entry);
          continue;
        }
        if (entry is Map) {
          addIfShard(_readString(entry, 'filename'));
          addIfShard(_readString(entry, 'path'));
          addIfShard(_readString(entry, 'name'));
        }
      }
      return _uniqueStable(result);
    }

    if (decoded is Map) {
      final shards = decoded['shards'];
      if (shards is List) {
        for (final entry in shards) {
          if (entry is String) {
            addIfShard(entry);
            continue;
          }
          if (entry is Map) {
            addIfShard(_readString(entry, 'filename'));
            addIfShard(_readString(entry, 'path'));
            addIfShard(_readString(entry, 'name'));
          }
        }
      }

      final files = decoded['files'];
      if (files is List) {
        for (final entry in files) {
          if (entry is String) {
            addIfShard(entry);
            continue;
          }
          if (entry is Map) {
            addIfShard(_readString(entry, 'filename'));
            addIfShard(_readString(entry, 'path'));
            addIfShard(_readString(entry, 'name'));
          }
        }
      }
    }

    return _uniqueStable(result);
  }

  String? _readString(Map map, String key) {
    final value = map[key];
    if (value is String) return value;
    return null;
  }

  List<String> _uniqueStable(List<String> values) {
    final seen = <String>{};
    final result = <String>[];
    for (final value in values) {
      if (seen.add(value)) {
        result.add(value);
      }
    }
    return result;
  }

  String _basename(String path) {
    var value = path.trim().replaceAll('\\', '/');
    while (value.endsWith('/') && value.length > 1) {
      value = value.substring(0, value.length - 1);
    }
    final slash = value.lastIndexOf('/');
    if (slash < 0) return value;
    if (slash >= value.length - 1) return '';
    return value.substring(slash + 1);
  }
}
