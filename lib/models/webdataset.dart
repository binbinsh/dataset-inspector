import 'common.dart';

enum LocalDatasetKind {
  litdataIndex,
  mdsIndex,
  webdatasetDir,
}

class LocalDatasetDetectResponse {
  const LocalDatasetDetectResponse({
    required this.kind,
    required this.path,
  });

  final LocalDatasetKind kind;
  final String path;
}

class WdsShardSummary {
  const WdsShardSummary({
    required this.filename,
    required this.path,
    required this.bytes,
    required this.exists,
  });

  final String filename;
  final String path;
  final int bytes;
  final bool exists;
}

class WdsDirSummary {
  const WdsDirSummary({
    required this.dirPath,
    required this.shards,
  });

  final String dirPath;
  final List<WdsShardSummary> shards;
}

class WdsFieldInfo {
  const WdsFieldInfo({
    required this.name,
    required this.memberPath,
    required this.size,
  });

  final String name;
  final String memberPath;
  final int size;
}

class WdsSampleInfo {
  const WdsSampleInfo({
    required this.sampleIndex,
    required this.key,
    required this.totalBytes,
    required this.fields,
  });

  final int sampleIndex;
  final String key;
  final int totalBytes;
  final List<WdsFieldInfo> fields;
}

class WdsSampleListResponse {
  const WdsSampleListResponse({
    required this.offset,
    required this.length,
    required this.numSamplesTotal,
    required this.partial,
    required this.samples,
  });

  final int offset;
  final int length;
  final int? numSamplesTotal;
  final bool partial;
  final List<WdsSampleInfo> samples;
}
