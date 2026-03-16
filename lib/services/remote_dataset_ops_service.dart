import 'dart:async';
import 'dart:typed_data';

import '../models/common.dart';
import 'litdata_service.dart';
import 'mosaicml_service.dart';
import 'retry_policy.dart';

class RemoteDatasetOpsService {
  RemoteDatasetOpsService();

  bool looksLikeMdsCorruption(String message) {
    final lower = message.toLowerCase();
    return lower.contains('zstderror') ||
        lower.contains('failed to stream decode') ||
        lower.contains('decoding error') ||
        lower.contains('corruption') ||
        lower.contains('malformed shard');
  }

  bool looksLikeLitdataCorruption(String message) {
    final lower = message.toLowerCase();
    return lower.contains('malformed chunk') ||
        lower.contains('chunk length mismatch') ||
        lower.contains('unexpected end') ||
        lower.contains('rangeerror') ||
        lower.contains('failed to stream decode') ||
        lower.contains('decoding error');
  }

  bool looksLikeRemoteStreamInstability(String message) {
    final lower = message.toLowerCase();
    return lower.contains('socketexception') ||
        lower.contains('broken pipe') ||
        lower.contains("can't read 4 from socket") ||
        lower.contains('streamsink is closed') ||
        lower.contains('connection reset') ||
        lower.contains('write failed');
  }

  Future<List<ItemMeta>> listMdsItemsFromCompressedStreamWithRetry({
    required MosaicmlService mosaicml,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required Stream<List<int>> Function(int? maxBytes) openCompressedStream,
    int? initialCompressedBytes,
    String? compressedShardCacheKey,
    int maxAttempts = 3,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote MDS index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeMdsCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (attempt) {
        final scanBytes = _resolveAttemptScanBytes(
          attempt: attempt,
          initialScanBytes: initialCompressedBytes,
          maxAttempts: maxAttempts,
        );
        return mosaicml.listSamplesFromZstdCompressedStream(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          openCompressedStream: (_) => openCompressedStream(scanBytes),
          decodedShardCacheKey: _buildMdsCompressedCacheKey(
            baseKey: compressedShardCacheKey,
            indexBytes: indexBytes,
            indexName: indexName,
            shardFilename: shardFilename,
            scanBytes: scanBytes,
          ),
        );
      },
    );
  }

  int? _resolveAttemptScanBytes({
    required int attempt,
    required int? initialScanBytes,
    required int maxAttempts,
    int maxGrowthAttempts = 2,
  }) {
    if (initialScanBytes == null || initialScanBytes <= 0) {
      return null;
    }
    if (attempt >= maxAttempts) {
      return null;
    }
    if (attempt <= 1) {
      return initialScanBytes;
    }
    if (attempt > maxGrowthAttempts + 1) {
      return null;
    }
    final growthShift = attempt - 1;
    final factor = 1 << growthShift;
    final scaled = initialScanBytes * factor;
    if (scaled < initialScanBytes) {
      return null;
    }
    return scaled;
  }

  Future<List<ItemMeta>> listMdsItemsFromRawStreamWithRetry({
    required MosaicmlService mosaicml,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required Stream<List<int>> Function() openRawStream,
    int maxAttempts = 3,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote MDS index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeMdsCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (_) {
        return mosaicml.listSamplesFromRawStream(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          rawStream: openRawStream(),
        );
      },
    );
  }

  Future<FieldPreview> peekMdsFieldFromCompressedStreamWithRetry({
    required MosaicmlService mosaicml,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> Function(int? maxBytes) openCompressedStream,
    int? initialCompressedBytes,
    String? compressedShardCacheKey,
    int maxAttempts = 2,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote MDS index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeMdsCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (attempt) {
        final scanBytes = _resolveAttemptScanBytes(
          attempt: attempt,
          initialScanBytes: initialCompressedBytes,
          maxAttempts: maxAttempts,
          maxGrowthAttempts: maxAttempts - 1,
        );
        return mosaicml.peekFieldFromZstdCompressedStream(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          openCompressedStream: (_) => openCompressedStream(scanBytes),
          decodedShardCacheKey: _buildMdsCompressedCacheKey(
            baseKey: compressedShardCacheKey,
            indexBytes: indexBytes,
            indexName: indexName,
            shardFilename: shardFilename,
            scanBytes: scanBytes,
          ),
        );
      },
    );
  }

  Future<FieldPreview> peekMdsFieldFromRawStreamWithRetry({
    required MosaicmlService mosaicml,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> Function() openRawStream,
    int maxAttempts = 2,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote MDS index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeMdsCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (_) {
        return mosaicml.peekFieldFromRawStream(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          rawStream: openRawStream(),
        );
      },
    );
  }

  Future<List<ItemMeta>> listLitdataItemsFromStreamWithRetry({
    required LitDataService litdata,
    required Uint8List? indexBytes,
    required String indexName,
    required String chunkFilename,
    required Stream<List<int>> Function() openChunkStream,
    int maxAttempts = 3,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote LitData index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeLitdataCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (_) {
        return litdata.listChunkItemsFromStream(
          indexBytes: indexBytes,
          indexName: indexName,
          chunkFilename: chunkFilename,
          chunkStream: openChunkStream(),
        );
      },
    );
  }

  Future<FieldPreview> peekLitdataFieldFromStreamWithRetry({
    required LitDataService litdata,
    required Uint8List? indexBytes,
    required String indexName,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> Function() openChunkStream,
    int maxAttempts = 2,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote LitData index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeLitdataCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (_) {
        return litdata.peekFieldFromStream(
          indexBytes: indexBytes,
          indexName: indexName,
          chunkFilename: chunkFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          chunkStream: openChunkStream(),
        );
      },
    );
  }

  Future<PreparedMediaResponse> prepareLitdataAudioFromStreamWithRetry({
    required LitDataService litdata,
    required Uint8List? indexBytes,
    required String indexName,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> Function() openChunkStream,
    int maxAttempts = 2,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote LitData index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeLitdataCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (_) {
        return litdata.prepareAudioPreviewFromStream(
          indexBytes: indexBytes,
          indexName: indexName,
          chunkFilename: chunkFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          chunkStream: openChunkStream(),
        );
      },
    );
  }

  Future<PreparedFileResponse> prepareLitdataFileFromStreamWithRetry({
    required LitDataService litdata,
    required Uint8List? indexBytes,
    required String indexName,
    required String chunkFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> Function() openChunkStream,
    int maxAttempts = 2,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote LitData index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeLitdataCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (_) {
        return litdata.prepareFieldFileFromStream(
          indexBytes: indexBytes,
          indexName: indexName,
          chunkFilename: chunkFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          chunkStream: openChunkStream(),
        );
      },
    );
  }

  Future<PreparedMediaResponse> prepareMdsAudioFromCompressedStreamWithRetry({
    required MosaicmlService mosaicml,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> Function(int? maxBytes) openCompressedStream,
    int? initialCompressedBytes,
    String? compressedShardCacheKey,
    int maxAttempts = 2,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote MDS index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeMdsCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (attempt) {
        final scanBytes = _resolveAttemptScanBytes(
          attempt: attempt,
          initialScanBytes: initialCompressedBytes,
          maxAttempts: maxAttempts,
          maxGrowthAttempts: maxAttempts - 1,
        );
        return mosaicml.prepareAudioPreviewFromZstdCompressedStream(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          openCompressedStream: (_) => openCompressedStream(scanBytes),
          decodedShardCacheKey: _buildMdsCompressedCacheKey(
            baseKey: compressedShardCacheKey,
            indexBytes: indexBytes,
            indexName: indexName,
            shardFilename: shardFilename,
            scanBytes: scanBytes,
          ),
        );
      },
    );
  }

  Future<PreparedMediaResponse> prepareMdsAudioFromRawStreamWithRetry({
    required MosaicmlService mosaicml,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> Function() openRawStream,
    int maxAttempts = 2,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote MDS index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeMdsCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (_) {
        return mosaicml.prepareAudioPreviewFromRawStream(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          rawStream: openRawStream(),
        );
      },
    );
  }

  Future<PreparedFileResponse> prepareMdsFileFromCompressedStreamWithRetry({
    required MosaicmlService mosaicml,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> Function(int? maxBytes) openCompressedStream,
    int? initialCompressedBytes,
    String? compressedShardCacheKey,
    int maxAttempts = 2,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote MDS index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeMdsCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (attempt) {
        final scanBytes = _resolveAttemptScanBytes(
          attempt: attempt,
          initialScanBytes: initialCompressedBytes,
          maxAttempts: maxAttempts,
          maxGrowthAttempts: maxAttempts - 1,
        );
        return mosaicml.prepareFieldFileFromZstdCompressedStream(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          openCompressedStream: (_) => openCompressedStream(scanBytes),
          decodedShardCacheKey: _buildMdsCompressedCacheKey(
            baseKey: compressedShardCacheKey,
            indexBytes: indexBytes,
            indexName: indexName,
            shardFilename: shardFilename,
            scanBytes: scanBytes,
          ),
        );
      },
    );
  }

  Future<PreparedFileResponse> prepareMdsFileFromRawStreamWithRetry({
    required MosaicmlService mosaicml,
    required Uint8List? indexBytes,
    required String indexName,
    required String shardFilename,
    required int itemIndex,
    required int fieldIndex,
    required Stream<List<int>> Function() openRawStream,
    int maxAttempts = 2,
  }) {
    if (indexBytes == null) {
      throw const FormatException('Missing remote MDS index bytes.');
    }
    return _runWithRetry(
      maxAttempts: maxAttempts,
      shouldRetry: (detail) =>
          looksLikeMdsCorruption(detail) ||
          looksLikeRemoteStreamInstability(detail),
      action: (_) {
        return mosaicml.prepareFieldFileFromRawStream(
          indexBytes: indexBytes,
          indexName: indexName,
          shardFilename: shardFilename,
          itemIndex: itemIndex,
          fieldIndex: fieldIndex,
          rawStream: openRawStream(),
        );
      },
    );
  }

  Future<T> _runWithRetry<T>({
    required int maxAttempts,
    required bool Function(String detail) shouldRetry,
    required Future<T> Function(int attempt) action,
  }) async {
    final policy = RetryPolicy(maxAttempts: maxAttempts < 1 ? 1 : maxAttempts);
    return policy.run(
      action: action,
      shouldRetry: (error) => shouldRetry(error.toString()),
    );
  }

  String? _buildMdsCompressedCacheKey({
    required String? baseKey,
    required Uint8List indexBytes,
    required String indexName,
    required String shardFilename,
    int? scanBytes,
  }) {
    final fingerprint = _fingerprintBytes(indexBytes);
    final fallbackBase = baseKey?.trim();
    final normalizedBase = (fallbackBase == null || fallbackBase.isEmpty)
        ? 'index-only'
        : fallbackBase;
    final scanTag = scanBytes == null ? 'full' : 'scan=$scanBytes';
    return '$normalizedBase|$indexName|$shardFilename|'
        '${indexBytes.length}|$fingerprint|$scanTag';
  }

  int _fingerprintBytes(Uint8List bytes) {
    const fnvOffset = 0x811C9DC5;
    const fnvPrime = 0x01000193;
    var hash = fnvOffset;
    final limit = bytes.length < 65536 ? bytes.length : 65536;
    for (var i = 0; i < limit; i += 1) {
      hash ^= bytes[i];
      hash = (hash * fnvPrime) & 0xFFFFFFFF;
    }
    hash ^= bytes.length;
    return hash & 0xFFFFFFFF;
  }
}
