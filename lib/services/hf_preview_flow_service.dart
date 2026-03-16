import '../models/huggingface.dart';

class HfPreviewFlowService {
  const HfPreviewFlowService();

  bool shouldLoadRemainingFeatures(HfDatasetPreview preview) {
    return preview.totalFeatureCount > 0 &&
        preview.featureCount < preview.totalFeatureCount;
  }

  bool isFeatureLoadComplete(HfDatasetPreview preview) {
    final total = preview.totalFeatureCount;
    return total > 0 && preview.features.length >= total;
  }

  HfDatasetPreview? mergeFeatureChunk(
    HfDatasetPreview current,
    HfDatasetPreview chunk,
  ) {
    if (current.rows.isEmpty) {
      return chunk;
    }

    final mergedRows = <Map<String, dynamic>>[];
    final mergedFeatures = <HfFeature>[...current.features];
    final featureNames = <String>{
      for (final feature in current.features) feature.name,
    };
    var chunkRowsAdded = 0;

    final rows = current.rows.length >= chunk.rows.length
        ? current.rows.length
        : chunk.rows.length;

    for (var index = 0; index < rows; index += 1) {
      final currentRow = <String, dynamic>{};
      final mapIndex = index < current.rows.length ? current.rows[index] : null;
      if (mapIndex is Map<String, dynamic>) {
        currentRow.addAll(mapIndex);
      }
      if (index < chunk.rows.length) {
        final chunkRow = chunk.rows[index];
        if (chunkRow is Map<String, dynamic>) {
          currentRow.addAll(chunkRow);
          chunkRowsAdded += 1;
        }
      }
      mergedRows.add(currentRow);
    }

    if (chunkRowsAdded == 0 && chunk.featureCount > 0) return null;

    for (final feature in chunk.features) {
      if (feature.name.isEmpty) continue;
      if (featureNames.add(feature.name)) {
        mergedFeatures.add(feature);
      }
    }

    return HfDatasetPreview(
      dataset: current.dataset,
      config: current.config,
      split: current.split,
      configs: current.configs,
      offset: current.offset,
      length: current.length,
      numRowsTotal: current.numRowsTotal,
      partial: mergedFeatures.length < current.totalFeatureCount,
      features: mergedFeatures,
      rows: mergedRows,
      featureOffset: current.featureOffset,
      featureCount: mergedFeatures.length,
      totalFeatureCount: current.totalFeatureCount,
    );
  }
}
