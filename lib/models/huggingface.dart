class HfConfigSummary {
  const HfConfigSummary({
    required this.config,
    required this.splits,
  });

  final String config;
  final List<String> splits;
}

class HfFeature {
  const HfFeature({
    required this.name,
    required this.dtype,
    required this.rawType,
  });

  final String name;
  final String? dtype;
  final dynamic rawType;
}

class HfDatasetPreview {
  const HfDatasetPreview({
    required this.dataset,
    required this.config,
    required this.split,
    required this.configs,
    required this.offset,
    required this.length,
    required this.numRowsTotal,
    required this.partial,
    required this.features,
    required this.rows,
    required this.featureOffset,
    required this.featureCount,
    required this.totalFeatureCount,
  });

  final String dataset;
  final String config;
  final String split;
  final List<HfConfigSummary> configs;
  final int offset;
  final int length;
  final int numRowsTotal;
  final bool partial;
  final List<HfFeature> features;
  final List<dynamic> rows;
  final int featureOffset;
  final int featureCount;
  final int totalFeatureCount;
}
