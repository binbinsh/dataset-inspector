import 'package:dataset_inspector/models/huggingface.dart';
import 'package:dataset_inspector/services/hf_preview_flow_service.dart';
import 'package:flutter_test/flutter_test.dart';

HfDatasetPreview _preview({
  required List<HfFeature> features,
  required List<dynamic> rows,
  required int totalFeatureCount,
  required int featureCount,
}) {
  return HfDatasetPreview(
    dataset: 'd',
    config: 'c',
    split: 's',
    configs: const <HfConfigSummary>[],
    offset: 0,
    length: 1,
    numRowsTotal: 1,
    partial: featureCount < totalFeatureCount,
    features: features,
    rows: rows,
    featureOffset: 0,
    featureCount: featureCount,
    totalFeatureCount: totalFeatureCount,
  );
}

void main() {
  test('mergeFeatureChunk merges rows and deduplicates features', () {
    const service = HfPreviewFlowService();
    final current = _preview(
      features: const <HfFeature>[
        HfFeature(name: 'a', dtype: 'int', rawType: <String, dynamic>{}),
      ],
      rows: const <Map<String, dynamic>>[
        <String, dynamic>{'a': 1},
      ],
      totalFeatureCount: 2,
      featureCount: 1,
    );
    final chunk = _preview(
      features: const <HfFeature>[
        HfFeature(name: 'b', dtype: 'int', rawType: <String, dynamic>{}),
      ],
      rows: const <Map<String, dynamic>>[
        <String, dynamic>{'b': 2},
      ],
      totalFeatureCount: 2,
      featureCount: 1,
    );

    final merged = service.mergeFeatureChunk(current, chunk);
    expect(merged, isNotNull);
    expect(merged!.features.map((f) => f.name), equals(<String>['a', 'b']));
    expect(
        merged.rows,
        equals(const <Map<String, dynamic>>[
          <String, dynamic>{'a': 1, 'b': 2},
        ]));
  });

  test('mergeFeatureChunk returns null on invalid chunk rows', () {
    const service = HfPreviewFlowService();
    final current = _preview(
      features: const <HfFeature>[
        HfFeature(name: 'a', dtype: 'int', rawType: <String, dynamic>{}),
      ],
      rows: const <Map<String, dynamic>>[
        <String, dynamic>{'a': 1},
      ],
      totalFeatureCount: 2,
      featureCount: 1,
    );
    final chunk = _preview(
      features: const <HfFeature>[
        HfFeature(name: 'b', dtype: 'int', rawType: <String, dynamic>{}),
      ],
      rows: const <dynamic>['bad-row'],
      totalFeatureCount: 2,
      featureCount: 1,
    );

    final merged = service.mergeFeatureChunk(current, chunk);
    expect(merged, isNull);
  });

  test('isFeatureLoadComplete and shouldLoadRemainingFeatures', () {
    const service = HfPreviewFlowService();
    final partial = _preview(
      features: const <HfFeature>[
        HfFeature(name: 'a', dtype: 'int', rawType: <String, dynamic>{}),
      ],
      rows: const <Map<String, dynamic>>[
        <String, dynamic>{'a': 1},
      ],
      totalFeatureCount: 3,
      featureCount: 1,
    );
    final complete = _preview(
      features: const <HfFeature>[
        HfFeature(name: 'a', dtype: 'int', rawType: <String, dynamic>{}),
        HfFeature(name: 'b', dtype: 'int', rawType: <String, dynamic>{}),
        HfFeature(name: 'c', dtype: 'int', rawType: <String, dynamic>{}),
      ],
      rows: const <Map<String, dynamic>>[
        <String, dynamic>{'a': 1, 'b': 2, 'c': 3},
      ],
      totalFeatureCount: 3,
      featureCount: 3,
    );

    expect(service.shouldLoadRemainingFeatures(partial), isTrue);
    expect(service.isFeatureLoadComplete(partial), isFalse);
    expect(service.shouldLoadRemainingFeatures(complete), isFalse);
    expect(service.isFeatureLoadComplete(complete), isTrue);
  });
}
