import 'dart:convert';

class PersistedDatasetSource {
  const PersistedDatasetSource({
    required this.mode,
    required this.sourceInput,
    this.payload,
    this.paths,
    this.expanded = true,
    this.identity,
    this.selectedChunkName,
    this.selectedShardName,
    this.selectedHfConfig,
    this.selectedHfSplit,
    this.selectedZenodoFileKey,
  });

  final String mode;
  final String sourceInput;
  final String? payload;
  final List<String>? paths;
  final bool expanded;
  final String? identity;
  final String? selectedChunkName;
  final String? selectedShardName;
  final String? selectedHfConfig;
  final String? selectedHfSplit;
  final String? selectedZenodoFileKey;

  Map<String, dynamic> toJson() {
    final data = <String, dynamic>{
      'mode': mode,
      'sourceInput': sourceInput,
      'expanded': expanded,
      if (payload != null && payload!.trim().isNotEmpty)
        'payload': payload!.trim(),
      if (paths != null && paths!.isNotEmpty) 'paths': paths,
      if (identity != null && identity!.trim().isNotEmpty)
        'identity': identity!.trim(),
      if (selectedChunkName != null && selectedChunkName!.trim().isNotEmpty)
        'selectedChunkName': selectedChunkName!.trim(),
      if (selectedShardName != null && selectedShardName!.trim().isNotEmpty)
        'selectedShardName': selectedShardName!.trim(),
      if (selectedHfConfig != null && selectedHfConfig!.trim().isNotEmpty)
        'selectedHfConfig': selectedHfConfig!.trim(),
      if (selectedHfSplit != null && selectedHfSplit!.trim().isNotEmpty)
        'selectedHfSplit': selectedHfSplit!.trim(),
      if (selectedZenodoFileKey != null &&
          selectedZenodoFileKey!.trim().isNotEmpty)
        'selectedZenodoFileKey': selectedZenodoFileKey!.trim(),
    };
    return data;
  }

  static PersistedDatasetSource? fromJson(Map<String, dynamic>? json) {
    if (json == null) return null;
    final mode = _readString(json, 'mode');
    final sourceInput = _readString(json, 'sourceInput');
    if (mode == null || sourceInput == null) return null;
    return PersistedDatasetSource(
      mode: mode,
      sourceInput: sourceInput,
      payload: _readString(json, 'payload'),
      paths: _readStringList(json['paths']),
      expanded: json['expanded'] is bool ? json['expanded'] as bool : true,
      identity: _readString(json, 'identity'),
      selectedChunkName: _readString(json, 'selectedChunkName'),
      selectedShardName: _readString(json, 'selectedShardName'),
      selectedHfConfig: _readString(json, 'selectedHfConfig'),
      selectedHfSplit: _readString(json, 'selectedHfSplit'),
      selectedZenodoFileKey: _readString(json, 'selectedZenodoFileKey'),
    );
  }

  static String? _readString(Map<String, dynamic> json, String key) {
    final value = json[key];
    if (value == null) return null;
    final text = value.toString().trim();
    return text.isEmpty ? null : text;
  }

  static List<String>? _readStringList(dynamic value) {
    if (value is! List) return null;
    final items = <String>[];
    for (final entry in value) {
      if (entry == null) continue;
      final text = entry.toString().trim();
      if (text.isEmpty) continue;
      items.add(text);
    }
    return items.isEmpty ? null : items;
  }
}

class ViewerSessionSnapshot {
  const ViewerSessionSnapshot({
    required this.datasets,
    this.activeIdentity,
    this.sourceInput,
    this.version = 1,
  });

  final int version;
  final List<PersistedDatasetSource> datasets;
  final String? activeIdentity;
  final String? sourceInput;

  Map<String, dynamic> toJson() {
    return <String, dynamic>{
      'version': version,
      'datasets': datasets.map((dataset) => dataset.toJson()).toList(),
      if (activeIdentity != null && activeIdentity!.trim().isNotEmpty)
        'activeIdentity': activeIdentity!.trim(),
      if (sourceInput != null && sourceInput!.trim().isNotEmpty)
        'sourceInput': sourceInput!.trim(),
    };
  }

  static ViewerSessionSnapshot? fromJson(Map<String, dynamic>? json) {
    if (json == null) return null;
    final datasetsRaw = json['datasets'];
    if (datasetsRaw is! List) return null;
    final datasets = <PersistedDatasetSource>[];
    for (final entry in datasetsRaw) {
      if (entry is Map<String, dynamic>) {
        final dataset = PersistedDatasetSource.fromJson(entry);
        if (dataset != null) datasets.add(dataset);
      } else if (entry is Map) {
        final converted = Map<String, dynamic>.from(entry);
        final dataset = PersistedDatasetSource.fromJson(converted);
        if (dataset != null) datasets.add(dataset);
      }
    }
    final version = json['version'] is int ? json['version'] as int : 1;
    final activeIdentity = _readString(json, 'activeIdentity');
    final sourceInput = _readString(json, 'sourceInput');
    return ViewerSessionSnapshot(
      datasets: datasets,
      activeIdentity: activeIdentity,
      sourceInput: sourceInput,
      version: version,
    );
  }

  String toJsonString() => jsonEncode(toJson());

  static ViewerSessionSnapshot? fromJsonString(String? raw) {
    if (raw == null) return null;
    final trimmed = raw.trim();
    if (trimmed.isEmpty) return null;
    try {
      final decoded = jsonDecode(trimmed);
      if (decoded is Map<String, dynamic>) {
        return ViewerSessionSnapshot.fromJson(decoded);
      }
      if (decoded is Map) {
        return ViewerSessionSnapshot.fromJson(Map<String, dynamic>.from(decoded));
      }
    } catch (_) {}
    return null;
  }

  static String? _readString(Map<String, dynamic> json, String key) {
    final value = json[key];
    if (value == null) return null;
    final text = value.toString().trim();
    return text.isEmpty ? null : text;
  }
}
