import 'dart:convert';

import 'package:shared_preferences/shared_preferences.dart';

class PreferencesService {
  static const _storeLastIndex = 'last_index';
  static const _storeOpenersByExt = 'openers_by_ext';
  static const _storeHfToken = 'hf_token';
  static const _storeRecentSources = 'recent_sources';

  Future<void> saveLastIndex(String indexPath) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(_storeLastIndex, indexPath);
  }

  Future<String?> readLastIndex() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getString(_storeLastIndex);
  }

  Future<String?> readPreferredOpenerForExt(String ext) async {
    final normalized = ext.trim().replaceFirst('.', '').toLowerCase();
    if (normalized.isEmpty) return null;
    final prefs = await SharedPreferences.getInstance();
    final raw = prefs.getString(_storeOpenersByExt);
    if (raw == null || raw.isEmpty) return null;
    final map = jsonDecode(raw) as Map<String, dynamic>;
    final value = map[normalized]?.toString();
    return value?.isNotEmpty == true ? value : null;
  }

  Future<void> savePreferredOpenerForExt(String ext, String appPath) async {
    final normalized = ext.trim().replaceFirst('.', '').toLowerCase();
    final trimmedPath = appPath.trim();
    if (normalized.isEmpty || trimmedPath.isEmpty) return;
    final prefs = await SharedPreferences.getInstance();
    final raw = prefs.getString(_storeOpenersByExt);
    final map = raw == null || raw.isEmpty
        ? <String, dynamic>{}
        : jsonDecode(raw) as Map<String, dynamic>;
    map[normalized] = trimmedPath;
    await prefs.setString(_storeOpenersByExt, jsonEncode(map));
  }

  Future<String?> readHfToken() async {
    final prefs = await SharedPreferences.getInstance();
    final value = prefs.getString(_storeHfToken);
    if (value == null) return null;
    final trimmed = value.trim();
    return trimmed.isEmpty ? null : trimmed;
  }

  Future<List<String>> readRecentSources() async {
    final prefs = await SharedPreferences.getInstance();
    final raw = prefs.getStringList(_storeRecentSources);
    return raw ?? <String>[];
  }

  Future<void> saveRecentSources(List<String> sources) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setStringList(_storeRecentSources, sources);
  }

  Future<void> saveHfToken(String token) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(_storeHfToken, token.trim());
  }

  Future<void> clearHfToken() async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove(_storeHfToken);
  }
}
