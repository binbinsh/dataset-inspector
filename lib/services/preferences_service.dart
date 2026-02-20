import 'dart:convert';
import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../models/remote_host.dart';
import '../models/viewer_persistence.dart';

class PreferencesService {
  PreferencesService({SharedPreferences? prefs})
      : _prefs = prefs != null
            ? Future.value(prefs)
            : SharedPreferences.getInstance();

  final Future<SharedPreferences> _prefs;

  static const _storeLastIndex = 'last_index';
  static const _storeOpenersByExt = 'openers_by_ext';
  static const _storeHfToken = 'hf_token';
  static const _storeRecentSources = 'recent_sources';
  static const _storeRemoteHosts = 'remote_hosts_v1';
  static const _storeRemoteCacheQuotaMb = 'remote_cache_quota_mb_v1';
  static const _storeViewerSession = 'viewer_session_v1';
  static const _viewerSessionFilename = 'viewer_session.json';

  Future<SharedPreferences> _getPrefs() => _prefs;

  Future<File> _viewerSessionFile() async {
    final dir = await getApplicationSupportDirectory();
    return File(p.join(dir.path, _viewerSessionFilename));
  }

  Future<void> saveLastIndex(String indexPath) async {
    final prefs = await _getPrefs();
    await prefs.setString(_storeLastIndex, indexPath);
  }

  Future<String?> readLastIndex() async {
    final prefs = await _getPrefs();
    return prefs.getString(_storeLastIndex);
  }

  Future<String?> readPreferredOpenerForExt(String ext) async {
    final normalized = ext.trim().replaceFirst('.', '').toLowerCase();
    if (normalized.isEmpty) return null;
    final prefs = await _getPrefs();
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
    final prefs = await _getPrefs();
    final raw = prefs.getString(_storeOpenersByExt);
    final map = raw == null || raw.isEmpty
        ? <String, dynamic>{}
        : jsonDecode(raw) as Map<String, dynamic>;
    map[normalized] = trimmedPath;
    await prefs.setString(_storeOpenersByExt, jsonEncode(map));
  }

  Future<String?> readHfToken() async {
    final prefs = await _getPrefs();
    final value = prefs.getString(_storeHfToken);
    if (value == null) return null;
    final trimmed = value.trim();
    return trimmed.isEmpty ? null : trimmed;
  }

  Future<List<String>> readRecentSources() async {
    final prefs = await _getPrefs();
    final raw = prefs.getStringList(_storeRecentSources);
    return raw ?? <String>[];
  }

  Future<void> saveRecentSources(List<String> sources) async {
    final prefs = await _getPrefs();
    await prefs.setStringList(_storeRecentSources, sources);
  }

  Future<List<RemoteHostConfig>> readRemoteHosts() async {
    final prefs = await _getPrefs();
    final raw = prefs.getString(_storeRemoteHosts);
    if (raw == null || raw.trim().isEmpty) {
      return const <RemoteHostConfig>[];
    }
    try {
      final decoded = jsonDecode(raw);
      if (decoded is! List) return const <RemoteHostConfig>[];
      final hosts = <RemoteHostConfig>[];
      for (final item in decoded) {
        Map<String, dynamic>? map;
        if (item is Map<String, dynamic>) {
          map = item;
        } else if (item is Map) {
          map = item.map((k, v) => MapEntry('$k', v));
        }
        if (map == null) continue;
        final host = RemoteHostConfig.fromJson(map);
        if (host == null) continue;
        hosts.add(host);
      }
      return hosts;
    } catch (_) {
      return const <RemoteHostConfig>[];
    }
  }

  Future<void> saveRemoteHosts(List<RemoteHostConfig> hosts) async {
    final prefs = await _getPrefs();
    if (hosts.isEmpty) {
      await prefs.remove(_storeRemoteHosts);
      return;
    }
    final payload = hosts.map((host) => host.toJson()).toList(growable: false);
    await prefs.setString(_storeRemoteHosts, jsonEncode(payload));
  }

  Future<int?> readRemoteCacheQuotaMb() async {
    final prefs = await _getPrefs();
    final value = prefs.getInt(_storeRemoteCacheQuotaMb);
    if (value == null || value <= 0) return null;
    return value;
  }

  Future<void> saveRemoteCacheQuotaMb(int? quotaMb) async {
    final prefs = await _getPrefs();
    if (quotaMb == null || quotaMb <= 0) {
      await prefs.remove(_storeRemoteCacheQuotaMb);
      return;
    }
    await prefs.setInt(_storeRemoteCacheQuotaMb, quotaMb);
  }

  Future<void> saveHfToken(String token) async {
    final prefs = await _getPrefs();
    await prefs.setString(_storeHfToken, token.trim());
  }

  Future<void> clearHfToken() async {
    final prefs = await _getPrefs();
    await prefs.remove(_storeHfToken);
  }

  Future<ViewerSessionSnapshot?> readViewerSession() async {
    if (kIsWeb) {
      final prefs = await _getPrefs();
      final raw = prefs.getString(_storeViewerSession);
      return ViewerSessionSnapshot.fromJsonString(raw);
    }
    try {
      final file = await _viewerSessionFile();
      if (!await file.exists()) return null;
      final raw = await file.readAsString();
      return ViewerSessionSnapshot.fromJsonString(raw);
    } catch (_) {
      return null;
    }
  }

  Future<void> saveViewerSession(ViewerSessionSnapshot session) async {
    if (kIsWeb) {
      final prefs = await _getPrefs();
      await prefs.setString(_storeViewerSession, session.toJsonString());
      return;
    }
    final file = await _viewerSessionFile();
    await file.parent.create(recursive: true);
    final temp = File('${file.path}.tmp');
    await temp.writeAsString(session.toJsonString());
    if (await file.exists()) {
      await file.delete();
    }
    await temp.rename(file.path);
  }

  Future<void> clearViewerSession() async {
    if (kIsWeb) {
      final prefs = await _getPrefs();
      await prefs.remove(_storeViewerSession);
      return;
    }
    try {
      final file = await _viewerSessionFile();
      if (await file.exists()) {
        await file.delete();
      }
    } catch (_) {}
  }
}
