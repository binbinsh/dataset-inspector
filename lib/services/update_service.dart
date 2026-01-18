import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:package_info_plus/package_info_plus.dart';

class UpdateInfo {
  const UpdateInfo({
    required this.currentVersion,
    required this.version,
    required this.notes,
    required this.pubDate,
    required this.downloadUrl,
    required this.signature,
  });

  final String currentVersion;
  final String version;
  final String? notes;
  final String? pubDate;
  final String downloadUrl;
  final String? signature;
}

class UpdateService {
  UpdateService({http.Client? client}) : _client = client ?? http.Client();

  final http.Client _client;

  static const _manifestUrl =
      'https://github.com/binbinsh/dataset-inspector/releases/latest/download/latest.json';

  Future<UpdateInfo?> checkForUpdate() async {
    final info = await PackageInfo.fromPlatform();
    final current = info.version;

    final response = await _client.get(Uri.parse(_manifestUrl));
    if (response.statusCode < 200 || response.statusCode >= 300) {
      return null;
    }
    final json = jsonDecode(response.body) as Map<String, dynamic>;
    final latest = json['version']?.toString();
    if (latest == null || latest.isEmpty) return null;

    if (!_isNewerVersion(current, latest)) return null;

    final platformKey = _platformKey();
    final platforms = json['platforms'] as Map<String, dynamic>? ?? {};
    final platform = platforms[platformKey] as Map<String, dynamic>?;
    if (platform == null) return null;

    final url = platform['url']?.toString();
    if (url == null || url.isEmpty) return null;

    return UpdateInfo(
      currentVersion: current,
      version: latest,
      notes: json['notes']?.toString(),
      pubDate: json['pub_date']?.toString(),
      downloadUrl: url,
      signature: platform['signature']?.toString(),
    );
  }

  Future<void> downloadAndInstall(UpdateInfo update, {void Function(int, int?)? onProgress}) async {
    final outFile = await download(update, onProgress: onProgress);
    await _openInstaller(outFile);
  }

  Future<File> download(UpdateInfo update, {void Function(int, int?)? onProgress}) async {
    final url = Uri.parse(update.downloadUrl);
    final request = http.Request('GET', url);
    final response = await _client.send(request);
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw Exception('Download failed: HTTP ${response.statusCode}');
    }

    final contentLength = response.contentLength;
    final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector/updates');
    await tempDir.create(recursive: true);
    final filename = url.pathSegments.isNotEmpty ? url.pathSegments.last : 'update';
    final outFile = File('${tempDir.path}/$filename');
    final sink = outFile.openWrite();
    var received = 0;

    try {
      await response.stream.listen((chunk) {
        received += chunk.length;
        sink.add(chunk);
        if (onProgress != null) {
          onProgress(received, contentLength);
        }
      }).asFuture();
    } finally {
      await sink.flush();
      await sink.close();
    }

    return outFile;
  }

  Future<void> installUpdate(File file) async {
    await _openInstaller(file);
  }

  Future<void> _openInstaller(File file) async {
    final path = file.path;
    if (Platform.isMacOS) {
      final lower = path.toLowerCase();
      if (lower.endsWith('.zip')) {
        final appBundle = await _extractMacAppBundle(file);
        if (appBundle != null) {
          await Process.start('open', [appBundle.path]);
          return;
        }
      }
      await Process.start('open', [path]);
      return;
    }
    if (Platform.isWindows) {
      await Process.start('cmd', ['/c', 'start', '', path], runInShell: true);
      return;
    }
    await Process.start('xdg-open', [path]);
  }

  Future<Directory?> _extractMacAppBundle(File file) async {
    final tempRoot = Directory('${Directory.systemTemp.path}/dataset-inspector/updates');
    final extractDir = Directory('${tempRoot.path}/unpacked-${DateTime.now().millisecondsSinceEpoch}');
    await extractDir.create(recursive: true);
    final result = await Process.run('ditto', ['-x', '-k', file.path, extractDir.path]);
    if (result.exitCode != 0) {
      return null;
    }
    await for (final entity in extractDir.list(recursive: true, followLinks: false)) {
      if (entity is Directory && entity.path.toLowerCase().endsWith('.app')) {
        return entity;
      }
    }
    return null;
  }

  bool _isNewerVersion(String current, String latest) {
    final currentParts = current.split('.').map((e) => int.tryParse(e) ?? 0).toList();
    final latestParts = latest.split('.').map((e) => int.tryParse(e) ?? 0).toList();
    final maxLen = currentParts.length > latestParts.length ? currentParts.length : latestParts.length;
    while (currentParts.length < maxLen) {
      currentParts.add(0);
    }
    while (latestParts.length < maxLen) {
      latestParts.add(0);
    }
    for (var i = 0; i < maxLen; i += 1) {
      if (latestParts[i] > currentParts[i]) return true;
      if (latestParts[i] < currentParts[i]) return false;
    }
    return false;
  }

  String _platformKey() {
    final arch = Platform.version.contains('arm64') || Platform.version.contains('aarch64')
        ? 'aarch64'
        : 'x86_64';
    if (Platform.isMacOS) return 'darwin-$arch';
    if (Platform.isWindows) return 'windows-$arch';
    return 'linux-$arch';
  }
}
