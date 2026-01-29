import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:package_info_plus/package_info_plus.dart';
import 'package:path/path.dart' as p;

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
    final tempDir = _updateTempRoot();
    await tempDir.create(recursive: true);
    final filename = url.pathSegments.isNotEmpty ? url.pathSegments.last : 'update';
    final outFile = File(p.join(tempDir.path, filename));
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
    final lower = path.toLowerCase();
    if (Platform.isMacOS) {
      if (lower.endsWith('.zip')) {
        await _installMacUpdate(file);
        return;
      }
      // For .dmg or .pkg, just open them
      await Process.start('open', [path]);
      return;
    }
    if (Platform.isWindows) {
      if (lower.endsWith('.zip')) {
        await _installWindowsUpdate(file);
        return;
      }
      await Process.start('cmd', ['/c', 'start', '', path], runInShell: true);
      return;
    }
    if (Platform.isLinux) {
      if (lower.endsWith('.tar.gz') || lower.endsWith('.tgz')) {
        await _installLinuxUpdate(file);
        return;
      }
    }
    await Process.start('xdg-open', [path]);
  }

  Future<void> _installMacUpdate(File zipFile) async {
    // 1. Find current app bundle location
    final currentExecutable = Platform.resolvedExecutable;
    // Executable is at: /path/to/App.app/Contents/MacOS/app_name
    final appBundlePath = currentExecutable.split('/Contents/MacOS/').first;
    final appBundleName = p.basename(appBundlePath);
    final appPid = pid;

    if (!appBundlePath.endsWith('.app')) {
      throw Exception('Cannot determine app bundle location');
    }

    // 2. Extract new app to temp directory
    final tempRoot = _updateTempRoot();
    final extractDir = Directory(p.join(tempRoot.path, 'unpacked-${DateTime.now().millisecondsSinceEpoch}'));
    await extractDir.create(recursive: true);

    final result = await Process.run('ditto', ['-x', '-k', zipFile.path, extractDir.path]);
    if (result.exitCode != 0) {
      throw Exception('Failed to extract update: ${result.stderr}');
    }

    // 3. Find the .app bundle in extracted files
    final newAppBundle = await _findMacAppBundle(extractDir, appBundleName);
    await _ensureWritableDirectory(Directory(p.dirname(appBundlePath)));

    // 4. Create updater script that will:
    //    - Wait for current app to exit
    //    - Replace old app with new app
    //    - Launch new app
    //    - Clean up
    final scriptFile = File(p.join(tempRoot.path, 'updater.sh'));
    final script = '''
#!/bin/bash
# Wait for the app to exit (check every 0.5 seconds, timeout after 30 seconds)
APP_PID=$appPid
TIMEOUT=60
ELAPSED=0

while kill -0 \$APP_PID 2>/dev/null; do
  sleep 0.5
  ELAPSED=\$((ELAPSED + 1))
  if [ \$ELAPSED -ge \$TIMEOUT ]; then
    echo "Timeout waiting for app to exit"
    exit 1
  fi
done

# Small delay to ensure file handles are released
sleep 1

# Remove old app and copy new app
rm -rf "$appBundlePath"
ditto "${newAppBundle.path}" "$appBundlePath"

# Fix permissions
chmod -R 755 "$appBundlePath"
xattr -cr "$appBundlePath" 2>/dev/null || true

# Launch new app
open "$appBundlePath"

# Clean up
rm -rf "${extractDir.path}"
rm -f "${zipFile.path}"
rm -f "${scriptFile.path}"
''';

    await scriptFile.writeAsString(script);
    await Process.run('chmod', ['+x', scriptFile.path]);

    // 5. Start the updater script in background
    await Process.start(
      '/bin/bash',
      [scriptFile.path],
      mode: ProcessStartMode.detached,
    );

    // 6. Exit current app
    exit(0);
  }

  Future<void> _installWindowsUpdate(File zipFile) async {
    final appExecutable = File(Platform.resolvedExecutable);
    final appDir = appExecutable.parent;
    final exeName = p.basename(appExecutable.path);
    final appPid = pid;

    await _ensureWritableDirectory(appDir);

    final tempRoot = _updateTempRoot();
    final extractDir = Directory(p.join(tempRoot.path, 'unpacked-${DateTime.now().millisecondsSinceEpoch}'));
    await extractDir.create(recursive: true);

    final result = await Process.run(
      'powershell',
      [
        '-NoProfile',
        '-NonInteractive',
        '-Command',
        r'& { Expand-Archive -LiteralPath $args[0] -DestinationPath $args[1] -Force }',
        zipFile.path,
        extractDir.path,
      ],
    );
    if (result.exitCode != 0) {
      throw Exception('Failed to extract update: ${result.stderr}');
    }

    final payloadDir = await _findPayloadDir(extractDir, exeName);

    final scriptFile = File(p.join(tempRoot.path, 'updater.cmd'));
    final script = '''
@echo off
setlocal
set "APP_PID=$appPid"
set "APP_DIR=${appDir.path}"
set "SRC_DIR=${payloadDir.path}"
set "CLEAN_DIR=${extractDir.path}"
set "EXE_NAME=$exeName"
set "ZIP_FILE=${zipFile.path}"

:wait
tasklist /FI "PID eq %APP_PID%" | findstr /I "%APP_PID%" >nul
if not errorlevel 1 (
  timeout /t 1 /nobreak >nul
  goto wait
)

robocopy "%SRC_DIR%" "%APP_DIR%" /E /IS /IT /NFL /NDL /NJH /NJS /NC /NS /NP >nul
start "" "%APP_DIR%\\%EXE_NAME%"

rmdir /s /q "%CLEAN_DIR%"
del "%ZIP_FILE%"
del "%~f0"
''';

    await scriptFile.writeAsString(script);

    await Process.start(
      'cmd',
      ['/c', scriptFile.path],
      mode: ProcessStartMode.detached,
    );

    exit(0);
  }

  Future<void> _installLinuxUpdate(File archiveFile) async {
    final appExecutable = File(Platform.resolvedExecutable);
    final appDir = appExecutable.parent;
    final exeName = p.basename(appExecutable.path);
    final appPid = pid;

    await _ensureWritableDirectory(appDir);

    final tempRoot = _updateTempRoot();
    final extractDir = Directory(p.join(tempRoot.path, 'unpacked-${DateTime.now().millisecondsSinceEpoch}'));
    await extractDir.create(recursive: true);

    final result = await Process.run('tar', ['-xzf', archiveFile.path, '-C', extractDir.path]);
    if (result.exitCode != 0) {
      throw Exception('Failed to extract update: ${result.stderr}');
    }

    final payloadDir = await _findPayloadDir(extractDir, exeName);

    final scriptFile = File(p.join(tempRoot.path, 'updater.sh'));
    final script = '''
#!/bin/bash
APP_PID=$appPid
APP_DIR="${appDir.path}"
SRC_DIR="${payloadDir.path}"
CLEAN_DIR="${extractDir.path}"
EXE_NAME="$exeName"
ARCHIVE_PATH="${archiveFile.path}"

while kill -0 \$APP_PID 2>/dev/null; do
  sleep 0.5
done

cp -a "\$SRC_DIR"/. "\$APP_DIR"/
chmod -R 755 "\$APP_DIR"
cd "\$APP_DIR"
"./\$EXE_NAME" >/dev/null 2>&1 &

rm -rf "\$CLEAN_DIR"
rm -f "\$ARCHIVE_PATH"
rm -f "${scriptFile.path}"
''';

    await scriptFile.writeAsString(script);
    await Process.run('chmod', ['+x', scriptFile.path]);

    await Process.start(
      '/bin/bash',
      [scriptFile.path],
      mode: ProcessStartMode.detached,
    );

    exit(0);
  }

  Future<Directory> _findMacAppBundle(Directory root, String preferredName) async {
    Directory? fallback;
    await for (final entity in root.list(recursive: true, followLinks: false)) {
      if (entity is! Directory) continue;
      final name = p.basename(entity.path);
      if (!name.toLowerCase().endsWith('.app')) continue;
      if (p.split(entity.path).contains('__MACOSX')) continue;
      if (name == preferredName) return entity;
      fallback ??= entity;
    }
    if (fallback != null) return fallback;
    throw Exception('No .app bundle found in update package');
  }

  Future<Directory> _findPayloadDir(Directory root, String executableName) async {
    final direct = File(p.join(root.path, executableName));
    if (await direct.exists()) {
      return root;
    }
    await for (final entity in root.list(recursive: true, followLinks: false)) {
      if (entity is! File) continue;
      if (p.basename(entity.path) != executableName) continue;
      return entity.parent;
    }
    throw Exception('No executable found in update package');
  }

  Future<void> _ensureWritableDirectory(Directory directory) async {
    final probe = File(p.join(
      directory.path,
      '.update_write_test_${DateTime.now().millisecondsSinceEpoch}',
    ));
    try {
      await probe.writeAsString('test');
    } on FileSystemException {
      throw Exception(
        'App location is not writable. Move the app to a writable folder and try again.',
      );
    } finally {
      if (await probe.exists()) {
        await probe.delete();
      }
    }
  }

  Directory _updateTempRoot() {
    return Directory(p.join(Directory.systemTemp.path, 'dataset-inspector', 'updates'));
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
