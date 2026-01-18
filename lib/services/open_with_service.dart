import 'dart:io';

class OpenWithResult {
  const OpenWithResult({required this.opened, required this.error});

  final bool opened;
  final String? error;
}

class OpenWithService {
  Future<OpenWithResult> openFile(String path, {String? appPath}) async {
    final target = path.trim();
    if (target.isEmpty) {
      return const OpenWithResult(opened: false, error: 'Missing file path.');
    }

    final app = appPath?.trim();
    try {
      if (app != null && app.isNotEmpty) {
        await _openWithApp(target, app);
        return const OpenWithResult(opened: true, error: null);
      }
      await _openDefault(target);
      return const OpenWithResult(opened: true, error: null);
    } catch (err) {
      return OpenWithResult(opened: false, error: err.toString());
    }
  }

  Future<void> openUrl(String url) async {
    final target = url.trim();
    if (target.isEmpty) return;
    await _openDefault(target);
  }

  Future<void> _openWithApp(String target, String appPath) async {
    if (Platform.isMacOS) {
      await Process.start('open', ['-a', appPath, target]);
      return;
    }
    await Process.start(appPath, [target]);
  }

  Future<void> _openDefault(String target) async {
    if (Platform.isMacOS) {
      await Process.start('open', [target]);
      return;
    }
    if (Platform.isWindows) {
      await Process.start('cmd', ['/c', 'start', '', target], runInShell: true);
      return;
    }
    await Process.start('xdg-open', [target]);
  }
}
