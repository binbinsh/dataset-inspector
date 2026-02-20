import 'dart:async';
import 'dart:io';

import 'package:audioplayers/audioplayers.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';

import '../services/app_logger.dart';
import '../models/common.dart';

class AudioPreview extends StatefulWidget {
  const AudioPreview(
      {super.key, this.path, this.bytes, this.label, this.loader})
      : assert(path != null || bytes != null || loader != null);

  final String? path;
  final Uint8List? bytes;
  final String? label;
  final Future<PreparedMediaResponse> Function()? loader;

  @override
  State<AudioPreview> createState() => _AudioPreviewState();
}

class _AudioPreviewState extends State<AudioPreview> {
  late final AudioPlayer _player;
  StreamSubscription<PlayerState>? _stateSub;
  PlayerState _playerState = PlayerState.stopped;
  bool _ready = false;
  bool _loading = true;
  String? _error;
  Uint8List? _bytes;
  String? _path;
  String? _label;
  String? _ext;
  String? _mimeType;
  String? _tempFilePath;
  bool _actionInFlight = false;

  @override
  void initState() {
    super.initState();
    _player = AudioPlayer();
    _stateSub = _player.onPlayerStateChanged.listen((state) {
      if (!mounted) return;
      setState(() {
        _playerState = state;
      });
    });
    _syncSource();
  }

  @override
  void didUpdateWidget(covariant AudioPreview oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.path != widget.path ||
        oldWidget.bytes != widget.bytes ||
        oldWidget.label != widget.label) {
      _syncSource();
    }
  }

  Future<void> _syncSource() async {
    await _cleanupTempFile();
    _bytes = widget.bytes;
    _path = widget.path;
    _label = widget.label;
    _ext = _normalizeExt(_extFromPath(_path));
    _mimeType = _ext != null ? _mimeTypeForExt(_ext!) : null;
    setState(() {
      _loading = true;
      _ready = false;
      _error = null;
    });
    if (_bytes == null && _path != null) {
      final file = File(_path!);
      if (!file.existsSync()) {
        setState(() {
          _loading = false;
          _error = 'Audio file missing.';
        });
        return;
      }
    }
    try {
      await _player.stop();
      await _player.setReleaseMode(ReleaseMode.stop);
      setState(() {
        _ready = _bytes != null || _path != null || widget.loader != null;
        _loading = false;
      });
    } catch (err, stack) {
      AppLogger.error('Audio load failed',
          tag: 'audio', error: err, stackTrace: stack);
      if (!mounted) return;
      setState(() {
        _loading = false;
        _error = err.toString();
      });
    }
  }

  Future<bool> _ensureSourceLoaded() async {
    if (_bytes != null || _path != null) return true;
    final loader = widget.loader;
    if (loader == null) {
      setState(() {
        _error = 'Audio unavailable.';
      });
      return false;
    }
    setState(() {
      _loading = true;
      _error = null;
    });
    try {
      final prepared = await loader();
      _bytes = prepared.bytes;
      _ext = _normalizeExt(prepared.ext);
      _mimeType = _ext != null ? _mimeTypeForExt(_ext!) : null;
      _label ??= 'Audio preview (${prepared.ext})';
      if (!mounted) return false;
      setState(() {
        _loading = false;
        _ready = true;
      });
      return true;
    } catch (err, stack) {
      AppLogger.error('Audio load failed',
          tag: 'audio', error: err, stackTrace: stack);
      if (!mounted) return false;
      setState(() {
        _loading = false;
        _error = err.toString();
      });
      return false;
    }
  }

  String? _normalizeExt(String? ext) {
    if (ext == null) return null;
    final cleaned = ext.trim().toLowerCase();
    if (cleaned.isEmpty) return null;
    return cleaned.startsWith('.') ? cleaned.substring(1) : cleaned;
  }

  String? _extFromPath(String? path) {
    if (path == null || path.isEmpty) return null;
    final name = path.split('/').last;
    final dot = name.lastIndexOf('.');
    if (dot <= 0 || dot == name.length - 1) return null;
    return name.substring(dot + 1);
  }

  String? _mimeTypeForExt(String ext) {
    switch (ext) {
      case 'wav':
      case 'wave':
        return 'audio/wav';
      case 'mp3':
        return 'audio/mpeg';
      case 'flac':
        return 'audio/flac';
      case 'ogg':
        return 'audio/ogg';
      case 'opus':
        return 'audio/opus';
      case 'aac':
        return 'audio/aac';
      case 'm4a':
      case 'mp4':
        return 'audio/mp4';
      case 'aif':
      case 'aiff':
        return 'audio/aiff';
      case 'caf':
        return 'audio/x-caf';
    }
    return null;
  }

  bool _shouldUseTempFileForBytes() {
    if (kIsWeb) return false;
    return defaultTargetPlatform == TargetPlatform.iOS ||
        defaultTargetPlatform == TargetPlatform.macOS ||
        defaultTargetPlatform == TargetPlatform.linux;
  }

  Future<String> _writeTempFile(Uint8List bytes, String ext) async {
    final tempDir = await getTemporaryDirectory();
    final outDir = Directory('${tempDir.path}/dataset-inspector/audio');
    if (!await outDir.exists()) {
      await outDir.create(recursive: true);
    }
    final uniqueName =
        '${DateTime.now().microsecondsSinceEpoch}-${bytes.length.toRadixString(16)}';
    final safeExt = ext.isEmpty ? 'bin' : ext;
    final file = File('${outDir.path}/$uniqueName.$safeExt');
    await file.writeAsBytes(bytes, flush: true);
    _tempFilePath = file.path;
    return file.path;
  }

  Future<void> _cleanupTempFile() async {
    final path = _tempFilePath;
    _tempFilePath = null;
    if (path == null) return;
    try {
      final file = File(path);
      if (await file.exists()) {
        await file.delete();
      }
    } catch (_) {}
  }

  @override
  void dispose() {
    _player.stop();
    _stateSub?.cancel();
    _player.dispose();
    unawaited(_cleanupTempFile());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    if (_loading) {
      return const SizedBox(
          height: 48, child: Center(child: CircularProgressIndicator()));
    }
    if (_error != null) {
      return SizedBox(
        height: 48,
        child: Center(
          child: Text(_error!, style: Theme.of(context).textTheme.bodySmall),
        ),
      );
    }
    if (!_ready) {
      return const SizedBox(
          height: 48, child: Center(child: Text('Audio unavailable')));
    }
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      decoration: BoxDecoration(
        color: const Color(0xFFF7F7F7),
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: const Color(0xFFE0E0E0)),
      ),
      child: Row(
        children: [
          IconButton.filledTonal(
            icon: Icon(_playerState == PlayerState.playing
                ? Icons.pause
                : Icons.play_arrow),
            onPressed: () async {
              if (_loading || _actionInFlight) return;
              _actionInFlight = true;
              try {
                if (_playerState == PlayerState.playing) {
                  await _player.pause();
                } else if (_playerState == PlayerState.paused) {
                  await _player.resume();
                } else {
                  final ready = await _ensureSourceLoaded();
                  if (!ready) return;
                  if (_bytes != null) {
                    final ext = _ext ?? '';
                    final mimeType = _mimeType;
                    if (ext.isEmpty || mimeType == null) {
                      if (!mounted) return;
                      setState(() {
                        _error = 'Unsupported audio format.';
                      });
                      return;
                    }
                    if (_shouldUseTempFileForBytes()) {
                      _path = await _writeTempFile(_bytes!, ext);
                      await _player
                          .play(DeviceFileSource(_path!, mimeType: mimeType));
                    } else {
                      await _player
                          .play(BytesSource(_bytes!, mimeType: mimeType));
                    }
                  } else if (_path != null) {
                    final ext =
                        _ext ?? _normalizeExt(_extFromPath(_path)) ?? '';
                    final mimeType = _mimeTypeForExt(ext);
                    await _player
                        .play(DeviceFileSource(_path!, mimeType: mimeType));
                  }
                }
              } catch (err, stack) {
                AppLogger.error('Audio play failed',
                    tag: 'audio', error: err, stackTrace: stack);
                if (!mounted) return;
                setState(() {
                  _error = err.toString();
                });
              } finally {
                _actionInFlight = false;
              }
            },
          ),
          const SizedBox(width: 8),
          IconButton(
            icon: const Icon(Icons.stop),
            onPressed: () async {
              try {
                await _player.stop();
              } catch (err, stack) {
                AppLogger.error('Audio stop failed',
                    tag: 'audio', error: err, stackTrace: stack);
              }
            },
          ),
          const SizedBox(width: 8),
          Expanded(
            child: Text(
              _label ?? _path ?? 'Audio preview',
              overflow: TextOverflow.ellipsis,
              style: Theme.of(context).textTheme.bodySmall,
            ),
          ),
        ],
      ),
    );
  }
}
