import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';
import 'package:video_player/video_player.dart';

import '../models/common.dart';
import '../services/app_logger.dart';

class VideoPreview extends StatefulWidget {
  const VideoPreview({
    super.key,
    this.path,
    this.bytes,
    this.ext,
    this.loader,
  }) : assert(path != null || bytes != null || loader != null);

  final String? path;
  final Uint8List? bytes;
  final String? ext;
  final Future<PreparedMediaResponse> Function()? loader;

  @override
  State<VideoPreview> createState() => _VideoPreviewState();
}

class _VideoPreviewState extends State<VideoPreview> {
  VideoPlayerController? _controller;
  bool _loading = true;
  String? _error;
  String? _tempFilePath;
  bool _actionInFlight = false;
  Uint8List? _bytes;
  String? _path;
  String? _ext;

  @override
  void initState() {
    super.initState();
    _syncController();
  }

  @override
  void didUpdateWidget(covariant VideoPreview oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.path != widget.path ||
        oldWidget.bytes != widget.bytes ||
        oldWidget.ext != widget.ext ||
        oldWidget.loader != widget.loader) {
      _syncController();
    }
  }

  Future<void> _syncController() async {
    await _disposeController();
    await _cleanupTempFile();
    _bytes = widget.bytes;
    _path = widget.path;
    _ext = _normalizeExt(widget.ext);
    setState(() {
      _loading = true;
      _error = null;
    });
    try {
      if (_bytes != null || _path != null) {
        await _loadControllerFromSource();
        return;
      }
      if (widget.loader != null) {
        if (!mounted) return;
        setState(() {
          _loading = false;
        });
        return;
      }
      if (!mounted) return;
      setState(() {
        _loading = false;
        _error = 'Video unavailable.';
      });
    } catch (err, stack) {
      AppLogger.error('Video load failed', tag: 'video', error: err, stackTrace: stack);
      if (!mounted) return;
      setState(() {
        _loading = false;
        _error = err.toString();
      });
    }
  }

  Future<void> _loadControllerFromSource() async {
    final path = await _resolvePath();
    if (path == null) {
      if (!mounted) return;
      setState(() {
        _loading = false;
        _error = 'Video unavailable.';
      });
      return;
    }
    final controller = VideoPlayerController.file(File(path));
    await controller.initialize();
    await controller.setLooping(false);
    if (!mounted) {
      await controller.dispose();
      return;
    }
    setState(() {
      _controller = controller;
      _loading = false;
    });
  }

  Future<bool> _ensureControllerLoaded() async {
    if (_controller != null) return true;
    final loader = widget.loader;
    if (loader == null) {
      setState(() {
        _error = 'Video unavailable.';
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
      await _loadControllerFromSource();
      return _controller != null;
    } catch (err, stack) {
      AppLogger.error('Video load failed', tag: 'video', error: err, stackTrace: stack);
      if (!mounted) return false;
      setState(() {
        _loading = false;
        _error = err.toString();
      });
      return false;
    }
  }

  Future<String?> _resolvePath() async {
    final existing = _path;
    if (existing != null && existing.isNotEmpty) {
      return existing;
    }
    final bytes = _bytes;
    if (bytes == null) return null;
    final ext = _ext ?? 'mp4';
    return _writeTempFile(bytes, ext);
  }

  String? _normalizeExt(String? ext) {
    if (ext == null) return null;
    final cleaned = ext.trim().toLowerCase();
    if (cleaned.isEmpty) return null;
    return cleaned.startsWith('.') ? cleaned.substring(1) : cleaned;
  }

  Future<String> _writeTempFile(Uint8List bytes, String ext) async {
    final tempDir = await getTemporaryDirectory();
    final outDir = Directory('${tempDir.path}/dataset-inspector/video');
    if (!await outDir.exists()) {
      await outDir.create(recursive: true);
    }
    final hash = Object.hashAll(bytes).toUnsigned(20).toRadixString(16).padLeft(5, '0');
    final safeExt = ext.isEmpty ? 'mp4' : ext;
    final file = File('${outDir.path}/$hash.$safeExt');
    if (!await file.exists()) {
      await file.writeAsBytes(bytes, flush: true);
    }
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

  Future<void> _disposeController() async {
    final controller = _controller;
    _controller = null;
    if (controller == null) return;
    try {
      await controller.pause();
    } catch (_) {}
    await controller.dispose();
  }

  @override
  void dispose() {
    unawaited(_disposeController());
    unawaited(_cleanupTempFile());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return LayoutBuilder(
      builder: (context, constraints) {
        final maxWidth = constraints.maxWidth.isFinite ? constraints.maxWidth : null;
        final maxHeight = constraints.maxHeight.isFinite ? constraints.maxHeight : null;
        if (_loading) {
          return SizedBox(
            height: _clampHeight(maxHeight, 140),
            child: const Center(child: CircularProgressIndicator()),
          );
        }
        if (_error != null) {
          return SizedBox(
            height: _clampHeight(maxHeight, 140),
            child: Center(
              child: Text(_error!, style: Theme.of(context).textTheme.bodySmall),
            ),
          );
        }
        final controller = _controller;
        if (controller == null) {
          final scheme = Theme.of(context).colorScheme;
          return ClipRRect(
            borderRadius: BorderRadius.circular(16),
            child: Container(
              height: _clampHeight(maxHeight, 180),
              width: double.infinity,
              color: scheme.surfaceContainerLow,
              child: Center(
                child: IconButton.filledTonal(
                  icon: const Icon(Icons.play_arrow),
                  onPressed: () async {
                    if (_actionInFlight) return;
                    _actionInFlight = true;
                    try {
                      final loaded = await _ensureControllerLoaded();
                      if (!loaded) return;
                      await _controller?.play();
                    } finally {
                      _actionInFlight = false;
                    }
                  },
                ),
              ),
            ),
          );
        }
        return _buildVideoWithControls(
          context,
          controller,
          maxWidth: maxWidth,
          maxHeight: maxHeight,
        );
      },
    );
  }

  double _clampHeight(double? maxHeight, double fallback) {
    if (maxHeight == null) return fallback;
    return maxHeight < fallback ? maxHeight : fallback;
  }

  Widget _buildVideoWithControls(
    BuildContext context,
    VideoPlayerController controller, {
    required double? maxWidth,
    required double? maxHeight,
  }) {
    final scheme = Theme.of(context).colorScheme;
    final aspectRatio = controller.value.aspectRatio <= 0 ? (16 / 9) : controller.value.aspectRatio;
    const spacing = 8.0;
    const controlsHeight = 44.0;
    Size? fitted;
    if (maxWidth != null) {
      final maxVideoHeight = maxHeight == null ? null : (maxHeight - controlsHeight - spacing);
      final initialHeight = maxWidth / aspectRatio;
      var videoHeight = maxVideoHeight == null ? initialHeight : initialHeight;
      if (maxVideoHeight != null && maxVideoHeight > 0 && videoHeight > maxVideoHeight) {
        videoHeight = maxVideoHeight;
      }
      var videoWidth = videoHeight * aspectRatio;
      if (videoWidth > maxWidth) {
        videoWidth = maxWidth;
        videoHeight = videoWidth / aspectRatio;
      }
      fitted = Size(videoWidth, videoHeight);
    }
    final videoWidget = fitted == null
        ? AspectRatio(
            aspectRatio: aspectRatio,
            child: VideoPlayer(controller),
          )
        : SizedBox(
            width: fitted.width,
            height: fitted.height,
            child: VideoPlayer(controller),
          );

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        ClipRRect(
          borderRadius: BorderRadius.circular(16),
          child: Center(child: videoWidget),
        ),
        const SizedBox(height: spacing),
        AnimatedBuilder(
          animation: controller,
          builder: (context, child) {
            final playing = controller.value.isPlaying;
            return SizedBox(
              height: controlsHeight,
              child: Row(
                children: [
                  IconButton.filledTonal(
                    icon: Icon(playing ? Icons.pause : Icons.play_arrow),
                    onPressed: () async {
                      if (_actionInFlight) return;
                      _actionInFlight = true;
                      try {
                        if (playing) {
                          await controller.pause();
                        } else {
                          await controller.play();
                        }
                      } catch (err, stack) {
                        AppLogger.error('Video play failed', tag: 'video', error: err, stackTrace: stack);
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
                        await controller.pause();
                        await controller.seekTo(Duration.zero);
                      } catch (err, stack) {
                        AppLogger.error('Video stop failed', tag: 'video', error: err, stackTrace: stack);
                      }
                    },
                  ),
                  const SizedBox(width: 8),
                  Expanded(
                    child: VideoProgressIndicator(
                      controller,
                      allowScrubbing: true,
                      colors: VideoProgressColors(
                        playedColor: scheme.primary,
                        bufferedColor: scheme.primary.withOpacity(0.2),
                        backgroundColor: scheme.outlineVariant.withOpacity(0.3),
                      ),
                    ),
                  ),
                ],
              ),
            );
          },
        ),
      ],
    );
  }
}
