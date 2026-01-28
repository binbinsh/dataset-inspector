import 'dart:convert';
import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:provider/provider.dart';
import 'package:window_manager/window_manager.dart';

import '../models/common.dart';
import '../models/huggingface.dart';
import '../models/webdataset.dart';
import '../models/zenodo.dart';
import '../services/update_service.dart';
import '../state/viewer_state.dart';
import '../utils/audio.dart';
import 'audio_preview.dart';
import 'copy_button.dart';
import 'hover_tile.dart';
import 'skeleton.dart';
import 'update_dialog.dart';
import 'video_preview.dart';

class InspectorScreen extends StatefulWidget {
  const InspectorScreen({super.key});

  @override
  State<InspectorScreen> createState() => _InspectorScreenState();
}

class _InspectorScreenState extends State<InspectorScreen> {
  late final TextEditingController _inputController;
  late final FocusNode _inputFocus;
  late final TextEditingController _hfOffsetController;
  late final FocusNode _hfOffsetFocus;
  final LayerLink _sourceFieldLink = LayerLink();
  final GlobalKey _sourceFieldKey = GlobalKey();
  OverlayEntry? _sourceHistoryOverlay;
  int _recentSourceIndex = 0;

  @override
  void initState() {
    super.initState();
    _inputController = TextEditingController();
    _inputFocus = FocusNode();
    _hfOffsetController = TextEditingController();
    _hfOffsetFocus = FocusNode();
  }

  @override
  void dispose() {
    _hideRecentSourcesOverlay();
    _inputController.dispose();
    _inputFocus.dispose();
    _hfOffsetController.dispose();
    _hfOffsetFocus.dispose();
    super.dispose();
  }

  void _showRecentSourcesOverlay(BuildContext context, ViewerState state) {
    if (_sourceHistoryOverlay != null) {
      _sourceHistoryOverlay!.markNeedsBuild();
      return;
    }
    if (state.recentSources.isEmpty) return;
    final overlay = Overlay.of(context);
    if (overlay == null) return;
    _recentSourceIndex = 0;
    _sourceHistoryOverlay = OverlayEntry(
      builder: (overlayContext) {
        final renderBox = _sourceFieldKey.currentContext?.findRenderObject() as RenderBox?;
        final size = renderBox?.size;
        final width = size?.width ?? 320.0;
        final height = size?.height ?? 0.0;
        final scheme = Theme.of(overlayContext).colorScheme;
        final sources = _recentSourcesForState(state);
        if (sources.isEmpty) return const SizedBox.shrink();
        final maxVisible = 5;
        final visibleCount = sources.length < maxVisible ? sources.length : maxVisible;
        final itemHeight = 36.0;
        final separatorHeight = 1.0;
        final paddingHeight = 12.0;
        final maxHeight =
            visibleCount * itemHeight + (visibleCount - 1) * separatorHeight + paddingHeight;
        final selectedIndex = _recentSourceIndex < sources.length ? _recentSourceIndex : 0;
        return Stack(
          children: [
            Positioned.fill(
              child: GestureDetector(
                behavior: HitTestBehavior.translucent,
                onTap: _hideRecentSourcesOverlay,
                child: const SizedBox.expand(),
              ),
            ),
            CompositedTransformFollower(
              link: _sourceFieldLink,
              showWhenUnlinked: false,
              offset: Offset(0, height + 8),
              child: Material(
                type: MaterialType.transparency,
                child: ConstrainedBox(
                  constraints: BoxConstraints(
                    minWidth: width,
                    maxWidth: width,
                    maxHeight: maxHeight,
                  ),
                  child: DecoratedBox(
                    decoration: BoxDecoration(
                      color: scheme.surface.withOpacity(0.8),
                      borderRadius: BorderRadius.circular(12),
                      border: Border.all(color: scheme.outlineVariant.withOpacity(0.6)),
                      boxShadow: [
                        BoxShadow(
                          color: Colors.black.withOpacity(0.08),
                          blurRadius: 16,
                          offset: const Offset(0, 8),
                        ),
                      ],
                    ),
                    child: ListView.separated(
                      padding: const EdgeInsets.symmetric(vertical: 6),
                      itemCount: sources.length,
                      separatorBuilder: (_, __) =>
                          Divider(height: separatorHeight, color: scheme.outlineVariant),
                      itemBuilder: (context, index) {
                        final source = sources[index];
                        final selected = index == selectedIndex;
                        return InkWell(
                          onTap: () => _applyRecentSource(source, state),
                          child: Container(
                            height: itemHeight,
                            padding: const EdgeInsets.symmetric(horizontal: 12),
                            alignment: Alignment.centerLeft,
                            color: selected ? scheme.primary.withOpacity(0.12) : Colors.transparent,
                            child: Text(
                              source,
                              style: Theme.of(context).textTheme.bodyMedium,
                              overflow: TextOverflow.ellipsis,
                            ),
                          ),
                        );
                      },
                    ),
                  ),
                ),
              ),
            ),
          ],
        );
      },
    );
    overlay.insert(_sourceHistoryOverlay!);
  }

  void _hideRecentSourcesOverlay() {
    _sourceHistoryOverlay?.remove();
    _sourceHistoryOverlay = null;
  }

  List<String> _recentSourcesForState(ViewerState state) {
    return state.recentSources.take(10).toList();
  }

  void _moveRecentSelection(int delta, ViewerState state) {
    final sources = _recentSourcesForState(state);
    if (sources.isEmpty) return;
    var next = _recentSourceIndex + delta;
    if (next < 0) next = sources.length - 1;
    if (next >= sources.length) next = 0;
    _recentSourceIndex = next;
    _sourceHistoryOverlay?.markNeedsBuild();
  }

  void _acceptRecentSelection(ViewerState state) {
    if (_sourceHistoryOverlay == null) return;
    final sources = _recentSourcesForState(state);
    if (sources.isEmpty) {
      _hideRecentSourcesOverlay();
      return;
    }
    final index = _recentSourceIndex < sources.length ? _recentSourceIndex : 0;
    _applyRecentSource(sources[index], state);
  }

  void _applyRecentSource(String source, ViewerState state) {
    _inputController.value = TextEditingValue(
      text: source,
      selection: TextSelection.collapsed(offset: source.length),
    );
    state.setSourceInput(source);
    _hideRecentSourcesOverlay();
    _inputFocus.requestFocus();
  }

  @override
  Widget build(BuildContext context) {
    return Consumer<ViewerState>(
      builder: (context, state, _) {
        if (!_inputFocus.hasFocus && _inputController.text != state.sourceInput) {
          _inputController.text = state.sourceInput;
        }

        return Scaffold(
          body: SafeArea(
            child: Stack(
              children: [
                _buildBackdrop(context),
                Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    children: [
                      _buildTopBar(context, state),
                      const SizedBox(height: 16),
                      Expanded(child: _buildPanels(context, state)),
                      if (state.statusMessage != null && state.statusMessage!.isNotEmpty)
                        _buildStatusBar(state.statusMessage!),
                    ],
                  ),
                ),
              ],
            ),
          ),
        );
      },
    );
  }

  Widget _buildBackdrop(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    return Container(
      decoration: BoxDecoration(
        gradient: LinearGradient(
          colors: [
            scheme.surfaceContainerLowest,
            scheme.surfaceContainerHigh,
          ],
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
        ),
      ),
      child: Stack(
        children: [
          Positioned(
            top: -120,
            right: -80,
            child: _GlowBlob(color: scheme.primary.withOpacity(0.18), size: 240),
          ),
          Positioned(
            bottom: -140,
            left: -60,
            child: _GlowBlob(color: scheme.secondary.withOpacity(0.18), size: 260),
          ),
          Positioned(
            top: 140,
            left: 140,
            child: _GlowBlob(color: scheme.tertiary.withOpacity(0.12), size: 180),
          ),
        ],
      ),
    );
  }

  Widget _buildPanels(BuildContext context, ViewerState state) {
    return Row(
      children: [
        Expanded(flex: 3, child: _buildSourcesPane(state)),
        const SizedBox(width: 16),
        Expanded(flex: 4, child: _buildItemsPane(state)),
        const SizedBox(width: 16),
        Expanded(
          flex: 5,
          child: LayoutBuilder(
            builder: (context, constraints) {
              final maxPreviewHeight = _previewMaxHeightForState(state, constraints.maxHeight);
              return Column(
                children: [
                  Expanded(child: _buildFieldsPane(state)),
                  const SizedBox(height: 16),
                  AnimatedSize(
                    duration: const Duration(milliseconds: 240),
                    curve: Curves.easeOutCubic,
                    alignment: Alignment.topCenter,
                    child: ConstrainedBox(
                      constraints: BoxConstraints(maxHeight: maxPreviewHeight),
                      child: _buildPreviewPane(state, shrinkWrap: true),
                    ),
                  ),
                ],
              );
            },
          ),
        ),
      ],
    );
  }

  double _previewMaxHeightForState(ViewerState state, double availableHeight) {
    final isVideo = _isVideoPreview(state);
    final factor = isVideo ? 0.62 : 0.3;
    final minHeight = isVideo ? 320.0 : 140.0;
    final maxHeight = isVideo ? 600.0 : 320.0;
    return (availableHeight * factor).clamp(minHeight, maxHeight);
  }

  bool _isVideoPreview(ViewerState state) {
    if (state.mode == ViewerMode.zenodo) {
      final entry = state.zenodoSelectedEntryName;
      if (entry != null && _isVideoPath(entry)) return true;
      final fileKey = state.zenodoSelectedFileKey;
      if (entry == null && fileKey != null && _isVideoPath(fileKey)) return true;
    }
    if (state.mode == ViewerMode.webdatasetDir) {
      final path = state.wdsSelectedMemberPath;
      if (path != null && _isVideoPath(path)) return true;
    }
    return false;
  }

  bool _isVideoPath(String path) {
    final name = path.split('/').last;
    final dot = name.lastIndexOf('.');
    if (dot <= 0 || dot == name.length - 1) return false;
    final ext = name.substring(dot + 1).toLowerCase();
    return _isVideoExt(ext);
  }

  Widget _buildTopBar(BuildContext context, ViewerState state) {
    final scheme = Theme.of(context).colorScheme;
    final textTheme = Theme.of(context).textTheme;
    final loadLabel = state.detectedSourceLabel;
    final showHfToken = state.isHuggingFaceDetected;
    final hfTokenActionLabel = (state.hfToken == null || state.hfToken!.isEmpty) ? 'Add token' : 'Edit token';

    final isDesktop = !kIsWeb &&
        (defaultTargetPlatform == TargetPlatform.macOS ||
            defaultTargetPlatform == TargetPlatform.windows ||
            defaultTargetPlatform == TargetPlatform.linux);
    final isMacOS = defaultTargetPlatform == TargetPlatform.macOS;

    Widget titleContent = Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'Dataset Inspector',
          style: textTheme.headlineSmall?.copyWith(fontWeight: FontWeight.w700),
        ),
        const SizedBox(height: 4),
        Text(
          'Inspect LitData, MosaicML, WebDataset, Hugging Face, and Zenodo sources.',
          style: textTheme.bodySmall?.copyWith(color: scheme.onSurface.withOpacity(0.7)),
        ),
      ],
    );

    Widget titleRow = Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Expanded(
          child: isDesktop
              ? GestureDetector(
                  behavior: HitTestBehavior.translucent,
                  onPanStart: (_) => windowManager.startDragging(),
                  onDoubleTap: () async {
                    if (await windowManager.isMaximized()) {
                      await windowManager.unmaximize();
                    } else {
                      await windowManager.maximize();
                    }
                  },
                  child: titleContent,
                )
              : titleContent,
        ),
        if (showHfToken) ...[
          Row(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              Text(
                'Hugging Face token',
                style: textTheme.labelMedium?.copyWith(color: scheme.onSurface.withOpacity(0.7)),
              ),
              const SizedBox(width: 8),
              OutlinedButton.icon(
                onPressed: () => _showHfTokenDialog(context, state),
                icon: const Icon(Icons.key),
                label: Text(hfTokenActionLabel),
              ),
          ],
        ),
        const SizedBox(width: 12),
      ],
        _buildUpdateButton(state),
      ],
    );

    return Column(
      children: [
        const SizedBox(height: 10),
        titleRow,
        const SizedBox(height: 14),
        Container(
          padding: const EdgeInsets.all(14),
          decoration: BoxDecoration(
            color: scheme.surface,
            borderRadius: BorderRadius.circular(18),
            border: Border.all(color: scheme.outlineVariant),
            boxShadow: [
              BoxShadow(
                color: Colors.black.withOpacity(0.04),
                blurRadius: 18,
                offset: const Offset(0, 8),
              ),
            ],
          ),
          child: Column(
            children: [
              Row(
                children: [
                  Expanded(
                    child: CompositedTransformTarget(
                      link: _sourceFieldLink,
                      child: Focus(
                        canRequestFocus: false,
                        onKeyEvent: (node, event) {
                          if (_sourceHistoryOverlay == null) return KeyEventResult.ignored;
                          if (event is! KeyDownEvent) return KeyEventResult.ignored;
                          if (event.logicalKey == LogicalKeyboardKey.arrowDown) {
                            _moveRecentSelection(1, state);
                            return KeyEventResult.handled;
                          }
                          if (event.logicalKey == LogicalKeyboardKey.arrowUp) {
                            _moveRecentSelection(-1, state);
                            return KeyEventResult.handled;
                          }
                          if (event.logicalKey == LogicalKeyboardKey.enter) {
                            _acceptRecentSelection(state);
                            return KeyEventResult.handled;
                          }
                          if (event.logicalKey == LogicalKeyboardKey.tab) {
                            _hideRecentSourcesOverlay();
                            return KeyEventResult.handled;
                          }
                          if (event.logicalKey == LogicalKeyboardKey.escape) {
                            _hideRecentSourcesOverlay();
                            return KeyEventResult.handled;
                          }
                          return KeyEventResult.ignored;
                        },
                        child: TextField(
                          key: _sourceFieldKey,
                          controller: _inputController,
                          focusNode: _inputFocus,
                          decoration: InputDecoration(
                            labelText: 'Dataset source',
                            hintText: 'Paste a dataset path or URL',
                            prefixIcon: const Icon(Icons.link),
                            suffixIcon: Tooltip(
                              message: 'Browse for folder',
                              child: IconButton(
                                onPressed: () => state.chooseIndexSource(),
                                icon: const Icon(Icons.folder_open),
                              ),
                            ),
                          ),
                          onTap: () => _showRecentSourcesOverlay(context, state),
                          onChanged: state.setSourceInput,
                          onSubmitted: (_) => _acceptRecentSelection(state),
                        ),
                      ),
                    ),
                  ),
                  const SizedBox(width: 8),
                  FilledButton.icon(
                    onPressed: () => state.loadFromSource(),
                    icon: const Icon(Icons.play_arrow),
                    label: Text('Load ($loadLabel)'),
                  ),
                ],
              ),
            ],
          ),
        ),
      ],
    );
  }

  Widget _buildUpdateButton(ViewerState state) {
    return FutureBuilder<UpdateInfo?>(
      future: state.updateCheckFuture,
      builder: (context, snapshot) {
        final update = snapshot.data;
        if (update == null) {
          return const SizedBox.shrink();
        }
        return FilledButton.tonalIcon(
          onPressed: () => showUpdateDialog(context, state, update),
          icon: const Icon(Icons.system_update_alt),
          label: Text('${update.currentVersion} → ${update.version}'),
        );
      },
    );
  }

  Future<void> _showHfTokenDialog(BuildContext context, ViewerState state) async {
    final controller = TextEditingController(text: state.hfToken ?? '');
    await showDialog<void>(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          title: const Text('Hugging Face token'),
          content: TextField(
            controller: controller,
            decoration: const InputDecoration(hintText: 'Paste your HF token (optional)'),
          ),
          actions: [
            TextButton(
              onPressed: () async {
                await state.clearHfToken();
                if (context.mounted) Navigator.of(dialogContext).pop();
              },
              child: const Text('Clear'),
            ),
            FilledButton(
              onPressed: () async {
                await state.saveHfToken(controller.text);
                if (context.mounted) Navigator.of(dialogContext).pop();
              },
              child: const Text('Save'),
            ),
          ],
        );
      },
    );
  }

  Widget _buildSourcesPane(ViewerState state) {
    return _PanelCard(
      title: 'Sources',
      subtitle: _sourcesSubtitle(state),
      child: AnimatedSwitcher(
        duration: const Duration(milliseconds: 250),
        child: Builder(
          key: ValueKey(state.mode),
          builder: (context) {
            if (state.mode == ViewerMode.huggingface) {
              return _buildHfConfigPane(state);
            }
            if (state.mode == ViewerMode.zenodo) {
              return _buildZenodoFilesPane(state);
            }
            if (state.mode == ViewerMode.webdatasetDir) {
              return _buildWdsShardsPane(state);
            }
            return _buildChunksPane(state);
          },
        ),
      ),
    );
  }

  String? _sourcesSubtitle(ViewerState state) {
    if (state.mode == ViewerMode.huggingface && state.hfConfigOptions != null) {
      final configs = state.hfConfigOptions!;
      final numSubsets = configs.length;
      final numParts = configs.fold<int>(0, (sum, c) => sum + c.splits.length);
      return '$numSubsets subsets, $numParts parts';
    }
    if (state.mode == ViewerMode.zenodo && state.zenodoRecord != null) {
      final record = state.zenodoRecord!;
      return 'Record ${record.recordId} · ${record.files.length} files';
    }
    if (state.mode == ViewerMode.webdatasetDir && state.wdsDirSummary != null) {
      return '${state.wdsDirSummary!.shards.length} shards';
    }
    if (state.indexSummary != null) {
      final count = state.indexSummary!.chunks.length;
      return '$count ${count == 1 ? 'chunk' : 'chunks'}';
    }
    return null;
  }

  String? _itemsSubtitle(ViewerState state) {
    if (state.mode == ViewerMode.huggingface && state.hfPreview != null) {
      final preview = state.hfPreview!;
      final totalLabel = preview.numRowsTotal > 0 ? preview.numRowsTotal.toString() : '-';
      return 'Total: $totalLabel';
    }
    if (state.mode == ViewerMode.webdatasetDir && state.wdsSamples != null) {
      final pageSize = state.wdsSamples!.length;
      final pageLabel = pageSize > 0 ? state.wdsOffset ~/ pageSize + 1 : 1;
      return 'Page $pageLabel';
    }
    if (state.mode == ViewerMode.zenodo && state.zenodoSelectedFileKey != null) {
      return 'File ${state.zenodoSelectedFileKey}';
    }
    if ((state.mode == ViewerMode.litdataIndex ||
            state.mode == ViewerMode.litdataChunks ||
            state.mode == ViewerMode.mdsIndex) &&
        state.indexSummary != null &&
        state.selectedChunkName != null) {
      final chunk = state.indexSummary!.chunks
          .where((c) => c.filename == state.selectedChunkName)
          .firstOrNull;
      if (chunk != null) {
        return 'Total: ${chunk.chunkSize}';
      }
    }
    if (state.selectedChunkName != null) {
      return state.selectedChunkName;
    }
    return null;
  }

  String? _previewSubtitle(ViewerState state) {
    if (state.mode == ViewerMode.huggingface &&
        state.hfSelectedRowIndex != null &&
        state.hfSelectedFieldName != null) {
      return 'Row ${state.hfSelectedRowIndex} · ${state.hfSelectedFieldName}';
    }
    if (state.mode == ViewerMode.webdatasetDir && state.wdsSelectedMemberName != null) {
      return state.wdsSelectedMemberName;
    }
    if (state.mode == ViewerMode.zenodo && state.zenodoSelectedEntryName != null) {
      return state.zenodoSelectedEntryName;
    }
    if (state.selectedFieldIndex != null) {
      return 'Field ${state.selectedFieldIndex}';
    }
    return null;
  }

  String? _fieldsSubtitle(ViewerState state) {
    if (state.mode == ViewerMode.huggingface && state.hfPreview != null) {
      return '${state.hfPreview!.features.length} fields';
    }
    if (state.mode == ViewerMode.webdatasetDir && state.wdsSelectedSampleKey != null) {
      return 'Sample ${state.wdsSelectedSampleKey}';
    }
    if (state.mode == ViewerMode.mdsIndex && state.selectedItemIndex != null) {
      return 'Sample ${state.selectedItemIndex}';
    }
    if ((state.mode == ViewerMode.litdataIndex || state.mode == ViewerMode.litdataChunks) &&
        state.selectedItemIndex != null) {
      return 'Item ${state.selectedItemIndex}';
    }
    return null;
  }

  Widget _buildChunksPane(ViewerState state) {
    final future = state.indexFuture;
    if (future == null) {
      return const Center(child: Text('No dataset loaded.'));
    }
    return FutureBuilder<IndexSummary>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No chunks found.'));
        }
        final chunks = snapshot.data!.chunks;
        return ListView.separated(
          itemCount: chunks.length,
          separatorBuilder: (_, __) => const SizedBox(height: 6),
          itemBuilder: (context, index) {
            final chunk = chunks[index];
            return HoverTile(
              selected: state.selectedChunkName == chunk.filename,
              onTap: () => state.selectChunk(chunk.filename),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(chunk.filename, style: Theme.of(context).textTheme.bodyMedium),
                  const SizedBox(height: 4),
                  Text('${chunk.chunkSize} items · ${_formatBytes(chunk.chunkBytes)}',
                      style: Theme.of(context).textTheme.bodySmall),
                ],
              ),
            );
          },
        );
      },
    );
  }

  Widget _buildWdsShardsPane(ViewerState state) {
    final future = state.wdsDirFuture;
    if (future == null) return const Center(child: Text('No dataset loaded.'));
    return FutureBuilder<WdsDirSummary>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No shards found.'));
        final shards = snapshot.data!.shards;
        return ListView.separated(
          itemCount: shards.length,
          separatorBuilder: (_, __) => const SizedBox(height: 6),
          itemBuilder: (context, index) {
            final shard = shards[index];
            return HoverTile(
              selected: state.selectedShardName == shard.filename,
              onTap: () => state.selectWdsShard(shard.filename),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(shard.filename, style: Theme.of(context).textTheme.bodyMedium),
                  const SizedBox(height: 4),
                  Text(_formatBytes(shard.bytes), style: Theme.of(context).textTheme.bodySmall),
                ],
              ),
            );
          },
        );
      },
    );
  }

  Widget _buildHfConfigPane(ViewerState state) {
    final preview = state.hfPreview;
    final cachedConfigs = state.hfConfigOptions ?? preview?.configs ?? const <HfConfigSummary>[];
    final cachedOptions = _flattenHfConfigOptions(cachedConfigs, preview);
    if (cachedOptions.isNotEmpty) {
      return _buildHfConfigOptionsList(state, cachedOptions, preview);
    }

    final future = state.hfPreviewFuture;
    if (future == null) return const Center(child: Text('No dataset loaded.'));
    return FutureBuilder<HfDatasetPreview>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No configs found.'));
        final data = snapshot.data!;
        final options = _flattenHfConfigOptions(data.configs, data);
        if (options.isEmpty) {
          return const Center(child: Text('No configs found.'));
        }
        return _buildHfConfigOptionsList(state, options, data);
      },
    );
  }

  List<_HfConfigSplitOption> _flattenHfConfigOptions(
    List<HfConfigSummary> configs,
    HfDatasetPreview? preview,
  ) {
    final options = <_HfConfigSplitOption>[];
    for (final config in configs) {
      if (config.splits.isEmpty) {
        options.add(_HfConfigSplitOption(config: config.config, split: ''));
        continue;
      }
      for (final split in config.splits) {
        options.add(_HfConfigSplitOption(config: config.config, split: split));
      }
    }
    if (options.isEmpty && preview != null) {
      options.add(_HfConfigSplitOption(config: preview.config, split: preview.split));
    }
    return options;
  }

  Widget _buildHfConfigOptionsList(
    ViewerState state,
    List<_HfConfigSplitOption> options,
    HfDatasetPreview? preview,
  ) {
    final desiredConfig = state.hfConfigOverride ?? preview?.config ?? options.first.config;
    final desiredSplit = state.hfSplitOverride ?? preview?.split ?? options.first.split;
    final selected = options.firstWhere(
      (option) => option.config == desiredConfig && option.split == desiredSplit,
      orElse: () => options.first,
    );
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Expanded(
          child: ListView.separated(
            itemCount: options.length,
            separatorBuilder: (_, __) => const SizedBox(height: 6),
            itemBuilder: (context, index) {
              final option = options[index];
              final splitLabel = option.split.isEmpty ? 'default' : option.split;
              return HoverTile(
                selected: option.config == selected.config && option.split == selected.split,
                onTap: () => state.setHfConfigSplit(option.config, option.split),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(option.config, style: Theme.of(context).textTheme.bodyMedium),
                    const SizedBox(height: 4),
                    Text(splitLabel, style: Theme.of(context).textTheme.bodySmall),
                  ],
                ),
              );
            },
          ),
        ),
      ],
    );
  }

  Widget _buildZenodoFilesPane(ViewerState state) {
    final future = state.zenodoRecordFuture;
    if (future == null) return const Center(child: Text('No record loaded.'));
    return FutureBuilder<ZenodoRecordSummary>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No files found.'));
        final record = snapshot.data!;
        return ListView.separated(
          itemCount: record.files.length,
          separatorBuilder: (_, __) => const SizedBox(height: 6),
          itemBuilder: (context, index) {
            final file = record.files[index];
            return HoverTile(
              selected: state.zenodoSelectedFileKey == file.key,
              onTap: () => state.selectZenodoFile(file.key),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(file.key, style: Theme.of(context).textTheme.bodyMedium),
                  const SizedBox(height: 4),
                  Text(_formatBytes(file.size), style: Theme.of(context).textTheme.bodySmall),
                ],
              ),
            );
          },
        );
      },
    );
  }

  Widget _buildItemsPane(ViewerState state) {
    return _PanelCard(
      title: 'Items',
      subtitle: _itemsSubtitle(state),
      child: AnimatedSwitcher(
        duration: const Duration(milliseconds: 250),
        child: Builder(
          key: ValueKey(state.mode),
          builder: (context) {
            if (state.mode == ViewerMode.huggingface) {
              return _buildHfRowsPane(state);
            }
            if (state.mode == ViewerMode.zenodo) {
              return _buildZenodoEntriesPane(state);
            }
            if (state.mode == ViewerMode.webdatasetDir) {
              return _buildWdsSamplesPane(state);
            }
            if (state.mode == ViewerMode.mdsIndex) {
              return _buildMdsItemsPane(state);
            }
            return _buildLitdataItemsPane(state);
          },
        ),
      ),
    );
  }

  Widget _buildFieldsPane(ViewerState state) {
    return _PanelCard(
      title: 'Fields',
      subtitle: _fieldsSubtitle(state),
      child: AnimatedSwitcher(
        duration: const Duration(milliseconds: 250),
        child: Builder(
          key: ValueKey('${state.mode}-fields'),
          builder: (context) {
            if (state.mode == ViewerMode.huggingface) {
              return _buildHfFieldsPaneFromState(state);
            }
            if (state.mode == ViewerMode.webdatasetDir) {
              return _buildWdsFieldsPaneFromState(state);
            }
            if (state.mode == ViewerMode.mdsIndex) {
              return _buildMdsFieldsPane(state);
            }
            if (state.mode == ViewerMode.zenodo) {
              return const Center(child: Text('No fields for this source.'));
            }
            return _buildLitdataFieldsPane(state);
          },
        ),
      ),
    );
  }

  Widget _buildLitdataFieldsPane(ViewerState state) {
    final future = state.litdataItemsFuture;
    if (future == null) return const Center(child: Text('Select an item.'));
    return FutureBuilder<List<ItemMeta>>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No fields found.'));
        return _buildFieldList(state, snapshot.data!);
      },
    );
  }

  Widget _buildMdsFieldsPane(ViewerState state) {
    final future = state.mdsItemsFuture;
    if (future == null) return const Center(child: Text('Select a sample.'));
    return FutureBuilder<List<ItemMeta>>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No fields found.'));
        return _buildFieldList(state, snapshot.data!);
      },
    );
  }

  Widget _buildWdsFieldsPaneFromState(ViewerState state) {
    final future = state.wdsSamplesFuture;
    if (future == null) return const Center(child: Text('Select a sample.'));
    return FutureBuilder<WdsSampleListResponse>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No fields found.'));
        return _buildWdsFieldsPane(state, snapshot.data!.samples);
      },
    );
  }

  Widget _buildHfFieldsPaneFromState(ViewerState state) {
    final preview = state.hfPreview;
    if (preview != null) return _buildHfFieldsPane(state, preview);
    final future = state.hfPreviewFuture;
    if (future == null) return const Center(child: Text('No dataset loaded.'));
    return FutureBuilder<HfDatasetPreview>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No fields found.'));
        return _buildHfFieldsPane(state, snapshot.data!);
      },
    );
  }

  Widget _buildLitdataItemsPane(ViewerState state) {
    final future = state.litdataItemsFuture;
    if (future == null) return const Center(child: Text('Select a chunk.'));
    return FutureBuilder<List<ItemMeta>>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No items found.'));
        final items = snapshot.data!;
        return _buildItemList(state, items);
      },
    );
  }

  Widget _buildMdsItemsPane(ViewerState state) {
    final future = state.mdsItemsFuture;
    if (future == null) return const Center(child: Text('Select a shard.'));
    return FutureBuilder<List<ItemMeta>>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No samples found.'));
        final items = snapshot.data!;
        return _buildItemList(state, items);
      },
    );
  }

  Widget _buildItemList(ViewerState state, List<ItemMeta> items) {
    return ListView.separated(
      itemCount: items.length,
      separatorBuilder: (_, __) => const SizedBox(height: 6),
      itemBuilder: (context, index) {
        final item = items[index];
        return HoverTile(
          selected: state.selectedItemIndex == item.itemIndex,
                onTap: () => state.selectItem(item.itemIndex, fieldCount: item.fields.length),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text('Item ${item.itemIndex}', style: Theme.of(context).textTheme.bodyMedium),
              const SizedBox(height: 4),
              Text('${item.fields.length} fields · ${_formatBytes(item.totalBytes)}',
                  style: Theme.of(context).textTheme.bodySmall),
            ],
          ),
        );
      },
    );
  }

  Widget _buildFieldList(ViewerState state, List<ItemMeta> items) {
    final selected = items.where((item) => item.itemIndex == state.selectedItemIndex).toList();
    if (selected.isEmpty) {
      return const Center(child: Text('Select an item to view fields.'));
    }
    final fields = selected.first.fields;
    final formatByIndex = _formatByFieldIndex(state);
    final previewMap =
        state.mode == ViewerMode.mdsIndex ? state.mdsFieldPreviewByIndex : state.litdataFieldPreviewByIndex;
    return ListView.separated(
      itemCount: fields.length,
      separatorBuilder: (_, __) => const SizedBox(height: 6),
      itemBuilder: (context, index) {
        final field = fields[index];
        final format = field.fieldIndex < formatByIndex.length ? formatByIndex[field.fieldIndex] : null;
        final preview = previewMap[field.fieldIndex];
        final meta = preview == null
            ? _fieldMetaFromFormat(format, field.size)
            : _fieldMetaFromPreview(preview, field.size);
        final rightWidget = _buildInlineMetaText(context, meta);
        return HoverTile(
          selected: state.selectedFieldIndex == field.fieldIndex,
          onTap: () => state.selectField(field.fieldIndex),
          child: Row(
            children: [
              Expanded(
                child: Text(
                  'Field ${field.fieldIndex}',
                  style: Theme.of(context).textTheme.bodyMedium,
                  overflow: TextOverflow.ellipsis,
                ),
              ),
              const SizedBox(width: 8),
              rightWidget,
            ],
          ),
        );
      },
    );
  }

  Widget _buildWdsSamplesPane(ViewerState state) {
    final future = state.wdsSamplesFuture;
    if (future == null) return const Center(child: Text('Select a shard.'));
    return FutureBuilder<WdsSampleListResponse>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No samples found.'));
        final response = snapshot.data!;
        final samples = response.samples;
        final total = response.numSamplesTotal;
        final pageSize = response.length;
        final canGoNext = total == null
            ? samples.length == pageSize && pageSize > 0
            : (response.offset + samples.length) < total;
        final canGoPrev = response.offset > 0 && pageSize > 0;
        final pageLabel = pageSize > 0 ? response.offset ~/ pageSize + 1 : 1;
        final prevOffset = response.offset - pageSize;
        return Column(
          children: [
            Expanded(
              child: ListView.separated(
                itemCount: samples.length,
                separatorBuilder: (_, __) => const SizedBox(height: 6),
                itemBuilder: (context, index) {
                  final sample = samples[index];
                  return HoverTile(
                    selected: state.wdsSelectedSampleKey == sample.key,
                    onTap: () => state.selectWdsSample(sample.key, fields: sample.fields),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(sample.key, style: Theme.of(context).textTheme.bodyMedium),
                        const SizedBox(height: 4),
                        Text('${sample.fields.length} fields · ${_formatBytes(sample.totalBytes)}',
                            style: Theme.of(context).textTheme.bodySmall),
                      ],
                    ),
                  );
                },
              ),
            ),
            const Divider(height: 1),
            Padding(
              padding: const EdgeInsets.only(top: 8),
              child: Row(
                children: [
                  IconButton(
                    onPressed: canGoPrev ? () => state.setWdsOffset(prevOffset < 0 ? 0 : prevOffset) : null,
                    icon: const Icon(Icons.chevron_left),
                  ),
                  Expanded(
                    child: Center(
                      child: Text('Samples ${response.offset + 1}–${response.offset + samples.length}'),
                    ),
                  ),
                  IconButton(
                    onPressed: canGoNext ? () => state.setWdsOffset(response.offset + pageSize) : null,
                    icon: const Icon(Icons.chevron_right),
                  ),
                ],
              ),
            ),
          ],
        );
      },
    );
  }

  Widget _buildWdsFieldsPane(ViewerState state, List<WdsSampleInfo> samples) {
    final selected = samples.where((sample) => sample.key == state.wdsSelectedSampleKey).toList();
    if (selected.isEmpty) return const Center(child: Text('Select a sample.'));
    final fields = selected.first.fields;
    return ListView.separated(
      itemCount: fields.length,
      separatorBuilder: (_, __) => const SizedBox(height: 6),
      itemBuilder: (context, index) {
        final field = fields[index];
        final meta = _fieldMetaFromPath(field.memberPath, field.size);
        final rightWidget = _buildInlineMetaText(context, meta);
        return HoverTile(
          selected: state.wdsSelectedMemberPath == field.memberPath,
          onTap: () => state.selectWdsMember(field.memberPath, memberName: field.name),
          child: Row(
            children: [
              Expanded(
                child: Text(
                  field.name,
                  style: Theme.of(context).textTheme.bodyMedium,
                  overflow: TextOverflow.ellipsis,
                ),
              ),
              const SizedBox(width: 8),
              rightWidget,
            ],
          ),
        );
      },
    );
  }

  Widget _buildHfRowsPane(ViewerState state) {
    final future = state.hfPreviewFuture;
    if (future == null) return const Center(child: Text('No dataset loaded.'));
    return FutureBuilder<HfDatasetPreview>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No rows available.'));
        final preview = snapshot.data!;
        final totalLabel = preview.numRowsTotal > 0 ? preview.numRowsTotal.toString() : '-';
        if (!_hfOffsetFocus.hasFocus) {
          final nextText = (preview.offset + 1).toString();
          if (_hfOffsetController.text != nextText) {
            _hfOffsetController.text = nextText;
            _hfOffsetController.selection = TextSelection.collapsed(offset: nextText.length);
          }
        }
        return Column(
          children: [
            Expanded(
              child: ListView.separated(
                itemCount: preview.rows.length,
                separatorBuilder: (_, __) => const SizedBox(height: 6),
                itemBuilder: (context, index) {
                  final rowIndex = preview.offset + index;
                  return HoverTile(
                    selected: state.hfSelectedRowIndex == rowIndex,
                    onTap: () => state.selectHfRow(rowIndex),
                    child: Text('Row $rowIndex', style: Theme.of(context).textTheme.bodyMedium),
                  );
                },
              ),
            ),
            const Divider(height: 1),
            Padding(
              padding: const EdgeInsets.only(top: 8),
              child: Row(
                children: [
                  IconButton(
                    onPressed: preview.offset > 0
                        ? () => state.setHfOffset(
                            (preview.offset - preview.length).clamp(0, preview.numRowsTotal).toInt(),
                          )
                        : null,
                    icon: const Icon(Icons.chevron_left),
                  ),
                  Expanded(
                    child: Center(
                      child: Row(
                        mainAxisSize: MainAxisSize.min,
                        children: [
                          SizedBox(
                            width: 72,
                            child: TextField(
                              controller: _hfOffsetController,
                              focusNode: _hfOffsetFocus,
                              textAlign: TextAlign.center,
                              keyboardType: TextInputType.number,
                              inputFormatters: [FilteringTextInputFormatter.digitsOnly],
                              decoration: const InputDecoration(
                                isDense: true,
                                contentPadding: EdgeInsets.symmetric(vertical: 6, horizontal: 8),
                              ),
                              onSubmitted: (_) => _applyHfOffsetInput(state, preview),
                              onEditingComplete: () => _applyHfOffsetInput(state, preview),
                            ),
                          ),
                          const SizedBox(width: 8),
                          Text('- ${preview.offset + preview.rows.length}'),
                        ],
                      ),
                    ),
                  ),
                  IconButton(
                    onPressed: preview.offset + preview.rows.length < preview.numRowsTotal
                        ? () => state.setHfOffset(preview.offset + preview.rows.length)
                        : null,
                    icon: const Icon(Icons.chevron_right),
                  ),
                ],
              ),
            ),
          ],
        );
      },
    );
  }

  void _applyHfOffsetInput(ViewerState state, HfDatasetPreview preview) {
    final raw = _hfOffsetController.text.trim();
    final requested = int.tryParse(raw);
    if (requested == null) return;
    final total = preview.numRowsTotal;
    if (total <= 0) {
      state.setHfOffset(0);
      return;
    }
    var rowNumber = requested;
    if (rowNumber < 1) rowNumber = 1;
    if (rowNumber > total) rowNumber = total;
    final offset = rowNumber - 1;
    if (offset != preview.offset) {
      state.setHfOffset(offset);
    }
    if (_hfOffsetController.text != rowNumber.toString()) {
      _hfOffsetController.text = rowNumber.toString();
      _hfOffsetController.selection = TextSelection.collapsed(
        offset: _hfOffsetController.text.length,
      );
    }
  }

  Widget _buildHfFieldsPane(ViewerState state, HfDatasetPreview preview) {
    final features = preview.features;
    if (features.isEmpty) return const Center(child: Text('No fields.'));
    final sizeByField = _hfFieldSizeMap(state);
    return ListView.separated(
      itemCount: features.length,
      separatorBuilder: (_, __) => const SizedBox(height: 6),
      itemBuilder: (context, index) {
        final feature = features[index];
        final size = sizeByField[feature.name];
        final meta = _fieldMetaFromFormat(feature.dtype, size);
        final rightWidget = _buildInlineMetaText(context, meta);
        return HoverTile(
          selected: state.hfSelectedFieldName == feature.name,
          onTap: () => state.selectHfField(feature.name),
          child: Row(
            children: [
              Expanded(
                child: Text(
                  feature.name,
                  style: Theme.of(context).textTheme.bodyMedium,
                  overflow: TextOverflow.ellipsis,
                ),
              ),
              const SizedBox(width: 8),
              rightWidget,
            ],
          ),
        );
      },
    );
  }

  Widget _buildInlineMetaText(BuildContext context, String text) {
    return ConstrainedBox(
      constraints: const BoxConstraints(minWidth: 160),
      child: Align(
        alignment: Alignment.centerRight,
        child: Text(
          text,
          style: Theme.of(context).textTheme.bodySmall,
          textAlign: TextAlign.right,
          maxLines: 1,
          overflow: TextOverflow.ellipsis,
        ),
      ),
    );
  }

  Widget _buildZenodoEntriesPane(ViewerState state) {
    final recordFuture = state.zenodoRecordFuture;
    if (recordFuture == null) return const Center(child: Text('No record loaded.'));
    return FutureBuilder<ZenodoRecordSummary>(
      future: recordFuture,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No entries.'));
        if (state.zenodoZipEntriesFuture != null) {
          return _buildZenodoZipEntries(state);
        }
        if (state.zenodoTarEntriesFuture != null) {
          return _buildZenodoTarEntries(state);
        }
        return const Center(child: Text('Select a file to preview.'));
      },
    );
  }

  Widget _buildZenodoZipEntries(ViewerState state) {
    final future = state.zenodoZipEntriesFuture;
    if (future == null) return const Center(child: Text('No ZIP entries.'));
    return FutureBuilder<List<ZenodoZipEntrySummary>>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No entries.'));
        final entries = snapshot.data!;
        return ListView.separated(
          itemCount: entries.length,
          separatorBuilder: (_, __) => const SizedBox(height: 6),
          itemBuilder: (context, index) {
            final entry = entries[index];
            return HoverTile(
              selected: state.zenodoSelectedEntryName == entry.name,
              onTap: () => state.selectZenodoEntry(entry.name),
              child: Row(
                children: [
                  Expanded(child: Text(entry.name, overflow: TextOverflow.ellipsis)),
                  Text(_formatBytes(entry.uncompressedSize),
                      style: Theme.of(context).textTheme.bodySmall),
                ],
              ),
            );
          },
        );
      },
    );
  }

  Widget _buildZenodoTarEntries(ViewerState state) {
    final future = state.zenodoTarEntriesFuture;
    if (future == null) return const Center(child: Text('No TAR entries.'));
    return FutureBuilder<ZenodoTarEntryListResponse>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) return const Center(child: Text('No entries.'));
        final response = snapshot.data!;
        final entries = response.entries;
        final pageSize = response.length;
        final total = response.numEntriesTotal;
        final canGoNext = total == null
            ? entries.length == pageSize && pageSize > 0
            : (response.offset + entries.length) < total;
        final canGoPrev = response.offset > 0 && pageSize > 0;
        final pageLabel = pageSize > 0 ? response.offset ~/ pageSize + 1 : 1;
        final prevOffset = response.offset - pageSize;
        return Column(
          children: [
            Expanded(
              child: ListView.separated(
                itemCount: entries.length,
                separatorBuilder: (_, __) => const SizedBox(height: 6),
                itemBuilder: (context, index) {
                  final entry = entries[index];
                  return HoverTile(
                    selected: state.zenodoSelectedEntryName == entry.name,
                    onTap: () => state.selectZenodoEntry(entry.name),
                    child: Row(
                      children: [
                        Expanded(child: Text(entry.name, overflow: TextOverflow.ellipsis)),
                        Text(_formatBytes(entry.size),
                            style: Theme.of(context).textTheme.bodySmall),
                      ],
                    ),
                  );
                },
              ),
            ),
            const Divider(height: 1),
            Row(
              children: [
                IconButton(
                  onPressed:
                      canGoPrev ? () => state.setZenodoEntriesOffset(prevOffset < 0 ? 0 : prevOffset) : null,
                  icon: const Icon(Icons.chevron_left),
                ),
                Text('Page $pageLabel'),
                const Spacer(),
                IconButton(
                  onPressed: canGoNext ? () => state.setZenodoEntriesOffset(response.offset + pageSize) : null,
                  icon: const Icon(Icons.chevron_right),
                ),
              ],
            ),
          ],
        );
      },
    );
  }

  Widget _buildPreviewPane(ViewerState state, {bool shrinkWrap = false}) {
    return _PanelCard(
      title: 'Preview',
      subtitle: _previewSubtitle(state),
      subtitleTrailing: _buildPreviewActions(state),
      expandBody: !shrinkWrap,
      bodyFlexible: shrinkWrap,
      child: AnimatedSwitcher(
        duration: const Duration(milliseconds: 250),
        child: Builder(
          key: ValueKey(state.mode),
          builder: (context) {
            if (state.mode == ViewerMode.huggingface) {
              return _buildHfPreview(state);
            }
            if (state.mode == ViewerMode.zenodo) {
              return _buildZenodoPreview(state);
            }
            if (state.mode == ViewerMode.webdatasetDir) {
              return _buildWdsPreview(state);
            }
            if (state.mode == ViewerMode.mdsIndex) {
              return _buildMdsPreview(state);
            }
            return _buildLitdataPreview(state);
          },
        ),
      ),
    );
  }

  Widget _buildLitdataPreview(ViewerState state) {
    final future = state.fieldPreviewFuture;
    if (future == null) return const Center(child: Text('Select a field.'));
    return FutureBuilder<FieldPreview>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingPreview();
        }
        if (!snapshot.hasData) return const Center(child: Text('No preview.'));
        final preview = snapshot.data!;
        return _buildPreviewContent(state, preview);
      },
    );
  }

  Widget _buildMdsPreview(ViewerState state) {
    final future = state.mdsFieldPreviewFuture;
    if (future == null) return const Center(child: Text('Select a field.'));
    return FutureBuilder<FieldPreview>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingPreview();
        }
        if (!snapshot.hasData) return const Center(child: Text('No preview.'));
        final preview = snapshot.data!;
        return _buildPreviewContent(state, preview);
      },
    );
  }

  Widget _buildWdsPreview(ViewerState state) {
    final future = state.wdsPreviewFuture;
    if (future == null) return const Center(child: Text('Select a field.'));
    return FutureBuilder<FieldPreview>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingPreview();
        }
        if (!snapshot.hasData) return const Center(child: Text('No preview.'));
        final preview = snapshot.data!;
        return _buildPreviewContent(state, preview, isWds: true);
      },
    );
  }

  String? _resolveHfPreviewText(ViewerState state) {
    final preview = state.hfPreview;
    if (preview == null) return null;
    if (state.hfSelectedRowIndex == null || state.hfSelectedFieldName == null) {
      return null;
    }
    final rowOffset = state.hfSelectedRowIndex! - preview.offset;
    if (rowOffset < 0 || rowOffset >= preview.rows.length) {
      return null;
    }
    final row = preview.rows[rowOffset];
    if (row is! Map<String, dynamic>) {
      return null;
    }
    final value = row[state.hfSelectedFieldName];
    return const JsonEncoder.withIndent('  ').convert(value);
  }

  Widget _buildHfPreview(ViewerState state) {
    final text = _resolveHfPreviewText(state);
    if (text == null) {
      return const Center(child: Text('Select a row and field.'));
    }
    return _PreviewSection(
      content: _CodeBlock(text: text),
    );
  }

  Widget _buildZenodoPreview(ViewerState state) {
    final filePreviewFuture = state.zenodoFilePreviewFuture;
    final entryPreviewFuture = state.zenodoEntryPreviewFuture;
    if (entryPreviewFuture != null) {
      return FutureBuilder<FieldPreview>(
        future: entryPreviewFuture,
        builder: (context, snapshot) {
          if (snapshot.connectionState == ConnectionState.waiting) {
            return const _LoadingPreview();
          }
          if (!snapshot.hasData) return const Center(child: Text('No preview.'));
          final preview = snapshot.data!;
          return _buildZenodoInlinePreview(state, preview);
        },
      );
    }
    if (filePreviewFuture != null) {
      return FutureBuilder<FieldPreview>(
        future: filePreviewFuture,
        builder: (context, snapshot) {
          if (snapshot.connectionState == ConnectionState.waiting) {
            return const _LoadingPreview();
          }
          if (!snapshot.hasData) return const Center(child: Text('No preview.'));
          final preview = snapshot.data!;
          return _buildPreviewContent(state, preview);
        },
      );
    }
    return const Center(child: Text('Select a file or entry.'));
  }

  Widget _buildZenodoInlinePreview(ViewerState state, FieldPreview preview) {
    final ext = preview.guessedExt?.toLowerCase() ?? '';
    if (_isVideoExt(ext)) {
      return _PreviewSection(
        scrollable: false,
        content: VideoPreview(
          ext: ext,
          loader: () => state.zenodoPrepareSelectedEntryMedia(),
        ),
      );
    }
    final inlineFuture = state.zenodoInlineMediaFuture;
    if (inlineFuture == null) {
      return _buildPreviewContent(state, preview);
    }
    return FutureBuilder<InlineMediaResponse>(
      future: inlineFuture,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return _buildPreviewContent(state, preview);
        }
        if (!snapshot.hasData) return _buildPreviewContent(state, preview);
        final media = snapshot.data!;
        return _buildInlineMediaPreview(media, preview);
      },
    );
  }

  Widget _buildInlineMediaPreview(InlineMediaResponse media, FieldPreview preview) {
    final ext = media.ext.toLowerCase();
    Widget content;
    if (_isImageExt(ext)) {
      final bytes = base64Decode(media.base64);
      content = ClipRRect(
        borderRadius: BorderRadius.circular(16),
        child: Image.memory(bytes, fit: BoxFit.contain),
      );
    } else if (_isAudioExt(ext)) {
      content = AudioPreview(
        label: 'Audio preview',
        loader: () async {
          final bytes = base64Decode(media.base64);
          if (ext == 'sph') {
            final wavBytes = await decodeSphereToWavWithFallback(bytes);
            return PreparedMediaResponse(bytes: wavBytes, size: wavBytes.length, ext: 'wav');
          }
          return PreparedMediaResponse(bytes: bytes, size: media.size, ext: ext);
        },
      );
    } else if (_isVideoExt(ext)) {
      final bytes = base64Decode(media.base64);
      content = VideoPreview(
        bytes: bytes,
        ext: ext,
      );
    } else {
      content = _buildPreviewBody(preview);
    }
    return _PreviewSection(
      scrollable: !_isVideoExt(ext),
      content: content,
    );
  }

  Widget _buildPreviewContent(ViewerState state, FieldPreview preview, {bool isWds = false}) {
    final ext = preview.guessedExt?.toLowerCase() ?? '';
    final showAudio = _isAudioExt(ext);
    final showImage = _isImageExt(ext);
    final content = showAudio
        ? _buildAudioPreview(state, preview, isWds: isWds)
        : showImage
            ? _buildImagePreview(state, preview, isWds: isWds)
            : _buildPreviewBody(preview);

    return _PreviewSection(content: content);
  }

  Widget? _buildPreviewActions(ViewerState state) {
    if (state.mode == ViewerMode.huggingface) {
      final preview = state.hfPreview;
      if (preview == null || state.hfSelectedRowIndex == null || state.hfSelectedFieldName == null) {
        return null;
      }
      final rowOffset = state.hfSelectedRowIndex! - preview.offset;
      if (rowOffset < 0 || rowOffset >= preview.rows.length) return null;
      final row = preview.rows[rowOffset];
      if (row is! Map<String, dynamic>) return null;
      final value = row[state.hfSelectedFieldName];
      final text = const JsonEncoder.withIndent('  ').convert(value);
      return _buildActionsRow(
        copyValue: text,
        onOpen: () => _openHfSelectedField(state, preview),
      );
    }

    if (state.mode == ViewerMode.zenodo) {
      final previewFuture = state.zenodoEntryPreviewFuture ?? state.zenodoFilePreviewFuture;
      if (previewFuture == null) return null;
      final inlineFuture = state.zenodoInlineMediaFuture;
      return FutureBuilder<FieldPreview>(
        future: previewFuture,
        builder: (context, snapshot) {
          if (!snapshot.hasData) return const SizedBox.shrink();
          final preview = snapshot.data!;
          if (inlineFuture != null) {
            return FutureBuilder<InlineMediaResponse>(
              future: inlineFuture,
              builder: (context, inlineSnapshot) {
                final copyValue = inlineSnapshot.data?.base64 ?? preview.previewText;
                return _buildActionsRow(
                  copyValue: copyValue,
                  onOpen: () => _openSelectedField(state, preview),
                );
              },
            );
          }
          return _buildActionsRow(
            copyValue: preview.previewText,
            onOpen: () => _openSelectedField(state, preview),
          );
        },
      );
    }

    Future<FieldPreview>? future;
    if (state.mode == ViewerMode.webdatasetDir) {
      future = state.wdsPreviewFuture;
    } else if (state.mode == ViewerMode.mdsIndex) {
      future = state.mdsFieldPreviewFuture;
    } else {
      future = state.fieldPreviewFuture;
    }
    if (future == null) return null;
    return FutureBuilder<FieldPreview>(
      future: future,
      builder: (context, snapshot) {
        if (!snapshot.hasData) return const SizedBox.shrink();
        final preview = snapshot.data!;
        return _buildActionsRow(
          copyValue: preview.previewText,
          onOpen: () => _openSelectedField(state, preview),
        );
      },
    );
  }

  Widget _buildActionsRow({String? copyValue, Future<void> Function()? onOpen}) {
    final actions = <Widget>[];
    if (copyValue != null) {
      actions.add(CopyButton(value: copyValue));
    }
    if (onOpen != null) {
      actions.add(
        IconButton(
          tooltip: 'Open',
          icon: const Icon(Icons.open_in_new),
          onPressed: () async => onOpen(),
          constraints: const BoxConstraints.tightFor(width: 32, height: 32),
          padding: EdgeInsets.zero,
          visualDensity: VisualDensity.compact,
          iconSize: 18,
        ),
      );
    }
    if (actions.isEmpty) return const SizedBox.shrink();
    return Wrap(
      spacing: 8,
      runSpacing: 4,
      alignment: WrapAlignment.end,
      children: actions,
    );
  }

  Widget _buildAudioPreview(ViewerState state, FieldPreview preview, {required bool isWds}) {
    return AudioPreview(
      label: 'Audio preview',
      loader: () => _prepareAudioPreview(state, preview, isWds: isWds),
    );
  }

  Widget _buildImagePreview(ViewerState state, FieldPreview preview, {required bool isWds}) {
    return FutureBuilder<PreparedFileResponse>(
      future: _prepareImagePreview(state, preview, isWds: isWds),
      builder: (context, snapshot) {
        if (!snapshot.hasData) {
          return _buildPreviewBody(preview);
        }
        return ClipRRect(
          borderRadius: BorderRadius.circular(16),
          child: Image.file(
            File(snapshot.data!.path),
            fit: BoxFit.contain,
          ),
        );
      },
    );
  }

  Future<PreparedMediaResponse> _prepareAudioPreview(ViewerState state, FieldPreview preview,
      {required bool isWds}) async {
    if (!_isAudioExt(preview.guessedExt ?? '')) {
      throw Exception('Not audio');
    }
    if (state.mode == ViewerMode.zenodo) {
      if (state.zenodoSelectedEntryName != null) {
        final media = await state.zenodoPrepareSelectedEntryMedia();
        return _normalizeAudioPreview(media);
      }
      final media = await state.zenodoPrepareSelectedFileMedia();
      return _normalizeAudioPreview(media);
    }
    if (isWds &&
        state.wdsDirSummary != null &&
        state.selectedShardName != null &&
        state.wdsSelectedMemberPath != null) {
      return state.webdatasetPrepareAudio(
        dirPath: state.wdsDirSummary!.dirPath,
        shardFilename: state.selectedShardName!,
        memberPath: state.wdsSelectedMemberPath!,
      );
    }
    if (state.mode == ViewerMode.mdsIndex && state.indexSummary != null) {
      return state.mosaicmlPrepareAudio(
        indexPath: state.indexSummary!.indexPath,
        shardFilename: state.selectedChunkName!,
        itemIndex: state.selectedItemIndex!,
        fieldIndex: state.selectedFieldIndex!,
      );
    }
    if (state.indexSummary != null) {
      return state.litdataPrepareAudio(
        indexPath: state.indexSummary!.indexPath,
        chunkFilename: state.selectedChunkName!,
        itemIndex: state.selectedItemIndex!,
        fieldIndex: state.selectedFieldIndex!,
      );
    }
    throw Exception('No audio preview');
  }

  Future<PreparedMediaResponse> _normalizeAudioPreview(PreparedMediaResponse media) async {
    final ext = media.ext.trim().toLowerCase();
    final cleaned = ext.startsWith('.') ? ext.substring(1) : ext;
    if (cleaned != 'sph') return media;
    final wavBytes = await decodeSphereToWavWithFallback(media.bytes);
    return PreparedMediaResponse(bytes: wavBytes, size: wavBytes.length, ext: 'wav');
  }

  Future<PreparedFileResponse> _prepareImagePreview(ViewerState state, FieldPreview preview,
      {required bool isWds}) async {
    if (!_isImageExt(preview.guessedExt ?? '')) {
      throw Exception('Not image');
    }
    if (isWds &&
        state.wdsDirSummary != null &&
        state.selectedShardName != null &&
        state.wdsSelectedMemberPath != null) {
      return state.webdatasetPrepareFile(
        dirPath: state.wdsDirSummary!.dirPath,
        shardFilename: state.selectedShardName!,
        memberPath: state.wdsSelectedMemberPath!,
      );
    }
    if (state.mode == ViewerMode.mdsIndex && state.indexSummary != null) {
      return state.mosaicmlPrepareFile(
        indexPath: state.indexSummary!.indexPath,
        shardFilename: state.selectedChunkName!,
        itemIndex: state.selectedItemIndex!,
        fieldIndex: state.selectedFieldIndex!,
      );
    }
    if (state.indexSummary != null) {
      return state.litdataPrepareFile(
        indexPath: state.indexSummary!.indexPath,
        chunkFilename: state.selectedChunkName!,
        itemIndex: state.selectedItemIndex!,
        fieldIndex: state.selectedFieldIndex!,
      );
    }
    throw Exception('No image preview');
  }

  String _formatFieldMetaInline({
    required String ext,
    required String type,
    required int? size,
  }) {
    final sizeLabel = size == null ? '-' : _formatBytes(size);
    return '$ext · $type · $sizeLabel';
  }

  List<String> _formatByFieldIndex(ViewerState state) {
    if (state.mode == ViewerMode.mdsIndex) {
      final raw = state.indexSummary?.configRaw['columnEncodings'];
      if (raw is List) {
        return raw.map((value) => value.toString()).toList();
      }
    }
    return state.indexSummary?.dataFormat ?? <String>[];
  }

  Map<String, int> _hfFieldSizeMap(ViewerState state) {
    final preview = state.hfPreview;
    if (preview == null || state.hfSelectedRowIndex == null) {
      return const <String, int>{};
    }
    final rowOffset = state.hfSelectedRowIndex! - preview.offset;
    if (rowOffset < 0 || rowOffset >= preview.rows.length) {
      return const <String, int>{};
    }
    final row = preview.rows[rowOffset];
    if (row is! Map<String, dynamic>) {
      return const <String, int>{};
    }
    final map = <String, int>{};
    for (final entry in row.entries) {
      try {
        final text = const JsonEncoder().convert(entry.value);
        map[entry.key] = utf8.encode(text).length;
      } catch (_) {
        map[entry.key] = entry.value.toString().length;
      }
    }
    return map;
  }

  String _fieldMetaFromFormat(String? format, int? size) {
    final ext = _extFromFormat(format);
    final type = _typeFromFormat(format);
    return _formatFieldMetaInline(ext: ext, type: type, size: size);
  }

  String _fieldMetaFromPreview(FieldPreview preview, int? size) {
    final ext = _normalizeExtLabel(preview.guessedExt);
    final type = _typeFromExt(ext);
    return _formatFieldMetaInline(ext: ext, type: type, size: size ?? preview.size);
  }

  String _fieldMetaFromPath(String path, int? size) {
    final ext = _extFromPath(path);
    final type = _typeFromExt(ext);
    return _formatFieldMetaInline(ext: ext, type: type, size: size);
  }

  Widget _buildPreviewBody(FieldPreview preview) {
    final text = preview.isBinary
        ? 'Hex: ${preview.hexSnippet}'
        : (preview.previewText ?? '');
    return _CodeBlock(text: text);
  }

  Future<void> _openSelectedField(ViewerState state, FieldPreview preview) async {
    try {
      final ext = preview.guessedExt ?? '';
      final preferredOpener = ext.isNotEmpty ? await state.preferredOpenerForExt(ext) : null;
      if (state.mode == ViewerMode.mdsIndex && state.indexSummary != null) {
        var response = await state.mosaicmlOpenField(openerAppPath: preferredOpener);
        response = await _handleOpenerFallback(state, response, ext, (appPath) {
          return state.mosaicmlOpenField(openerAppPath: appPath);
        });
        state.setStatusMessage(response.message);
        return;
      }
      if (state.mode == ViewerMode.webdatasetDir && state.wdsDirSummary != null) {
        var response = await state.webdatasetOpenMember(openerAppPath: preferredOpener);
        response = await _handleOpenerFallback(state, response, ext, (appPath) {
          return state.webdatasetOpenMember(openerAppPath: appPath);
        });
        state.setStatusMessage(response.message);
        return;
      }
      if (state.indexSummary != null) {
        var response = await state.litdataOpenField(openerAppPath: preferredOpener);
        response = await _handleOpenerFallback(state, response, ext, (appPath) {
          return state.litdataOpenField(openerAppPath: appPath);
        });
        state.setStatusMessage(response.message);
        return;
      }
      if (state.mode == ViewerMode.zenodo && state.zenodoRecord != null) {
        final record = state.zenodoRecord!;
        final file = record.files.firstWhere(
          (f) => f.key == state.zenodoSelectedFileKey,
          orElse: () => record.files.first,
        );
        if (state.zenodoSelectedEntryName != null) {
          final entryName = state.zenodoSelectedEntryName!;
          final lower = file.key.toLowerCase();
          var response = lower.endsWith('.zip')
              ? await state.zenodoZipOpenEntry(
                  contentUrl: file.contentUrl,
                  filename: file.key,
                  entryName: entryName,
                  openerAppPath: preferredOpener,
                )
              : await state.zenodoTarOpenEntry(
                  contentUrl: file.contentUrl,
                  filename: file.key,
                  entryName: entryName,
                  openerAppPath: preferredOpener,
                );
          response = await _handleOpenerFallback(state, response, ext, (appPath) {
            return lower.endsWith('.zip')
                ? state.zenodoZipOpenEntry(
                    contentUrl: file.contentUrl,
                    filename: file.key,
                    entryName: entryName,
                    openerAppPath: appPath,
                  )
                : state.zenodoTarOpenEntry(
                    contentUrl: file.contentUrl,
                    filename: file.key,
                    entryName: entryName,
                    openerAppPath: appPath,
                  );
          });
          state.setStatusMessage(response.message);
          return;
        }
        var response = await state.zenodoOpenFile(
          contentUrl: file.contentUrl,
          filename: file.key,
          openerAppPath: preferredOpener,
        );
        response = await _handleOpenerFallback(state, response, ext, (appPath) {
          return state.zenodoOpenFile(
            contentUrl: file.contentUrl,
            filename: file.key,
            openerAppPath: appPath,
          );
        });
        state.setStatusMessage(response.message);
        return;
      }
    } catch (err) {
      state.setStatusMessage(err.toString());
    }
  }

  Future<void> _openHfSelectedField(ViewerState state, HfDatasetPreview preview) async {
    if (state.hfSelectedRowIndex == null || state.hfSelectedFieldName == null) return;
    try {
      var open = await state.huggingfaceOpenField(
        input: state.sourceInput,
        config: preview.config,
        split: preview.split,
        rowIndex: state.hfSelectedRowIndex!,
        fieldName: state.hfSelectedFieldName!,
      );
      open = await _handleOpenerFallback(state, open, open.ext, (appPath) {
        return state.huggingfaceOpenField(
          input: state.sourceInput,
          config: preview.config,
          split: preview.split,
          rowIndex: state.hfSelectedRowIndex!,
          fieldName: state.hfSelectedFieldName!,
          openerAppPath: appPath,
        );
      });
      state.setStatusMessage(open.message);
    } catch (err) {
      state.setStatusMessage(err.toString());
    }
  }

  Future<OpenLeafResponse> _handleOpenerFallback(
    ViewerState state,
    OpenLeafResponse response,
    String ext,
    Future<OpenLeafResponse> Function(String appPath) retry,
  ) async {
    if (!response.needsOpener) return response;
    final picked = await state.chooseOpenerApp();
    if (picked == null || picked.trim().isEmpty) return response;
    await state.savePreferredOpenerForExt(ext, picked);
    return retry(picked);
  }

  Widget _buildStatusBar(String message) {
    final scheme = Theme.of(context).colorScheme;
    return Padding(
      padding: const EdgeInsets.only(top: 12),
      child: Container(
        width: double.infinity,
        padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 10),
        decoration: BoxDecoration(
          color: scheme.surfaceContainerHighest,
          borderRadius: BorderRadius.circular(12),
          border: Border.all(color: scheme.outlineVariant),
        ),
        child: Text(message, style: Theme.of(context).textTheme.bodySmall),
      ),
    );
  }

  String _normalizeExtLabel(String? ext) {
    if (ext == null) return 'unknown';
    final trimmed = ext.trim().toLowerCase();
    if (trimmed.isEmpty || trimmed == 'unknown') return 'unknown';
    if (trimmed.startsWith('.')) return trimmed;
    return '.${trimmed}';
  }

  String _extFromFormat(String? format) {
    if (format == null || format.trim().isEmpty) return 'unknown';
    final normalized = format.trim().toLowerCase();
    final colonParts = normalized.split(':');
    if (colonParts.length > 1) {
      final candidate = colonParts.last.trim();
      final ext = _extFromToken(candidate);
      if (ext != 'unknown') return ext;
    }
    final dotParts = normalized.split('.');
    if (dotParts.length > 1) {
      final candidate = dotParts.last.trim();
      final ext = _extFromToken(candidate);
      if (ext != 'unknown') return ext;
    }
    final ext = _extFromToken(normalized);
    if (ext != 'unknown') return ext;
    if (normalized.contains('image')) return '.png';
    if (normalized.contains('audio')) return '.wav';
    if (normalized.contains('video')) return '.mp4';
    return 'unknown';
  }

  String _extFromToken(String token) {
    final cleaned = token.trim().toLowerCase();
    const map = {
      'jpeg': 'jpg',
      'jpg': 'jpg',
      'png': 'png',
      'tiff': 'tiff',
      'bmp': 'bmp',
      'gif': 'gif',
      'webp': 'webp',
      'svg': 'svg',
      'pil': 'png',
      'str': 'txt',
      'string': 'txt',
      'text': 'txt',
      'json': 'json',
      'int': 'txt',
      'int32': 'txt',
      'int64': 'txt',
      'float': 'txt',
      'float32': 'txt',
      'float64': 'txt',
      'double': 'txt',
      'bool': 'txt',
      'bytes': 'bin',
      'bin': 'bin',
      'audio': 'wav',
      'wav': 'wav',
      'mp3': 'mp3',
      'flac': 'flac',
      'm4a': 'm4a',
      'ogg': 'ogg',
      'opus': 'opus',
      'aac': 'aac',
      'sph': 'sph',
      'mp4': 'mp4',
      'webm': 'webm',
      'mov': 'mov',
      'video': 'mp4',
    };
    if (map.containsKey(cleaned)) {
      return _normalizeExtLabel(map[cleaned]);
    }
    if (_isImageExt(cleaned) || _isAudioExt(cleaned)) {
      return _normalizeExtLabel(cleaned);
    }
    return 'unknown';
  }

  String _typeFromFormat(String? format) {
    if (format == null || format.trim().isEmpty) return 'unknown';
    final normalized = format.trim().toLowerCase();
    final primary = normalized.split(':').first.split('.').first;
    if (primary.isEmpty) return 'unknown';
    if (_isImageExt(primary) || primary.contains('image')) return 'image';
    if (_isAudioExt(primary) || primary.contains('audio')) return 'audio';
    if (_isVideoExt(primary) || primary.contains('video')) return 'video';
    if (primary == 'bytes' || primary == 'bin') return 'binary';
    if (primary == 'str') return 'string';
    return primary;
  }

  String _extFromPath(String path) {
    final name = path.split('/').last;
    final dotIndex = name.lastIndexOf('.');
    if (dotIndex <= 0 || dotIndex == name.length - 1) {
      return 'unknown';
    }
    return _normalizeExtLabel(name.substring(dotIndex + 1));
  }

  String _typeFromExt(String ext) {
    final cleaned = ext.replaceFirst('.', '').toLowerCase();
    if (cleaned.isEmpty || cleaned == 'unknown') return 'unknown';
    if (_isImageExt(cleaned)) return 'image';
    if (_isAudioExt(cleaned)) return 'audio';
    if (_isVideoExt(cleaned)) return 'video';
    if (cleaned == 'txt' || cleaned == 'json' || cleaned == 'csv' || cleaned == 'tsv' ||
        cleaned == 'md' || cleaned == 'yaml' || cleaned == 'yml') {
      return 'text';
    }
    return 'binary';
  }

  bool _isImageExt(String ext) {
    switch (ext.toLowerCase()) {
      case 'png':
      case 'jpg':
      case 'jpeg':
      case 'gif':
      case 'webp':
      case 'bmp':
      case 'svg':
        return true;
      default:
        return false;
    }
  }

  bool _isAudioExt(String ext) {
    switch (ext.toLowerCase()) {
      case 'wav':
      case 'mp3':
      case 'flac':
      case 'm4a':
      case 'ogg':
      case 'opus':
      case 'aac':
      case 'sph':
        return true;
      default:
        return false;
    }
  }

  bool _isVideoExt(String ext) {
    switch (ext.toLowerCase()) {
      case 'mp4':
      case 'webm':
      case 'mov':
        return true;
      default:
        return false;
    }
  }

  String _formatBytes(int value) {
    if (value <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var v = value.toDouble();
    var idx = 0;
    while (v >= 1024 && idx < units.length - 1) {
      v /= 1024;
      idx += 1;
    }
    final decimals = v >= 10 || v < 1 ? 0 : 1;
    return '${v.toStringAsFixed(decimals)} ${units[idx]}';
  }
}

class _PanelCard extends StatelessWidget {
  const _PanelCard({
    required this.title,
    required this.child,
    this.subtitle,
    this.trailing,
    this.subtitleTrailing,
    this.expandBody = true,
    this.bodyFlexible = false,
  });

  final String title;
  final String? subtitle;
  final Widget? trailing;
  final Widget? subtitleTrailing;
  final Widget child;
  final bool expandBody;
  final bool bodyFlexible;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    final textTheme = Theme.of(context).textTheme;
    return Container(
      decoration: BoxDecoration(
        color: scheme.surface,
        borderRadius: BorderRadius.circular(20),
        border: Border.all(color: scheme.outlineVariant),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 16,
            offset: const Offset(0, 10),
          ),
        ],
      ),
      child: Column(
        mainAxisSize: expandBody ? MainAxisSize.max : MainAxisSize.min,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Padding(
            padding: const EdgeInsets.fromLTRB(16, 16, 16, 12),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Expanded(
                      child: Text(
                        title,
                        style: textTheme.titleMedium?.copyWith(fontWeight: FontWeight.w700),
                      ),
                    ),
                    if (trailing != null) trailing!,
                  ],
                ),
                if (subtitle != null || subtitleTrailing != null) ...[
                  const SizedBox(height: 6),
                  Row(
                    children: [
                      Expanded(
                        child: subtitle == null
                            ? const SizedBox.shrink()
                            : Text(
                                subtitle!,
                                style: textTheme.bodySmall?.copyWith(
                                  color: scheme.onSurface.withOpacity(0.6),
                                ),
                              ),
                      ),
                      if (subtitleTrailing != null) subtitleTrailing!,
                    ],
                  ),
                ],
              ],
            ),
          ),
          Divider(height: 1, color: scheme.outlineVariant),
          if (expandBody)
            Expanded(
              child: Padding(
                padding: const EdgeInsets.all(12),
                child: child,
              ),
            )
          else
            if (bodyFlexible)
              Flexible(
                fit: FlexFit.loose,
                child: Padding(
                  padding: const EdgeInsets.all(12),
                  child: child,
                ),
              )
            else
              Padding(
                padding: const EdgeInsets.all(12),
                child: child,
              ),
        ],
      ),
    );
  }
}

class _LoadingList extends StatelessWidget {
  const _LoadingList();

  @override
  Widget build(BuildContext context) {
    return ListView.separated(
      itemCount: 6,
      separatorBuilder: (_, __) => const SizedBox(height: 6),
      itemBuilder: (context, index) {
        return const Padding(
          padding: EdgeInsets.symmetric(horizontal: 6),
          child: Skeleton(height: 48),
        );
      },
    );
  }
}

class _GlowBlob extends StatelessWidget {
  const _GlowBlob({required this.color, required this.size});

  final Color color;
  final double size;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: size,
      height: size,
      decoration: BoxDecoration(
        shape: BoxShape.circle,
        gradient: RadialGradient(
          colors: [
            color,
            color.withOpacity(0.0),
          ],
        ),
      ),
    );
  }
}

class _CodeBlock extends StatelessWidget {
  const _CodeBlock({required this.text});

  final String text;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: scheme.surfaceContainerLowest,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: scheme.outlineVariant),
      ),
      child: SelectableText(
        text,
        style: Theme.of(context).textTheme.bodySmall?.copyWith(
              fontFamily: 'JetBrainsMono',
              height: 1.4,
            ),
      ),
    );
  }
}

class _LoadingPreview extends StatelessWidget {
  const _LoadingPreview();

  @override
  Widget build(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: const [
        Skeleton(height: 20, width: 140),
        SizedBox(height: 12),
        Skeleton(height: 16, width: double.infinity),
        SizedBox(height: 8),
        Skeleton(height: 16, width: double.infinity),
        SizedBox(height: 8),
        Skeleton(height: 16, width: 240),
      ],
    );
  }
}

class _PreviewSection extends StatefulWidget {
  const _PreviewSection({
    this.header,
    required this.content,
    this.scrollable = true,
  });

  final Widget? header;
  final Widget content;
  final bool scrollable;

  @override
  State<_PreviewSection> createState() => _PreviewSectionState();
}

class _PreviewSectionState extends State<_PreviewSection> {
  late final ScrollController _controller;

  @override
  void initState() {
    super.initState();
    _controller = ScrollController();
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final header = widget.header;
    if (!widget.scrollable) {
      return LayoutBuilder(
        builder: (context, constraints) {
          final bounded = constraints.maxHeight.isFinite && constraints.maxHeight > 0;
          final headerWidgets = [
            if (header != null) header,
            if (header != null) const SizedBox(height: 12),
          ];
          if (!bounded) {
            return Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              mainAxisSize: MainAxisSize.min,
              children: [
                ...headerWidgets,
                widget.content,
              ],
            );
          }
          return SizedBox(
            height: constraints.maxHeight,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                ...headerWidgets,
                Expanded(child: widget.content),
              ],
            ),
          );
        },
      );
    }
    return Scrollbar(
      controller: _controller,
      thumbVisibility: true,
      child: SingleChildScrollView(
        controller: _controller,
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            if (header != null) header,
            if (header != null) const SizedBox(height: 12),
            widget.content,
          ],
        ),
      ),
    );
  }
}

class _HfConfigSplitOption {
  const _HfConfigSplitOption({required this.config, required this.split});

  final String config;
  final String split;
}
