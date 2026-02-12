import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:google_fonts/google_fonts.dart';
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

const double _kPaneHeaderHeight = 36;

class InspectorScreen extends StatefulWidget {
  const InspectorScreen({super.key});

  @override
  State<InspectorScreen> createState() => _InspectorScreenState();
}

class _InspectorScreenState extends State<InspectorScreen> {
  late final TextEditingController _hfOffsetController;
  late final FocusNode _hfOffsetFocus;

  @override
  void initState() {
    super.initState();
    _hfOffsetController = TextEditingController();
    _hfOffsetFocus = FocusNode();
  }

  @override
  void dispose() {
    _hfOffsetController.dispose();
    _hfOffsetFocus.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Consumer<ViewerState>(
      builder: (context, state, _) {
        return Scaffold(
          body: SafeArea(
            child: Stack(
              children: [
                _buildBackdrop(context),
                Padding(
                  padding: const EdgeInsets.fromLTRB(12, 8, 12, 12),
                  child: Column(
                    children: [
                      _buildTopBar(context, state),
                      const SizedBox(height: 4),
                      Expanded(child: _buildPanels(context, state)),
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
    return Container(color: const Color(0xFFF5F5F5));
  }

  Widget _buildPanels(BuildContext context, ViewerState state) {
    final scheme = Theme.of(context).colorScheme;
    return Row(
      children: [
        SizedBox(width: 340, child: _buildSourcesPane(state)),
        const SizedBox(width: 8),
        Expanded(
          child: Container(
            decoration: BoxDecoration(
              color: Colors.white,
              borderRadius: BorderRadius.circular(8),
              border: Border.all(color: const Color(0xFFE0E0E0)),
            ),
            child: LayoutBuilder(
              builder: (context, outerConstraints) {
                return Row(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: [
                    Expanded(
                      flex: 1,
                      child: _buildInlinePane(
                        title: 'Items',
                        subtitle: _itemsSubtitle(state),
                        child: _buildItemsContent(state),
                      ),
                    ),
                    VerticalDivider(width: 1, thickness: 1, color: scheme.outlineVariant),
                    Expanded(
                      flex: 1,
                      child: LayoutBuilder(
                        builder: (context, constraints) {
                          final maxPreviewHeight =
                              _previewMaxHeightForState(state, constraints.maxHeight);
                          return Column(
                            children: [
                              Expanded(
                                child: _buildInlinePane(
                                  title: 'Fields',
                                  subtitle: _fieldsSubtitle(state),
                                  child: _buildFieldsContent(state),
                                ),
                              ),
                              Divider(height: 1, thickness: 1, color: scheme.outlineVariant),
                              AnimatedSize(
                                duration: const Duration(milliseconds: 240),
                                curve: Curves.easeOutCubic,
                                alignment: Alignment.topCenter,
                                child: ConstrainedBox(
                                  constraints: BoxConstraints(maxHeight: maxPreviewHeight),
                                  child: _buildInlinePane(
                                    title: 'Preview',
                                    subtitle: _previewSubtitle(state),
                                    subtitleTrailing: _buildPreviewActions(state),
                                    expand: false,
                                    flexible: true,
                                    child: _buildPreviewContent2(state),
                                  ),
                                ),
                              ),
                            ],
                          );
                        },
                      ),
                    ),
                  ],
                );
              },
            ),
          ),
        ),
      ],
    );
  }

  Widget _buildInlinePane({
    required String title,
    required Widget child,
    String? subtitle,
    Widget? subtitleTrailing,
    bool expand = true,
    bool flexible = false,
  }) {
    final scheme = Theme.of(context).colorScheme;
    final textTheme = Theme.of(context).textTheme;
    return Column(
      mainAxisSize: expand ? MainAxisSize.max : MainAxisSize.min,
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        SizedBox(
          height: _kPaneHeaderHeight,
          child: Padding(
            padding: const EdgeInsets.symmetric(horizontal: 12),
            child: Row(
              children: [
                Text(
                  title,
                  style: textTheme.labelLarge
                      ?.copyWith(fontWeight: FontWeight.w700),
                ),
                if (subtitle != null) ...[
                  const SizedBox(width: 8),
                  Expanded(
                    child: Text(
                      subtitle,
                      style: textTheme.labelSmall?.copyWith(
                        color: scheme.onSurface.withOpacity(0.5),
                      ),
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                    ),
                  ),
                ] else
                  const Spacer(),
                if (subtitleTrailing != null) subtitleTrailing,
              ],
            ),
          ),
        ),
        Divider(height: 1, color: scheme.outlineVariant),
        if (expand)
          Expanded(
            child: Padding(
              padding: const EdgeInsets.all(12),
              child: child,
            ),
          )
        else if (flexible)
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
      if (entry == null && fileKey != null && _isVideoPath(fileKey))
        return true;
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
    final hasToken = state.hfToken != null && state.hfToken!.isNotEmpty;

    final isDesktop = !kIsWeb &&
        (defaultTargetPlatform == TargetPlatform.macOS ||
            defaultTargetPlatform == TargetPlatform.windows ||
            defaultTargetPlatform == TargetPlatform.linux);
    final isMacOS = !kIsWeb && defaultTargetPlatform == TargetPlatform.macOS;

    Widget titleContent = Row(
      children: [
        Text(
          'Dataset Inspector',
          style: textTheme.labelMedium?.copyWith(fontWeight: FontWeight.w600),
        ),
        if (state.statusMessage != null && state.statusMessage!.isNotEmpty) ...[
          const SizedBox(width: 12),
          Expanded(
            child: Text(
              state.statusMessage!,
              style: textTheme.labelSmall?.copyWith(
                color: scheme.onSurface.withOpacity(0.5),
              ),
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
            ),
          ),
        ] else
          const Spacer(),
      ],
    );

    return SizedBox(
      height: 28,
      child: Row(
        children: [
          if (isMacOS) const SizedBox(width: 64),
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
          _buildUpdateButton(state),
          const SizedBox(width: 4),
          Tooltip(
            message: hasToken
                ? 'HF token configured'
                : 'Set Hugging Face token',
            child: IconButton(
              onPressed: () => _showHfTokenDialog(context, state),
              icon: Icon(
                Icons.vpn_key,
                size: 18,
                color: hasToken
                    ? scheme.primary
                    : scheme.onSurface.withOpacity(0.4),
              ),
              constraints: const BoxConstraints.tightFor(width: 32, height: 32),
              padding: EdgeInsets.zero,
              visualDensity: VisualDensity.compact,
            ),
          ),
        ],
      ),
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

  Future<void> _showHfTokenDialog(
      BuildContext context, ViewerState state) async {
    final controller = TextEditingController(text: state.hfToken ?? '');
    await showDialog<void>(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          backgroundColor: Colors.white,
          title: const Text('Hugging Face token'),
          content: TextField(
            controller: controller,
            decoration: const InputDecoration(
                hintText: 'Paste your HF token (optional)',
                fillColor: Colors.white),
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

  Future<void> _showAddDatasetDialog(
      BuildContext context, ViewerState state) async {
    final controller = TextEditingController();
    final recentSources = state.recentSources.take(8).toList();
    await showDialog<void>(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          backgroundColor: Colors.white,
          title: const Text('Open Dataset'),
          content: SizedBox(
            width: 520,
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                TextField(
                  controller: controller,
                  autofocus: true,
                  decoration: InputDecoration(
                    hintText: 'Path, URL, or HF dataset name',
                    fillColor: Colors.white,
                    prefixIcon: const Icon(Icons.link),
                    suffixIcon: IconButton(
                      tooltip: 'Browse folder...',
                      icon: const Icon(Icons.folder_open, size: 20),
                      onPressed: () async {
                        final result = await FilePicker.getDirectoryPath();
                        if (result == null || result.trim().isEmpty) return;
                        if (dialogContext.mounted) {
                          Navigator.of(dialogContext).pop();
                        }
                        await state.addSource(result.trim());
                      },
                    ),
                  ),
                  onSubmitted: (_) async {
                    final input = controller.text.trim();
                    if (input.isEmpty) return;
                    await _smartAddSource(state, input);
                    if (dialogContext.mounted) {
                      Navigator.of(dialogContext).pop();
                    }
                  },
                ),
                if (recentSources.isNotEmpty) ...[
                  const SizedBox(height: 8),
                  Text(
                    'Recent',
                    style: Theme.of(context).textTheme.labelMedium,
                  ),
                  const SizedBox(height: 8),
                  ...recentSources.map((source) => InkWell(
                    onTap: () async {
                      Navigator.of(dialogContext).pop();
                      await _smartAddSource(state, source);
                    },
                    borderRadius: BorderRadius.circular(6),
                    child: Padding(
                      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 6),
                      child: Text(
                        source,
                        style: Theme.of(context).textTheme.bodySmall,
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                      ),
                    ),
                  )),
                ],
              ],
            ),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(),
              child: const Text('Cancel'),
            ),
            FilledButton.icon(
              onPressed: () async {
                final input = controller.text.trim();
                if (input.isEmpty) return;
                await _smartAddSource(state, input);
                if (dialogContext.mounted) {
                  Navigator.of(dialogContext).pop();
                }
              },
              icon: const Icon(Icons.add),
              label: const Text('Open'),
            ),
          ],
        );
      },
    );
  }

  Future<void> _smartAddSource(ViewerState state, String input) async {
    final trimmed = input.trim();
    if (trimmed.isEmpty) return;
    await state.addSource(trimmed);
  }

  Widget _buildSourcesPane(ViewerState state) {
    final scheme = Theme.of(context).colorScheme;
    final textTheme = Theme.of(context).textTheme;
    final scanning = state.scanningDatasets;
    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: const Color(0xFFE0E0E0)),
      ),
      child: Column(
        children: [
          SizedBox(
            height: _kPaneHeaderHeight,
            child: Padding(
              padding: const EdgeInsets.symmetric(horizontal: 12),
              child: Row(
                children: [
                  Text(
                    'Explorer',
                    style: textTheme.labelLarge
                        ?.copyWith(fontWeight: FontWeight.w700),
                  ),
                  if (scanning) ...[
                    const SizedBox(width: 8),
                    SizedBox(
                      width: 12, height: 12,
                      child: CircularProgressIndicator(
                        strokeWidth: 2,
                        color: scheme.primary,
                      ),
                    ),
                    const SizedBox(width: 6),
                    Expanded(
                      child: Text(
                        'Scanning... ${state.scanAddedCount} added',
                        style: textTheme.labelSmall?.copyWith(
                          color: scheme.onSurface.withOpacity(0.5),
                        ),
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                      ),
                    ),
                    IconButton(
                      onPressed: state.cancelDatasetScan,
                      icon: const Icon(Icons.close, size: 14),
                      constraints: const BoxConstraints.tightFor(width: 22, height: 22),
                      padding: EdgeInsets.zero,
                      visualDensity: VisualDensity.compact,
                    ),
                  ] else
                    const Spacer(),
                ],
              ),
            ),
          ),
          Divider(height: 1, color: scheme.outlineVariant),
          Expanded(child: _buildDatasetTree(state)),
          Divider(height: 1, color: scheme.outlineVariant),
          Padding(
            padding: const EdgeInsets.all(6),
            child: SizedBox(
              width: double.infinity,
              height: 28,
              child: TextButton.icon(
                onPressed: () => _showAddDatasetDialog(context, state),
                icon: const Icon(Icons.add, size: 16),
                label: const Text('Add Dataset'),
                style: TextButton.styleFrom(
                  textStyle: const TextStyle(fontSize: 12),
                  padding: const EdgeInsets.symmetric(horizontal: 8),
                ),
              ),
            ),
          ),
        ],
      ),
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
      options.add(
          _HfConfigSplitOption(config: preview.config, split: preview.split));
    }
    return options;
  }

  Widget _buildDatasetTree(ViewerState state) {
    if (state.openedDatasets.isEmpty) {
      return Center(
        child: Text(
          'No datasets',
          style: Theme.of(context).textTheme.bodySmall?.copyWith(
                color: Theme.of(context).colorScheme.onSurface.withOpacity(0.4),
              ),
        ),
      );
    }
    final rows = <Widget>[];
    for (final dataset in state.openedDatasets) {
      rows.add(_buildDatasetRootTile(state, dataset));
      if (dataset.expanded) {
        final children = _buildDatasetChildTiles(state, dataset);
        if (children.isEmpty) {
          final isLoading = _isDatasetPendingData(dataset);
          rows.add(
            _buildDatasetChildTile(
              selected: false,
              onTap: null,
              indent: 20,
              icon: isLoading ? Icons.hourglass_empty : Icons.info_outline,
              title: isLoading
                  ? Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        SizedBox(
                          width: 12,
                          height: 12,
                          child: CircularProgressIndicator(
                            strokeWidth: 1.5,
                            color: Theme.of(context)
                                .colorScheme
                                .onSurface
                                .withOpacity(0.4),
                          ),
                        ),
                        const SizedBox(width: 8),
                        const Text('Loading...'),
                      ],
                    )
                  : const Text('No entries'),
            ),
          );
        } else {
          rows.addAll(children);
        }
      }
    }
    return ListView.builder(
      itemCount: rows.length,
      itemBuilder: (context, index) => rows[index],
    );
  }

  /// Returns true if the dataset's primary data hasn't been loaded yet.
  /// When data loads (or fails), the fallback sets a non-null empty object,
  /// so null means "still waiting for the response".
  bool _isDatasetPendingData(LoadedDatasetSource dataset) {
    switch (dataset.mode) {
      case ViewerMode.huggingface:
        return dataset.hfPreview == null && dataset.hfConfigOptions == null;
      case ViewerMode.webdatasetDir:
        return dataset.wdsDirSummary == null;
      case ViewerMode.zenodo:
        return dataset.zenodoRecord == null;
      case ViewerMode.litdataIndex:
      case ViewerMode.litdataChunks:
      case ViewerMode.mdsIndex:
        return dataset.indexSummary == null;
    }
  }

  Widget _buildDatasetRootTile(ViewerState state, LoadedDatasetSource dataset) {
    final scheme = Theme.of(context).colorScheme;
    final selected = state.isDatasetActive(dataset.id);
    final leadingIcon = _datasetModeIcon(dataset.mode);
    final bg = selected
        ? scheme.secondary.withOpacity(0.12)
        : Colors.transparent;
    return MouseRegion(
      cursor: SystemMouseCursors.click,
      child: GestureDetector(
        onTap: () {
          state.toggleDatasetExpanded(dataset.id);
          unawaited(state.activateDataset(dataset.id));
        },
        behavior: HitTestBehavior.opaque,
        child: Container(
          width: double.infinity,
          color: bg,
          padding: const EdgeInsets.fromLTRB(8, 6, 8, 6),
          child: Row(
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              Icon(
                dataset.expanded ? Icons.expand_more : Icons.chevron_right,
                size: 14,
                color: scheme.onSurface.withOpacity(0.5),
              ),
              const SizedBox(width: 2),
              Icon(leadingIcon, size: 14, color: scheme.primary),
              const SizedBox(width: 6),
              Expanded(
                child: Text(
                  dataset.label,
                  maxLines: 2,
                  overflow: TextOverflow.ellipsis,
                  style: Theme.of(context).textTheme.bodySmall?.copyWith(
                        fontWeight: FontWeight.w600,
                        color: scheme.primary,
                      ),
                ),
              ),
              IconButton(
                tooltip: 'Remove',
                icon: Icon(Icons.close, size: 12,
                    color: scheme.onSurface.withOpacity(0.4)),
                onPressed: () => unawaited(state.removeDataset(dataset.id)),
                constraints: const BoxConstraints.tightFor(width: 20, height: 20),
                padding: EdgeInsets.zero,
                visualDensity: VisualDensity.compact,
              ),
            ],
          ),
        ),
      ),
    );
  }

  List<Widget> _buildDatasetChildTiles(
      ViewerState state, LoadedDatasetSource dataset) {
    final selectedDataset = state.isDatasetActive(dataset.id);

    if (dataset.mode == ViewerMode.webdatasetDir) {
      final shards = dataset.wdsDirSummary?.shards ?? const <WdsShardSummary>[];
      return shards
          .map(
            (shard) => _buildDatasetChildTile(
              selected:
                  selectedDataset && state.selectedShardName == shard.filename,
              onTap: () => unawaited(
                  state.activateDatasetShard(dataset.id, shard.filename)),
              indent: 20,
              icon: Icons.archive_outlined,
              title: Text(shard.filename),
              subtitle: _formatBytes(shard.bytes),
            ),
          )
          .toList();
    }

    if (dataset.mode == ViewerMode.huggingface) {
      final preview = dataset.hfPreview;
      final configs = dataset.hfConfigOptions ??
          preview?.configs ??
          const <HfConfigSummary>[];
      final options = _flattenHfConfigOptions(configs, preview);
      final selectedConfig = selectedDataset
          ? state.hfConfigOverride
          : (dataset.selectedHfConfig ?? preview?.config);
      final selectedSplit = selectedDataset
          ? state.hfSplitOverride
          : (dataset.selectedHfSplit ?? preview?.split);
      return options
          .map(
            (option) => _buildDatasetChildTile(
              selected: selectedDataset &&
                  option.config == selectedConfig &&
                  option.split == selectedSplit,
              onTap: () => unawaited(
                state.activateDatasetHfConfig(
                  dataset.id,
                  config: option.config,
                  split: option.split,
                ),
              ),
              indent: 20,
              icon: Icons.settings_outlined,
              title: Text(
                option.split.isEmpty
                    ? option.config
                    : '${option.config} / ${option.split}',
              ),
            ),
          )
          .toList();
    }

    if (dataset.mode == ViewerMode.zenodo) {
      final files = dataset.zenodoRecord?.files ?? const <ZenodoFileSummary>[];
      return files
          .map(
            (file) => _buildDatasetChildTile(
              selected:
                  selectedDataset && state.zenodoSelectedFileKey == file.key,
              onTap: () => unawaited(
                  state.activateDatasetZenodoFile(dataset.id, file.key)),
              indent: 20,
              icon: Icons.description_outlined,
              title: Text(file.key),
              subtitle: _formatBytes(file.size),
            ),
          )
          .toList();
    }

    final chunks = dataset.indexSummary?.chunks ?? const <ChunkSummary>[];
    return chunks
        .map(
          (chunk) => _buildDatasetChildTile(
            selected:
                selectedDataset && state.selectedChunkName == chunk.filename,
            onTap: () => unawaited(
                state.activateDatasetChunk(dataset.id, chunk.filename)),
            indent: 20,
            icon: dataset.mode == ViewerMode.mdsIndex
                ? Icons.view_module_outlined
                : Icons.segment_outlined,
            title: Text(chunk.filename),
            subtitle:
                '${chunk.chunkSize} items · ${_formatBytes(chunk.chunkBytes)}',
          ),
        )
        .toList();
  }

  Widget _buildDatasetChildTile({
    required bool selected,
    required VoidCallback? onTap,
    required int indent,
    required IconData icon,
    required Widget title,
    String? subtitle,
  }) {
    return Padding(
      padding: EdgeInsets.only(left: indent.toDouble()),
      child: _ExplorerTile(
        selected: selected,
        onTap: onTap ?? () {},
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Padding(
              padding: const EdgeInsets.only(top: 2),
              child: Icon(icon, size: 13),
            ),
            const SizedBox(width: 6),
            Expanded(
              child: DefaultTextStyle(
                style: Theme.of(context).textTheme.bodySmall ??
                    const TextStyle(),
                softWrap: true,
                child: title,
              ),
            ),
          ],
        ),
      ),
    );
  }


  IconData _datasetModeIcon(ViewerMode mode) {
    switch (mode) {
      case ViewerMode.litdataIndex:
      case ViewerMode.litdataChunks:
        return Icons.bolt_outlined;
      case ViewerMode.mdsIndex:
        return Icons.view_module_outlined;
      case ViewerMode.webdatasetDir:
        return Icons.inventory_2_outlined;
      case ViewerMode.huggingface:
        return Icons.emoji_emotions_outlined;
      case ViewerMode.zenodo:
        return Icons.school_outlined;
    }
  }

  String? _itemsSubtitle(ViewerState state) {
    if (state.mode == ViewerMode.huggingface && state.hfPreview != null) {
      final preview = state.hfPreview!;
      final totalLabel =
          preview.numRowsTotal > 0 ? preview.numRowsTotal.toString() : '-';
      return 'Total: $totalLabel';
    }
    if (state.mode == ViewerMode.webdatasetDir && state.wdsSamples != null) {
      final pageSize = state.wdsSamples!.length;
      final pageLabel = pageSize > 0 ? state.wdsOffset ~/ pageSize + 1 : 1;
      return 'Page $pageLabel';
    }
    if (state.mode == ViewerMode.zenodo &&
        state.zenodoSelectedFileKey != null) {
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
    if (state.mode == ViewerMode.webdatasetDir &&
        state.wdsSelectedMemberName != null) {
      return state.wdsSelectedMemberName;
    }
    if (state.mode == ViewerMode.zenodo &&
        state.zenodoSelectedEntryName != null) {
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
    if (state.mode == ViewerMode.webdatasetDir &&
        state.wdsSelectedSampleKey != null) {
      return 'Sample ${state.wdsSelectedSampleKey}';
    }
    if (state.mode == ViewerMode.mdsIndex && state.selectedItemIndex != null) {
      return 'Sample ${state.selectedItemIndex}';
    }
    if ((state.mode == ViewerMode.litdataIndex ||
            state.mode == ViewerMode.litdataChunks) &&
        state.selectedItemIndex != null) {
      return 'Item ${state.selectedItemIndex}';
    }
    return null;
  }

  Widget _buildItemsContent(ViewerState state) {
    return AnimatedSwitcher(
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
    );
  }

  Widget _buildFieldsContent(ViewerState state) {
    return AnimatedSwitcher(
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
    );
  }

  Widget _buildPreviewContent2(ViewerState state) {
    return AnimatedSwitcher(
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
        if (!snapshot.hasData)
          return const Center(child: Text('No fields found.'));
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
        if (!snapshot.hasData)
          return const Center(child: Text('No fields found.'));
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
        if (!snapshot.hasData)
          return const Center(child: Text('No fields found.'));
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
        if (!snapshot.hasData)
          return const Center(child: Text('No fields found.'));
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
        if (!snapshot.hasData)
          return const Center(child: Text('No items found.'));
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
        if (!snapshot.hasData)
          return const Center(child: Text('No samples found.'));
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
          onTap: () =>
              state.selectItem(item.itemIndex, fieldCount: item.fields.length),
          child: Row(
            children: [
              Expanded(
                child: Text('Item ${item.itemIndex}',
                    style: Theme.of(context).textTheme.bodyMedium,
                    overflow: TextOverflow.ellipsis),
              ),
              const SizedBox(width: 8),
              _buildInlineMetaText(context,
                  '${item.fields.length} fields · ${_formatBytes(item.totalBytes)}'),
            ],
          ),
        );
      },
    );
  }

  Widget _buildFieldList(ViewerState state, List<ItemMeta> items) {
    final selected = items
        .where((item) => item.itemIndex == state.selectedItemIndex)
        .toList();
    if (selected.isEmpty) {
      return const Center(child: Text('Select an item to view fields.'));
    }
    final fields = selected.first.fields;
    final formatByIndex = _formatByFieldIndex(state);
    final previewMap = state.mode == ViewerMode.mdsIndex
        ? state.mdsFieldPreviewByIndex
        : state.litdataFieldPreviewByIndex;
    return ListView.separated(
      itemCount: fields.length,
      separatorBuilder: (_, __) => const SizedBox(height: 6),
      itemBuilder: (context, index) {
        final field = fields[index];
        final format = field.fieldIndex < formatByIndex.length
            ? formatByIndex[field.fieldIndex]
            : null;
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
        if (!snapshot.hasData)
          return const Center(child: Text('No samples found.'));
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
                    onTap: () => state.selectWdsSample(sample.key,
                        fields: sample.fields),
                    child: Row(
                      children: [
                        Expanded(
                          child: Text(sample.key,
                              style: Theme.of(context).textTheme.bodyMedium,
                              overflow: TextOverflow.ellipsis),
                        ),
                        const SizedBox(width: 8),
                        _buildInlineMetaText(context,
                            '${sample.fields.length} fields · ${_formatBytes(sample.totalBytes)}'),
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
                    onPressed: canGoPrev
                        ? () =>
                            state.setWdsOffset(prevOffset < 0 ? 0 : prevOffset)
                        : null,
                    icon: const Icon(Icons.chevron_left),
                  ),
                  Expanded(
                    child: Center(
                      child: Text(
                          'Samples ${response.offset + 1}–${response.offset + samples.length}'),
                    ),
                  ),
                  IconButton(
                    onPressed: canGoNext
                        ? () => state.setWdsOffset(response.offset + pageSize)
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

  Widget _buildWdsFieldsPane(ViewerState state, List<WdsSampleInfo> samples) {
    final selected = samples
        .where((sample) => sample.key == state.wdsSelectedSampleKey)
        .toList();
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
          onTap: () =>
              state.selectWdsMember(field.memberPath, memberName: field.name),
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
        if (!snapshot.hasData)
          return const Center(child: Text('No rows available.'));
        final preview = snapshot.data!;
        final totalLabel =
            preview.numRowsTotal > 0 ? preview.numRowsTotal.toString() : '-';
        if (!_hfOffsetFocus.hasFocus) {
          final nextText = (preview.offset + 1).toString();
          if (_hfOffsetController.text != nextText) {
            _hfOffsetController.text = nextText;
            _hfOffsetController.selection =
                TextSelection.collapsed(offset: nextText.length);
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
                    child: Row(
                      children: [
                        Expanded(
                          child: Text('Row $rowIndex',
                              style: Theme.of(context).textTheme.bodyMedium,
                              overflow: TextOverflow.ellipsis),
                        ),
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
                    onPressed: preview.offset > 0
                        ? () => state.setHfOffset(
                              (preview.offset - preview.length)
                                  .clamp(0, preview.numRowsTotal)
                                  .toInt(),
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
                              inputFormatters: [
                                FilteringTextInputFormatter.digitsOnly
                              ],
                              decoration: const InputDecoration(
                                isDense: true,
                                contentPadding: EdgeInsets.symmetric(
                                    vertical: 6, horizontal: 8),
                              ),
                              onSubmitted: (_) =>
                                  _applyHfOffsetInput(state, preview),
                              onEditingComplete: () =>
                                  _applyHfOffsetInput(state, preview),
                            ),
                          ),
                          const SizedBox(width: 8),
                          Text('- ${preview.offset + preview.rows.length}'),
                        ],
                      ),
                    ),
                  ),
                  IconButton(
                    onPressed: preview.offset + preview.rows.length <
                            preview.numRowsTotal
                        ? () => state
                            .setHfOffset(preview.offset + preview.rows.length)
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
    if (recordFuture == null)
      return const Center(child: Text('No record loaded.'));
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
                  Expanded(
                      child: Text(entry.name, overflow: TextOverflow.ellipsis)),
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
                        Expanded(
                            child: Text(entry.name,
                                overflow: TextOverflow.ellipsis)),
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
                  onPressed: canGoPrev
                      ? () => state.setZenodoEntriesOffset(
                          prevOffset < 0 ? 0 : prevOffset)
                      : null,
                  icon: const Icon(Icons.chevron_left),
                ),
                Text('Page $pageLabel'),
                const Spacer(),
                IconButton(
                  onPressed: canGoNext
                      ? () => state
                          .setZenodoEntriesOffset(response.offset + pageSize)
                      : null,
                  icon: const Icon(Icons.chevron_right),
                ),
              ],
            ),
          ],
        );
      },
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
          if (!snapshot.hasData)
            return const Center(child: Text('No preview.'));
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
          if (!snapshot.hasData)
            return const Center(child: Text('No preview.'));
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

  Widget _buildInlineMediaPreview(
      InlineMediaResponse media, FieldPreview preview) {
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
            return PreparedMediaResponse(
                bytes: wavBytes, size: wavBytes.length, ext: 'wav');
          }
          return PreparedMediaResponse(
              bytes: bytes, size: media.size, ext: ext);
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

  Widget _buildPreviewContent(ViewerState state, FieldPreview preview,
      {bool isWds = false}) {
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
      if (preview == null ||
          state.hfSelectedRowIndex == null ||
          state.hfSelectedFieldName == null) {
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
      final previewFuture =
          state.zenodoEntryPreviewFuture ?? state.zenodoFilePreviewFuture;
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
                final copyValue =
                    inlineSnapshot.data?.base64 ?? preview.previewText;
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

  Widget _buildActionsRow(
      {String? copyValue, Future<void> Function()? onOpen}) {
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

  Widget _buildAudioPreview(ViewerState state, FieldPreview preview,
      {required bool isWds}) {
    return AudioPreview(
      label: 'Audio preview',
      loader: () => _prepareAudioPreview(state, preview, isWds: isWds),
    );
  }

  Widget _buildImagePreview(ViewerState state, FieldPreview preview,
      {required bool isWds}) {
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

  Future<PreparedMediaResponse> _prepareAudioPreview(
      ViewerState state, FieldPreview preview,
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

  Future<PreparedMediaResponse> _normalizeAudioPreview(
      PreparedMediaResponse media) async {
    final ext = media.ext.trim().toLowerCase();
    final cleaned = ext.startsWith('.') ? ext.substring(1) : ext;
    if (cleaned != 'sph') return media;
    final wavBytes = await decodeSphereToWavWithFallback(media.bytes);
    return PreparedMediaResponse(
        bytes: wavBytes, size: wavBytes.length, ext: 'wav');
  }

  Future<PreparedFileResponse> _prepareImagePreview(
      ViewerState state, FieldPreview preview,
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
    return _formatFieldMetaInline(
        ext: ext, type: type, size: size ?? preview.size);
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

  Future<void> _openSelectedField(
      ViewerState state, FieldPreview preview) async {
    try {
      final ext = preview.guessedExt ?? '';
      final preferredOpener =
          ext.isNotEmpty ? await state.preferredOpenerForExt(ext) : null;
      if (state.mode == ViewerMode.mdsIndex && state.indexSummary != null) {
        var response =
            await state.mosaicmlOpenField(openerAppPath: preferredOpener);
        response = await _handleOpenerFallback(state, response, ext, (appPath) {
          return state.mosaicmlOpenField(openerAppPath: appPath);
        });
        state.setStatusMessage(response.message);
        return;
      }
      if (state.mode == ViewerMode.webdatasetDir &&
          state.wdsDirSummary != null) {
        var response =
            await state.webdatasetOpenMember(openerAppPath: preferredOpener);
        response = await _handleOpenerFallback(state, response, ext, (appPath) {
          return state.webdatasetOpenMember(openerAppPath: appPath);
        });
        state.setStatusMessage(response.message);
        return;
      }
      if (state.indexSummary != null) {
        var response =
            await state.litdataOpenField(openerAppPath: preferredOpener);
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
          response =
              await _handleOpenerFallback(state, response, ext, (appPath) {
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

  Future<void> _openHfSelectedField(
      ViewerState state, HfDatasetPreview preview) async {
    if (state.hfSelectedRowIndex == null || state.hfSelectedFieldName == null)
      return;
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
    if (cleaned == 'txt' ||
        cleaned == 'json' ||
        cleaned == 'csv' ||
        cleaned == 'tsv' ||
        cleaned == 'md' ||
        cleaned == 'yaml' ||
        cleaned == 'yml') {
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
        color: const Color(0xFFF7F7F7),
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: const Color(0xFFE0E0E0)),
      ),
      child: SelectableText(
        text,
        style: GoogleFonts.googleSansCode(
          textStyle: Theme.of(context).textTheme.bodySmall,
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
          final bounded =
              constraints.maxHeight.isFinite && constraints.maxHeight > 0;
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

class _ExplorerTile extends StatefulWidget {
  const _ExplorerTile({
    required this.child,
    required this.selected,
    required this.onTap,
  });

  final Widget child;
  final bool selected;
  final VoidCallback onTap;

  @override
  State<_ExplorerTile> createState() => _ExplorerTileState();
}

class _ExplorerTileState extends State<_ExplorerTile> {
  bool _hovered = false;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    final bg = widget.selected
        ? scheme.primary.withOpacity(0.12)
        : _hovered
            ? scheme.onSurface.withOpacity(0.06)
            : Colors.transparent;

    return MouseRegion(
      onEnter: (_) => setState(() => _hovered = true),
      onExit: (_) => setState(() => _hovered = false),
      cursor: SystemMouseCursors.click,
      child: GestureDetector(
        onTap: widget.onTap,
        behavior: HitTestBehavior.opaque,
        child: Container(
          width: double.infinity,
          decoration: BoxDecoration(
            color: bg,
            border: widget.selected
                ? Border(left: BorderSide(width: 2, color: scheme.primary))
                : null,
          ),
          padding: EdgeInsets.fromLTRB(
            widget.selected ? 6 : 8, 4, 8, 4,
          ),
          child: widget.child,
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
