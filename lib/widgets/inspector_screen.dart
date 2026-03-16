import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:path/path.dart' as p;
import 'package:provider/provider.dart';
import 'package:window_manager/window_manager.dart';

import '../models/common.dart';
import '../models/huggingface.dart';
import '../models/webdataset.dart';
import '../models/zenodo.dart';
import '../services/update_service.dart';
import '../state/viewer_state.dart';
import '../utils/audio.dart';
import '../utils/app_fonts.dart';
import '../utils/dialog_action_styles.dart';
import 'dataset_add_dialog.dart';
import 'audio_preview.dart';
import 'copy_button.dart';
import 'hover_tile.dart';
import 'remote_hosts_dialog.dart';
import 'remote_path_picker.dart';
import 'skeleton.dart';
import 'update_dialog.dart';
import 'video_preview.dart';

const double _kPaneHeaderHeight = 36;
const double _kOuterHorizontalPadding = 12;
const double _kSourcesPaneWidth = 340;
const double _kPaneGap = 8;
const double _kLocalDirectoryIndentStep = 16.0;
const int _kLocalTabularMaxRows = 2000;

class InspectorScreen extends StatefulWidget {
  const InspectorScreen({super.key});

  @override
  State<InspectorScreen> createState() => _InspectorScreenState();
}

class _InspectorScreenState extends State<InspectorScreen> {
  late final TextEditingController _hfOffsetController;
  late final FocusNode _hfOffsetFocus;
  String? _lastStatusMessage;
  String? _statusOverlayMessage;
  bool _statusOverlayVisible = false;
  Timer? _statusOverlayTimer;
  String? _localDirectoryTreeDatasetId;
  final Set<String> _localDirectoryExpandedDirs = <String>{};
  final Set<String> _localDirectoryLoadingDirs = <String>{};
  String? _localDirectoryPrimarySelection;
  String? _localDirectorySecondarySelection;
  final Map<String, LocalDirectoryItem> _localDirectoryItemByPath = {};
  List<LocalDirectoryItem> _localDirectoryRootItems = <LocalDirectoryItem>[];
  final Map<String, List<LocalDirectoryItem>> _localDirectoryChildren = {};
  final Map<String, Future<List<LocalDirectoryItem>>>
      _localDirectoryChildrenFutures = {};
  final Map<String, Future<_LocalTabularData>> _localTabularDataFutures = {};
  final Map<String, Future<List<ItemMeta>>> _localDirectoryMdsItemsFutures = {};
  final Map<String, Future<List<String>>> _localDirectoryMdsFormatFutures = {};
  final Map<String, int> _localTabularSelectedRowByPath = <String, int>{};
  bool _localDirectoryTreeIsRemote = false;

  TextStyle _explorerListTextStyle(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    return (Theme.of(context).textTheme.bodySmall ??
            Theme.of(context).textTheme.bodyMedium ??
            const TextStyle())
        .copyWith(color: scheme.onSurface);
  }

  @override
  void initState() {
    super.initState();
    _hfOffsetController = TextEditingController();
    _hfOffsetFocus = FocusNode();
  }

  @override
  void dispose() {
    _statusOverlayTimer?.cancel();
    _hfOffsetController.dispose();
    _hfOffsetFocus.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Consumer<ViewerState>(
      builder: (context, state, _) {
        _handleStatusMessage(context, state);
        return Scaffold(
          body: SafeArea(
            child: Stack(
              children: [
                _buildBackdrop(context),
                Padding(
                  padding: const EdgeInsets.fromLTRB(
                    _kOuterHorizontalPadding,
                    8,
                    _kOuterHorizontalPadding,
                    12,
                  ),
                  child: Column(
                    children: [
                      _buildTopBar(context, state),
                      const SizedBox(height: 4),
                      Expanded(
                        child: Stack(
                          fit: StackFit.expand,
                          children: [
                            _buildPanels(context, state),
                          ],
                        ),
                      ),
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

  void _handleStatusMessage(BuildContext context, ViewerState state) {
    final message = state.statusMessage?.trim();
    if (message == null || message.isEmpty) {
      _lastStatusMessage = null;
      return;
    }
    if (message == _lastStatusMessage) return;

    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (!mounted) return;
      if (state.statusMessage?.trim() != message) return;
      if (_lastStatusMessage == message) return;

      _statusOverlayTimer?.cancel();
      setState(() {
        _statusOverlayMessage = message;
        _statusOverlayVisible = true;
      });
      _statusOverlayTimer = Timer(const Duration(milliseconds: 2200), () {
        if (!mounted) return;
        setState(() => _statusOverlayVisible = false);
      });
      _lastStatusMessage = message;
    });
  }

  Widget _buildBackdrop(BuildContext context) {
    return Container(color: const Color(0xFFF5F5F5));
  }

  Widget _buildPanels(BuildContext context, ViewerState state) {
    final scheme = Theme.of(context).colorScheme;
    return Row(
      children: [
        SizedBox(width: _kSourcesPaneWidth, child: _buildSourcesPane(state)),
        const SizedBox(width: _kPaneGap),
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
                    VerticalDivider(
                        width: 1, thickness: 1, color: scheme.outlineVariant),
                    Expanded(
                      flex: 1,
                      child: Stack(
                        children: [
                          LayoutBuilder(
                            builder: (context, constraints) {
                              final maxPreviewHeight =
                                  _previewMaxHeightForState(
                                      state, constraints.maxHeight);
                              return Column(
                                children: [
                                  Expanded(
                                    child: _buildInlinePane(
                                      title: 'Fields',
                                      subtitle: _fieldsSubtitle(state),
                                      child: _buildFieldsContent(state),
                                    ),
                                  ),
                                  Divider(
                                      height: 1,
                                      thickness: 1,
                                      color: scheme.outlineVariant),
                                  AnimatedSize(
                                    duration:
                                        const Duration(milliseconds: 240),
                                    curve: Curves.easeOutCubic,
                                    alignment: Alignment.topCenter,
                                    child: ConstrainedBox(
                                      constraints: BoxConstraints(
                                          maxHeight: maxPreviewHeight),
                                      child: _buildInlinePane(
                                        title: 'Preview',
                                        subtitle: _previewSubtitle(state),
                                        subtitleTrailing:
                                            _buildPreviewActions(state),
                                        expand: false,
                                        flexible: true,
                                        child:
                                            _buildPreviewContent2(state),
                                      ),
                                    ),
                                  ),
                                ],
                              );
                            },
                          ),
                          if (_statusOverlayMessage != null)
                            Positioned(
                              left: 8,
                              right: 8,
                              bottom: 8,
                              child: IgnorePointer(
                                ignoring: !_statusOverlayVisible,
                                child: AnimatedOpacity(
                                  opacity:
                                      _statusOverlayVisible ? 1.0 : 0.0,
                                  duration:
                                      const Duration(milliseconds: 200),
                                  onEnd: () {
                                    if (!_statusOverlayVisible) {
                                      setState(() =>
                                          _statusOverlayMessage = null);
                                    }
                                  },
                                  child: Material(
                                    elevation: 4,
                                    borderRadius: BorderRadius.circular(8),
                                    color: const Color(0xFF323232),
                                    child: Padding(
                                      padding: const EdgeInsets.symmetric(
                                          horizontal: 16, vertical: 10),
                                      child: Text(
                                        _statusOverlayMessage!,
                                        style: const TextStyle(
                                            color: Colors.white,
                                            fontSize: 13),
                                      ),
                                    ),
                                  ),
                                ),
                              ),
                            ),
                        ],
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
                  style: AppFonts.flexTextStyle(textTheme.labelLarge)
                      .copyWith(fontWeight: FontWeight.w700),
                ),
                if (subtitle != null) ...[
                  const SizedBox(width: 8),
                  Expanded(
                    child: Text(
                      subtitle,
                      style: textTheme.labelSmall?.copyWith(
                        color: scheme.onSurface.withValues(alpha: 0.5),
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
      if (entry != null && _isVideoPath(entry)) {
        return true;
      }
      final fileKey = state.zenodoSelectedFileKey;
      if (entry == null && fileKey != null && _isVideoPath(fileKey)) {
        return true;
      }
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
    final isDesktop = !kIsWeb &&
        (defaultTargetPlatform == TargetPlatform.macOS ||
            defaultTargetPlatform == TargetPlatform.windows ||
            defaultTargetPlatform == TargetPlatform.linux);
    final scheme = Theme.of(context).colorScheme;
    final textTheme = Theme.of(context).textTheme;
    final titleWidget = Text(
      'Dataset Inspector',
      textAlign: TextAlign.center,
      style: AppFonts.flexTextStyle(textTheme.titleSmall).copyWith(
        fontSize: 14,
        fontWeight: FontWeight.w600,
        letterSpacing: 0.1,
        height: 1.0,
        color: scheme.onSurface.withValues(alpha: 0.9),
      ),
      maxLines: 1,
      overflow: TextOverflow.ellipsis,
    );

    return SizedBox(
      height: 36,
      child: Stack(
        children: [
          if (isDesktop)
            Positioned.fill(
              child: GestureDetector(
                behavior: HitTestBehavior.translucent,
                onPanStart: (_) => windowManager.startDragging(),
                onDoubleTap: () async {
                  if (await windowManager.isMaximized()) {
                    await windowManager.unmaximize();
                  } else {
                    await windowManager.maximize();
                  }
                },
                child: const SizedBox.expand(),
              ),
            ),
          Positioned.fill(
            child: IgnorePointer(
              child: Center(child: titleWidget),
            ),
          ),
          Align(
            alignment: Alignment.centerRight,
            child: Padding(
              padding: const EdgeInsets.only(
                left: 8,
                right: 8,
              ),
              child: Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  _buildRemoteHostsButton(state),
                  const SizedBox(width: 8),
                  _buildHfTokenButton(state),
                  const SizedBox(width: 8),
                  _buildApiButton(state),
                  const SizedBox(width: 8),
                  _buildUpdateButton(state),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildRemoteHostsButton(ViewerState state) {
    final hostCount = state.remoteHosts.length;
    final label = hostCount > 0 ? 'Remote Hosts ($hostCount)' : 'Remote Hosts';
    return OutlinedButton.icon(
      onPressed: () {
        unawaited(_showRemoteHostsSettings(context, state));
      },
      style: _topBarActionButtonStyle(),
      icon: const Icon(Icons.dns_outlined, size: 16),
      label: Text(label),
    );
  }

  Widget _buildApiButton(ViewerState state) {
    final host = state.apiEndpointHost;
    final port = state.apiEndpointPort;
    final running = state.apiRunning;
    final configuredHost = state.apiHost;
    final configuredPort = state.apiPort;
    final concurrency = state.apiMaxConcurrency;
    final statusLabel = running ? 'API ON' : 'API OFF';
    return Tooltip(
      message: running
          ? 'API service running at http://$host:$port/api/v1/opened\n'
                'Max concurrency: $concurrency'
          : 'API service is off.\n'
                'Configured endpoint: http://$configuredHost:$configuredPort/api/v1/opened\n'
                'Configured max concurrency: $concurrency',
      child: OutlinedButton.icon(
        onPressed: () {
          unawaited(_showApiSettings(context, state));
        },
        style: _topBarActionButtonStyle(),
        icon: Icon(
          running ? Icons.cloud_done_outlined : Icons.cloud_off_outlined,
          size: 16,
        ),
        label: Text(
          running
              ? '$statusLabel ($host:$port)'
              : '$statusLabel ($configuredHost:$configuredPort)',
        ),
      ),
    );
  }

  Widget _buildHfTokenButton(ViewerState state) {
    final hasToken = (state.hfToken?.trim().isNotEmpty ?? false);
    return OutlinedButton.icon(
      onPressed: () {
        unawaited(_showHfTokenSettings(context, state));
      },
      style: _topBarActionButtonStyle(),
      icon: Icon(
        hasToken ? Icons.key_outlined : Icons.key_off_outlined,
        size: 16,
      ),
      label: Text(hasToken ? 'HF Token: Set' : 'HF Token'),
    );
  }

  ButtonStyle _topBarActionButtonStyle() {
    return OutlinedButton.styleFrom(
      minimumSize: const Size(0, 30),
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 0),
      tapTargetSize: MaterialTapTargetSize.shrinkWrap,
      visualDensity: VisualDensity.compact,
      textStyle: const TextStyle(fontSize: 12),
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

  Future<void> _showAddDatasetDialog(
      BuildContext context, ViewerState state) async {
    await showDatasetAddDialog(
      context: context,
      recentSources: state.recentSources.take(8).toList(growable: false),
      remoteHosts: state.remoteHosts,
      onOpenLocalSource: (input) => _smartAddSource(state, input),
      onOpenRemoteSource: ({required hostId, required datasetPath}) {
        return state.addSourceFromRemoteHost(
          hostId: hostId,
          datasetPath: datasetPath,
        );
      },
      onBrowseRemotePath: ({required hostId, required initialPath}) async {
        final host = state.findRemoteHostById(hostId);
        if (host == null) return null;
        return showRemotePathPickerDialog(
          context: context,
          host: host,
          initialPath: initialPath,
          onListEntries: ({
            required String hostId,
            required String directoryPath,
          }) {
            return state.listRemoteHostEntries(
              hostId: hostId,
              directoryPath: directoryPath,
            );
          },
        );
      },
      onOpenRemoteHostsSettings: () {
        unawaited(_showRemoteHostsSettings(context, state));
      },
    );
  }

  Future<void> _showRemoteHostsSettings(
    BuildContext context,
    ViewerState state,
  ) async {
    final updated = await showRemoteHostsSettingsDialog(
      context: context,
      initialHosts: state.remoteHosts,
      onTestConnection: (host) => state.testRemoteHostConnection(host),
    );
    if (updated == null) return;
    await state.saveRemoteHosts(updated);
  }

  Future<void> _showHfTokenSettings(
    BuildContext context,
    ViewerState state,
  ) async {
    final tokenController = TextEditingController(text: state.hfToken ?? '');
    final messenger = ScaffoldMessenger.maybeOf(context);
    final dialogTheme = Theme.of(context);
    final dialogBorder = OutlineInputBorder(
      borderRadius: BorderRadius.circular(10),
      borderSide: const BorderSide(color: Color(0xFFE0E0E0)),
    );
    final themedData = dialogTheme.copyWith(
      inputDecorationTheme: dialogTheme.inputDecorationTheme.copyWith(
        filled: true,
        fillColor: const Color(0xFFF7F7F7),
        border: dialogBorder,
        enabledBorder: dialogBorder,
        contentPadding:
            const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
        focusedBorder: dialogBorder.copyWith(
          borderSide: BorderSide(
            color: dialogTheme.colorScheme.primary.withValues(alpha: 0.65),
            width: 1.2,
          ),
        ),
      ),
    );
    var obscureToken = true;
    bool? changed;
    try {
      changed = await showDialog<bool>(
        context: context,
        builder: (dialogContext) {
          return Theme(
            data: themedData,
            child: StatefulBuilder(
              builder: (context, setState) {
                final hasSavedToken =
                    (state.hfToken?.trim().isNotEmpty ?? false);
                final hasInputToken = tokenController.text.trim().isNotEmpty;
                return AlertDialog(
                  backgroundColor: Colors.white,
                  surfaceTintColor: Colors.transparent,
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(12),
                    side: const BorderSide(color: Color(0xFFE0E0E0)),
                  ),
                  titleTextStyle: AppFonts.flexTextStyle(
                          Theme.of(context).textTheme.titleMedium)
                      .copyWith(fontWeight: FontWeight.w700),
                  title: const Text('Settings · Hugging Face Token'),
                  content: SizedBox(
                    width: 560,
                    child: Column(
                      mainAxisSize: MainAxisSize.min,
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          'Optional token for private/gated datasets and higher Hugging Face API limits.',
                          style: Theme.of(context).textTheme.bodySmall,
                        ),
                        const SizedBox(height: 12),
                        TextField(
                          controller: tokenController,
                          autofocus: true,
                          obscureText: obscureToken,
                          onChanged: (_) {
                            setState(() {});
                          },
                          decoration: InputDecoration(
                            labelText: 'Access Token',
                            hintText: 'hf_...',
                            helperText: 'Stored locally on this machine.',
                            suffixIcon: IconButton(
                              tooltip:
                                  obscureToken ? 'Show token' : 'Hide token',
                              onPressed: () {
                                setState(() {
                                  obscureToken = !obscureToken;
                                });
                              },
                              icon: Icon(
                                obscureToken
                                    ? Icons.visibility_outlined
                                    : Icons.visibility_off_outlined,
                              ),
                            ),
                          ),
                          onSubmitted: (_) async {
                            await state.saveHfToken(tokenController.text);
                            if (!dialogContext.mounted) return;
                            Navigator.of(dialogContext).pop(true);
                          },
                        ),
                      ],
                    ),
                  ),
                  actions: [
                    OutlinedButton(
                      style: buildDialogSecondaryButtonStyle(context),
                      onPressed: (!hasSavedToken && !hasInputToken)
                          ? null
                          : () async {
                              await state.clearHfToken();
                              if (!dialogContext.mounted) return;
                              Navigator.of(dialogContext).pop(true);
                            },
                      child: const Text('Clear'),
                    ),
                    OutlinedButton(
                      style: buildDialogSecondaryButtonStyle(context),
                      onPressed: () => Navigator.of(dialogContext).pop(false),
                      child: const Text('Cancel'),
                    ),
                    FilledButton(
                      style: buildDialogPrimaryButtonStyle(context),
                      onPressed: () async {
                        await state.saveHfToken(tokenController.text);
                        if (!dialogContext.mounted) return;
                        Navigator.of(dialogContext).pop(true);
                      },
                      child: const Text('Save'),
                    ),
                  ],
                );
              },
            ),
          );
        },
      );
    } finally {
      WidgetsBinding.instance.addPostFrameCallback((_) {
        tokenController.dispose();
      });
    }
    if (changed != true || !mounted) return;
    final hasToken = (state.hfToken?.trim().isNotEmpty ?? false);
    messenger?.showSnackBar(
      SnackBar(
        content: Text(
          hasToken
              ? 'Hugging Face token saved.'
              : 'Hugging Face token cleared.',
        ),
      ),
    );
  }

  Future<void> _showApiSettings(BuildContext context, ViewerState state) async {
    final messenger = ScaffoldMessenger.maybeOf(context);
    final hostController = TextEditingController(text: state.apiHost);
    final portController = TextEditingController(text: state.apiPort.toString());
    final concurrencyController =
        TextEditingController(text: state.apiMaxConcurrency.toString());
    var enabled = state.apiEnabled;
    bool? saved;
    try {
      saved = await showDialog<bool>(
        context: context,
        builder: (dialogContext) {
          final scheme = Theme.of(context).colorScheme;
          final textTheme = Theme.of(context).textTheme;
          String? fieldError;
          return StatefulBuilder(
            builder: (context, setState) {
              final host = hostController.text.trim().isEmpty
                  ? '127.0.0.1'
                  : hostController.text.trim();
              final port = int.tryParse(portController.text.trim());
              final concurrency = int.tryParse(
                concurrencyController.text.trim(),
              );
              final hasInputError = enabled && (port == null ||
                  port <= 0 ||
                  port > 65535 ||
                  concurrency == null ||
                  concurrency < 1 ||
                  concurrency > 64);
              final previewPort = port ?? state.apiPort;
              final previewConcurrency = concurrency ?? state.apiMaxConcurrency;
              return AlertDialog(
                backgroundColor: Colors.white,
                surfaceTintColor: Colors.transparent,
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(12),
                  side: const BorderSide(color: Color(0xFFE0E0E0)),
                ),
                titleTextStyle: AppFonts.flexTextStyle(
                        Theme.of(context).textTheme.titleMedium)
                    .copyWith(fontWeight: FontWeight.w700),
                title: const Text('Settings · API'),
                content: SizedBox(
                  width: 560,
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      SwitchListTile(
                        contentPadding: EdgeInsets.zero,
                        title: Text(
                          'Enable API service',
                          style: textTheme.labelLarge,
                        ),
                        subtitle: Text(
                          enabled
                              ? 'Service is enabled.'
                              : 'Service is disabled.',
                          style: textTheme.bodySmall?.copyWith(
                            color: scheme.onSurface.withValues(alpha: 0.65),
                          ),
                        ),
                        value: enabled,
                        onChanged: (value) {
                          setState(() {
                            enabled = value;
                          });
                        },
                      ),
                      const SizedBox(height: 12),
                      TextField(
                        controller: hostController,
                        onChanged: (_) {
                          setState(() {});
                        },
                        decoration: const InputDecoration(
                          labelText: 'API Host',
                          helperText:
                              'Use 127.0.0.1 for local, 0.0.0.0 for LAN.',
                        ),
                      ),
                      const SizedBox(height: 12),
                      Row(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Expanded(
                            child: TextField(
                              controller: portController,
                              keyboardType: TextInputType.number,
                              onChanged: (_) {
                                setState(() {});
                              },
                              decoration: const InputDecoration(
                                labelText: 'API Port',
                                helperText: '1 - 65535',
                              ),
                            ),
                          ),
                          const SizedBox(width: 10),
                          Expanded(
                            child: TextField(
                              controller: concurrencyController,
                              keyboardType: TextInputType.number,
                              onChanged: (_) {
                                setState(() {});
                              },
                              decoration: const InputDecoration(
                                labelText: 'Max Concurrency',
                                helperText: '1 - 64',
                              ),
                            ),
                          ),
                        ],
                      ),
                      if (state.apiRuntimeError != null) ...[
                        const SizedBox(height: 12),
                        Text(
                          state.apiRuntimeError!,
                          style: textTheme.bodySmall?.copyWith(
                            color: scheme.error,
                          ),
                        ),
                      ] else ...[
                        const SizedBox(height: 10),
                        Text(
                          enabled
                              ? 'Endpoint: http://$host:$previewPort/api/v1/opened'
                              : 'Endpoint will stop when disabled.',
                          style: textTheme.bodySmall?.copyWith(
                            color: scheme.onSurface.withValues(alpha: 0.7),
                          ),
                        ),
                        const SizedBox(height: 6),
                        Text(
                          'Default concurrency for current settings: $previewConcurrency',
                          style: textTheme.bodySmall?.copyWith(
                            color: scheme.onSurface.withValues(alpha: 0.7),
                          ),
                        ),
                      ],
                      if (fieldError != null) ...[
                        const SizedBox(height: 8),
                        Text(
                          fieldError!,
                          style: textTheme.bodySmall?.copyWith(
                            color: scheme.error,
                          ),
                        ),
                      ],
                    ],
                  ),
                ),
                actions: [
                  OutlinedButton(
                    style: buildDialogSecondaryButtonStyle(context),
                    onPressed: () => Navigator.of(dialogContext).pop(false),
                    child: const Text('Close'),
                  ),
                  FilledButton(
                    style: buildDialogPrimaryButtonStyle(context),
                    onPressed: hasInputError
                        ? null
                        : () async {
                            if (enabled && hostController.text.trim().isEmpty) {
                              setState(() {
                                fieldError =
                                    'API host cannot be empty. Use 127.0.0.1.';
                              });
                              return;
                            }
                            final normalizedPort = enabled
                                ? (port ?? state.apiPort)
                                : state.apiPort;
                            final normalizedConcurrency = enabled
                                ? (concurrency ?? state.apiMaxConcurrency)
                                : state.apiMaxConcurrency;
                            final changed = await state.applyApiSettings(
                              enabled: enabled,
                              host: host.isEmpty ? '127.0.0.1' : host,
                              port: normalizedPort,
                              maxConcurrency: normalizedConcurrency,
                            );
                            if (!dialogContext.mounted) return;
                            if (changed && state.apiRuntimeError == null) {
                              Navigator.of(dialogContext).pop(true);
                            } else if (!changed) {
                              Navigator.of(dialogContext).pop(false);
                            } else {
                              setState(() {
                                fieldError = state.apiRuntimeError ??
                                    'Failed to apply API settings.';
                              });
                            }
                          },
                    child: const Text('Apply'),
                  ),
                ],
              );
            },
          );
        },
      );
    } finally {
      WidgetsBinding.instance.addPostFrameCallback((_) {
        hostController.dispose();
        portController.dispose();
        concurrencyController.dispose();
      });
    }
    if (saved != true) return;
    if (!mounted) return;
    if (state.apiRuntimeError == null && state.apiEnabled) {
      messenger?.showSnackBar(
        const SnackBar(content: Text('API settings applied.')),
      );
    } else if (!state.apiEnabled) {
      messenger?.showSnackBar(
        const SnackBar(content: Text('API disabled.')),
      );
    } else {
      messenger?.showSnackBar(
        SnackBar(
          content: Text(
            'Failed to apply API settings: ${state.apiRuntimeError ?? 'unknown error'}',
          ),
        ),
      );
    }
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
                    style: AppFonts.flexTextStyle(textTheme.labelLarge)
                        .copyWith(fontWeight: FontWeight.w700),
                  ),
                  if (scanning) ...[
                    const SizedBox(width: 8),
                    SizedBox(
                      width: 12,
                      height: 12,
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
                          color: scheme.onSurface.withValues(alpha: 0.5),
                        ),
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                      ),
                    ),
                    IconButton(
                      onPressed: state.cancelDatasetScan,
                      icon: const Icon(Icons.close, size: 14),
                      constraints:
                          const BoxConstraints.tightFor(width: 22, height: 22),
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
                color: Theme.of(context).colorScheme.onSurface.withValues(alpha: 0.4),
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
                                .withValues(alpha: 0.4),
                          ),
                        ),
                        const SizedBox(width: 8),
                        Text(
                          'Loading...',
                          style: _explorerListTextStyle(context),
                          maxLines: 1,
                          overflow: TextOverflow.ellipsis,
                        ),
                      ],
                    )
                  : Text(
                      'No entries',
                      style: _explorerListTextStyle(context),
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                    ),
            ),
          );
        } else {
          rows.addAll(children);
        }
      }
    }
    return ListView.separated(
      separatorBuilder: (_, __) => const SizedBox(height: 6),
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
      case ViewerMode.localDirectory:
        return false;
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
    final bg =
        selected ? scheme.secondary.withValues(alpha: 0.12) : Colors.transparent;
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
                color: scheme.onSurface.withValues(alpha: 0.5),
              ),
              const SizedBox(width: 2),
              Icon(leadingIcon, size: 14, color: scheme.primary),
              const SizedBox(width: 6),
              Expanded(
                child: Text(
                  dataset.label,
                  maxLines: 2,
                  overflow: TextOverflow.ellipsis,
                  style: Theme.of(context).textTheme.bodyMedium?.copyWith(
                        fontWeight: FontWeight.w600,
                        color: scheme.primary,
                      ),
                ),
              ),
              IconButton(
                tooltip: 'Remove',
                icon: Icon(Icons.close,
                    size: 12, color: scheme.onSurface.withValues(alpha: 0.4)),
                onPressed: () => unawaited(state.removeDataset(dataset.id)),
                constraints:
                    const BoxConstraints.tightFor(width: 20, height: 20),
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
    final activeLocalSelection =
        selectedDataset && state.mode == ViewerMode.localDirectory;

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
              title: Text(
                shard.filename,
                style: _explorerListTextStyle(context),
                maxLines: 1,
                overflow: TextOverflow.ellipsis,
              ),
              subtitle: _formatBytes(shard.bytes),
            ),
          )
          .toList();
    }

    if (dataset.mode == ViewerMode.localDirectory) {
      final localEntries = _localEntriesForDataset(state, dataset);
      if (!activeLocalSelection) {
        return localEntries
            .map(
              (entry) => _buildDatasetChildTile(
                selected: selectedDataset &&
                    state.selectedLocalDirectoryItem?.path == entry.path,
                onTap: () => unawaited(
                    _activateLocalDirectoryChild(state, dataset, entry.path)),
                indent: 20,
                icon: entry.isDirectory
                    ? Icons.folder_outlined
                    : Icons.description_outlined,
                title: Text(
                  entry.name,
                  style: _explorerListTextStyle(context),
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                ),
                subtitle: entry.size == null ? null : _formatBytes(entry.size!),
              ),
            )
            .toList();
      }
      _syncLocalDirectoryTree(state, localEntries);
      final rows = _buildLocalDirectoryTreeRows();
      return rows
          .map(
            (row) => _buildLocalExplorerTreeTile(
              state: state,
              dataset: dataset,
              row: row,
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
                style: _explorerListTextStyle(context),
                maxLines: 1,
                overflow: TextOverflow.ellipsis,
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
              title: Text(
                file.key,
                style: _explorerListTextStyle(context),
                maxLines: 1,
                overflow: TextOverflow.ellipsis,
              ),
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
            title: Text(
              chunk.filename,
              style: _explorerListTextStyle(context),
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
            ),
            subtitle:
                '${chunk.chunkSize} items · ${_formatBytes(chunk.chunkBytes)}',
          ),
        )
        .toList();
  }

  List<LocalDirectoryItem> _localEntriesForDataset(
    ViewerState state,
    LoadedDatasetSource dataset,
  ) {
    if (state.mode == ViewerMode.localDirectory &&
        state.isDatasetActive(dataset.id) &&
        state.localDirectoryItems.isNotEmpty) {
      return state.localDirectoryItems;
    }
    final entries = <LocalDirectoryItem>[];
    for (final path in dataset.paths ?? const <String>[]) {
      var isDirectory = false;
      try {
        final type = FileSystemEntity.typeSync(path, followLinks: true);
        isDirectory = type == FileSystemEntityType.directory;
      } catch (_) {
        isDirectory = false;
      }
      entries.add(LocalDirectoryItem(
        name: p.basename(path),
        path: path,
        isDirectory: isDirectory,
      ));
    }
    entries.sort((left, right) {
      if (left.isDirectory != right.isDirectory) {
        return left.isDirectory ? -1 : 1;
      }
      return left.name.toLowerCase().compareTo(right.name.toLowerCase());
    });
    return entries;
  }

  Future<void> _activateLocalDirectoryChild(
    ViewerState state,
    LoadedDatasetSource dataset,
    String path,
  ) async {
    await state.activateDataset(dataset.id);
    final idx =
        state.localDirectoryItems.indexWhere((item) => item.path == path);
    if (idx < 0) return;
    _onLocalDirectoryItemTap(state, state.localDirectoryItems[idx]);
  }

  Widget _buildLocalExplorerTreeTile({
    required ViewerState state,
    required LoadedDatasetSource dataset,
    required _LocalDirectoryTreeItem row,
  }) {
    final item = row.item;
    final normalizedPath = _normalizeDirectoryPath(item.path);
    final canExpand = item.isDirectory;
    final isExpanded =
        canExpand && _localDirectoryExpandedDirs.contains(normalizedPath);
    final isLoading =
        canExpand && _localDirectoryLoadingDirs.contains(normalizedPath);
    final secondaryPath =
        _normalizeDirectoryPathOrNull(_localDirectorySecondarySelection);
    final isSelected = secondaryPath != null
        ? secondaryPath == normalizedPath
        : _isPrimaryLocalDirectorySelected(item.path);
    final indent = 20.0 + (row.depth * _kLocalDirectoryIndentStep);
    return Padding(
      padding: EdgeInsets.only(left: indent),
      child: _ExplorerTile(
        selected: isSelected,
        onTap: () => unawaited(
            _activateLocalDirectoryExplorerItem(state, dataset, item)),
        child: Row(
          children: [
            SizedBox(
              width: 16,
              height: 16,
              child: canExpand
                  ? (isLoading
                      ? const Padding(
                          padding: EdgeInsets.all(2),
                          child: CircularProgressIndicator(
                            strokeWidth: 1.6,
                          ),
                        )
                      : IconButton(
                          onPressed: () => unawaited(
                            _toggleLocalDirectoryExplorerItem(
                              state,
                              dataset,
                              item,
                            ),
                          ),
                          icon: Icon(
                            isExpanded
                                ? Icons.expand_more
                                : Icons.chevron_right,
                            size: 13,
                          ),
                          padding: EdgeInsets.zero,
                          constraints: const BoxConstraints.tightFor(
                            width: 16,
                            height: 16,
                          ),
                          visualDensity: VisualDensity.compact,
                        ))
                  : const SizedBox.shrink(),
            ),
            const SizedBox(width: 4),
            Icon(
              canExpand
                  ? Icons.folder_outlined
                  : Icons.insert_drive_file_outlined,
              size: 13,
            ),
            const SizedBox(width: 6),
            Expanded(
              child: Text(
                item.name,
                style: _explorerListTextStyle(context),
                maxLines: 1,
                overflow: TextOverflow.ellipsis,
              ),
            ),
            if (!_isDirectoryItem(item) && item.size != null)
              SizedBox(
                width: 76,
                child: Padding(
                  padding: const EdgeInsets.only(left: 8),
                  child: _buildInlineMetaText(
                    context,
                    _formatBytes(item.size!),
                  ),
                ),
              ),
          ],
        ),
      ),
    );
  }

  Future<void> _activateLocalDirectoryExplorerItem(
    ViewerState state,
    LoadedDatasetSource dataset,
    LocalDirectoryItem item,
  ) async {
    if (!state.isDatasetActive(dataset.id)) {
      await state.activateDataset(dataset.id);
    }
    if (!mounted) return;
    _onLocalDirectoryItemTap(state, item);
  }

  Future<void> _toggleLocalDirectoryExplorerItem(
    ViewerState state,
    LoadedDatasetSource dataset,
    LocalDirectoryItem item,
  ) async {
    if (!item.isDirectory) return;
    if (!state.isDatasetActive(dataset.id)) {
      await state.activateDataset(dataset.id);
    }
    if (!mounted) return;
    _onLocalDirectoryToggle(state, item);
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
          crossAxisAlignment: CrossAxisAlignment.center,
          children: [
            Padding(
              padding: const EdgeInsets.only(top: 2),
              child: Icon(icon, size: 13),
            ),
            const SizedBox(width: 6),
            Expanded(
              flex: 3,
              child: ConstrainedBox(
                constraints: const BoxConstraints(minWidth: 0),
                child: DefaultTextStyle(
                  style: _explorerListTextStyle(context),
                  softWrap: true,
                  child: title,
                ),
              ),
            ),
            if (subtitle != null)
              SizedBox(
                width: 76,
                child: Padding(
                  padding: const EdgeInsets.only(left: 8),
                  child: _buildInlineMetaText(context, subtitle),
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
      case ViewerMode.localDirectory:
        return Icons.folder_open_outlined;
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
    if (state.mode == ViewerMode.localDirectory) {
      if (state.selectedLocalDirectoryItem != null) {
        return state.selectedLocalDirectoryItem!.name;
      }
      final count = state.localDirectoryItems.length;
      if (count > 0) return '$count items';
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
    if (state.mode == ViewerMode.localDirectory) {
      final selectedPath = _resolveSelectedLocalItemPath(state);
      if (selectedPath != null && _isLocalTabularFilePath(selectedPath)) {
        final fieldIndex = state.selectedFieldIndex;
        if (fieldIndex != null && fieldIndex >= 0) {
          return 'Column ${fieldIndex + 1}';
        }
      }
      return state.localSelectedFileName ??
          _activeSecondaryLocalDirectoryItem()?.name;
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
      final preview = state.hfPreview!;
      final visibleFeatureCount = _visibleHfFeatures(preview).length;
      if (preview.totalFeatureCount > 0 &&
          preview.totalFeatureCount != preview.features.length) {
        return '$visibleFeatureCount/${preview.totalFeatureCount} fields';
      }
      return '$visibleFeatureCount fields';
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
    if (state.mode == ViewerMode.localDirectory) {
      return state.localSelectedFileName ??
          _activeSecondaryLocalDirectoryItem()?.name;
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
          if (state.mode == ViewerMode.localDirectory) {
            return _buildLocalItemsPane(state);
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
          if (state.mode == ViewerMode.localDirectory) {
            return _buildLocalFieldsPane(state);
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
          if (state.mode == ViewerMode.localDirectory) {
            return _buildLocalPreview(state);
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
        if (!snapshot.hasData) {
          return const Center(child: Text('No fields found.'));
        }
        return _buildFieldList(state, snapshot.data!);
      },
    );
  }

  Widget _buildMdsFieldsPane(ViewerState state) {
    final future = state.mdsItemsFuture;
    if (future == null) {
      return const Center(child: Text('Select a sample.'));
    }
    return FutureBuilder<List<ItemMeta>>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No fields found.'));
        }
        return _buildFieldList(state, snapshot.data!);
      },
    );
  }

  Widget _buildLocalItemsPane(ViewerState state) {
    final future = state.localDirectoryItemsFuture;
    if (future == null) {
      return const Center(child: Text('Select a local directory.'));
    }
    return FutureBuilder<List<LocalDirectoryItem>>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No files found.'));
        }
        final items = snapshot.data!;
        if (items.isEmpty) {
          return const Center(child: Text('No files found.'));
        }
        _syncLocalDirectoryTree(state, items);
        _syncLocalDirectorySelectionFromState(state);
        final selectedPath = _resolveSelectedLocalItemPath(state);
        if (selectedPath != null && _isLocalTabularFilePath(selectedPath)) {
          final selectedItem = _localDirectoryItemByPath[selectedPath] ??
              _localDirectoryItemForPath(selectedPath) ??
              LocalDirectoryItem(
                name: p.basename(selectedPath),
                path: selectedPath,
                isDirectory: false,
              );
          return _buildLocalTabularItemsPane(state, selectedItem);
        }
        if (selectedPath != null &&
            _isLocalMdsShardFilePath(state, selectedPath)) {
          return _buildLocalMdsItemsPane(state, selectedPath);
        }
        final primaryPath =
            _normalizeDirectoryPathOrNull(_localDirectoryPrimarySelection);
        if (primaryPath == null) {
          return const Center(child: Text('Select a directory in Explorer.'));
        }
        return _buildLocalDirectoryChildrenPane(state, primaryPath);
      },
    );
  }

  bool _isLocalTabularFilePath(String path) {
    final normalized = _normalizeDirectoryPath(path);
    final ext = p.extension(normalized).toLowerCase();
    return ext == '.tsv' || ext == '.csv' || ext == '.parquet';
  }

  bool _isLocalMdsShardFilePath(ViewerState state, String path) {
    return state.isLocalDirectoryMdsShardPath(path);
  }

  Widget _buildLocalTabularItemsPane(
    ViewerState state,
    LocalDirectoryItem fileItem,
  ) {
    final normalizedPath = _normalizeDirectoryPath(fileItem.path);
    final future = _localTabularDataFutures[normalizedPath] ??
        _loadLocalTabularData(state, normalizedPath);
    return FutureBuilder<_LocalTabularData>(
      key: ValueKey('local-tabular-items-$normalizedPath'),
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No rows available.'));
        }
        final data = snapshot.data!;
        if (data.rows.isEmpty) {
          return const Center(child: Text('No rows available.'));
        }
        final selectedRow = _localTabularSelectedRowByPath[normalizedPath] ?? 0;
        return ListView.separated(
          itemCount: data.rows.length,
          separatorBuilder: (_, __) => const SizedBox(height: 6),
          itemBuilder: (context, index) {
            final row = data.rows[index];
            final rowKey = _tabularRowKey(index, row);
            final meta = '${row.length} fields';
            return HoverTile(
              selected: selectedRow == index,
              onTap: () {
                setState(() {
                  _localTabularSelectedRowByPath[normalizedPath] = index;
                });
                _ensureLocalTabularFieldSelection(state, row.length);
              },
              child: Row(
                children: [
                  Expanded(
                    child: Text(
                      rowKey,
                      style: Theme.of(context).textTheme.bodyMedium,
                      overflow: TextOverflow.ellipsis,
                    ),
                  ),
                  const SizedBox(width: 8),
                  _buildInlineMetaText(context, meta),
                ],
              ),
            );
          },
        );
      },
    );
  }

  Widget _buildLocalMdsItemsPane(ViewerState state, String shardPath) {
    final normalizedPath = _normalizeDirectoryPath(shardPath);
    final future = _localDirectoryMdsItemsFutures[normalizedPath] ??
        state.listLocalDirectoryMdsItems(normalizedPath);
    _localDirectoryMdsItemsFutures[normalizedPath] = future;
    return FutureBuilder<List<ItemMeta>>(
      key: ValueKey('local-mds-items-$normalizedPath'),
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (snapshot.hasError) {
          _localDirectoryMdsItemsFutures.remove(normalizedPath);
          return Center(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                Text(
                  'Failed to load MDS items: ${snapshot.error}',
                  textAlign: TextAlign.center,
                ),
                const SizedBox(height: 10),
                OutlinedButton(
                  onPressed: () => setState(() {}),
                  child: const Text('Retry'),
                ),
              ],
            ),
          );
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No samples found.'));
        }
        final items = snapshot.data!;
        if (items.isEmpty) {
          return const Center(child: Text('No samples found.'));
        }
        return ListView.separated(
          itemCount: items.length,
          separatorBuilder: (_, __) => const SizedBox(height: 6),
          itemBuilder: (context, index) {
            final item = items[index];
            return HoverTile(
              selected: state.selectedItemIndex == item.itemIndex,
              onTap: () => state.selectItem(item.itemIndex,
                  fieldCount: item.fields.length),
              child: Row(
                children: [
                  Expanded(
                    child: Text(
                      'Item ${item.itemIndex}',
                      style: Theme.of(context).textTheme.bodyMedium,
                      overflow: TextOverflow.ellipsis,
                    ),
                  ),
                  const SizedBox(width: 8),
                  _buildInlineMetaText(
                    context,
                    '${item.fields.length} fields · ${_formatBytes(item.totalBytes)}',
                  ),
                ],
              ),
            );
          },
        );
      },
    );
  }

  String _tabularRowKey(int index, List<String> row) {
    if (row.isNotEmpty) {
      final key = row.first.trim();
      if (key.isNotEmpty) {
        return key;
      }
    }
    return 'Row ${index + 1}';
  }

  void _ensureLocalTabularFieldSelection(ViewerState state, int fieldCount) {
    if (fieldCount <= 0) {
      state.selectField(null);
      return;
    }
    final current = state.selectedFieldIndex;
    if (current == null || current < 0 || current >= fieldCount) {
      state.selectField(0);
    }
  }

  Future<_LocalTabularData> _loadLocalTabularData(
    ViewerState state,
    String filePath,
  ) {
    final normalizedPath = _normalizeDirectoryPath(filePath);
    final existing = _localTabularDataFutures[normalizedPath];
    if (existing != null) return existing;
    final future = () async {
      try {
        final ext = p.extension(normalizedPath).toLowerCase();
        if (ext == '.parquet') {
          final preview = await state.readDirectoryParquetTable(
            normalizedPath,
            offset: 0,
            length: _kLocalTabularMaxRows,
          );
          return _LocalTabularData(
            headers: preview.headers,
            rows: preview.rows,
          );
        }
        final text = await state.readDirectoryFileText(normalizedPath);
        final lines = <String>[];
        for (final line in const LineSplitter().convert(text)) {
          if (line.trim().isEmpty) {
            continue;
          }
          lines.add(line);
          if (lines.length >= _kLocalTabularMaxRows + 1) {
            break;
          }
        }
        if (lines.isEmpty) {
          return const _LocalTabularData(
            headers: <String>[],
            rows: <List<String>>[],
          );
        }
        final delimiter = ext == '.tsv' ? '\t' : ',';
        final parsedRows = <List<String>>[];
        for (final line in lines) {
          parsedRows.add(_splitLocalDelimitedLine(line, delimiter));
        }
        if (parsedRows.isEmpty) {
          return const _LocalTabularData(
            headers: <String>[],
            rows: <List<String>>[],
          );
        }
        final hasHeader = _looksLikeTabularHeader(parsedRows);
        final headers = hasHeader
            ? parsedRows.first.map((cell) => cell.trim()).toList()
            : const <String>[];
        final rows =
            hasHeader ? parsedRows.skip(1).toList(growable: false) : parsedRows;
        return _LocalTabularData(headers: headers, rows: rows);
      } catch (_) {
        return const _LocalTabularData(
          headers: <String>[],
          rows: <List<String>>[],
        );
      }
    }();
    _localTabularDataFutures[normalizedPath] = future;
    return future;
  }

  bool _looksLikeTabularHeader(List<List<String>> rows) {
    if (rows.length < 2) return false;
    final first = rows.first;
    final second = rows[1];
    if (first.isEmpty || second.isEmpty) return false;
    if (first.length != second.length) return false;

    var headerLikeCount = 0;
    var firstRowDataLikeCount = 0;
    var secondRowDataLikeCount = 0;

    for (var i = 0; i < first.length; i += 1) {
      final firstCell = first[i].trim();
      final secondCell = second[i].trim();
      if (_isHeaderLikeToken(firstCell)) {
        headerLikeCount += 1;
      }
      if (_isDataLikeToken(firstCell)) {
        firstRowDataLikeCount += 1;
      }
      if (_isDataLikeToken(secondCell)) {
        secondRowDataLikeCount += 1;
      }
    }

    final width = first.length;
    final minHeaderLike = (width / 2).ceil();
    final minSecondDataLike = (width / 2).ceil();
    return headerLikeCount >= minHeaderLike &&
        secondRowDataLikeCount >= minSecondDataLike &&
        firstRowDataLikeCount < headerLikeCount;
  }

  bool _isHeaderLikeToken(String token) {
    if (token.isEmpty) return false;
    final compact = token.trim();
    if (compact.length > 64) return false;
    final headerPattern = RegExp(r'^[A-Za-z_][A-Za-z0-9_ ]*$');
    if (!headerPattern.hasMatch(compact)) return false;
    if (_isNumericToken(compact)) return false;
    if (_isIdLikeToken(compact)) return false;
    return true;
  }

  bool _isDataLikeToken(String token) {
    if (token.isEmpty) return false;
    final compact = token.trim();
    if (_isNumericToken(compact)) return true;
    if (_isIdLikeToken(compact)) return true;
    if (compact.length > 24) return true;
    if (compact.contains(' ')) return true;
    if (compact.runes.any((code) => code > 127)) return true;
    return false;
  }

  bool _isNumericToken(String token) {
    final numericPattern = RegExp(r'^[+-]?(?:\d+|\d*\.\d+)$');
    return numericPattern.hasMatch(token);
  }

  bool _isIdLikeToken(String token) {
    final idPattern = RegExp(r'^[A-Za-z0-9]+(?:[-_][A-Za-z0-9]+)+$');
    return idPattern.hasMatch(token);
  }

  List<String> _splitLocalDelimitedLine(String line, String delimiter) {
    if (delimiter == '\t') {
      return line.split('\t');
    }
    final parts = <String>[];
    final buffer = StringBuffer();
    var inQuotes = false;
    for (var i = 0; i < line.length; i += 1) {
      final char = line[i];
      if (char == '"') {
        final hasNextQuote = i + 1 < line.length && line[i + 1] == '"';
        if (inQuotes && hasNextQuote) {
          buffer.write('"');
          i += 1;
          continue;
        }
        inQuotes = !inQuotes;
        continue;
      }
      if (!inQuotes && char == delimiter) {
        parts.add(buffer.toString());
        buffer.clear();
        continue;
      }
      buffer.write(char);
    }
    parts.add(buffer.toString());
    return parts;
  }

  void _syncLocalDirectoryTree(
    ViewerState state,
    List<LocalDirectoryItem> rootItems,
  ) {
    final datasetId = state.activeDatasetId ?? state.sourceInput;
    if (_localDirectoryTreeDatasetId != datasetId) {
      _localDirectoryTreeDatasetId = datasetId;
      _localDirectoryTreeIsRemote = state.isRemoteDirectoryMode;
      _localDirectoryExpandedDirs.clear();
      _localDirectoryLoadingDirs.clear();
      _localDirectoryChildren.clear();
      _localDirectoryChildrenFutures.clear();
      _localTabularDataFutures.clear();
      _localDirectoryMdsItemsFutures.clear();
      _localDirectoryMdsFormatFutures.clear();
      _localTabularSelectedRowByPath.clear();
      _localDirectoryItemByPath.clear();
      _localDirectoryPrimarySelection = null;
      _localDirectorySecondarySelection = null;
    }
    _localDirectoryTreeIsRemote = state.isRemoteDirectoryMode;
    _indexLocalDirectoryItems(rootItems);
    _localDirectoryRootItems = List<LocalDirectoryItem>.from(rootItems);
    state.registerLocalDirectoryItems(rootItems);
    if (rootItems.isEmpty) {
      _localDirectoryPrimarySelection = null;
      _localDirectorySecondarySelection = null;
    } else {
      final fallback = _resolvePrimaryLocalDirectorySelection(rootItems);
      _localDirectoryPrimarySelection = fallback;
    }
    _validateLocalSelections();
  }

  String? _resolvePrimaryLocalDirectorySelection(
    List<LocalDirectoryItem> rootItems,
  ) {
    final normalizedCurrent = _normalizeDirectoryPathOrNull(
      _localDirectoryPrimarySelection,
    );
    if (normalizedCurrent != null && _isDirectoryPathValid(normalizedCurrent)) {
      return normalizedCurrent;
    }
    final firstDirectory =
        rootItems.where((item) => item.isDirectory).firstOrNull;
    if (firstDirectory != null) {
      return _normalizeDirectoryPath(firstDirectory.path);
    }
    final fallback = rootItems.first;
    final parent = _normalizeDirectoryPath(p.dirname(fallback.path));
    if (parent.isNotEmpty) {
      return parent;
    }
    return _normalizeDirectoryPath(fallback.path);
  }

  bool _isDirectoryItem(LocalDirectoryItem item) {
    return item.isDirectory;
  }

  bool _localDirectoryPathExists(String path) {
    final normalized = _normalizeDirectoryPath(path);
    if (_localDirectoryItemByPath.containsKey(normalized)) {
      return true;
    }
    if (_localDirectoryTreeIsRemote) {
      return false;
    }
    try {
      return FileSystemEntity.typeSync(normalized, followLinks: true) !=
          FileSystemEntityType.notFound;
    } catch (_) {
      return false;
    }
  }

  LocalDirectoryItem? _localDirectoryItemForPath(String path) {
    final normalized = _normalizeDirectoryPath(path);
    final inCache = _localDirectoryItemByPath[normalized];
    if (inCache != null) return inCache;

    if (_localDirectoryTreeIsRemote) return null;
    if (!_localDirectoryPathExists(normalized)) return null;
    try {
      final stat = FileStat.statSync(normalized);
      return LocalDirectoryItem(
        name: p.basename(normalized),
        path: normalized,
        isDirectory: stat.type == FileSystemEntityType.directory,
        size: stat.type == FileSystemEntityType.directory ? null : stat.size,
        modifiedAt: stat.modified,
      );
    } catch (_) {
      return null;
    }
  }

  void _indexLocalDirectoryItems(Iterable<LocalDirectoryItem> items) {
    for (final item in items) {
      final normalized = _normalizeDirectoryPath(item.path);
      _localDirectoryItemByPath[normalized] = item;
    }
  }

  void _setLocalPrimaryDirectorySelection(String? path) {
    final selected =
        path == null || path.isEmpty ? null : _normalizeDirectoryPath(path);
    _localDirectoryPrimarySelection = selected;
  }

  void _setLocalSecondaryDirectorySelection(String? path) {
    final selected =
        path == null || path.isEmpty ? null : _normalizeDirectoryPath(path);
    _localDirectorySecondarySelection = selected;
  }

  void _validateLocalSelections() {
    final primary = _localDirectoryPrimarySelection;
    if (primary == null || !_isDirectoryPathValid(primary)) {
      _localDirectorySecondarySelection = null;
      return;
    }
    final secondary =
        _normalizeDirectoryPathOrNull(_localDirectorySecondarySelection);
    if (secondary == null) {
      return;
    }
    final selected = _localDirectoryItemByPath[secondary] ??
        _localDirectoryItemForPath(secondary);
    if (selected == null) {
      _localDirectorySecondarySelection = null;
      return;
    }
    final secondParent = _normalizeDirectoryPath(p.dirname(selected.path));
    if (!_samePath(secondParent, primary)) {
      _localDirectorySecondarySelection = null;
    }
  }

  bool _samePath(String? left, String? right) {
    if (left == null || right == null) return false;
    return _normalizeDirectoryPath(left) == _normalizeDirectoryPath(right);
  }

  LocalDirectoryItem? _activeSecondaryLocalDirectoryItem() {
    final path =
        _normalizeDirectoryPathOrNull(_localDirectorySecondarySelection);
    if (path == null) return null;
    return _localDirectoryItemByPath[path] ?? _localDirectoryItemForPath(path);
  }

  bool _isDirectoryPathValid(String path) {
    final normalizedPath = _normalizeDirectoryPath(path);
    final cached = _localDirectoryItemByPath[normalizedPath];
    if (cached != null) return cached.isDirectory;
    if (_localDirectoryTreeIsRemote) {
      final hasRootChild = _localDirectoryRootItems.any(
        (item) =>
            _normalizeDirectoryPath(p.dirname(item.path)) == normalizedPath,
      );
      if (hasRootChild) return true;
      return _localDirectoryChildren.containsKey(normalizedPath);
    }
    try {
      return FileSystemEntity.typeSync(
            normalizedPath,
            followLinks: true,
          ) ==
          FileSystemEntityType.directory;
    } catch (_) {
      return false;
    }
  }

  bool _isPrimaryLocalDirectorySelected(String path) {
    if (_localDirectoryPrimarySelection == null) return false;
    return _normalizeDirectoryPath(path) == _localDirectoryPrimarySelection;
  }

  List<_LocalDirectoryTreeItem> _buildLocalDirectoryTreeRows() {
    final rows = <_LocalDirectoryTreeItem>[];
    void visit(List<LocalDirectoryItem> items, int depth) {
      for (final item in items) {
        rows.add(_LocalDirectoryTreeItem(item: item, depth: depth));
        if (!_isDirectoryItem(item)) {
          continue;
        }
        final path = _normalizeDirectoryPath(item.path);
        if (!_localDirectoryExpandedDirs.contains(path)) {
          continue;
        }
        final children = _localDirectoryChildren[path];
        if (children == null || children.isEmpty) {
          continue;
        }
        visit(children, depth + 1);
      }
    }

    visit(_localDirectoryRootItems, 0);
    return rows;
  }

  void _onLocalDirectoryItemTap(
    ViewerState state,
    LocalDirectoryItem item,
  ) {
    final normalizedPath = _normalizeDirectoryPath(item.path);
    if (_isDirectoryItem(item)) {
      _setLocalPrimaryDirectorySelection(normalizedPath);
      _setLocalSecondaryDirectorySelection(null);
      _localTabularSelectedRowByPath.remove(normalizedPath);
      state.selectField(null);
      _ensureLocalDirectoryChildrenForFields(state, normalizedPath);
      state.selectLocalDirectoryItemByPath(normalizedPath);
    } else {
      final parentPath = _normalizeDirectoryPath(p.dirname(normalizedPath));
      if (_localDirectoryItemByPath[parentPath]?.isDirectory == true ||
          _isDirectoryPathValid(parentPath)) {
        _setLocalPrimaryDirectorySelection(parentPath);
      } else {
        _setLocalPrimaryDirectorySelection(normalizedPath);
      }
      _setLocalSecondaryDirectorySelection(normalizedPath);
      state.selectLocalDirectoryItemByPath(normalizedPath);
      if (_isLocalTabularFilePath(normalizedPath)) {
        _localTabularSelectedRowByPath.putIfAbsent(normalizedPath, () => 0);
        _ensureLocalTabularFieldSelection(state, 1);
      } else {
        _localTabularSelectedRowByPath.remove(normalizedPath);
        state.selectField(null);
      }
      final parent = _localDirectoryPrimarySelection;
      if (parent != null && parent.isNotEmpty) {
        _ensureLocalDirectoryChildrenForFields(state, parent);
      }
    }
    _validateLocalSelections();
    setState(() {});
  }

  void _onLocalDirectoryToggle(ViewerState state, LocalDirectoryItem item) {
    if (!_isDirectoryItem(item)) return;
    final path = _normalizeDirectoryPath(item.path);
    _setLocalPrimaryDirectorySelection(path);
    _setLocalSecondaryDirectorySelection(null);
    state.selectLocalDirectoryItemByPath(path);
    _ensureLocalDirectoryChildrenForFields(state, path);
    if (_localDirectoryExpandedDirs.contains(path)) {
      _collapseLocalDirectoryTree(path);
      return;
    }
    unawaited(_expandLocalDirectoryFolder(state, path));
  }

  Future<void> _expandLocalDirectoryFolder(
    ViewerState state,
    String path,
  ) async {
    final normalizedPath = _normalizeDirectoryPath(path);
    final queryPath = _directoryListQueryPath(state, normalizedPath);
    if (_localDirectoryLoadingDirs.contains(normalizedPath)) return;
    _localDirectoryLoadingDirs.add(normalizedPath);
    setState(() {});
    try {
      final children = await state.listLocalDirectoryItems(queryPath);
      if (!mounted) return;
      _indexLocalDirectoryItems(children);
      state.registerLocalDirectoryItems(children);
      if (children.isEmpty) {
        _localDirectoryChildren[normalizedPath] = const <LocalDirectoryItem>[];
        _localDirectoryExpandedDirs.remove(normalizedPath);
      } else {
        _localDirectoryChildren[normalizedPath] = children;
        _localDirectoryExpandedDirs.add(normalizedPath);
      }
    } finally {
      _localDirectoryLoadingDirs.remove(normalizedPath);
      if (mounted) {
        setState(() {});
      }
    }
  }

  void _collapseLocalDirectoryTree(String path) {
    _localDirectoryExpandedDirs.removeWhere(
      (candidate) => _isDirectoryDescendant(candidate, path),
    );
    _localDirectoryLoadingDirs.removeWhere(
      (candidate) => _isDirectoryDescendant(candidate, path),
    );
    setState(() {});
  }

  bool _isDirectoryDescendant(String candidatePath, String ancestorPath) {
    final candidate = _normalizeDirectoryPath(candidatePath);
    final ancestor = _normalizeDirectoryPath(ancestorPath);
    if (candidate == ancestor) {
      return true;
    }
    return candidate.startsWith('$ancestor${p.separator}');
  }

  Widget _buildLocalFieldsPane(ViewerState state) {
    _syncLocalDirectorySelectionFromState(state);
    final selectedPath = _resolveSelectedLocalItemPath(state);
    if (selectedPath != null && _isLocalTabularFilePath(selectedPath)) {
      final tabularItem = _localDirectoryItemByPath[selectedPath] ??
          _localDirectoryItemForPath(selectedPath) ??
          LocalDirectoryItem(
            name: p.basename(selectedPath),
            path: selectedPath,
            isDirectory: false,
          );
      return _buildLocalTabularFieldsPane(state, tabularItem);
    }
    if (selectedPath != null && _isLocalMdsShardFilePath(state, selectedPath)) {
      return _buildLocalMdsFieldsPane(state, selectedPath);
    }
    final selectedItem = state.selectedLocalDirectoryItem ??
        _activeSecondaryLocalDirectoryItem();
    if (selectedItem == null) {
      return const Center(child: Text('Select an item.'));
    }
    return _buildLocalFileFieldsPane(state, item: selectedItem);
  }

  Widget _buildLocalMdsFieldsPane(ViewerState state, String shardPath) {
    final normalizedPath = _normalizeDirectoryPath(shardPath);
    final future = _localDirectoryMdsItemsFutures[normalizedPath] ??
        state.listLocalDirectoryMdsItems(normalizedPath);
    _localDirectoryMdsItemsFutures[normalizedPath] = future;
    final formatFuture = _localDirectoryMdsFormatFutures[normalizedPath] ??
        state.localDirectoryMdsFieldFormats(normalizedPath);
    _localDirectoryMdsFormatFutures[normalizedPath] = formatFuture;
    return FutureBuilder<List<ItemMeta>>(
      key: ValueKey('local-mds-fields-$normalizedPath'),
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No fields found.'));
        }
        final items = snapshot.data!;
        if (items.isEmpty) {
          return const Center(child: Text('No fields found.'));
        }
        final selectedItemIndex = state.selectedItemIndex;
        if (selectedItemIndex == null) {
          return const Center(child: Text('Select an item.'));
        }
        final selected = items
            .where((item) => item.itemIndex == selectedItemIndex)
            .firstOrNull;
        if (selected == null) {
          return const Center(child: Text('Select an item.'));
        }
        final fields = selected.fields;
        if (fields.isEmpty) {
          return const Center(child: Text('No fields found.'));
        }
        return FutureBuilder<List<String>>(
          future: formatFuture,
          builder: (context, formatSnapshot) {
            if (formatSnapshot.hasError) {
              _localDirectoryMdsFormatFutures.remove(normalizedPath);
            }
            final formatByIndex = formatSnapshot.data ?? const <String>[];
            return ListView.separated(
              itemCount: fields.length,
              separatorBuilder: (_, __) => const SizedBox(height: 6),
              itemBuilder: (context, index) {
                final field = fields[index];
                final format = field.fieldIndex < formatByIndex.length
                    ? formatByIndex[field.fieldIndex]
                    : null;
                final meta = _fieldMetaFromFormat(format, field.size);
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
                      _buildInlineMetaText(context, meta),
                    ],
                  ),
                );
              },
            );
          },
        );
      },
    );
  }

  Widget _buildLocalTabularFieldsPane(
    ViewerState state,
    LocalDirectoryItem fileItem,
  ) {
    final normalizedPath = _normalizeDirectoryPath(fileItem.path);
    final future = _localTabularDataFutures[normalizedPath] ??
        _loadLocalTabularData(state, normalizedPath);
    return FutureBuilder<_LocalTabularData>(
      key: ValueKey('local-tabular-fields-$normalizedPath'),
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No fields found.'));
        }
        final data = snapshot.data!;
        if (data.rows.isEmpty) {
          return const Center(child: Text('No fields found.'));
        }
        final rowIndex = _localTabularSelectedRowByPath[normalizedPath];
        if (rowIndex == null || rowIndex < 0 || rowIndex >= data.rows.length) {
          return const Center(child: Text('Select a row.'));
        }
        final row = data.rows[rowIndex];
        final headers = data.headers;
        return ListView.separated(
          itemCount: row.length,
          separatorBuilder: (_, __) => const SizedBox(height: 6),
          itemBuilder: (context, index) {
            final name = index < headers.length && headers[index].isNotEmpty
                ? headers[index]
                : 'Column ${index + 1}';
            final value = row[index];
            return HoverTile(
              selected: state.selectedFieldIndex == index,
              onTap: () => state.selectField(index),
              child: Row(
                children: [
                  Expanded(
                    child: Text(
                      name,
                      style: Theme.of(context).textTheme.bodyMedium,
                      overflow: TextOverflow.ellipsis,
                    ),
                  ),
                  const SizedBox(width: 8),
                  _buildInlineMetaText(context, value),
                ],
              ),
            );
          },
        );
      },
    );
  }

  Widget _buildLocalDirectoryChildrenPane(
    ViewerState state,
    String primaryDirectoryPath,
  ) {
    final path = _normalizeDirectoryPath(primaryDirectoryPath);
    final cachedChildren = _localDirectoryChildren[path];
    if (cachedChildren != null) {
      _validateLocalChildSelection(path, cachedChildren);
      return _buildLocalDirectoryChildrenList(state, cachedChildren);
    }
    final childrenFuture = _localDirectoryChildrenFutures[path] ??
        _loadLocalDirectoryChildrenForFields(
          state,
          path,
        );
    return FutureBuilder<List<LocalDirectoryItem>>(
      key: ValueKey('local-directory-children-$path'),
      future: childrenFuture,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData || snapshot.data == null) {
          return const Center(child: Text('No files found.'));
        }
        final children = snapshot.data!;
        _localDirectoryChildren[path] = children;
        if (children.isEmpty) {
          return const Center(child: Text('No files found.'));
        }
        _indexLocalDirectoryItems(children);
        state.registerLocalDirectoryItems(children);
        _validateLocalChildSelection(path, children);
        return _buildLocalDirectoryChildrenList(state, children);
      },
    );
  }

  void _validateLocalChildSelection(
    String parentPath,
    List<LocalDirectoryItem> children,
  ) {
    final selected =
        _normalizeDirectoryPathOrNull(_localDirectorySecondarySelection);
    if (selected == null) return;
    final found =
        children.any((item) => _normalizeDirectoryPath(item.path) == selected);
    if (found) return;
    final selectedItem = _localDirectoryItemByPath[selected];
    if (selectedItem == null) {
      _localDirectorySecondarySelection = null;
      return;
    }
    final selectedParent =
        _normalizeDirectoryPath(p.dirname(selectedItem.path));
    if (!_samePath(selectedParent, parentPath)) {
      _localDirectorySecondarySelection = null;
    }
  }

  Widget _buildLocalDirectoryChildrenList(
    ViewerState state,
    List<LocalDirectoryItem> children,
  ) {
    if (children.isEmpty) {
      return const Center(child: Text('No files found.'));
    }
    _indexLocalDirectoryItems(children);
    state.registerLocalDirectoryItems(children);
    final selectedPath =
        _normalizeDirectoryPathOrNull(_localDirectorySecondarySelection);
    return ListView.separated(
      itemCount: children.length,
      separatorBuilder: (_, __) => const SizedBox(height: 6),
      itemBuilder: (context, index) {
        final item = children[index];
        final normalizedItemPath = _normalizeDirectoryPath(item.path);
        final isSelected = selectedPath == normalizedItemPath;
        return HoverTile(
          selected: isSelected,
          onTap: () => _onLocalDirectoryChildTap(state, item),
          child: Row(
            children: [
              Icon(
                _isDirectoryItem(item)
                    ? Icons.folder_outlined
                    : Icons.insert_drive_file_outlined,
                size: 16,
                color: Theme.of(context).colorScheme.primary,
              ),
              const SizedBox(width: 8),
              Expanded(
                child: Text(
                  item.name,
                  style: Theme.of(context).textTheme.bodyMedium,
                  overflow: TextOverflow.ellipsis,
                ),
              ),
              const SizedBox(width: 8),
              _buildInlineMetaText(
                context,
                _isDirectoryItem(item) || item.size == null
                    ? 'Directory'
                    : _formatBytes(item.size!),
              ),
            ],
          ),
        );
      },
    );
  }

  void _onLocalDirectoryChildTap(ViewerState state, LocalDirectoryItem item) {
    final normalizedPath = _normalizeDirectoryPath(item.path);
    _setLocalSecondaryDirectorySelection(normalizedPath);
    state.selectLocalDirectoryItemByPath(normalizedPath);
    setState(() {
      if (_isLocalTabularFilePath(normalizedPath)) {
        _localTabularSelectedRowByPath.putIfAbsent(normalizedPath, () => 0);
        _ensureLocalTabularFieldSelection(state, 1);
      } else {
        _localTabularSelectedRowByPath.remove(normalizedPath);
        state.selectField(null);
      }
    });
  }

  void _ensureLocalDirectoryChildrenForFields(
    ViewerState state,
    String path,
  ) {
    final normalizedPath = _normalizeDirectoryPath(path);
    final queryPath = _directoryListQueryPath(state, normalizedPath);
    if (_localDirectoryChildrenFutures.containsKey(normalizedPath)) {
      return;
    }
    _localDirectoryChildrenFutures[normalizedPath] =
        state.listLocalDirectoryItems(queryPath);
  }

  String _normalizeDirectoryPath(String path) {
    return p.normalize(path);
  }

  String _directoryListQueryPath(ViewerState state, String normalizedPath) {
    if (!state.isRemoteDirectoryMode) {
      return normalizedPath;
    }
    // `p.dirname('file.ext')` produces ".", but remote providers use "" for root.
    if (normalizedPath == '.') {
      return '';
    }
    return normalizedPath;
  }

  String? _normalizeDirectoryPathOrNull(String? path) {
    if (path == null || path.isEmpty) return null;
    return _normalizeDirectoryPath(path);
  }

  String? _resolveSelectedLocalItemPath(ViewerState state) {
    final candidates = <String?>[
      _normalizeDirectoryPathOrNull(state.selectedChunkName),
      _normalizeDirectoryPathOrNull(state.selectedLocalDirectoryItem?.path),
      _normalizeDirectoryPathOrNull(_localDirectorySecondarySelection),
    ];
    for (final candidate in candidates) {
      if (candidate == null) continue;
      final resolved = _resolveLocalPathAgainstDatasetRoot(state, candidate);
      if (resolved != null) {
        return resolved;
      }
    }
    return null;
  }

  String? _resolveLocalPathAgainstDatasetRoot(ViewerState state, String path) {
    final normalized = _normalizeDirectoryPath(path);
    if (state.isRemoteDirectoryMode) {
      return normalized;
    }
    if (p.isAbsolute(normalized)) {
      return normalized;
    }
    final root = _normalizeDirectoryPathOrNull(state.sourceInput);
    if (root == null) {
      return normalized;
    }
    return _normalizeDirectoryPath(p.join(root, normalized));
  }

  Future<List<LocalDirectoryItem>> _loadLocalDirectoryChildrenForFields(
    ViewerState state,
    String path,
  ) {
    final normalizedPath = _normalizeDirectoryPath(path);
    final queryPath = _directoryListQueryPath(state, normalizedPath);
    final future = state.listLocalDirectoryItems(queryPath);
    _localDirectoryChildrenFutures[normalizedPath] = future;
    return future;
  }

  void _syncLocalDirectorySelectionFromState(ViewerState state) {
    final selectedPath = _normalizeDirectoryPathOrNull(state.selectedChunkName);
    if (selectedPath == null) {
      return;
    }
    final selectedItem = _localDirectoryItemByPath[selectedPath] ??
        _localDirectoryItemForPath(selectedPath);
    if (selectedItem == null || selectedItem.isDirectory) {
      return;
    }
    _localDirectorySecondarySelection = selectedPath;
    final parentPath = _normalizeDirectoryPath(p.dirname(selectedItem.path));
    if (_isDirectoryPathValid(parentPath)) {
      _localDirectoryPrimarySelection = parentPath;
    }
  }

  Widget _buildLocalFileFieldsPane(ViewerState state,
      {LocalDirectoryItem? item}) {
    final targetItem = item ??
        state.selectedLocalDirectoryItem ??
        _activeSecondaryLocalDirectoryItem();
    if (targetItem == null) {
      return const Center(child: Text('Select a file.'));
    }
    final fields = _localDirectoryMetadata(targetItem);
    if (fields.isEmpty) {
      return const Center(child: Text('No metadata found.'));
    }
    return ListView.separated(
      itemCount: fields.length,
      separatorBuilder: (_, __) => const SizedBox(height: 6),
      itemBuilder: (context, index) {
        final field = fields[index];
        return HoverTile(
          selected: state.selectedFieldIndex == index,
          onTap: () => state.selectField(index),
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
              _buildInlineMetaText(context, field.value),
            ],
          ),
        );
      },
    );
  }

  List<LocalDirectoryField> _localDirectoryMetadata(LocalDirectoryItem item) {
    final size = item.size == null ? 0 : item.size!;
    return <LocalDirectoryField>[
      LocalDirectoryField(name: 'Name', value: item.name),
      LocalDirectoryField(
          name: 'Type', value: item.isDirectory ? 'Directory' : 'File'),
      LocalDirectoryField(name: 'Path', value: item.path),
      LocalDirectoryField(
          name: 'Size', value: item.size == null ? '-' : _formatBytes(size)),
      LocalDirectoryField(
        name: 'Modified',
        value: item.modifiedAt == null
            ? '-'
            : item.modifiedAt!.toLocal().toString(),
      ),
    ];
  }

  Widget _buildLocalPreview(ViewerState state) {
    _syncLocalDirectorySelectionFromState(state);
    final selectedPath = _resolveSelectedLocalItemPath(state);
    if (selectedPath != null && _isLocalTabularFilePath(selectedPath)) {
      return _buildLocalTabularPreview(state, selectedPath);
    }
    final item = state.selectedLocalDirectoryItem ??
        _activeSecondaryLocalDirectoryItem();
    if (item == null) {
      return const Center(child: Text('Select an item.'));
    }
    if (item.isDirectory) {
      return const Center(child: Text('Directory selected.'));
    }
    if (_isLocalMdsShardFilePath(state, item.path)) {
      if (state.selectedItemIndex == null) {
        return const Center(child: Text('Select an item.'));
      }
      if (state.selectedFieldIndex == null) {
        return const Center(child: Text('Select a field.'));
      }
    }
    final future = state.localFilePreviewFuture;
    if (future == null) {
      return const Center(child: Text('No preview.'));
    }
    return FutureBuilder<FieldPreview>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingPreview();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No preview.'));
        }
        final preview = snapshot.data!;
        return _buildPreviewContent(
          state,
          preview,
          sourcePath: selectedPath,
        );
      },
    );
  }

  Widget _buildLocalTabularPreview(ViewerState state, String filePath) {
    final normalizedPath = _normalizeDirectoryPath(filePath);
    final future = _localTabularDataFutures[normalizedPath] ??
        _loadLocalTabularData(state, normalizedPath);
    return FutureBuilder<_LocalTabularData>(
      key: ValueKey('local-tabular-preview-$normalizedPath'),
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingPreview();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No preview.'));
        }
        final data = snapshot.data!;
        if (data.rows.isEmpty) {
          return const Center(child: Text('No preview.'));
        }
        final rowIndex = _localTabularSelectedRowByPath[normalizedPath];
        if (rowIndex == null || rowIndex < 0 || rowIndex >= data.rows.length) {
          return const Center(child: Text('Select a row.'));
        }
        final row = data.rows[rowIndex];
        final fieldIndex = state.selectedFieldIndex;
        if (fieldIndex == null || fieldIndex < 0 || fieldIndex >= row.length) {
          return const Center(child: Text('Select a field.'));
        }
        final value = row[fieldIndex];
        return _PreviewSection(content: _CodeBlock(text: value));
      },
    );
  }

  Widget _buildWdsFieldsPaneFromState(ViewerState state) {
    final future = state.wdsSamplesFuture;
    if (future == null) {
      return const Center(child: Text('Select a sample.'));
    }
    return FutureBuilder<WdsSampleListResponse>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No fields found.'));
        }
        return _buildWdsFieldsPane(state, snapshot.data!.samples);
      },
    );
  }

  Widget _buildHfFieldsPaneFromState(ViewerState state) {
    final preview = state.hfPreview;
    if (preview != null) {
      return _buildHfFieldsPane(state, preview);
    }
    final future = state.hfPreviewFuture;
    if (future == null) {
      return const Center(child: Text('No dataset loaded.'));
    }
    return FutureBuilder<HfDatasetPreview>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No fields found.'));
        }
        return _buildHfFieldsPane(state, snapshot.data!);
      },
    );
  }

  Widget _buildLitdataItemsPane(ViewerState state) {
    final future = state.litdataItemsFuture;
    if (future == null) {
      return const Center(child: Text('Select a chunk.'));
    }
    return FutureBuilder<List<ItemMeta>>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No items found.'));
        }
        final items = snapshot.data!;
        return _buildItemList(state, items);
      },
    );
  }

  Widget _buildMdsItemsPane(ViewerState state) {
    final future = state.mdsItemsFuture;
    if (future == null) {
      return const Center(child: Text('Select a shard.'));
    }
    return FutureBuilder<List<ItemMeta>>(
      future: future,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No samples found.'));
        }
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
        if (!snapshot.hasData) {
          return const Center(child: Text('No samples found.'));
        }
        final response = snapshot.data!;
        final samples = response.samples;
        final total = response.numSamplesTotal;
        final pageSize = response.length;
        final canGoNext = total == null
            ? samples.length == pageSize && pageSize > 0
            : (response.offset + samples.length) < total;
        final canGoPrev = response.offset > 0 && pageSize > 0;
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
        if (!snapshot.hasData) {
          return const Center(child: Text('No rows available.'));
        }
        final preview = snapshot.data!;
        final hasKnownTotal = preview.numRowsTotal > 0;
        final pageSize = preview.length;
        final pageCount = preview.rows.length;
        final canGoPrev = preview.offset > 0 && pageSize > 0;
        final prevOffset = preview.offset - pageSize;
        final canGoNext = hasKnownTotal
            ? preview.offset + pageCount < preview.numRowsTotal
            : preview.partial || (pageSize > 0 && pageCount >= pageSize);
        final nextOffset =
            preview.offset + (pageCount > 0 ? pageCount : pageSize);
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
                    onPressed: canGoPrev
                        ? () => state.setHfOffset(prevOffset < 0 ? 0 : prevOffset)
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
                    onPressed: canGoNext
                        ? () => state.setHfOffset(nextOffset)
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
    var rowNumber = requested;
    if (rowNumber < 1) rowNumber = 1;
    if (total <= 0) {
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
      return;
    }
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
    final features = _visibleHfFeatures(preview);
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
      constraints: const BoxConstraints(maxWidth: 180),
      child: Align(
        alignment: Alignment.centerRight,
        child: Text(
          text,
          style: Theme.of(context).textTheme.bodySmall,
          textAlign: TextAlign.right,
          maxLines: 1,
          overflow: TextOverflow.ellipsis,
          softWrap: false,
        ),
      ),
    );
  }

  Widget _buildZenodoEntriesPane(ViewerState state) {
    final recordFuture = state.zenodoRecordFuture;
    if (recordFuture == null) {
      return const Center(child: Text('No record loaded.'));
    }
    return FutureBuilder<ZenodoRecordSummary>(
      future: recordFuture,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return const _LoadingList();
        }
        if (!snapshot.hasData) {
          return const Center(child: Text('No entries.'));
        }
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
    final selectedField = state.hfSelectedFieldName;
    if (state.hfSelectedRowIndex == null ||
        selectedField == null ||
        _isHiddenHfField(selectedField)) {
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
    final value = row[selectedField];
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
        if (!snapshot.hasData) {
          return const Center(child: Text('No preview.'));
        }
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
        if (!snapshot.hasData) {
          return const Center(child: Text('No preview.'));
        }
          final preview = snapshot.data!;
          return _buildPreviewContent(state, preview);
        },
      );
    }
    return const Center(child: Text('Select a file or entry.'));
  }

  Widget _buildZenodoInlinePreview(ViewerState state, FieldPreview preview) {
    final ext = _resolvePreviewExt(preview, state: state);
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
    final ext = _normalizeExtToken(media.ext);
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
      {bool isWds = false, String? sourcePath}) {
    final ext = _resolvePreviewExt(
      preview,
      state: state,
      fallbackPath: sourcePath,
    );
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
      final selectedField = state.hfSelectedFieldName;
      if (preview == null ||
          state.hfSelectedRowIndex == null ||
          selectedField == null ||
          _isHiddenHfField(selectedField)) {
        return null;
      }
      final rowOffset = state.hfSelectedRowIndex! - preview.offset;
      if (rowOffset < 0 || rowOffset >= preview.rows.length) return null;
      final row = preview.rows[rowOffset];
      if (row is! Map<String, dynamic>) return null;
      final value = row[selectedField];
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
    } else if (state.mode == ViewerMode.localDirectory) {
      return null;
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
    final selectionKey = [
      state.mode?.name ?? '',
      state.selectedChunkName ?? '',
      state.selectedShardName ?? '',
      state.selectedItemIndex?.toString() ?? '',
      state.selectedFieldIndex?.toString() ?? '',
      if (isWds) state.wdsSelectedMemberPath ?? '',
      state.zenodoSelectedEntryName ?? '',
      state.zenodoSelectedFileKey ?? '',
      state.sourceInput,
    ].join('|');
    return AudioPreview(
      key: ValueKey('audio:$selectionKey'),
      label: 'Audio preview',
      loader: () => _prepareAudioPreview(
        state,
        preview,
        isWds: isWds,
        sourcePath: _resolveSelectedLocalItemPath(state),
      ),
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
    ViewerState state,
    FieldPreview preview, {
    required bool isWds,
    String? sourcePath,
  }) async {
    final ext = _resolvePreviewExt(
      preview,
      state: state,
      fallbackPath: sourcePath,
    );
    if (!_isAudioExt(ext)) {
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
    if (state.mode == ViewerMode.localDirectory) {
      final media = await state.localPrepareSelectedAudio();
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
    if (state.mode == ViewerMode.localDirectory) {
      return state.localPrepareSelectedFile();
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

  bool _isHiddenHfField(String name) {
    final trimmed = name.trim();
    return trimmed.length >= 4 &&
        trimmed.startsWith('__') &&
        trimmed.endsWith('__');
  }

  List<HfFeature> _visibleHfFeatures(HfDatasetPreview preview) {
    if (preview.features.isEmpty) return const <HfFeature>[];
    return preview.features
        .where((feature) => !_isHiddenHfField(feature.name))
        .toList(growable: false);
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
      if (state.mode == ViewerMode.localDirectory) {
        var response = await state.localOpenSelectedFile(
          openerAppPath: preferredOpener,
        );
        response = await _handleOpenerFallback(
          state,
          response,
          ext,
          (appPath) => state.localOpenSelectedFile(openerAppPath: appPath),
        );
        state.setStatusMessage(response.message);
        return;
      }
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
    if (state.hfSelectedRowIndex == null || state.hfSelectedFieldName == null) {
      return;
    }
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
    return '.$trimmed';
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
        cleaned == 'parquet' ||
        cleaned == 'md' ||
        cleaned == 'yaml' ||
        cleaned == 'yml') {
      return 'text';
    }
    return 'binary';
  }

  bool _isImageExt(String ext) {
    switch (_normalizeExtToken(ext)) {
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
    switch (_normalizeExtToken(ext)) {
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
    switch (_normalizeExtToken(ext)) {
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

  String _resolvePreviewExt(
    FieldPreview preview, {
    required ViewerState state,
    String? fallbackPath,
  }) {
    final guessedExt = _normalizeExtToken(preview.guessedExt);
    if (guessedExt.isNotEmpty) return guessedExt;
    if (state.mode == ViewerMode.localDirectory) {
      final sourcePath = fallbackPath ?? _resolveSelectedLocalItemPath(state);
      if (sourcePath == null || sourcePath.isEmpty) {
        return '';
      }
      final fromPath = _normalizeExtToken(_extFromPath(sourcePath));
      if (fromPath.isNotEmpty) {
        return fromPath;
      }
    }
    return '';
  }

  String _normalizeExtToken(String? ext) {
    if (ext == null) return '';
    var normalized = ext.trim().toLowerCase();
    if (normalized.isEmpty) return '';
    final queryIndex = normalized.indexOf('?');
    if (queryIndex >= 0) {
      normalized = normalized.substring(0, queryIndex);
    }
    final semicolonIndex = normalized.indexOf(';');
    if (semicolonIndex >= 0) {
      normalized = normalized.substring(0, semicolonIndex);
    }
    final slashIndex = normalized.lastIndexOf('/');
    if (slashIndex >= 0 && slashIndex + 1 < normalized.length) {
      normalized = normalized.substring(slashIndex + 1);
    }
    if (normalized.isEmpty) return '';
    if (normalized.startsWith('.')) {
      normalized = normalized.substring(1);
    }
    if (normalized.isEmpty) return '';
    final audioSuffixIndex = normalized.indexOf('audio/');
    if (audioSuffixIndex == 0) {
      normalized = normalized.substring('audio/'.length);
    }
    if (normalized.endsWith('.')) return '';
    final dotIndex = normalized.lastIndexOf('.');
    if (dotIndex >= 0) {
      if (dotIndex + 1 >= normalized.length) return '';
      normalized = normalized.substring(dotIndex + 1);
    }
    return normalized;
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

class _LocalTabularData {
  const _LocalTabularData({
    required this.headers,
    required this.rows,
  });

  final List<String> headers;
  final List<List<String>> rows;
}

class _LocalDirectoryTreeItem {
  const _LocalDirectoryTreeItem({
    required this.item,
    required this.depth,
  });

  final LocalDirectoryItem item;
  final int depth;
}

class _CodeBlock extends StatelessWidget {
  const _CodeBlock({required this.text});

  final String text;

  @override
  Widget build(BuildContext context) {
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
        style: AppFonts.codeTextStyle(
          base: Theme.of(context).textTheme.bodySmall,
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
    required this.content,
    this.scrollable = true,
  });

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
    if (!widget.scrollable) {
      return LayoutBuilder(
        builder: (context, constraints) {
          final bounded =
              constraints.maxHeight.isFinite && constraints.maxHeight > 0;
          if (!bounded) {
            return Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              mainAxisSize: MainAxisSize.min,
              children: [
                widget.content,
              ],
            );
          }
          return SizedBox(
            height: constraints.maxHeight,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
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
        ? scheme.primary.withValues(alpha: 0.12)
        : _hovered
            ? scheme.onSurface.withValues(alpha: 0.06)
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
            widget.selected ? 6 : 8,
            6,
            10,
            6,
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
