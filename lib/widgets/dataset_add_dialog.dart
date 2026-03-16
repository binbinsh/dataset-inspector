import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';

import '../models/remote_host.dart';
import '../utils/app_fonts.dart';
import '../utils/dialog_action_styles.dart';

const Color _kDialogBorderColor = Color(0xFFE0E0E0);
const Color _kDialogInputFillColor = Color(0xFFF7F7F7);

ThemeData _dialogTheme(BuildContext context) {
  final theme = Theme.of(context);
  final baseBorder = OutlineInputBorder(
    borderRadius: BorderRadius.circular(10),
    borderSide: const BorderSide(color: _kDialogBorderColor),
  );
  return theme.copyWith(
    inputDecorationTheme: theme.inputDecorationTheme.copyWith(
      filled: true,
      fillColor: _kDialogInputFillColor,
      border: baseBorder,
      enabledBorder: baseBorder,
      contentPadding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
      focusedBorder: baseBorder.copyWith(
        borderSide: BorderSide(
          color: theme.colorScheme.primary.withValues(alpha: 0.65),
          width: 1.2,
        ),
      ),
    ),
  );
}

RoundedRectangleBorder _dialogShape() {
  return RoundedRectangleBorder(
    borderRadius: BorderRadius.circular(12),
    side: const BorderSide(color: _kDialogBorderColor),
  );
}

TextStyle _dialogTitleStyle(BuildContext context) {
  return AppFonts.flexTextStyle(Theme.of(context).textTheme.titleMedium)
      .copyWith(fontWeight: FontWeight.w700);
}

Future<void> showDatasetAddDialog({
  required BuildContext context,
  required List<String> recentSources,
  required List<RemoteHostConfig> remoteHosts,
  required Future<void> Function(String input) onOpenLocalSource,
  required Future<bool> Function({
    required String hostId,
    required String datasetPath,
  }) onOpenRemoteSource,
  required Future<String?> Function({
    required String hostId,
    required String initialPath,
  }) onBrowseRemotePath,
  required VoidCallback onOpenRemoteHostsSettings,
}) async {
  await showDialog<void>(
    context: context,
    builder: (dialogContext) {
      return Theme(
        data: _dialogTheme(dialogContext),
        child: _DatasetAddDialog(
          recentSources: recentSources,
          remoteHosts: remoteHosts,
          onOpenLocalSource: onOpenLocalSource,
          onOpenRemoteSource: onOpenRemoteSource,
          onBrowseRemotePath: onBrowseRemotePath,
          onOpenRemoteHostsSettings: onOpenRemoteHostsSettings,
        ),
      );
    },
  );
}

class _DatasetAddDialog extends StatefulWidget {
  const _DatasetAddDialog({
    required this.recentSources,
    required this.remoteHosts,
    required this.onOpenLocalSource,
    required this.onOpenRemoteSource,
    required this.onBrowseRemotePath,
    required this.onOpenRemoteHostsSettings,
  });

  final List<String> recentSources;
  final List<RemoteHostConfig> remoteHosts;
  final Future<void> Function(String input) onOpenLocalSource;
  final Future<bool> Function({
    required String hostId,
    required String datasetPath,
  }) onOpenRemoteSource;
  final Future<String?> Function({
    required String hostId,
    required String initialPath,
  }) onBrowseRemotePath;
  final VoidCallback onOpenRemoteHostsSettings;

  @override
  State<_DatasetAddDialog> createState() => _DatasetAddDialogState();
}

class _DatasetAddDialogState extends State<_DatasetAddDialog> {
  late final TextEditingController _localInputController;
  late final TextEditingController _remotePathController;
  late final List<RemoteHostConfig> _remoteCandidates;

  int _selectedTab = 0;
  String? _selectedRemoteId;

  @override
  void initState() {
    super.initState();
    _localInputController = TextEditingController();
    _remotePathController = TextEditingController();
    _remoteCandidates = List<RemoteHostConfig>.from(widget.remoteHosts);
    _selectedRemoteId =
        _remoteCandidates.isEmpty ? null : _remoteCandidates.first.id.trim();
  }

  @override
  void dispose() {
    _localInputController.dispose();
    _remotePathController.dispose();
    super.dispose();
  }

  Future<void> _submitLocal() async {
    final input = _localInputController.text.trim();
    if (input.isEmpty || !mounted) return;
    await widget.onOpenLocalSource(input);
    if (!mounted) return;
    Navigator.of(context).pop();
  }

  Future<void> _submitRemote() async {
    final hostId = _selectedRemoteId?.trim() ?? '';
    final datasetPath = _remotePathController.text.trim();
    if (hostId.isEmpty || !mounted) return;

    final opened = await widget.onOpenRemoteSource(
      hostId: hostId,
      datasetPath: datasetPath,
    );
    if (!mounted || !opened) return;
    Navigator.of(context).pop();
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      backgroundColor: Colors.white,
      surfaceTintColor: Colors.transparent,
      shape: _dialogShape(),
      titleTextStyle: _dialogTitleStyle(context),
      title: const Text('Open Dataset'),
      content: SizedBox(
        width: 620,
        child: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            SegmentedButton<int>(
              segments: const [
                ButtonSegment(
                  value: 0,
                  icon: Icon(Icons.folder_open),
                  label: Text('Local'),
                ),
                ButtonSegment(
                  value: 1,
                  icon: Icon(Icons.cloud_outlined),
                  label: Text('Remote Host'),
                ),
              ],
              selected: <int>{_selectedTab},
              onSelectionChanged: (values) {
                if (values.isEmpty) return;
                setState(() {
                  _selectedTab = values.first;
                });
              },
            ),
            const SizedBox(height: 12),
            if (_selectedTab == 0) ...[
              TextField(
                controller: _localInputController,
                autofocus: true,
                decoration: InputDecoration(
                  hintText: 'Path, URL, or HF dataset name',
                  fillColor: Colors.white,
                  prefixIcon: const Icon(Icons.link),
                  suffixIcon: IconButton(
                    tooltip: 'Browse folder...',
                    icon: const Icon(Icons.folder_open, size: 20),
                    onPressed: () async {
                      final result = await FilePicker.platform.getDirectoryPath();
                      if (result == null || result.trim().isEmpty) {
                        return;
                      }
                      if (!mounted) return;
                      Navigator.of(context).pop();
                      await widget.onOpenLocalSource(result.trim());
                    },
                  ),
                ),
                onSubmitted: (_) => _submitLocal(),
              ),
              if (widget.recentSources.isNotEmpty) ...[
                const SizedBox(height: 10),
                Text(
                  'Recent',
                  style: Theme.of(context).textTheme.labelMedium,
                ),
                const SizedBox(height: 8),
                Container(
                  height: 150,
                  decoration: BoxDecoration(
                    color: _kDialogInputFillColor,
                    borderRadius: BorderRadius.circular(10),
                    border: Border.all(color: _kDialogBorderColor),
                  ),
                  child: ListView(
                    children: widget.recentSources
                        .map(
                          (source) => ListTile(
                            dense: true,
                            contentPadding: const EdgeInsets.symmetric(
                              horizontal: 10,
                            ),
                            title: Text(
                              source,
                              maxLines: 1,
                              overflow: TextOverflow.ellipsis,
                            ),
                            onTap: () async {
                              Navigator.of(context).pop();
                              await widget.onOpenLocalSource(source);
                            },
                          ),
                        )
                        .toList(growable: false),
                  ),
                ),
              ],
            ] else ...[
              if (_remoteCandidates.isEmpty)
                Container(
                  width: double.infinity,
                  padding: const EdgeInsets.all(12),
                  decoration: BoxDecoration(
                    color: _kDialogInputFillColor,
                    border: Border.all(
                      color: _kDialogBorderColor,
                    ),
                    borderRadius: BorderRadius.circular(10),
                  ),
                  child: const Text(
                    'No remote hosts configured.\nOpen Settings → Remote Hosts first.',
                  ),
                )
              else ...[
                DropdownButtonFormField<String>(
                  initialValue: _selectedRemoteId,
                  items: _remoteCandidates
                      .map(
                        (host) => DropdownMenuItem<String>(
                          value: host.id.trim(),
                          child: Text(
                            '${host.label} (${host.type.name})',
                            overflow: TextOverflow.ellipsis,
                          ),
                        ),
                      )
                      .toList(growable: false),
                  onChanged: (value) {
                    setState(() {
                      _selectedRemoteId = value;
                    });
                  },
                  decoration: const InputDecoration(
                    labelText: 'Remote host',
                  ),
                ),
                const SizedBox(height: 8),
                TextField(
                  controller: _remotePathController,
                  decoration: InputDecoration(
                    labelText: 'Dataset path / prefix',
                    hintText: 'th/train or leave empty for host base path',
                    suffixIcon: IconButton(
                      tooltip: 'Browse remote host',
                      icon: const Icon(Icons.folder_open_outlined),
                      onPressed: _selectedRemoteId == null
                          ? null
                          : () async {
                              final picked = await widget.onBrowseRemotePath(
                                hostId: _selectedRemoteId!,
                                initialPath: _remotePathController.text.trim(),
                              );
                              if (!mounted ||
                                  picked == null ||
                                  picked.trim().isEmpty) {
                                return;
                              }
                              _remotePathController.text = picked.trim();
                              _remotePathController.selection =
                                  TextSelection.collapsed(
                                offset: _remotePathController.text.length,
                              );
                            },
                    ),
                  ),
                  onSubmitted: (_) => _submitRemote(),
                ),
              ],
              const SizedBox(height: 8),
              OutlinedButton.icon(
                style: buildDialogSecondaryButtonStyle(context),
                onPressed: () {
                  Navigator.of(context).pop();
                  widget.onOpenRemoteHostsSettings();
                },
                icon: const Icon(Icons.settings_outlined, size: 16),
                label: const Text('Remote Hosts Settings'),
              ),
            ],
          ],
        ),
      ),
      actions: [
        OutlinedButton(
          style: buildDialogSecondaryButtonStyle(context),
          onPressed: () => Navigator.of(context).pop(),
          child: const Text('Cancel'),
        ),
        FilledButton.icon(
          style: buildDialogPrimaryButtonStyle(context),
          onPressed: _selectedTab == 0
              ? _submitLocal
              : (_selectedRemoteId == null ? null : _submitRemote),
          icon: const Icon(Icons.add),
          label: const Text('Open'),
        ),
      ],
    );
  }
}
