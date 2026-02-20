import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';

import '../models/remote_host.dart';
import '../utils/app_fonts.dart';

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
  final localInputController = TextEditingController();
  final remotePathController = TextEditingController();
  var selectedTab = 0;
  final remoteCandidates = List<RemoteHostConfig>.from(remoteHosts);
  String? selectedRemoteId =
      remoteCandidates.isEmpty ? null : remoteCandidates.first.id.trim();

  await showDialog<void>(
    context: context,
    builder: (dialogContext) {
      return Theme(
        data: _dialogTheme(dialogContext),
        child: StatefulBuilder(
          builder: (context, setState) {
            Future<void> submitLocal() async {
              final input = localInputController.text.trim();
              if (input.isEmpty) return;
              await onOpenLocalSource(input);
              if (dialogContext.mounted) {
                Navigator.of(dialogContext).pop();
              }
            }

            Future<void> submitRemote() async {
              final hostId = selectedRemoteId?.trim() ?? '';
              final datasetPath = remotePathController.text.trim();
              if (hostId.isEmpty) return;
              final opened = await onOpenRemoteSource(
                hostId: hostId,
                datasetPath: datasetPath,
              );
              if (opened && dialogContext.mounted) {
                Navigator.of(dialogContext).pop();
              }
            }

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
                      selected: <int>{selectedTab},
                      onSelectionChanged: (values) {
                        if (values.isEmpty) return;
                        setState(() {
                          selectedTab = values.first;
                        });
                      },
                    ),
                    const SizedBox(height: 12),
                    if (selectedTab == 0) ...[
                      TextField(
                        controller: localInputController,
                        autofocus: true,
                        decoration: InputDecoration(
                          hintText: 'Path, URL, or HF dataset name',
                          fillColor: Colors.white,
                          prefixIcon: const Icon(Icons.link),
                          suffixIcon: IconButton(
                            tooltip: 'Browse folder...',
                            icon: const Icon(Icons.folder_open, size: 20),
                            onPressed: () async {
                              final result =
                                  await FilePicker.platform.getDirectoryPath();
                              if (result == null || result.trim().isEmpty) {
                                return;
                              }
                              if (dialogContext.mounted) {
                                Navigator.of(dialogContext).pop();
                              }
                              await onOpenLocalSource(result.trim());
                            },
                          ),
                        ),
                        onSubmitted: (_) => submitLocal(),
                      ),
                      if (recentSources.isNotEmpty) ...[
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
                            children: recentSources
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
                                      Navigator.of(dialogContext).pop();
                                      await onOpenLocalSource(source);
                                    },
                                  ),
                                )
                                .toList(growable: false),
                          ),
                        ),
                      ],
                    ] else ...[
                      if (remoteCandidates.isEmpty)
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
                          initialValue: selectedRemoteId,
                          items: remoteCandidates
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
                              selectedRemoteId = value;
                            });
                          },
                          decoration: const InputDecoration(
                            labelText: 'Remote host',
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: remotePathController,
                          decoration: InputDecoration(
                            labelText: 'Dataset path / prefix',
                            hintText:
                                'th/train or leave empty for host base path',
                            suffixIcon: IconButton(
                              tooltip: 'Browse remote host',
                              icon: const Icon(Icons.folder_open_outlined),
                              onPressed: selectedRemoteId == null
                                  ? null
                                  : () async {
                                      final picked = await onBrowseRemotePath(
                                        hostId: selectedRemoteId!,
                                        initialPath:
                                            remotePathController.text.trim(),
                                      );
                                      if (picked == null ||
                                          picked.trim().isEmpty) {
                                        return;
                                      }
                                      remotePathController.text = picked.trim();
                                      remotePathController.selection =
                                          TextSelection.collapsed(
                                        offset:
                                            remotePathController.text.length,
                                      );
                                    },
                            ),
                          ),
                          onSubmitted: (_) => submitRemote(),
                        ),
                      ],
                      const SizedBox(height: 8),
                      TextButton.icon(
                        onPressed: () {
                          Navigator.of(dialogContext).pop();
                          onOpenRemoteHostsSettings();
                        },
                        icon: const Icon(Icons.settings_outlined, size: 16),
                        label: const Text('Remote Hosts Settings'),
                      ),
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
                  onPressed: selectedTab == 0
                      ? submitLocal
                      : (selectedRemoteId == null ? null : submitRemote),
                  icon: const Icon(Icons.add),
                  label: const Text('Open'),
                ),
              ],
            );
          },
        ),
      );
    },
  );

  localInputController.dispose();
  remotePathController.dispose();
}
