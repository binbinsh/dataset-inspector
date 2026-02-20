import 'package:flutter/material.dart';

import '../models/remote_host.dart';
import '../services/remote_dataset_service.dart';

Future<String?> showRemotePathPickerDialog({
  required BuildContext context,
  required RemoteHostConfig host,
  required Future<List<RemotePathEntry>> Function({
    required String hostId,
    required String directoryPath,
  }) onListEntries,
  String initialPath = '',
}) async {
  var currentPath = initialPath.trim();
  var selectedPath = '';
  var loading = false;
  String? errorText;
  List<RemotePathEntry> entries = const <RemotePathEntry>[];
  var initialized = false;

  Future<void> loadEntries(StateSetter setState, String path) async {
    setState(() {
      loading = true;
      errorText = null;
    });
    try {
      final listed = await onListEntries(
        hostId: host.id,
        directoryPath: path,
      );
      setState(() {
        currentPath = path;
        selectedPath = '';
        entries = listed;
      });
    } catch (error) {
      setState(() {
        errorText = error.toString();
      });
    } finally {
      setState(() {
        loading = false;
      });
    }
  }

  String parentPathOf(String path) {
    final normalized = path.trim().replaceAll('\\', '/');
    if (normalized.isEmpty) return '';
    final parts = normalized.split('/')..removeWhere((part) => part.isEmpty);
    if (parts.isEmpty) return '';
    parts.removeLast();
    return parts.join('/');
  }

  return showDialog<String>(
    context: context,
    builder: (dialogContext) {
      return StatefulBuilder(
        builder: (context, setState) {
          if (!initialized) {
            initialized = true;
            loadEntries(setState, currentPath);
          }
          Future<void> openParent() async {
            final parent = parentPathOf(currentPath);
            await loadEntries(setState, parent);
          }

          Future<void> refresh() async {
            await loadEntries(setState, currentPath);
          }

          return AlertDialog(
            title: Text('Browse Remote Path · ${host.label}'),
            content: SizedBox(
              width: 760,
              height: 460,
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    children: [
                      IconButton(
                        tooltip: 'Up',
                        onPressed:
                            currentPath.trim().isEmpty ? null : openParent,
                        icon: const Icon(Icons.arrow_upward),
                      ),
                      Expanded(
                        child: Container(
                          padding: const EdgeInsets.symmetric(
                            horizontal: 10,
                            vertical: 8,
                          ),
                          decoration: BoxDecoration(
                            border: Border.all(
                              color:
                                  Theme.of(context).colorScheme.outlineVariant,
                            ),
                            borderRadius: BorderRadius.circular(8),
                          ),
                          child: Text(
                            currentPath.trim().isEmpty ? '/' : currentPath,
                            maxLines: 1,
                            overflow: TextOverflow.ellipsis,
                          ),
                        ),
                      ),
                      const SizedBox(width: 8),
                      IconButton(
                        tooltip: 'Refresh',
                        onPressed: loading ? null : refresh,
                        icon: const Icon(Icons.refresh),
                      ),
                    ],
                  ),
                  const SizedBox(height: 8),
                  if (errorText != null && errorText!.trim().isNotEmpty)
                    Padding(
                      padding: const EdgeInsets.only(bottom: 8),
                      child: Text(
                        errorText!,
                        style: Theme.of(context).textTheme.bodySmall?.copyWith(
                              color: Theme.of(context).colorScheme.error,
                            ),
                      ),
                    ),
                  Expanded(
                    child: loading
                        ? const Center(child: CircularProgressIndicator())
                        : entries.isEmpty
                            ? const Center(child: Text('No entries.'))
                            : ListView.builder(
                                itemCount: entries.length,
                                itemBuilder: (context, index) {
                                  final entry = entries[index];
                                  final selected = selectedPath == entry.path;
                                  return ListTile(
                                    dense: true,
                                    selected: selected,
                                    leading: Icon(
                                      entry.isDirectory
                                          ? Icons.folder_outlined
                                          : Icons.insert_drive_file_outlined,
                                    ),
                                    title: Text(
                                      entry.name,
                                      maxLines: 1,
                                      overflow: TextOverflow.ellipsis,
                                    ),
                                    subtitle: entry.isDirectory
                                        ? null
                                        : Text(
                                            _formatBytes(entry.sizeBytes ?? 0)),
                                    trailing: entry.isDirectory
                                        ? const Icon(
                                            Icons.chevron_right,
                                            size: 16,
                                          )
                                        : null,
                                    onTap: () {
                                      if (entry.isDirectory) {
                                        loadEntries(setState, entry.path);
                                        return;
                                      }
                                      setState(() {
                                        selectedPath = entry.path;
                                      });
                                    },
                                    onLongPress: () {
                                      setState(() {
                                        selectedPath = entry.path;
                                      });
                                    },
                                  );
                                },
                              ),
                  ),
                ],
              ),
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.of(dialogContext).pop(),
                child: const Text('Cancel'),
              ),
              TextButton(
                onPressed: () => Navigator.of(dialogContext).pop(currentPath),
                child: const Text('Use Current Folder'),
              ),
              FilledButton(
                onPressed: selectedPath.trim().isEmpty
                    ? null
                    : () => Navigator.of(dialogContext).pop(selectedPath),
                child: const Text('Select Item'),
              ),
            ],
          );
        },
      );
    },
  );
}

String _formatBytes(int bytes) {
  if (bytes < 1024) return '$bytes B';
  const units = <String>['KB', 'MB', 'GB', 'TB'];
  var value = bytes.toDouble();
  var unitIndex = -1;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  final fixed =
      value >= 100 ? value.toStringAsFixed(0) : value.toStringAsFixed(1);
  return '$fixed ${units[unitIndex]}';
}
