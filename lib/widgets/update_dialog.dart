import 'package:flutter/material.dart';

import '../services/update_service.dart';
import '../state/viewer_state.dart';

Future<void> showUpdateDialog(BuildContext context, ViewerState state, UpdateInfo update) async {
  var downloaded = 0;
  int? total;
  var isDownloading = false;
  String? errorMessage;
  return showDialog<void>(
    context: context,
    builder: (dialogContext) {
      return StatefulBuilder(
        builder: (context, setState) {
          final theme = Theme.of(context);
          final showProgress = isDownloading;
          final totalBytes = total;
          final progress = (totalBytes != null && totalBytes > 0) ? downloaded / totalBytes : null;
          return AlertDialog(
            title: const Text('Update available'),
            content: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Version ${update.version}'),
                if (update.notes != null) ...[
                  const SizedBox(height: 12),
                  Text(update.notes!, style: theme.textTheme.bodySmall),
                ],
                const SizedBox(height: 16),
                if (errorMessage != null) ...[
                  Text(errorMessage!, style: TextStyle(color: theme.colorScheme.error)),
                  const SizedBox(height: 12),
                ],
                if (showProgress) LinearProgressIndicator(value: progress),
              ],
            ),
            actions: [
              TextButton(
                onPressed: isDownloading ? null : () => Navigator.of(dialogContext).pop(),
                child: const Text('Later'),
              ),
              FilledButton(
                onPressed: isDownloading
                    ? null
                    : () async {
                        setState(() {
                          isDownloading = true;
                          downloaded = 0;
                          total = null;
                          errorMessage = null;
                        });
                        try {
                          final file = await state.downloadUpdate(update, onProgress: (value, totalBytes) {
                            if (!dialogContext.mounted) return;
                            setState(() {
                              downloaded = value;
                              total = totalBytes;
                            });
                          });
                          if (!dialogContext.mounted) return;
                          await state.installUpdate(file);
                          if (dialogContext.mounted) {
                            Navigator.of(dialogContext).pop();
                          }
                        } catch (_) {
                          if (!dialogContext.mounted) return;
                          setState(() {
                            isDownloading = false;
                            downloaded = 0;
                            total = null;
                            errorMessage = 'Update failed. Please try again.';
                          });
                        }
                      },
                child: Text(isDownloading ? 'Downloading...' : 'Download'),
              ),
            ],
          );
        },
      );
    },
  );
}
