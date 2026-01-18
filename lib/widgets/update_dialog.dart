import 'package:flutter/material.dart';

import '../services/update_service.dart';
import '../state/viewer_state.dart';

Future<void> showUpdateDialog(BuildContext context, ViewerState state, UpdateInfo update) async {
  var downloaded = 0;
  int? total;
  return showDialog<void>(
    context: context,
    builder: (dialogContext) {
      return StatefulBuilder(
        builder: (context, setState) {
          final progress = total == null || total == 0 ? null : downloaded / total!;
          return AlertDialog(
            title: const Text('Update available'),
            content: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text('Version ${update.version}'),
                if (update.notes != null) ...[
                  const SizedBox(height: 12),
                  Text(update.notes!, style: Theme.of(context).textTheme.bodySmall),
                ],
                const SizedBox(height: 16),
                if (progress != null)
                  LinearProgressIndicator(value: progress)
                else
                  const LinearProgressIndicator(),
              ],
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.of(dialogContext).pop(),
                child: const Text('Later'),
              ),
              FilledButton(
                onPressed: () async {
                  await state.downloadUpdate(update, onProgress: (value, totalBytes) {
                    setState(() {
                      downloaded = value;
                      total = totalBytes;
                    });
                  });
                },
                child: const Text('Download'),
              ),
            ],
          );
        },
      );
    },
  );
}
