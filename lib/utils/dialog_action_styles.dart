import 'package:flutter/material.dart';

ButtonStyle buildDialogSecondaryButtonStyle(BuildContext context) {
  final theme = Theme.of(context);
  return OutlinedButton.styleFrom(
    textStyle:
        theme.textTheme.labelLarge?.copyWith(fontWeight: FontWeight.w600),
    foregroundColor: theme.colorScheme.primary,
    side: BorderSide(color: theme.colorScheme.outline),
    shape: RoundedRectangleBorder(
      borderRadius: BorderRadius.circular(10),
    ),
    minimumSize: const Size(0, 36),
    padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 8),
  );
}

ButtonStyle buildDialogPrimaryButtonStyle(BuildContext context) {
  final theme = Theme.of(context);
  return FilledButton.styleFrom(
    textStyle:
        theme.textTheme.labelLarge?.copyWith(fontWeight: FontWeight.w600),
    shape: RoundedRectangleBorder(
      borderRadius: BorderRadius.circular(10),
    ),
    minimumSize: const Size(0, 36),
    padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 8),
    elevation: 0,
  );
}
