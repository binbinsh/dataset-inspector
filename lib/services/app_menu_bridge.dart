import 'dart:async';

import 'package:flutter/material.dart';
import 'package:provider/provider.dart';

import '../services/update_service.dart';
import '../state/viewer_state.dart';
import '../widgets/update_dialog.dart';

class AppMenuBridge {
  AppMenuBridge._();

  static final navigatorKey = GlobalKey<NavigatorState>();
  static final messengerKey = GlobalKey<ScaffoldMessengerState>();

  static Future<void> handleCheckForUpdates() async {
    final navigator = navigatorKey.currentState;
    if (navigator == null) return;
    final messenger = messengerKey.currentState;
    final state = navigator.context.read<ViewerState>();
    UpdateInfo? update;
    try {
      update = await state.checkForUpdateNow();
    } catch (err) {
      messenger?.showSnackBar(
        SnackBar(content: Text('Update check failed: $err')),
      );
      return;
    }
    if (update == null) {
      messenger?.showSnackBar(
        const SnackBar(content: Text('You are up to date.')),
      );
      return;
    }
    final updateToShow = update;

    WidgetsBinding.instance.addPostFrameCallback((_) {
      final currentNavigator = navigatorKey.currentState;
      if (currentNavigator == null) return;
      unawaited(showUpdateDialog(currentNavigator.context, state, updateToShow));
    });
  }
}
