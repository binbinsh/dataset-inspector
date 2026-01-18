import 'package:flutter/material.dart';
import 'package:provider/provider.dart';

import '../services/update_service.dart';
import '../state/viewer_state.dart';
import '../widgets/update_dialog.dart';

class AppMenuBridge {
  AppMenuBridge._();

  static final navigatorKey = GlobalKey<NavigatorState>();
  static final messengerKey = GlobalKey<ScaffoldMessengerState>();

  static BuildContext? get _context => navigatorKey.currentState?.context;

  static void _showSnack(String message) {
    messengerKey.currentState?.showSnackBar(SnackBar(content: Text(message)));
  }

  static Future<void> handleCheckForUpdates() async {
    final context = _context;
    if (context == null) return;
    final state = context.read<ViewerState>();
    UpdateInfo? update;
    try {
      update = await state.checkForUpdateNow();
    } catch (err) {
      _showSnack('Update check failed: $err');
      return;
    }
    if (update == null) {
      _showSnack('You are up to date.');
      return;
    }
    await showUpdateDialog(context, state, update);
  }
}
