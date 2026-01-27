import 'dart:async';
import 'dart:ui';

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:window_manager/window_manager.dart';

import 'app.dart';
import 'services/app_menu_bridge.dart';
import 'services/app_logger.dart';

const _menuChannel = MethodChannel('sh.train.datasetinspector/menu');

Future<void> main() async {
  runZonedGuarded(() async {
    WidgetsFlutterBinding.ensureInitialized();
    _menuChannel.setMethodCallHandler((call) async {
      if (call.method == 'checkForUpdates') {
        await AppMenuBridge.handleCheckForUpdates();
      }
    });
    await _configureWindow();
    FlutterError.onError = (details) {
      AppLogger.error(
        'Flutter error',
        tag: 'flutter',
        error: details.exception,
        stackTrace: details.stack,
      );
      FlutterError.presentError(details);
    };

    PlatformDispatcher.instance.onError = (error, stack) {
      AppLogger.error('Unhandled platform error', tag: 'flutter', error: error, stackTrace: stack);
      return true;
    };

    AppLogger.info('App start');
    runApp(const DatasetInspectorApp());
  }, (error, stack) {
    AppLogger.error('Unhandled zone error', tag: 'flutter', error: error, stackTrace: stack);
  });
}

Future<void> _configureWindow() async {
  if (kIsWeb) return;
  final platform = defaultTargetPlatform;
  final isDesktop = platform == TargetPlatform.macOS ||
      platform == TargetPlatform.windows ||
      platform == TargetPlatform.linux;
  if (!isDesktop) return;
  await windowManager.ensureInitialized();
  const minSize = Size(1080, 720);
  const initialSize = Size(1440, 980);
  final options = WindowOptions(
    size: initialSize,
    minimumSize: minSize,
    center: true,
    titleBarStyle: TitleBarStyle.hidden,
  );
  await windowManager.waitUntilReadyToShow(options, () async {
    await windowManager.show();
    await windowManager.focus();
  });
  await windowManager.setMinimumSize(minSize);
}
