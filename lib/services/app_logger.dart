import 'package:flutter/foundation.dart';

class AppLogger {
  AppLogger._();

  static const bool _forceVerbose =
      bool.fromEnvironment('DATASET_INSPECTOR_VERBOSE', defaultValue: false);

  static bool get _enabled => _forceVerbose || kDebugMode;

  static void info(String message, {String tag = 'app'}) {
    if (!_enabled) return;
    debugPrint('[INFO][$tag] $message');
  }

  static void warn(String message, {String tag = 'app'}) {
    if (!_enabled) return;
    debugPrint('[WARN][$tag] $message');
  }

  static void error(String message, {String tag = 'app', Object? error, StackTrace? stackTrace}) {
    if (!_enabled) return;
    debugPrint('[ERROR][$tag] $message');
    if (error != null) {
      debugPrint('error: $error');
    }
    if (stackTrace != null) {
      debugPrint(stackTrace.toString());
    }
  }
}
