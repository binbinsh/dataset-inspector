class AppLogger {
  AppLogger._();

  static const bool _forceVerbose =
      bool.fromEnvironment('DATASET_INSPECTOR_VERBOSE', defaultValue: false);
  static const bool _isProductMode =
      bool.fromEnvironment('dart.vm.product', defaultValue: false);
  static bool? _enabledOverride;

  static bool get _enabled =>
      _enabledOverride ?? (_forceVerbose || !_isProductMode);

  /// Override logger verbosity at runtime for non-UI environments.
  static void configure({bool? enabled}) {
    _enabledOverride = enabled;
  }

  static void info(String message, {String tag = 'app'}) {
    if (!_enabled) return;
    // ignore: avoid_print
    print('[INFO][$tag] $message');
  }

  static void warn(String message, {String tag = 'app'}) {
    if (!_enabled) return;
    // ignore: avoid_print
    print('[WARN][$tag] $message');
  }

  static void error(String message,
      {String tag = 'app', Object? error, StackTrace? stackTrace}) {
    if (!_enabled) return;
    // ignore: avoid_print
    print('[ERROR][$tag] $message');
    if (error != null) {
      // ignore: avoid_print
      print('error: $error');
    }
    if (stackTrace != null) {
      // ignore: avoid_print
      print(stackTrace.toString());
    }
  }
}
