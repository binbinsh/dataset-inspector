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
    print('[INFO][$tag] $message');
  }

  static void warn(String message, {String tag = 'app'}) {
    if (!_enabled) return;
    print('[WARN][$tag] $message');
  }

  static void error(String message,
      {String tag = 'app', Object? error, StackTrace? stackTrace}) {
    if (!_enabled) return;
    print('[ERROR][$tag] $message');
    if (error != null) {
      print('error: $error');
    }
    if (stackTrace != null) {
      print(stackTrace.toString());
    }
  }
}
