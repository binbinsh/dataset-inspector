import 'dart:async';
import 'dart:io';
import 'dart:ui';

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:window_manager/window_manager.dart';

import 'app.dart';
import 'state/viewer_state.dart';
import 'services/dataset_inspector_api_server.dart';
import 'services/app_menu_bridge.dart';
import 'services/app_logger.dart';

const _menuChannel = MethodChannel('sh.train.datasetinspector/menu');

Future<void> main(List<String> args) async {
  final startup = _parseStartupArgs(args);
  if (startup.showHelp) {
    if (startup.parseError) {
      // ignore: avoid_print
      print('Invalid startup argument for API options.');
    }
    _printStartupUsage();
    return;
  }

  runZonedGuarded(() async {
    WidgetsFlutterBinding.ensureInitialized();

    final viewerState = ViewerState();
    await viewerState.bootstrap();
    if (startup.initialSources.isNotEmpty) {
      for (final source in startup.initialSources) {
        final added = await viewerState.addSource(source, recordRecent: false);
        if (!added) {
          AppLogger.error(
            'Failed to open startup source',
            tag: 'startup',
            error: source,
          );
        } else {
          AppLogger.info(
            'Opened startup source: $source',
            tag: 'startup',
          );
        }
      }
    }
    DatasetInspectorApiServer? apiServer;
    viewerState.setApiServerCallbacks(
      startServer: ({
        required String host,
        required int port,
        required int maxConcurrency,
      }) async {
        await apiServer?.stop();
        apiServer = DatasetInspectorApiServer(
          state: viewerState,
          host: host,
          port: port,
          defaultConcurrency: maxConcurrency,
        );
        final resolvedPort = await apiServer!.start();
        AppLogger.info(
          'Dataset Inspector API available at http://$host:$resolvedPort',
          tag: 'api',
        );
        return resolvedPort;
      },
      stopServer: () async {
        final server = apiServer;
        if (server == null) {
          return;
        }
        await server.stop();
        apiServer = null;
      },
    );

    if (startup.enableApi) {
      final configuredPort = startup.apiPort ?? viewerState.apiPort;
      final configuredHost =
          startup.apiHost.isEmpty ? viewerState.apiHost : startup.apiHost;
      final configuredMaxConcurrency =
          startup.apiMaxConcurrency ?? viewerState.apiMaxConcurrency;
      await viewerState.applyApiSettings(
        enabled: true,
        host: configuredHost,
        port: configuredPort,
        maxConcurrency: configuredMaxConcurrency,
      );
    } else if (viewerState.apiEnabled) {
      await viewerState.startApiServer();
    }

    if (startup.apiOnly) {
      await _runIndefinitely();
    }

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
      AppLogger.error('Unhandled platform error',
          tag: 'flutter', error: error, stackTrace: stack);
      return true;
    };

    AppLogger.info('App start');
    runApp(DatasetInspectorApp(viewerState: viewerState));
  }, (error, stack) {
    AppLogger.error('Unhandled zone error',
        tag: 'flutter', error: error, stackTrace: stack);
  });
}

Future<void> _runIndefinitely() {
  final completer = Completer<void>();
  return completer.future;
}

_ApiStartupConfig _parseStartupArgs(List<String> args) {
  final envHost = Platform.environment['DATASET_INSPECTOR_API_HOST']?.trim();
  final envPort = Platform.environment['DATASET_INSPECTOR_API_PORT'];
  final envConcurrency =
      Platform.environment['DATASET_INSPECTOR_API_MAX_CONCURRENCY'];
  final envSources = Platform.environment['DATASET_INSPECTOR_SOURCES'];
  final fallbackHost = envHost == null || envHost.isEmpty ? '' : envHost.trim();
  var apiHost =
      envHost == null || envHost.isEmpty ? '127.0.0.1' : envHost.trim();
  int? apiPort = int.tryParse(envPort ?? '');
  final envApiConcurrency = int.tryParse(envConcurrency ?? '');
  bool concurrencyFromEnv = envApiConcurrency != null;
  int? apiMaxConcurrency = envApiConcurrency;
  final initialSources = <String>[];
  var enableApi = false;
  var apiOnly = false;
  var showHelp = false;
  var parseError = false;
  var hostSpecified = false;
  var portSpecified = false;
  var concurrencySpecified = false;

  void _appendSources(String rawSources) {
    final values = rawSources
        .split(RegExp(r'[,\n;]'))
        .map((value) => value.trim())
        .where((value) => value.isNotEmpty);
    for (final value in values) {
      if (!initialSources.contains(value)) {
        initialSources.add(value);
      }
    }
  }

  void _appendSourceFile(String path) {
    final file = File(path);
    if (!file.existsSync()) {
      parseError = true;
      return;
    }
    _appendSources(file.readAsStringSync());
  }

  _appendSources(envSources ?? '');

  for (var i = 0; i < args.length; i += 1) {
    final arg = args[i];
    if (arg == '--api') {
      enableApi = true;
    } else if (arg == '--api-only') {
      enableApi = true;
      apiOnly = true;
    } else if (arg == '--help' || arg == '-h') {
      showHelp = true;
    } else if (arg.startsWith('--api-host=')) {
      apiHost = arg.substring('--api-host='.length);
      hostSpecified = true;
      enableApi = true;
    } else if (arg == '--api-host' && i + 1 < args.length) {
      apiHost = args[i + 1];
      hostSpecified = true;
      i += 1;
      enableApi = true;
    } else if (arg == '--api-host') {
      parseError = true;
    } else if (arg.startsWith('--api-port=')) {
      final parsed = int.tryParse(arg.substring('--api-port='.length));
      if (parsed == null || parsed <= 0 || parsed > 65535) {
        parseError = true;
      } else {
        apiPort = parsed;
        portSpecified = true;
        enableApi = true;
      }
    } else if (arg == '--api-port' && i + 1 < args.length) {
      final parsed = int.tryParse(args[i + 1]);
      if (parsed == null || parsed <= 0 || parsed > 65535) {
        parseError = true;
      } else {
        apiPort = parsed;
        portSpecified = true;
        i += 1;
        enableApi = true;
      }
    } else if (arg == '--api-port') {
      parseError = true;
    } else if (arg.startsWith('--api-concurrency=')) {
      final parsed = int.tryParse(arg.substring('--api-concurrency='.length));
      if (parsed == null || parsed <= 0) {
        parseError = true;
      } else {
        apiMaxConcurrency = parsed;
        concurrencySpecified = true;
      }
      enableApi = true;
    } else if (arg == '--api-concurrency' && i + 1 < args.length) {
      final parsed = int.tryParse(args[i + 1]);
      if (parsed == null || parsed <= 0) {
        parseError = true;
      } else {
        apiMaxConcurrency = parsed;
        concurrencySpecified = true;
        i += 1;
      }
      enableApi = true;
    } else if (arg == '--api-concurrency') {
      parseError = true;
    } else if (arg.startsWith('--source=')) {
      _appendSources(arg.substring('--source='.length));
    } else if (arg == '--source' && i + 1 < args.length) {
      _appendSources(args[i + 1]);
      i += 1;
    } else if (arg == '--source') {
      parseError = true;
    } else if (arg.startsWith('--sources=')) {
      _appendSources(arg.substring('--sources='.length));
    } else if (arg == '--sources' && i + 1 < args.length) {
      _appendSources(args[i + 1]);
      i += 1;
    } else if (arg == '--sources') {
      parseError = true;
    } else if (arg.startsWith('--source-file=')) {
      _appendSourceFile(arg.substring('--source-file='.length));
    } else if (arg == '--source-file' && i + 1 < args.length) {
      _appendSourceFile(args[i + 1]);
      i += 1;
    } else if (arg == '--source-file') {
      parseError = true;
    }
  }

  if (concurrencySpecified && !_isValidApiConcurrency(apiMaxConcurrency)) {
    parseError = true;
  }
  if (portSpecified && apiPort != null && !_isValidApiPort(apiPort)) {
    parseError = true;
  }

  final parsedApiPort = enableApi && !portSpecified
      ? (apiPort ?? 9292)
      : (enableApi ? apiPort : null);
  final parsedApiHost = !enableApi
      ? apiHost
      : (hostSpecified || fallbackHost.isNotEmpty ? apiHost : '');
  final parsedApiConcurrency = !enableApi
      ? null
      : (concurrencySpecified || concurrencyFromEnv
          ? (apiMaxConcurrency ?? 32)
          : null);

  return _ApiStartupConfig(
    enableApi: enableApi,
    apiHost: parsedApiHost,
    apiPort: parsedApiPort,
    apiMaxConcurrency: parsedApiConcurrency,
    initialSources: initialSources,
    apiOnly: apiOnly,
    showHelp: showHelp || parseError,
    parseError: parseError,
  );
}

bool _isValidApiPort(int? value) {
  return value != null && value > 0 && value <= 65535;
}

bool _isValidApiConcurrency(int? value) {
  return value != null && value > 0 && value <= 64;
}

void _printStartupUsage() {
  // ignore: avoid_print
  print('''
Dataset Inspector API options:
  --api                enable API server for running dataset-inspector process.
  --api-only           start API server only, no UI window.
  --api-host <value>   API host, default: 127.0.0.1 (or DATASET_INSPECTOR_API_HOST).
  --api-port <value>   API port, default: 9292 (or DATASET_INSPECTOR_API_PORT).
  --api-concurrency <value> API default concurrency per request, default: 32.
  --source <value>     open a source at startup before serving API/UI. Repeatable.
  --sources <value>    open multiple sources (comma/semicolon/newline separated).
  --source-file <value> load sources from file.
  DATASET_INSPECTOR_SOURCES env var supports source list format.

Examples:
  flutter run -- --api --api-port 9090
  flutter run -- --api --api-only
  flutter run -- --api --api-only --source remote://host/path/to/mds_shards
  flutter run -- --api --api-only --sources remote://host/path/to/train,remote://host/path/to/dev
  flutter run -- --api --api-concurrency 16 --api-host 0.0.0.0 --api-port 9090
''');
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

class _ApiStartupConfig {
  const _ApiStartupConfig({
    required this.enableApi,
    required this.apiHost,
    required this.apiPort,
    required this.apiMaxConcurrency,
    required this.initialSources,
    required this.apiOnly,
    required this.showHelp,
    required this.parseError,
  });

  final bool enableApi;
  final String apiHost;
  final int? apiPort;
  final int? apiMaxConcurrency;
  final List<String> initialSources;
  final bool apiOnly;
  final bool showHelp;
  final bool parseError;
}
