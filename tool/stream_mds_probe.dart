import 'dart:convert';
import 'dart:io';

class _Args {
  const _Args({
    required this.baseUrl,
    required this.datasetId,
    required this.source,
    required this.shardName,
    required this.offset,
    required this.limit,
    required this.audioFieldIndex,
    required this.textFieldIndex,
    required this.idFieldIndex,
    required this.audioEncoding,
  });

  final Uri baseUrl;
  final String? datasetId;
  final String? source;
  final String shardName;
  final int offset;
  final int limit;
  final int audioFieldIndex;
  final int textFieldIndex;
  final int idFieldIndex;
  final String audioEncoding;
}

Never _usage(String message) {
  stderr.writeln(message);
  stderr.writeln('');
  stderr.writeln('Usage:');
  stderr.writeln('  dart run tool/stream_mds_probe.dart \\');
  stderr.writeln('    --shard shard.00000.mds.zstd \\');
  stderr.writeln('    [--base-url http://127.0.0.1:9292] \\');
  stderr.writeln('    [--dataset-id <opened-dataset-id>] \\');
  stderr.writeln('    [--source remote://host/path/to/mds_shards] \\');
  stderr.writeln(
      '    [--offset 0] [--limit 8] [--audio-field 0] [--text-field 4] [--id-field 1] [--audio-encoding base64|none]');
  exit(2);
}

_Args _parseArgs(List<String> args) {
  Uri? baseUrl;
  String? datasetId;
  String? source;
  String? shardName;
  var offset = 0;
  var limit = 8;
  var audioFieldIndex = 0;
  var textFieldIndex = 4;
  var idFieldIndex = 1;
  var audioEncoding = 'base64';

  for (var i = 0; i < args.length; i += 1) {
    final arg = args[i];
    String next() {
      if (i + 1 >= args.length) _usage('Missing value for $arg');
      i += 1;
      return args[i];
    }

    switch (arg) {
      case '--base-url':
        baseUrl = Uri.parse(next());
      case '--dataset-id':
        datasetId = next();
      case '--source':
        source = next();
      case '--shard':
        shardName = next();
      case '--offset':
        offset = int.parse(next());
      case '--limit':
        limit = int.parse(next());
      case '--audio-field':
        audioFieldIndex = int.parse(next());
      case '--text-field':
        textFieldIndex = int.parse(next());
      case '--id-field':
        idFieldIndex = int.parse(next());
      case '--audio-encoding':
        audioEncoding = next().trim().toLowerCase();
      default:
        _usage('Unknown argument: $arg');
    }
  }

  if (shardName == null || shardName.trim().isEmpty) {
    _usage('Missing required --shard');
  }
  if (limit < 1) _usage('--limit must be >= 1');
  if (audioEncoding != 'base64' && audioEncoding != 'none') {
    _usage('--audio-encoding must be base64 or none');
  }

  return _Args(
    baseUrl: baseUrl ?? Uri.parse('http://127.0.0.1:9292'),
    datasetId: datasetId?.trim().isEmpty == true ? null : datasetId?.trim(),
    source: source?.trim().isEmpty == true ? null : source?.trim(),
    shardName: shardName.trim(),
    offset: offset,
    limit: limit,
    audioFieldIndex: audioFieldIndex,
    textFieldIndex: textFieldIndex,
    idFieldIndex: idFieldIndex,
    audioEncoding: audioEncoding,
  );
}

Uri _resolveUri(Uri base, String path) {
  final normalizedBase = base.path.endsWith('/')
      ? base.path.substring(0, base.path.length - 1)
      : base.path;
  return base.replace(path: '$normalizedBase$path');
}

Future<Map<String, dynamic>> _readJsonResponse(
  HttpClientResponse response,
) async {
  final raw = await utf8.decoder.bind(response).join();
  final decoded = raw.trim().isEmpty
      ? <String, dynamic>{}
      : Map<String, dynamic>.from(jsonDecode(raw) as Map);
  return decoded;
}

Future<List<Map<String, dynamic>>> _listOpenedDatasets(
  HttpClient client,
  Uri baseUrl,
) async {
  final request = await client.getUrl(_resolveUri(baseUrl, '/api/v1/opened'));
  request.headers.set(HttpHeaders.acceptHeader, 'application/json');
  final response = await request.close();
  final payload = await _readJsonResponse(response);
  if (response.statusCode != HttpStatus.ok) {
    throw StateError('Failed to list opened datasets: ${jsonEncode(payload)}');
  }
  final data = Map<String, dynamic>.from(payload['data'] as Map);
  return List<Map<String, dynamic>>.from(
    (data['datasets'] as List<dynamic>)
        .map((item) => Map<String, dynamic>.from(item as Map)),
  );
}

Future<String> _resolveDatasetId(HttpClient client, _Args args) async {
  if (args.datasetId != null) return args.datasetId!;
  final datasets = await _listOpenedDatasets(client, args.baseUrl);
  if (args.source != null) {
    for (final dataset in datasets) {
      final sourceInput = dataset['sourceInput']?.toString();
      if (sourceInput == args.source && dataset['isActive'] == true) {
        return dataset['id']!.toString();
      }
    }
    for (final dataset in datasets) {
      final sourceInput = dataset['sourceInput']?.toString();
      if (sourceInput == args.source && dataset['mode'] == 'mdsIndex') {
        return dataset['id']!.toString();
      }
    }
    for (final dataset in datasets) {
      final sourceInput = dataset['sourceInput']?.toString();
      if (sourceInput == args.source) {
        return dataset['id']!.toString();
      }
    }
    throw StateError('Opened dataset not found for --source ${args.source}');
  }

  for (final dataset in datasets) {
    if (dataset['isActive'] == true) {
      final id = dataset['id']?.toString();
      if (id != null && id.isNotEmpty) {
        return id;
      }
    }
  }
  throw StateError('No active opened dataset found.');
}

Future<void> main(List<String> args) async {
  final parsed = _parseArgs(args);
  final client = HttpClient();
  try {
    final datasetId = await _resolveDatasetId(client, parsed);
    final request = await client.postUrl(
      _resolveUri(
        parsed.baseUrl,
        '/api/v1/opened/${Uri.encodeComponent(datasetId)}/extract',
      ),
    );
    request.headers.set(HttpHeaders.acceptHeader, 'application/x-ndjson');
    request.headers.set(HttpHeaders.contentTypeHeader, 'application/json');
    request.write(jsonEncode(<String, dynamic>{
      'shardName': parsed.shardName,
      'offset': parsed.offset,
      'limit': parsed.limit,
      'audioFieldIndex': parsed.audioFieldIndex,
      'textFieldIndex': parsed.textFieldIndex,
      'idFieldIndex': parsed.idFieldIndex,
      'audioEncoding': parsed.audioEncoding,
      'responseMode': 'stream',
    }));
    final response = await request.close();
    if (response.statusCode != HttpStatus.ok) {
      final payload = await _readJsonResponse(response);
      throw StateError('Streaming extract failed: ${jsonEncode(payload)}');
    }
    await stdout.addStream(response);
  } finally {
    client.close(force: true);
  }
}
