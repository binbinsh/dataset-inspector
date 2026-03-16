import 'dart:async';
import 'dart:typed_data';

import 'package:http/http.dart' as http;

typedef HttpStatusCallback = void Function(String message);

class HttpDatasetService {
  HttpDatasetService({http.Client? client}) : _client = client ?? http.Client();

  final http.Client _client;

  Future<Uint8List> readBytes({
    required Uri url,
    int? maxBytes,
    HttpStatusCallback? onStatus,
  }) async {
    final request = http.Request('GET', url);
    request.headers['Cache-Control'] = 'no-cache';
    request.headers['Pragma'] = 'no-cache';
    final cap = maxBytes != null && maxBytes > 0 ? maxBytes : null;
    if (cap != null) {
      request.headers['Range'] = 'bytes=0-${cap - 1}';
    }

    onStatus?.call('HTTP GET $url');
    final response = await _client.send(request);
    final status = response.statusCode;
    final ok = status == 200 || status == 206;
    if (!ok) {
      throw FormatException('HTTP $status while reading $url');
    }

    final builder = BytesBuilder(copy: false);
    var total = 0;
    await for (final chunk in response.stream) {
      if (chunk.isEmpty) continue;
      if (cap == null) {
        builder.add(chunk);
        continue;
      }
      final remain = cap - total;
      if (remain <= 0) break;
      if (chunk.length <= remain) {
        builder.add(chunk);
        total += chunk.length;
        continue;
      }
      builder.add(chunk.sublist(0, remain));
      total += remain;
      break;
    }
    return Uint8List.fromList(builder.takeBytes());
  }

  Stream<List<int>> openRead({
    required Uri url,
    int? maxBytes,
    HttpStatusCallback? onStatus,
  }) async* {
    final request = http.Request('GET', url);
    request.headers['Cache-Control'] = 'no-cache';
    request.headers['Pragma'] = 'no-cache';
    final cap = maxBytes != null && maxBytes > 0 ? maxBytes : null;
    if (cap != null) {
      request.headers['Range'] = 'bytes=0-${cap - 1}';
    }
    onStatus?.call('Streaming HTTP $url');
    final response = await _client.send(request);
    final status = response.statusCode;
    if (status < 200 || status >= 300) {
      throw FormatException('HTTP $status while streaming $url');
    }
    var total = 0;
    await for (final chunk in response.stream) {
      if (chunk.isEmpty) continue;
      if (cap != null) {
        final remain = cap - total;
        if (remain <= 0) {
          break;
        }
        if (chunk.length > remain) {
          yield chunk.sublist(0, remain);
          break;
        }
        total += chunk.length;
      }
      yield chunk;
    }
  }

  Uri parentDirectoryUri(Uri source) {
    final segments = source.pathSegments.toList(growable: true);
    if (segments.isNotEmpty) {
      if (segments.last.isNotEmpty) {
        segments.removeLast();
      }
    }
    if (segments.isEmpty || segments.last.isNotEmpty) {
      segments.add('');
    }
    return source.replace(
      pathSegments: segments,
      query: null,
      fragment: null,
    );
  }

  Uri resolveFromDirectory(Uri directory, String relativePath) {
    final normalized = relativePath.trim().replaceAll('\\', '/');
    final base = directory.path.endsWith('/')
        ? directory
        : directory.replace(path: '${directory.path}/');
    return base.resolve(normalized);
  }
}
