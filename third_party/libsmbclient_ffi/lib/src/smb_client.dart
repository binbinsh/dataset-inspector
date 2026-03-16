/// High-level async SMB3 client backed by libsmbclient FFI.
///
/// Uses a process-global singleton worker isolate.  smbc_init is called
/// exactly once for the entire process lifetime, avoiding talloc corruption.
/// Auth credentials are updated via the callback before each open().
library;

import 'dart:async';
import 'dart:ffi';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'bindings.dart';

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

class SmbEntry {
  const SmbEntry({
    required this.name,
    required this.isDirectory,
    this.sizeBytes,
    this.type,
  });

  final String name;
  final bool isDirectory;
  final int? sizeBytes;
  final int? type;
}

// ---------------------------------------------------------------------------
// Worker protocol
// ---------------------------------------------------------------------------

enum _Op { readFile, listDir, writeFile, deleteFile, mkDir, stat }

class _Request {
  const _Request(this.op, this.url, this.replyPort, this.creds, [this.data]);
  final _Op op;
  final String url;
  final SendPort replyPort;
  final _Creds creds;
  final Uint8List? data;
}

class _Creds {
  const _Creds(this.workgroup, this.username, this.password);
  final String workgroup;
  final String username;
  final String password;
}

// ---------------------------------------------------------------------------
// Auth callback — process-global, updated before each smbc_open
// ---------------------------------------------------------------------------

String _authWg = 'WORKGROUP';
String _authUn = '';
String _authPw = '';

void _authCallback(
  Pointer<Utf8> server, Pointer<Utf8> share,
  Pointer<Void> wg, int wgLen,
  Pointer<Void> un, int unLen,
  Pointer<Void> pw, int pwLen,
) {
  _writeC(wg, wgLen, _authWg);
  _writeC(un, unLen, _authUn);
  _writeC(pw, pwLen, _authPw);
}

void _writeC(Pointer<Void> buf, int maxLen, String value) {
  if (maxLen <= 0) return;
  final dst = buf.cast<Uint8>();
  final bytes = value.codeUnits;
  final len = bytes.length < maxLen - 1 ? bytes.length : maxLen - 1;
  for (var i = 0; i < len; i++) dst[i] = bytes[i];
  dst[len] = 0;
}

// ---------------------------------------------------------------------------
// Singleton worker isolate
// ---------------------------------------------------------------------------

Completer<SendPort>? _workerReady;
Isolate? _workerIsolate;

Future<SendPort> _getWorker() async {
  if (_workerReady != null) return _workerReady!.future;
  _workerReady = Completer<SendPort>();

  final receivePort = ReceivePort();
  _workerIsolate = await Isolate.spawn(_workerMain, receivePort.sendPort);

  final first = await receivePort.first;
  if (first is SendPort) {
    _workerReady!.complete(first);
  } else {
    _workerReady!.completeError(first is Error ? first : StateError('$first'));
  }
  return _workerReady!.future;
}

void _workerMain(SendPort initPort) {
  final lib = LibSmbClient();
  final fnPtr = Pointer.fromFunction<SmcAuthFnNative>(_authCallback);
  final rc = lib.init(fnPtr, 0);
  if (rc < 0) {
    initPort.send(StateError('smbc_init failed: $rc'));
    return;
  }

  final port = ReceivePort();
  initPort.send(port.sendPort);

  port.listen((message) {
    if (message is! _Request) return;
    final req = message;
    // Update credentials before each operation
    _authWg = req.creds.workgroup;
    _authUn = req.creds.username;
    _authPw = req.creds.password;

    try {
      final result = _dispatch(lib, req);
      req.replyPort.send(result);
    } catch (e) {
      req.replyPort.send(e);
    }
  });
}

Object _dispatch(LibSmbClient lib, _Request req) {
  switch (req.op) {
    case _Op.readFile:  return _doRead(lib, req.url);
    case _Op.listDir:   return _doList(lib, req.url);
    case _Op.writeFile: _doWrite(lib, req.url, req.data!); return true;
    case _Op.deleteFile: _doDelete(lib, req.url); return true;
    case _Op.mkDir:     _doMkdir(lib, req.url); return true;
    case _Op.stat:      return _doStat(lib, req.url);
  }
}

// ---------------------------------------------------------------------------
// SmbClient
// ---------------------------------------------------------------------------

class SmbClient {
  SmbClient({
    required this.host,
    required this.share,
    this.username,
    this.password,
    this.basePath,
    String workgroup = 'WORKGROUP',
  }) : _workgroup = workgroup;

  final String host;
  final String share;
  final String? username;
  final String? password;
  final String? basePath;
  final String _workgroup;

  static bool get isAvailable => LibSmbClient.isAvailable;

  _Creds get _creds => _Creds(_workgroup, username ?? '', password ?? '');

  String _url(String rel) => buildUrl(
    host: host, share: share, basePath: basePath, relativePath: rel,
  );

  static String buildUrl({
    required String host, required String share,
    String? basePath, required String relativePath,
  }) {
    final parts = <String>[share.trim()];
    final b = basePath?.trim();
    if (b != null && b.isNotEmpty) parts.add(b);
    final r = relativePath.trim();
    if (r.isNotEmpty) parts.add(r);
    return 'smb://${host.trim()}/${parts.join('/').replaceAll(RegExp(r'/+'), '/')}';
  }

  Future<T> _send<T>(_Op op, String url, [Uint8List? data]) async {
    final worker = await _getWorker();
    final reply = ReceivePort();
    worker.send(_Request(op, url, reply.sendPort, _creds, data));
    final result = await reply.first;
    if (result is Error) throw result;
    if (result is Exception) throw result;
    return result as T;
  }

  Future<Uint8List> readFile(String p) => _send<Uint8List>(_Op.readFile, _url(p));

  Stream<Uint8List> openRead(String p, {int? maxBytes}) async* {
    final d = await readFile(p);
    yield maxBytes != null && maxBytes < d.length
        ? Uint8List.sublistView(d, 0, maxBytes) : d;
  }

  Future<List<SmbEntry>> listDir(String p) =>
      _send<List<SmbEntry>>(_Op.listDir, _url(p.isEmpty ? '' : p));

  Future<void> writeFile(String p, Uint8List d) =>
      _send<bool>(_Op.writeFile, _url(p), d);

  Future<void> deleteFile(String p) => _send<bool>(_Op.deleteFile, _url(p));

  Future<void> mkDir(String p) => _send<bool>(_Op.mkDir, _url(p));

  Future<({bool exists, bool isDirectory})> stat(String p) =>
      _send<({bool exists, bool isDirectory})>(_Op.stat, _url(p));

  Future<bool> testConnection() async {
    try { await listDir(''); return true; } catch (_) { return false; }
  }

  void dispose() {} // Worker is process-global, never disposed
}

// ---------------------------------------------------------------------------
// FFI operations (worker isolate only)
// ---------------------------------------------------------------------------

const int _oRdonly = 0;
const int _bufSz = 4 * 1024 * 1024;

Uint8List _doRead(LibSmbClient lib, String url) {
  final p = url.toNativeUtf8();
  final fd = lib.open(p, _oRdonly, 0);
  malloc.free(p);
  if (fd < 0) throw StateError('smbc_open failed: $url (fd=$fd)');
  final buf = malloc<Uint8>(_bufSz);
  final b = BytesBuilder(copy: false);
  try {
    while (true) {
      final n = lib.read(fd, buf, _bufSz);
      if (n <= 0) break;
      b.add(Uint8List.fromList(buf.asTypedList(n)));
    }
  } finally { malloc.free(buf); lib.close(fd); }
  return b.toBytes();
}

List<SmbEntry> _doList(LibSmbClient lib, String url) {
  final p = url.toNativeUtf8();
  final dh = lib.opendir(p);
  malloc.free(p);
  if (dh < 0) throw StateError('smbc_opendir failed: $url');
  final out = <SmbEntry>[];
  try {
    while (true) {
      final ptr = lib.readdir(dh);
      if (ptr == nullptr) break;
      final raw = ptr.cast<Uint8>();
      final type = raw.cast<Uint32>().value;
      final nlen = raw.elementAt(24).cast<Uint32>().value;
      if (nlen <= 0) continue;
      final name = raw.elementAt(28).cast<Utf8>().toDartString(length: nlen);
      if (name == '.' || name == '..') continue;
      out.add(SmbEntry(name: name, isDirectory: type == SmbcType.dir, type: type));
    }
  } finally { lib.closedir(dh); }
  return out;
}

void _doWrite(LibSmbClient lib, String url, Uint8List data) {
  final p = url.toNativeUtf8();
  final fd = lib.open(p, 0x601, 0x1A4);
  malloc.free(p);
  if (fd < 0) throw StateError('smbc_open(write) failed: $url');
  final buf = malloc<Uint8>(data.length);
  try {
    buf.asTypedList(data.length).setAll(0, data);
    var w = 0;
    while (w < data.length) {
      final n = lib.write(fd, buf.elementAt(w), data.length - w);
      if (n <= 0) throw StateError('smbc_write failed');
      w += n;
    }
  } finally { malloc.free(buf); lib.close(fd); }
}

void _doDelete(LibSmbClient lib, String url) {
  final p = url.toNativeUtf8();
  final rc = lib.unlink(p);
  malloc.free(p);
  if (rc < 0) throw StateError('smbc_unlink failed: $url');
}

void _doMkdir(LibSmbClient lib, String url) {
  final p = url.toNativeUtf8();
  lib.mkdir(p, 0x1ED);
  malloc.free(p);
}

({bool exists, bool isDirectory}) _doStat(LibSmbClient lib, String url) {
  final sb = malloc<Uint8>(256);
  final p = url.toNativeUtf8();
  try {
    final rc = lib.stat(p, sb);
    if (rc < 0) return (exists: false, isDirectory: false);
    final mode = sb.cast<Uint16>().elementAt(2).value;
    return (exists: true, isDirectory: (mode & 0xF000) == 0x4000);
  } finally { malloc.free(p); malloc.free(sb); }
}
