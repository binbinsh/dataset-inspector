/// Raw FFI bindings to libsmbclient.
library;

import 'dart:ffi';

import 'package:ffi/ffi.dart';

// ---------------------------------------------------------------------------
// Auth callback: writable buffers MUST be Pointer<Void>, not Pointer<Utf8>.
// Using Pointer<Utf8> causes Dart to treat them as immutable strings.
// ---------------------------------------------------------------------------

typedef SmcAuthFnNative = Void Function(
  Pointer<Utf8> server,   // read-only
  Pointer<Utf8> share,    // read-only
  Pointer<Void> workgroup, Int32 wgLen,   // writable buffer
  Pointer<Void> username,  Int32 unLen,   // writable buffer
  Pointer<Void> password,  Int32 pwLen,   // writable buffer
);

// int smbc_init(smbc_get_auth_data_fn fn, int debug)
typedef _InitC = Int32 Function(Pointer<NativeFunction<SmcAuthFnNative>>, Int32);
typedef _InitDart = int Function(Pointer<NativeFunction<SmcAuthFnNative>>, int);

// int smbc_open(const char *furl, int flags, mode_t mode)
typedef _OpenC = Int32 Function(Pointer<Utf8>, Int32, Uint32);
typedef _OpenDart = int Function(Pointer<Utf8>, int, int);

// ssize_t smbc_read(int fd, void *buf, size_t bufsize)
typedef _ReadC = IntPtr Function(Int32, Pointer<Uint8>, IntPtr);
typedef _ReadDart = int Function(int, Pointer<Uint8>, int);

// off_t smbc_lseek(int fd, off_t offset, int whence)
typedef _LseekC = Int64 Function(Int32, Int64, Int32);
typedef _LseekDart = int Function(int, int, int);

// int smbc_close(int fd)
typedef _CloseC = Int32 Function(Int32);
typedef _CloseDart = int Function(int);

// int smbc_opendir(const char *durl)
typedef _OpendirC = Int32 Function(Pointer<Utf8>);
typedef _OpendirDart = int Function(Pointer<Utf8>);

// struct smbc_dirent* smbc_readdir(unsigned int dh)
typedef _ReaddirC = Pointer<Void> Function(Uint32);
typedef _ReaddirDart = Pointer<Void> Function(int);

// int smbc_closedir(int dh)
typedef _ClosedirC = Int32 Function(Int32);
typedef _ClosedirDart = int Function(int);

// ssize_t smbc_write(int fd, const void *buf, size_t bufsize)
typedef _WriteC = IntPtr Function(Int32, Pointer<Uint8>, IntPtr);
typedef _WriteDart = int Function(int, Pointer<Uint8>, int);

// int smbc_unlink(const char *furl)
typedef _UnlinkC = Int32 Function(Pointer<Utf8>);
typedef _UnlinkDart = int Function(Pointer<Utf8>);

// int smbc_mkdir(const char *durl, mode_t mode)
typedef _MkdirC = Int32 Function(Pointer<Utf8>, Uint32);
typedef _MkdirDart = int Function(Pointer<Utf8>, int);

// int smbc_stat(const char *url, struct stat *st)
typedef _StatC = Int32 Function(Pointer<Utf8>, Pointer<Uint8>);
typedef _StatDart = int Function(Pointer<Utf8>, Pointer<Uint8>);

// ---------------------------------------------------------------------------
// smbc_dirent type constants
// ---------------------------------------------------------------------------

abstract class SmbcType {
  static const int workgroup = 1;
  static const int server = 2;
  static const int fileShare = 3;
  static const int dir = 7;
  static const int file = 8;
  static const int link = 9;
}

// ---------------------------------------------------------------------------
// Library loader + bindings
// ---------------------------------------------------------------------------

class LibSmbClient {
  LibSmbClient._(this._lib);

  final DynamicLibrary _lib;
  static LibSmbClient? _instance;

  factory LibSmbClient() {
    if (_instance != null) return _instance!;
    _instance = LibSmbClient._(_openLib());
    return _instance!;
  }

  static DynamicLibrary _openLib() {
    const paths = [
      '/opt/homebrew/lib/libsmbclient.dylib',
      '/usr/local/lib/libsmbclient.dylib',
      'libsmbclient.so',
      '/usr/lib/libsmbclient.so',
      '/usr/lib/x86_64-linux-gnu/libsmbclient.so',
      '/usr/lib/aarch64-linux-gnu/libsmbclient.so',
    ];
    for (final p in paths) {
      try { return DynamicLibrary.open(p); } catch (_) {}
    }
    throw StateError(
      'libsmbclient not found. '
      'macOS: brew install samba  |  Linux: apt install libsmbclient-dev',
    );
  }

  static bool get isAvailable {
    try { LibSmbClient(); return true; } catch (_) { return false; }
  }

  late final init = _lib.lookupFunction<_InitC, _InitDart>('smbc_init');
  late final open = _lib.lookupFunction<_OpenC, _OpenDart>('smbc_open');
  late final read = _lib.lookupFunction<_ReadC, _ReadDart>('smbc_read');
  late final lseek = _lib.lookupFunction<_LseekC, _LseekDart>('smbc_lseek');
  late final close = _lib.lookupFunction<_CloseC, _CloseDart>('smbc_close');
  late final opendir = _lib.lookupFunction<_OpendirC, _OpendirDart>('smbc_opendir');
  late final readdir = _lib.lookupFunction<_ReaddirC, _ReaddirDart>('smbc_readdir');
  late final closedir = _lib.lookupFunction<_ClosedirC, _ClosedirDart>('smbc_closedir');
  late final write = _lib.lookupFunction<_WriteC, _WriteDart>('smbc_write');
  late final unlink = _lib.lookupFunction<_UnlinkC, _UnlinkDart>('smbc_unlink');
  late final mkdir = _lib.lookupFunction<_MkdirC, _MkdirDart>('smbc_mkdir');
  late final stat = _lib.lookupFunction<_StatC, _StatDart>('smbc_stat');
}
