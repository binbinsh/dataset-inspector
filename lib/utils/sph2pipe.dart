import 'dart:ffi';
import 'dart:io';

import 'package:ffi/ffi.dart';

typedef _Sph2PipeNative = Int32 Function(Pointer<Utf8>, Int64, Pointer<Utf8>);
typedef _Sph2PipeDart = int Function(Pointer<Utf8>, int, Pointer<Utf8>);

class Sph2Pipe {
  Sph2Pipe._();

  static _Sph2PipeDart? _decodeFn;
  static Object? _loadError;

  static bool get isAvailable {
    if (Platform.isWindows) return false;
    if (_decodeFn != null) return true;
    if (_loadError != null) return false;
    try {
      _decodeFn = DynamicLibrary.process()
          .lookupFunction<_Sph2PipeNative, _Sph2PipeDart>('sph2pipe_shorten_to_pcm16le');
      return true;
    } catch (err) {
      _loadError = err;
      return false;
    }
  }

  static int decodeToPcm16Le(String sphPath, int headerBytes, String pcmPath) {
    if (!isAvailable) {
      throw UnsupportedError('Shorten-compressed SPHERE audio is not supported on this platform.');
    }
    final sphPtr = sphPath.toNativeUtf8();
    final pcmPtr = pcmPath.toNativeUtf8();
    try {
      return _decodeFn!(sphPtr, headerBytes, pcmPtr);
    } finally {
      malloc.free(sphPtr);
      malloc.free(pcmPtr);
    }
  }
}
