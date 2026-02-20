class Sph2Pipe {
  Sph2Pipe._();

  static bool get isAvailable => false;

  static int decodeToPcm16Le(String sphPath, int headerBytes, String pcmPath) {
    throw UnsupportedError(
      'Shorten-compressed SPHERE audio is not supported on this platform.',
    );
  }
}
