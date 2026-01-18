import 'dart:typed_data';

import 'package:zstd/zstd.dart';

Uint8List decodeZstd(Uint8List input) {
  final codec = ZstdCodec();
  final decoded = codec.decode(input);
  return Uint8List.fromList(decoded);
}
