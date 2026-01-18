import 'dart:io';
import 'dart:typed_data';

import 'sph2pipe_stub.dart' if (dart.library.ffi) 'sph2pipe.dart';

class SphereHeader {
  const SphereHeader({
    required this.channelCount,
    required this.sampleRate,
    required this.sampleNBytes,
    required this.sampleByteFormat,
    required this.sampleCoding,
  });

  final int channelCount;
  final int sampleRate;
  final int sampleNBytes;
  final String? sampleByteFormat;
  final String? sampleCoding;
}

bool isSphereFile(Uint8List data) {
  const magic = [0x4e, 0x49, 0x53, 0x54, 0x5f, 0x31, 0x41];
  if (data.length < magic.length) return false;
  for (var i = 0; i < magic.length; i += 1) {
    if (data[i] != magic[i]) return false;
  }
  return true;
}

(SphereHeader, int) parseSphereHeader(Uint8List data) {
  if (!isSphereFile(data)) {
    throw const FormatException('Not a SPHERE file.');
  }
  if (data.length < 16) {
    throw const FormatException('SPHERE file is too short.');
  }

  final headerSizeRaw = String.fromCharCodes(data.sublist(8, 16))
      .split(RegExp(r'[\r\n]'))
      .first
      .trim();
  final headerBytes = int.tryParse(headerSizeRaw);
  if (headerBytes == null || headerBytes <= 0 || headerBytes > data.length) {
    throw const FormatException('Invalid SPHERE header size.');
  }

  final headerText = String.fromCharCodes(data.sublist(0, headerBytes));
  final map = <String, String>{};
  for (final line in headerText.split(RegExp(r'[\r\n]+'))) {
    final trimmed = line.trim();
    if (trimmed.isEmpty) continue;
    if (trimmed == 'NIST_1A' || trimmed == 'end_head') continue;
    final parts = trimmed.split(RegExp(r'\s+'));
    if (parts.length < 3) continue;
    final key = parts[0];
    final value = parts.sublist(2).join(' ').trim();
    if (value.isEmpty) continue;
    map[key] = value;
  }

  int parseIntField(String key) {
    final value = map[key] ?? map['$key:'];
    if (value == null) {
      throw FormatException('Missing `$key` in SPHERE header.');
    }
    final parsed = int.tryParse(value);
    if (parsed == null) {
      throw FormatException('Invalid `$key` in SPHERE header.');
    }
    return parsed;
  }

  final header = SphereHeader(
    channelCount: parseIntField('channel_count'),
    sampleRate: parseIntField('sample_rate'),
    sampleNBytes: parseIntField('sample_n_bytes'),
    sampleByteFormat: map['sample_byte_format'],
    sampleCoding: map['sample_coding'],
  );

  return (header, headerBytes);
}

int _muLawToI16(int byte) {
  final inverted = byte ^ 0xff;
  final sign = inverted & 0x80;
  final exponent = (inverted >> 4) & 0x07;
  final mantissa = inverted & 0x0f;
  var sample = (mantissa << 3) + 0x84;
  sample <<= exponent;
  return sign != 0 ? -sample : sample;
}

int _aLawToI16(int byte) {
  final decoded = byte ^ 0x55;
  final sign = decoded & 0x80;
  final exponent = (decoded >> 4) & 0x07;
  final mantissa = decoded & 0x0f;
  var sample = exponent == 0 ? (mantissa << 4) | 0x08 : (mantissa << 4) + 0x108;
  if (exponent > 1) {
    sample <<= (exponent - 1);
  }
  return sign != 0 ? sample : -sample;
}

Uint8List _encodeWavHeader({
  required int dataLength,
  required int sampleRate,
  required int channels,
}) {
  final byteRate = sampleRate * channels * 2;
  final blockAlign = channels * 2;
  final totalSize = 36 + dataLength;
  final buffer = BytesBuilder();

  buffer.add('RIFF'.codeUnits);
  buffer.add(_u32le(totalSize));
  buffer.add('WAVE'.codeUnits);
  buffer.add('fmt '.codeUnits);
  buffer.add(_u32le(16));
  buffer.add(_u16le(1));
  buffer.add(_u16le(channels));
  buffer.add(_u32le(sampleRate));
  buffer.add(_u32le(byteRate));
  buffer.add(_u16le(blockAlign));
  buffer.add(_u16le(16));
  buffer.add('data'.codeUnits);
  buffer.add(_u32le(dataLength));

  return buffer.toBytes();
}

Uint8List _u16le(int value) {
  final data = ByteData(2)..setUint16(0, value, Endian.little);
  return data.buffer.asUint8List();
}

Uint8List _u32le(int value) {
  final data = ByteData(4)..setUint32(0, value, Endian.little);
  return data.buffer.asUint8List();
}

Uint8List decodeSphereToWav(Uint8List data) {
  final (header, headerBytes) = parseSphereHeader(data);
  final coding = (header.sampleCoding ?? 'pcm').toLowerCase();
  if (coding.contains('shorten')) {
    throw const FormatException('Shorten-compressed SPHERE audio is not supported.');
  }

  final payload = data.sublist(headerBytes);
  final isBigEndian = (header.sampleByteFormat ?? '').trim() == '10';

  final pcm = BytesBuilder();
  if (coding.contains('pcm') && header.sampleNBytes == 2) {
    for (var i = 0; i + 1 < payload.length; i += 2) {
      final sample = isBigEndian
          ? (payload[i] << 8) | payload[i + 1]
          : (payload[i + 1] << 8) | payload[i];
      final value = sample >= 0x8000 ? sample - 0x10000 : sample;
      pcm.add(_u16le(value & 0xffff));
    }
  } else if (coding.contains('pcm') && header.sampleNBytes == 1) {
    for (final b in payload) {
      final signed = b >= 0x80 ? b - 0x100 : b;
      final sample = signed << 8;
      pcm.add(_u16le(sample & 0xffff));
    }
  } else if ((coding.contains('ulaw') || coding.contains('mulaw') || coding.contains('mu-law')) &&
      header.sampleNBytes == 1) {
    for (final b in payload) {
      final sample = _muLawToI16(b);
      pcm.add(_u16le(sample & 0xffff));
    }
  } else if ((coding.contains('alaw') || coding.contains('a-law')) && header.sampleNBytes == 1) {
    for (final b in payload) {
      final sample = _aLawToI16(b);
      pcm.add(_u16le(sample & 0xffff));
    }
  } else {
    throw FormatException(
      'Unsupported SPHERE coding (coding=$coding, sample_n_bytes=${header.sampleNBytes}).',
    );
  }

  final pcmBytes = pcm.toBytes();
  final headerBytesWav = _encodeWavHeader(
    dataLength: pcmBytes.length,
    sampleRate: header.sampleRate,
    channels: header.channelCount,
  );

  final wav = BytesBuilder();
  wav.add(headerBytesWav);
  wav.add(pcmBytes);
  return wav.toBytes();
}

Future<Uint8List> decodeSphereToWavWithFallback(Uint8List data) async {
  final (header, headerBytes) = parseSphereHeader(data);
  final coding = (header.sampleCoding ?? 'pcm').toLowerCase();
  if (!coding.contains('shorten')) {
    return decodeSphereToWav(data);
  }
  return _decodeShortenSphereToWav(data, header, headerBytes);
}

Future<Uint8List> _decodeShortenSphereToWav(
  Uint8List data,
  SphereHeader header,
  int headerBytes,
) async {
  if (!Sph2Pipe.isAvailable) {
    throw const FormatException('Shorten-compressed SPHERE audio is not supported on this platform.');
  }
  final tempDir = Directory('${Directory.systemTemp.path}/dataset-inspector/sph2pipe');
  await tempDir.create(recursive: true);
  final hash = Object.hashAll(data).toUnsigned(20).toRadixString(16).padLeft(5, '0');
  final sphFile = File('${tempDir.path}/$hash.sph');
  final pcmFile = File('${tempDir.path}/$hash.pcm');
  await sphFile.writeAsBytes(data, flush: true);
  try {
    final rc = Sph2Pipe.decodeToPcm16Le(sphFile.path, headerBytes, pcmFile.path);
    if (rc != 0) {
      throw FormatException('Shorten decode failed (code $rc).');
    }
    final pcmBytes = await pcmFile.readAsBytes();
    final headerBytesWav = _encodeWavHeader(
      dataLength: pcmBytes.length,
      sampleRate: header.sampleRate,
      channels: header.channelCount,
    );
    final wav = BytesBuilder();
    wav.add(headerBytesWav);
    wav.add(pcmBytes);
    return wav.toBytes();
  } finally {
    try {
      if (await sphFile.exists()) {
        await sphFile.delete();
      }
    } catch (_) {}
    try {
      if (await pcmFile.exists()) {
        await pcmFile.delete();
      }
    } catch (_) {}
  }
}

Future<File> writeSphereAsWav(Uint8List data, File out) async {
  final wavBytes = await decodeSphereToWavWithFallback(data);
  await out.writeAsBytes(wavBytes, flush: true);
  return out;
}
