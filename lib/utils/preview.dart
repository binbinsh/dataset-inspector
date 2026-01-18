import 'dart:convert';
import 'dart:typed_data';

const previewTextChars = 8 * 1024;

String? previewUtf8Text(Uint8List data) {
  if (data.isEmpty) return '';
  try {
    final text = utf8.decode(data);
    return _takeChars(text, previewTextChars);
  } on FormatException catch (e) {
    final offset = e.offset ?? 0;
    if (offset >= data.length - 4) {
      final prefix = data.sublist(0, offset.clamp(0, data.length));
      final text = utf8.decode(prefix, allowMalformed: true);
      if (text.contains('\uFFFD')) return null;
      return _takeChars(text, previewTextChars);
    }
    return null;
  }
}

String hexSnippet(Uint8List data, {int maxBytes = 48}) {
  final limit = data.length < maxBytes ? data.length : maxBytes;
  final buffer = StringBuffer();
  for (var i = 0; i < limit; i += 1) {
    buffer.write(data[i].toRadixString(16).padLeft(2, '0'));
  }
  return buffer.toString();
}

String _takeChars(String text, int maxChars) {
  var count = 0;
  final buffer = StringBuffer();
  for (final rune in text.runes) {
    if (count >= maxChars) break;
    buffer.writeCharCode(rune);
    count += 1;
  }
  return buffer.toString();
}
