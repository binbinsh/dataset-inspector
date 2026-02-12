import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'parquet_reader.dart';
import 'zstd.dart' as zstd_util;

/// Page header types
enum PageType {
  dataPage(0),
  indexPage(1),
  dictionaryPage(2),
  dataPageV2(3);

  const PageType(this.value);
  final int value;

  static PageType? fromValue(int value) {
    for (final t in values) {
      if (t.value == value) return t;
    }
    return null;
  }
}

/// Parsed page header from Parquet data.
class PageHeader {
  PageHeader({
    required this.type,
    required this.uncompressedPageSize,
    required this.compressedPageSize,
    this.numValues,
    this.encoding,
    this.definitionLevelEncoding,
    this.repetitionLevelEncoding,
    this.numNulls,
    this.numRows,
    this.isCompressed,
    this.definitionLevelsByteLength,
    this.repetitionLevelsByteLength,
  });

  final PageType type;
  final int uncompressedPageSize;
  final int compressedPageSize;
  final int? numValues;
  final ParquetEncoding? encoding;
  final ParquetEncoding? definitionLevelEncoding;
  final ParquetEncoding? repetitionLevelEncoding;
  final int? numNulls;
  final int? numRows;
  final bool? isCompressed;
  final int? definitionLevelsByteLength;
  final int? repetitionLevelsByteLength;
}

/// Thrift Compact Protocol reader.
class ThriftCompactReader {
  ThriftCompactReader(this.data, [this.offset = 0]);

  final Uint8List data;
  int offset;

  int get remaining => data.length - offset;

  bool get hasMore => offset < data.length;

  int readByte() {
    if (offset >= data.length) throw const FormatException('Unexpected end of data');
    return data[offset++];
  }

  int readVarInt() {
    var result = 0;
    var shift = 0;
    while (offset < data.length) {
      final byte = data[offset++];
      result |= (byte & 0x7F) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
      if (shift > 63) throw const FormatException('VarInt too large');
    }
    return result;
  }

  int readZigZag() {
    final n = readVarInt();
    return (n >>> 1) ^ -(n & 1);
  }

  int readI32() => readZigZag();
  int readI64() => readZigZag();

  String readString() {
    final len = readVarInt();
    if (len < 0 || offset + len > data.length) {
      throw const FormatException('Invalid string length');
    }
    final bytes = data.sublist(offset, offset + len);
    offset += len;
    return String.fromCharCodes(bytes);
  }

  Uint8List readBinary() {
    final len = readVarInt();
    if (len < 0 || offset + len > data.length) {
      throw const FormatException('Invalid binary length');
    }
    final bytes = data.sublist(offset, offset + len);
    offset += len;
    return bytes;
  }

  (int, int)? readFieldHeader(int lastFieldId) {
    if (offset >= data.length) return null;
    final byte = data[offset++];
    if (byte == 0) return null;

    final typeId = byte & 0x0F;
    final fieldIdDelta = (byte >> 4) & 0x0F;

    int fieldId;
    if (fieldIdDelta == 0) {
      fieldId = readZigZag();
    } else {
      fieldId = lastFieldId + fieldIdDelta;
    }

    return (fieldId, typeId);
  }

  void skipField(int typeId) {
    switch (typeId) {
      case 0: // STOP
        break;
      case 1: // BOOL_TRUE
      case 2: // BOOL_FALSE
        break;
      case 3: // BYTE
        if (offset < data.length) offset++;
        break;
      case 4: // I16
      case 5: // I32
        readVarInt();
        break;
      case 6: // I64
        readVarInt();
        break;
      case 7: // DOUBLE
        offset += 8;
        break;
      case 8: // BINARY/STRING
        final len = readVarInt();
        offset += len;
        break;
      case 9: // LIST
      case 10: // SET
        final header = readByte();
        var size = (header >> 4) & 0x0F;
        if (size == 15) size = readVarInt();
        final elemType = header & 0x0F;
        for (var i = 0; i < size; i++) {
          skipField(elemType);
        }
        break;
      case 11: // MAP
        final size = readVarInt();
        if (size > 0) {
          final types = readByte();
          final keyType = (types >> 4) & 0x0F;
          final valType = types & 0x0F;
          for (var i = 0; i < size; i++) {
            skipField(keyType);
            skipField(valType);
          }
        }
        break;
      case 12: // STRUCT
        var lastId = 0;
        while (offset < data.length) {
          final field = readFieldHeader(lastId);
          if (field == null) break;
          lastId = field.$1;
          skipField(field.$2);
        }
        break;
      default:
        // Unknown type - skip silently
        break;
    }
  }
}

/// Parse a page header from Thrift-encoded data.
/// Returns the header and the number of bytes consumed.
(PageHeader, int) parsePageHeader(Uint8List data, int startOffset) {
  final reader = ThriftCompactReader(data, startOffset);

  PageType type = PageType.dataPage;
  int uncompressedPageSize = 0;
  int compressedPageSize = 0;
  int? numValues;
  ParquetEncoding? encoding;
  ParquetEncoding? definitionLevelEncoding;
  ParquetEncoding? repetitionLevelEncoding;
  int? numNulls;
  int? numRows;
  bool? isCompressed;
  int? definitionLevelsByteLength;
  int? repetitionLevelsByteLength;

  var lastFieldId = 0;
  while (reader.hasMore) {
    final field = reader.readFieldHeader(lastFieldId);
    if (field == null) break;
    lastFieldId = field.$1;
    final fieldId = field.$1;
    final typeId = field.$2;

    switch (fieldId) {
      case 1: // type
        type = PageType.fromValue(reader.readI32()) ?? PageType.dataPage;
        break;
      case 2: // uncompressed_page_size
        uncompressedPageSize = reader.readI32();
        break;
      case 3: // compressed_page_size
        compressedPageSize = reader.readI32();
        break;
      case 4: // crc
        reader.readI32();
        break;
      case 5: // data_page_header
        _parseDataPageHeader(
          reader,
          (n) => numValues = n,
          (e) => encoding = e,
          (e) => definitionLevelEncoding = e,
          (e) => repetitionLevelEncoding = e,
        );
        break;
      case 7: // dictionary_page_header
        _parseDictionaryPageHeader(
          reader,
          (n) => numValues = n,
          (e) => encoding = e,
        );
        break;
      case 8: // data_page_header_v2
        _parseDataPageHeaderV2(
          reader,
          (n) => numValues = n,
          (n) => numNulls = n,
          (n) => numRows = n,
          (e) => encoding = e,
          (c) => isCompressed = c,
          (n) => definitionLevelsByteLength = n,
          (n) => repetitionLevelsByteLength = n,
        );
        break;
      default:
        reader.skipField(typeId);
    }
  }

  final bytesConsumed = reader.offset - startOffset;

  return (
    PageHeader(
      type: type,
      uncompressedPageSize: uncompressedPageSize,
      compressedPageSize: compressedPageSize,
      numValues: numValues,
      encoding: encoding,
      definitionLevelEncoding: definitionLevelEncoding,
      repetitionLevelEncoding: repetitionLevelEncoding,
      numNulls: numNulls,
      numRows: numRows,
      isCompressed: isCompressed,
      definitionLevelsByteLength: definitionLevelsByteLength,
      repetitionLevelsByteLength: repetitionLevelsByteLength,
    ),
    bytesConsumed
  );
}

void _parseDataPageHeader(
  ThriftCompactReader reader,
  void Function(int) setNumValues,
  void Function(ParquetEncoding) setEncoding,
  void Function(ParquetEncoding) setDefEncoding,
  void Function(ParquetEncoding) setRepEncoding,
) {
  var lastFieldId = 0;
  while (reader.hasMore) {
    final field = reader.readFieldHeader(lastFieldId);
    if (field == null) break;
    lastFieldId = field.$1;
    final fieldId = field.$1;
    final typeId = field.$2;

    switch (fieldId) {
      case 1: // num_values
        setNumValues(reader.readI32());
        break;
      case 2: // encoding
        setEncoding(ParquetEncoding.fromValue(reader.readI32()) ?? ParquetEncoding.plain);
        break;
      case 3: // definition_level_encoding
        setDefEncoding(ParquetEncoding.fromValue(reader.readI32()) ?? ParquetEncoding.rle);
        break;
      case 4: // repetition_level_encoding
        setRepEncoding(ParquetEncoding.fromValue(reader.readI32()) ?? ParquetEncoding.rle);
        break;
      default:
        reader.skipField(typeId);
    }
  }
}

void _parseDictionaryPageHeader(
  ThriftCompactReader reader,
  void Function(int) setNumValues,
  void Function(ParquetEncoding) setEncoding,
) {
  var lastFieldId = 0;
  while (reader.hasMore) {
    final field = reader.readFieldHeader(lastFieldId);
    if (field == null) break;
    lastFieldId = field.$1;
    final fieldId = field.$1;
    final typeId = field.$2;

    switch (fieldId) {
      case 1: // num_values
        setNumValues(reader.readI32());
        break;
      case 2: // encoding
        setEncoding(ParquetEncoding.fromValue(reader.readI32()) ?? ParquetEncoding.plain);
        break;
      default:
        reader.skipField(typeId);
    }
  }
}

void _parseDataPageHeaderV2(
  ThriftCompactReader reader,
  void Function(int) setNumValues,
  void Function(int) setNumNulls,
  void Function(int) setNumRows,
  void Function(ParquetEncoding) setEncoding,
  void Function(bool) setIsCompressed,
  void Function(int) setDefLevelsByteLen,
  void Function(int) setRepLevelsByteLen,
) {
  var lastFieldId = 0;
  while (reader.hasMore) {
    final field = reader.readFieldHeader(lastFieldId);
    if (field == null) break;
    lastFieldId = field.$1;
    final fieldId = field.$1;
    final typeId = field.$2;

    switch (fieldId) {
      case 1: // num_values
        setNumValues(reader.readI32());
        break;
      case 2: // num_nulls
        setNumNulls(reader.readI32());
        break;
      case 3: // num_rows
        setNumRows(reader.readI32());
        break;
      case 4: // encoding
        setEncoding(ParquetEncoding.fromValue(reader.readI32()) ?? ParquetEncoding.plain);
        break;
      case 5: // definition_levels_byte_length
        setDefLevelsByteLen(reader.readI32());
        break;
      case 6: // repetition_levels_byte_length
        setRepLevelsByteLen(reader.readI32());
        break;
      case 7: // is_compressed
        // Boolean in compact protocol: type 1 = true, type 2 = false
        setIsCompressed(typeId == 1);
        break;
      default:
        reader.skipField(typeId);
    }
  }
}

/// Decompress data based on codec.
Uint8List decompress(Uint8List data, ParquetCompression codec, int uncompressedSize) {
  switch (codec) {
    case ParquetCompression.uncompressed:
      return data;
    case ParquetCompression.gzip:
      return Uint8List.fromList(gzip.decode(data));
    case ParquetCompression.snappy:
      return _snappyDecompress(data);
    case ParquetCompression.zstd:
      return _zstdDecompress(data);
    default:
      throw FormatException('Unsupported compression codec: $codec');
  }
}

/// Snappy decompression implementation.
Uint8List _snappyDecompress(Uint8List input) {
  if (input.isEmpty) return Uint8List(0);

  var pos = 0;

  // Read uncompressed length (varint)
  var uncompressedLen = 0;
  var shift = 0;
  while (pos < input.length) {
    final b = input[pos++];
    uncompressedLen |= (b & 0x7F) << shift;
    if ((b & 0x80) == 0) break;
    shift += 7;
  }

  if (uncompressedLen <= 0 || uncompressedLen > 100 * 1024 * 1024) {
    throw FormatException('Invalid Snappy uncompressed length: $uncompressedLen');
  }

  final output = Uint8List(uncompressedLen);
  var outPos = 0;

  while (pos < input.length && outPos < uncompressedLen) {
    final tag = input[pos++];
    final tagType = tag & 0x03;

    if (tagType == 0) {
      // Literal
      var len = (tag >> 2) + 1;
      if (len > 60) {
        final extraBytes = len - 60;
        len = 1;
        for (var i = 0; i < extraBytes && pos < input.length; i++) {
          len += input[pos++] << (8 * i);
        }
      }
      for (var i = 0; i < len && pos < input.length && outPos < uncompressedLen; i++) {
        output[outPos++] = input[pos++];
      }
    } else {
      // Copy
      int copyOffset;
      int length;

      if (tagType == 1) {
        length = ((tag >> 2) & 0x07) + 4;
        copyOffset = ((tag >> 5) << 8) | input[pos++];
      } else if (tagType == 2) {
        length = (tag >> 2) + 1;
        copyOffset = input[pos] | (input[pos + 1] << 8);
        pos += 2;
      } else {
        length = (tag >> 2) + 1;
        copyOffset = input[pos] | (input[pos + 1] << 8) | (input[pos + 2] << 16) | (input[pos + 3] << 24);
        pos += 4;
      }

      if (copyOffset == 0 || copyOffset > outPos) {
        throw FormatException('Invalid Snappy copy offset: $copyOffset at position $outPos');
      }

      final copyStart = outPos - copyOffset;
      for (var i = 0; i < length && outPos < uncompressedLen; i++) {
        output[outPos] = output[copyStart + (i % copyOffset)];
        outPos++;
      }
    }
  }

  return output;
}

Uint8List _zstdDecompress(Uint8List data) {
  return zstd_util.decodeZstd(data);
}

/// Decode PLAIN encoded values.
List<dynamic> decodePlain(
  Uint8List data,
  ParquetType type,
  int numValues, {
  int? typeLength,
  ParquetConvertedType? convertedType,
}) {
  final result = <dynamic>[];
  var offset = 0;

  for (var i = 0; i < numValues && offset < data.length; i++) {
    switch (type) {
      case ParquetType.boolean:
        final byteIndex = i ~/ 8;
        final bitIndex = i % 8;
        if (byteIndex < data.length) {
          result.add((data[byteIndex] >> bitIndex) & 1 == 1);
        }
        if (i % 8 == 7 || i == numValues - 1) {
          offset = byteIndex + 1;
        }
        break;

      case ParquetType.int32:
        if (offset + 4 <= data.length) {
          final value = _readI32Le(data, offset);
          offset += 4;
          if (convertedType == ParquetConvertedType.date) {
            result.add(DateTime.utc(1970, 1, 1).add(Duration(days: value)).toIso8601String().split('T')[0]);
          } else {
            result.add(value);
          }
        }
        break;

      case ParquetType.int64:
        if (offset + 8 <= data.length) {
          final value = _readI64Le(data, offset);
          offset += 8;
          if (convertedType == ParquetConvertedType.timestampMillis) {
            result.add(DateTime.fromMillisecondsSinceEpoch(value, isUtc: true).toIso8601String());
          } else if (convertedType == ParquetConvertedType.timestampMicros) {
            result.add(DateTime.fromMicrosecondsSinceEpoch(value, isUtc: true).toIso8601String());
          } else {
            result.add(value);
          }
        }
        break;

      case ParquetType.int96:
        if (offset + 12 <= data.length) {
          final nanos = _readI64Le(data, offset);
          final julianDay = _readI32Le(data, offset + 8);
          offset += 12;
          final unixDays = julianDay - 2440588;
          final millis = unixDays * 86400000 + (nanos ~/ 1000000);
          result.add(DateTime.fromMillisecondsSinceEpoch(millis, isUtc: true).toIso8601String());
        }
        break;

      case ParquetType.float:
        if (offset + 4 <= data.length) {
          final bytes = ByteData.sublistView(data, offset, offset + 4);
          result.add(bytes.getFloat32(0, Endian.little));
          offset += 4;
        }
        break;

      case ParquetType.double_:
        if (offset + 8 <= data.length) {
          final bytes = ByteData.sublistView(data, offset, offset + 8);
          result.add(bytes.getFloat64(0, Endian.little));
          offset += 8;
        }
        break;

      case ParquetType.byteArray:
        if (offset + 4 <= data.length) {
          final len = _readI32Le(data, offset);
          offset += 4;
          if (len >= 0 && offset + len <= data.length) {
            final bytes = data.sublist(offset, offset + len);
            offset += len;
            if (convertedType == ParquetConvertedType.utf8 || convertedType == ParquetConvertedType.json) {
              result.add(utf8.decode(bytes, allowMalformed: true));
            } else {
              result.add(bytes);
            }
          }
        }
        break;

      case ParquetType.fixedLenByteArray:
        final len = typeLength ?? 0;
        if (offset + len <= data.length) {
          result.add(data.sublist(offset, offset + len));
          offset += len;
        }
        break;
    }
  }

  return result;
}

/// Decode RLE/Bit-packed hybrid encoding.
List<int> decodeRleBitPackedHybrid(Uint8List data, int bitWidth, int numValues) {
  if (bitWidth == 0) {
    return List.filled(numValues, 0);
  }

  final result = <int>[];
  var offset = 0;

  while (result.length < numValues && offset < data.length) {
    // Read header varint
    var header = 0;
    var shift = 0;
    while (offset < data.length) {
      final byte = data[offset++];
      header |= (byte & 0x7F) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
    }

    if ((header & 1) == 0) {
      // RLE run: header >> 1 = repeat count
      final count = header >> 1;
      final byteWidth = (bitWidth + 7) ~/ 8;
      var value = 0;
      for (var i = 0; i < byteWidth && offset < data.length; i++) {
        value |= data[offset++] << (8 * i);
      }
      for (var i = 0; i < count && result.length < numValues; i++) {
        result.add(value);
      }
    } else {
      // Bit-packed run: header >> 1 = number of groups of 8 values
      final numGroups = header >> 1;
      final totalValues = numGroups * 8;
      final mask = (1 << bitWidth) - 1;

      var bitBuffer = 0;
      var bitsInBuffer = 0;

      for (var i = 0; i < totalValues && result.length < numValues && offset <= data.length; i++) {
        while (bitsInBuffer < bitWidth && offset < data.length) {
          bitBuffer |= data[offset++] << bitsInBuffer;
          bitsInBuffer += 8;
        }
        result.add(bitBuffer & mask);
        bitBuffer >>>= bitWidth;
        bitsInBuffer -= bitWidth;
      }
    }
  }

  return result.take(numValues).toList();
}

/// Decode dictionary-encoded column.
List<dynamic> decodeDictionary(List<dynamic> dictionary, List<int> indices) {
  return indices.map((i) => i >= 0 && i < dictionary.length ? dictionary[i] : null).toList();
}

int _readI32Le(Uint8List data, int offset) {
  return data[offset] |
      (data[offset + 1] << 8) |
      (data[offset + 2] << 16) |
      ((data[offset + 3] << 24) & 0xFFFFFFFF);
}

int _readI64Le(Uint8List data, int offset) {
  final low = _readI32Le(data, offset) & 0xFFFFFFFF;
  final high = _readI32Le(data, offset + 4) & 0xFFFFFFFF;
  return (high << 32) | low;
}

/// High-level column decoder that handles pages, dictionary encoding and compression.
class ParquetColumnDecoder {
  /// Decode all values from a column chunk.
  static List<dynamic> decodeColumn(
    Uint8List data,
    ParquetCompression codec,
    ParquetType type, {
    int? typeLength,
    ParquetConvertedType? convertedType,
    int maxDefLevel = 1,
  }) {
    final result = <dynamic>[];
    var offset = 0;
    List<dynamic>? dictionary;

    while (offset < data.length) {
      // Parse page header
      final (header, headerSize) = parsePageHeader(data, offset);
      offset += headerSize;

      if (header.compressedPageSize <= 0) break;
      if (offset + header.compressedPageSize > data.length) break;

      final compressedData = data.sublist(offset, offset + header.compressedPageSize);
      offset += header.compressedPageSize;

      // Decompress page data
      Uint8List pageData;
      try {
        if (header.type == PageType.dataPageV2) {
          // V2 pages: levels are not compressed, only values
          final defLevelsLen = header.definitionLevelsByteLength ?? 0;
          final repLevelsLen = header.repetitionLevelsByteLength ?? 0;
          final levelsLen = defLevelsLen + repLevelsLen;

          if (header.isCompressed != false && codec != ParquetCompression.uncompressed) {
            final levels = compressedData.sublist(0, levelsLen);
            final compressedValues = compressedData.sublist(levelsLen);
            final decompressedValues = decompress(compressedValues, codec, header.uncompressedPageSize - levelsLen);
            pageData = Uint8List(levels.length + decompressedValues.length);
            pageData.setRange(0, levels.length, levels);
            pageData.setRange(levels.length, pageData.length, decompressedValues);
          } else {
            pageData = compressedData;
          }
        } else {
          // V1 pages: entire page is compressed
          if (codec != ParquetCompression.uncompressed) {
            pageData = decompress(compressedData, codec, header.uncompressedPageSize);
          } else {
            pageData = compressedData;
          }
        }
      } catch (e) {
        // Decompression failed, try treating as uncompressed
        pageData = compressedData;
      }

      final numValues = header.numValues ?? 0;
      if (numValues <= 0) continue;

      if (header.type == PageType.dictionaryPage) {
        // Parse dictionary values
        dictionary = decodePlain(
          pageData,
          type,
          numValues,
          typeLength: typeLength,
          convertedType: convertedType,
        );
      } else if (header.type == PageType.dataPage || header.type == PageType.dataPageV2) {
        var pageOffset = 0;

        // Read definition levels
        List<int> defLevels;
        if (maxDefLevel > 0) {
          if (header.type == PageType.dataPageV2) {
            // V2: levels are stored without length prefix
            final defLen = header.definitionLevelsByteLength ?? 0;
            if (defLen > 0 && pageOffset + defLen <= pageData.length) {
              final bitWidth = _bitWidth(maxDefLevel);
              defLevels = decodeRleBitPackedHybrid(
                pageData.sublist(pageOffset, pageOffset + defLen),
                bitWidth,
                numValues,
              );
              pageOffset += defLen;
            } else {
              defLevels = List.filled(numValues, maxDefLevel);
            }
            // Skip repetition levels for V2
            final repLen = header.repetitionLevelsByteLength ?? 0;
            pageOffset += repLen;
          } else {
            // V1: levels have 4-byte length prefix
            if (pageOffset + 4 <= pageData.length) {
              final levelLen = _readI32Le(pageData, pageOffset);
              pageOffset += 4;
              if (levelLen > 0 && pageOffset + levelLen <= pageData.length) {
                final bitWidth = _bitWidth(maxDefLevel);
                defLevels = decodeRleBitPackedHybrid(
                  pageData.sublist(pageOffset, pageOffset + levelLen),
                  bitWidth,
                  numValues,
                );
                pageOffset += levelLen;
              } else {
                defLevels = List.filled(numValues, maxDefLevel);
              }
            } else {
              defLevels = List.filled(numValues, maxDefLevel);
            }
          }
        } else {
          defLevels = List.filled(numValues, 0);
        }

        // Count non-null values
        final nonNullCount = maxDefLevel > 0
            ? defLevels.where((l) => l == maxDefLevel).length
            : numValues;

        // Read values
        final valueData = pageData.sublist(pageOffset);
        List<dynamic> values;

        final encoding = header.encoding ?? ParquetEncoding.plain;
        if (encoding == ParquetEncoding.rleDictionary ||
            encoding == ParquetEncoding.plainDictionary) {
          if (dictionary == null || dictionary.isEmpty) {
            // No dictionary, fill with nulls
            values = List.filled(nonNullCount, null);
          } else {
            // Read bit width and indices
            if (valueData.isNotEmpty) {
              final bitWidth = valueData[0];
              if (bitWidth > 0 && valueData.length > 1) {
                final indices = decodeRleBitPackedHybrid(
                  valueData.sublist(1),
                  bitWidth,
                  nonNullCount,
                );
                values = decodeDictionary(dictionary, indices);
              } else {
                // bitWidth 0 means all values are the same (index 0)
                values = List.filled(nonNullCount, dictionary.isNotEmpty ? dictionary[0] : null);
              }
            } else {
              values = List.filled(nonNullCount, null);
            }
          }
        } else {
          // Plain encoding
          values = decodePlain(
            valueData,
            type,
            nonNullCount,
            typeLength: typeLength,
            convertedType: convertedType,
          );
        }

        // Reconstruct with nulls based on definition levels
        if (maxDefLevel > 0) {
          var valueIdx = 0;
          for (final level in defLevels) {
            if (level == maxDefLevel && valueIdx < values.length) {
              result.add(values[valueIdx++]);
            } else {
              result.add(null);
            }
          }
        } else {
          result.addAll(values);
        }
      }
    }

    return result;
  }

  static int _bitWidth(int maxValue) {
    if (maxValue <= 0) return 0;
    var width = 0;
    var v = maxValue;
    while (v > 0) {
      width++;
      v >>= 1;
    }
    return width;
  }
}
