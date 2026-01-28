import 'dart:io';
import 'dart:typed_data';

import 'package:http/http.dart' as http;

import '../services/app_logger.dart';

/// Parquet file magic bytes: "PAR1"
const _parquetMagic = [0x50, 0x41, 0x52, 0x31];

/// Parquet physical types
enum ParquetType {
  boolean(0),
  int32(1),
  int64(2),
  int96(3),
  float(4),
  double_(5),
  byteArray(6),
  fixedLenByteArray(7);

  const ParquetType(this.value);
  final int value;

  static ParquetType? fromValue(int value) {
    for (final type in values) {
      if (type.value == value) return type;
    }
    return null;
  }
}

/// Parquet encoding types
enum ParquetEncoding {
  plain(0),
  plainDictionary(2),
  rle(3),
  bitPacked(4),
  deltaBinaryPacked(5),
  deltaLengthByteArray(6),
  deltaByteArray(7),
  rleDictionary(8),
  byteStreamSplit(9);

  const ParquetEncoding(this.value);
  final int value;

  static ParquetEncoding? fromValue(int value) {
    for (final enc in values) {
      if (enc.value == value) return enc;
    }
    return null;
  }
}

/// Parquet compression codecs
enum ParquetCompression {
  uncompressed(0),
  snappy(1),
  gzip(2),
  lzo(3),
  brotli(4),
  lz4(5),
  zstd(6),
  lz4Raw(7);

  const ParquetCompression(this.value);
  final int value;

  static ParquetCompression? fromValue(int value) {
    for (final c in values) {
      if (c.value == value) return c;
    }
    return null;
  }
}

/// Parquet field repetition types
enum ParquetFieldRepetition {
  required(0),
  optional(1),
  repeated(2);

  const ParquetFieldRepetition(this.value);
  final int value;

  static ParquetFieldRepetition? fromValue(int value) {
    for (final r in values) {
      if (r.value == value) return r;
    }
    return null;
  }
}

/// Parquet converted types (logical types in older format)
enum ParquetConvertedType {
  utf8(0),
  map(1),
  mapKeyValue(2),
  list(3),
  enum_(4),
  decimal(5),
  date(6),
  timeMillis(7),
  timeMicros(8),
  timestampMillis(9),
  timestampMicros(10),
  uint8(11),
  uint16(12),
  uint32(13),
  uint64(14),
  int8(15),
  int16(16),
  int32(17),
  int64(18),
  json(19),
  bson(20),
  interval(21);

  const ParquetConvertedType(this.value);
  final int value;

  static ParquetConvertedType? fromValue(int value) {
    for (final c in values) {
      if (c.value == value) return c;
    }
    return null;
  }
}

/// Schema element from Parquet file metadata.
class ParquetSchemaElement {
  ParquetSchemaElement({
    required this.name,
    this.type,
    this.typeLength,
    this.repetitionType,
    this.numChildren,
    this.convertedType,
    this.scale,
    this.precision,
  });

  final String name;
  final ParquetType? type;
  final int? typeLength;
  final ParquetFieldRepetition? repetitionType;
  final int? numChildren;
  final ParquetConvertedType? convertedType;
  final int? scale;
  final int? precision;

  bool get isGroup => type == null && (numChildren ?? 0) > 0;

  String get dtypeLabel {
    if (convertedType == ParquetConvertedType.utf8) return 'string';
    if (convertedType == ParquetConvertedType.json) return 'json';
    if (convertedType == ParquetConvertedType.date) return 'date';
    if (convertedType == ParquetConvertedType.timestampMillis ||
        convertedType == ParquetConvertedType.timestampMicros) {
      return 'timestamp';
    }
    if (convertedType == ParquetConvertedType.list) return 'list';
    if (convertedType == ParquetConvertedType.map) return 'map';

    switch (type) {
      case ParquetType.boolean:
        return 'bool';
      case ParquetType.int32:
        return 'int32';
      case ParquetType.int64:
        return 'int64';
      case ParquetType.int96:
        return 'int96';
      case ParquetType.float:
        return 'float';
      case ParquetType.double_:
        return 'double';
      case ParquetType.byteArray:
        return 'bytes';
      case ParquetType.fixedLenByteArray:
        return 'fixed_bytes[${typeLength ?? 0}]';
      case null:
        return 'group';
    }
  }
}

/// Column chunk metadata from a row group.
class ParquetColumnChunk {
  ParquetColumnChunk({
    required this.pathInSchema,
    required this.fileOffset,
    required this.codec,
    required this.numValues,
    required this.totalUncompressedSize,
    required this.totalCompressedSize,
    required this.dataPageOffset,
    this.dictionaryPageOffset,
  });

  final List<String> pathInSchema;
  final int fileOffset;
  final ParquetCompression codec;
  final int numValues;
  final int totalUncompressedSize;
  final int totalCompressedSize;
  final int dataPageOffset;
  final int? dictionaryPageOffset;

  String get columnName => pathInSchema.join('.');

  /// The offset to start reading from (dictionary page if present, else data page).
  int get startOffset => dictionaryPageOffset ?? dataPageOffset;
}

/// Row group metadata from Parquet file.
class ParquetRowGroup {
  ParquetRowGroup({
    required this.columns,
    required this.totalByteSize,
    required this.numRows,
    this.fileOffset,
  });

  final List<ParquetColumnChunk> columns;
  final int totalByteSize;
  final int numRows;
  final int? fileOffset;
}

/// Parquet file metadata (parsed from footer).
class ParquetFileMetadata {
  ParquetFileMetadata({
    required this.version,
    required this.schema,
    required this.numRows,
    required this.rowGroups,
    this.createdBy,
  });

  final int version;
  final List<ParquetSchemaElement> schema;
  final int numRows;
  final List<ParquetRowGroup> rowGroups;
  final String? createdBy;

  /// Get leaf (non-group) schema elements (the actual columns).
  List<ParquetSchemaElement> get leafColumns {
    return schema.where((s) => !s.isGroup).toList();
  }

  /// Find row groups that contain rows in the given range.
  List<(ParquetRowGroup, int, int)> findRowGroupsForRange(int offset, int length) {
    final result = <(ParquetRowGroup, int, int)>[];
    var currentRow = 0;
    final endRow = offset + length;

    for (final rg in rowGroups) {
      final rgStart = currentRow;
      final rgEnd = currentRow + rg.numRows;

      if (rgEnd > offset && rgStart < endRow) {
        // This row group overlaps with our range
        final startInRg = (offset - rgStart).clamp(0, rg.numRows);
        final endInRg = (endRow - rgStart).clamp(0, rg.numRows);
        result.add((rg, startInRg, endInRg - startInRg));
      }

      currentRow = rgEnd;
      if (currentRow >= endRow) break;
    }

    return result;
  }
}

/// Thrift Compact Protocol reader for parsing Parquet metadata.
class _ThriftCompactReader {
  _ThriftCompactReader(this.data, [this.offset = 0]);

  final Uint8List data;
  int offset;

  int get remaining => data.length - offset;

  int readByte() {
    if (offset >= data.length) throw const FormatException('Unexpected end of data');
    return data[offset++];
  }

  int readVarInt() {
    var result = 0;
    var shift = 0;
    while (true) {
      final byte = readByte();
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

  int readI16() => readZigZag();
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

  /// Read field header, returns (fieldId, typeId) or null if stop field.
  (int, int)? readFieldHeader(int lastFieldId) {
    final byte = readByte();
    if (byte == 0) return null; // Stop field

    final typeId = byte & 0x0F;
    var fieldIdDelta = (byte >> 4) & 0x0F;

    int fieldId;
    if (fieldIdDelta == 0) {
      fieldId = readI16();
    } else {
      fieldId = lastFieldId + fieldIdDelta;
    }

    return (fieldId, typeId);
  }

  /// Skip a field of the given type.
  void skipField(int typeId) {
    switch (typeId) {
      case 1: // BOOL_TRUE
      case 2: // BOOL_FALSE
        break;
      case 3: // BYTE
        readByte();
        break;
      case 4: // I16
        readI16();
        break;
      case 5: // I32
        readI32();
        break;
      case 6: // I64
        readI64();
        break;
      case 7: // DOUBLE
        offset += 8;
        break;
      case 8: // BINARY/STRING
        readBinary();
        break;
      case 9: // LIST
        final header = readByte();
        var size = (header >> 4) & 0x0F;
        if (size == 15) size = readVarInt();
        final elemType = header & 0x0F;
        for (var i = 0; i < size; i++) {
          skipField(elemType);
        }
        break;
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
        while (true) {
          final field = readFieldHeader(lastId);
          if (field == null) break;
          lastId = field.$1;
          skipField(field.$2);
        }
        break;
      default:
        throw FormatException('Unknown Thrift type: $typeId');
    }
  }

  /// Read a list header, returns (size, elementTypeId).
  (int, int) readListHeader() {
    final header = readByte();
    var size = (header >> 4) & 0x0F;
    if (size == 15) size = readVarInt();
    final elemType = header & 0x0F;
    return (size, elemType);
  }
}

/// Parses Parquet FileMetaData from Thrift-encoded footer.
ParquetFileMetadata parseFileMetadata(Uint8List footer) {
  final reader = _ThriftCompactReader(footer);

  int version = 1;
  List<ParquetSchemaElement> schema = [];
  int numRows = 0;
  List<ParquetRowGroup> rowGroups = [];
  String? createdBy;

  var lastFieldId = 0;
  while (true) {
    final field = reader.readFieldHeader(lastFieldId);
    if (field == null) break;
    lastFieldId = field.$1;
    final fieldId = field.$1;
    final typeId = field.$2;

    switch (fieldId) {
      case 1: // version
        version = reader.readI32();
        break;
      case 2: // schema
        final listHeader = reader.readListHeader();
        schema = [];
        for (var i = 0; i < listHeader.$1; i++) {
          schema.add(_parseSchemaElement(reader));
        }
        break;
      case 3: // num_rows
        numRows = reader.readI64();
        break;
      case 4: // row_groups
        final listHeader = reader.readListHeader();
        rowGroups = [];
        for (var i = 0; i < listHeader.$1; i++) {
          rowGroups.add(_parseRowGroup(reader));
        }
        break;
      case 6: // created_by
        createdBy = reader.readString();
        break;
      default:
        reader.skipField(typeId);
    }
  }

  return ParquetFileMetadata(
    version: version,
    schema: schema,
    numRows: numRows,
    rowGroups: rowGroups,
    createdBy: createdBy,
  );
}

ParquetSchemaElement _parseSchemaElement(_ThriftCompactReader reader) {
  String name = '';
  ParquetType? type;
  int? typeLength;
  ParquetFieldRepetition? repetitionType;
  int? numChildren;
  ParquetConvertedType? convertedType;
  int? scale;
  int? precision;

  var lastFieldId = 0;
  while (true) {
    final field = reader.readFieldHeader(lastFieldId);
    if (field == null) break;
    lastFieldId = field.$1;
    final fieldId = field.$1;
    final typeId = field.$2;

    switch (fieldId) {
      case 1: // type
        type = ParquetType.fromValue(reader.readI32());
        break;
      case 2: // type_length
        typeLength = reader.readI32();
        break;
      case 3: // repetition_type
        repetitionType = ParquetFieldRepetition.fromValue(reader.readI32());
        break;
      case 4: // name
        name = reader.readString();
        break;
      case 5: // num_children
        numChildren = reader.readI32();
        break;
      case 6: // converted_type
        convertedType = ParquetConvertedType.fromValue(reader.readI32());
        break;
      case 7: // scale
        scale = reader.readI32();
        break;
      case 8: // precision
        precision = reader.readI32();
        break;
      default:
        reader.skipField(typeId);
    }
  }

  return ParquetSchemaElement(
    name: name,
    type: type,
    typeLength: typeLength,
    repetitionType: repetitionType,
    numChildren: numChildren,
    convertedType: convertedType,
    scale: scale,
    precision: precision,
  );
}

ParquetRowGroup _parseRowGroup(_ThriftCompactReader reader) {
  List<ParquetColumnChunk> columns = [];
  int totalByteSize = 0;
  int numRows = 0;
  int? fileOffset;

  var lastFieldId = 0;
  while (true) {
    final field = reader.readFieldHeader(lastFieldId);
    if (field == null) break;
    lastFieldId = field.$1;
    final fieldId = field.$1;
    final typeId = field.$2;

    switch (fieldId) {
      case 1: // columns
        final listHeader = reader.readListHeader();
        columns = [];
        for (var i = 0; i < listHeader.$1; i++) {
          columns.add(_parseColumnChunk(reader));
        }
        break;
      case 2: // total_byte_size
        totalByteSize = reader.readI64();
        break;
      case 3: // num_rows
        numRows = reader.readI64();
        break;
      case 5: // file_offset
        fileOffset = reader.readI64();
        break;
      default:
        reader.skipField(typeId);
    }
  }

  return ParquetRowGroup(
    columns: columns,
    totalByteSize: totalByteSize,
    numRows: numRows,
    fileOffset: fileOffset,
  );
}

ParquetColumnChunk _parseColumnChunk(_ThriftCompactReader reader) {
  List<String> pathInSchema = [];
  int fileOffset = 0;
  ParquetCompression codec = ParquetCompression.uncompressed;
  int numValues = 0;
  int totalUncompressedSize = 0;
  int totalCompressedSize = 0;
  int dataPageOffset = 0;
  int? dictionaryPageOffset;

  var lastFieldId = 0;
  while (true) {
    final field = reader.readFieldHeader(lastFieldId);
    if (field == null) break;
    lastFieldId = field.$1;
    final fieldId = field.$1;
    final typeId = field.$2;

    switch (fieldId) {
      case 1: // file_path (optional string)
        reader.readString();
        break;
      case 2: // file_offset (i64)
        fileOffset = reader.readI64();
        break;
      case 3: // meta_data (nested ColumnMetaData struct)
        _parseColumnMetaData(
          reader,
          (path) => pathInSchema = path,
          (c) => codec = c,
          (n) => numValues = n,
          (s) => totalUncompressedSize = s,
          (s) => totalCompressedSize = s,
          (o) => dataPageOffset = o,
          (o) => dictionaryPageOffset = o,
        );
        break;
      case 4: // offset_index_offset
      case 5: // offset_index_length
      case 6: // column_index_offset
      case 7: // column_index_length
        reader.skipField(typeId);
        break;
      default:
        reader.skipField(typeId);
    }
  }

  return ParquetColumnChunk(
    pathInSchema: pathInSchema,
    fileOffset: fileOffset,
    codec: codec,
    numValues: numValues,
    totalUncompressedSize: totalUncompressedSize,
    totalCompressedSize: totalCompressedSize,
    dataPageOffset: dataPageOffset,
    dictionaryPageOffset: dictionaryPageOffset,
  );
}

void _parseColumnMetaData(
  _ThriftCompactReader reader,
  void Function(List<String>) setPath,
  void Function(ParquetCompression) setCodec,
  void Function(int) setNumValues,
  void Function(int) setTotalUncompressedSize,
  void Function(int) setTotalCompressedSize,
  void Function(int) setDataPageOffset,
  void Function(int?) setDictionaryPageOffset,
) {
  var lastFieldId = 0;
  while (true) {
    final field = reader.readFieldHeader(lastFieldId);
    if (field == null) break;
    lastFieldId = field.$1;
    final fieldId = field.$1;
    final typeId = field.$2;

    switch (fieldId) {
      case 1: // type (physical type)
        reader.readI32(); // Skip, we get it from schema
        break;
      case 2: // encodings
        final listHeader = reader.readListHeader();
        for (var i = 0; i < listHeader.$1; i++) {
          reader.readI32();
        }
        break;
      case 3: // path_in_schema
        final listHeader = reader.readListHeader();
        final path = <String>[];
        for (var i = 0; i < listHeader.$1; i++) {
          path.add(reader.readString());
        }
        setPath(path);
        break;
      case 4: // codec
        setCodec(ParquetCompression.fromValue(reader.readI32()) ?? ParquetCompression.uncompressed);
        break;
      case 5: // num_values
        setNumValues(reader.readI64());
        break;
      case 6: // total_uncompressed_size
        setTotalUncompressedSize(reader.readI64());
        break;
      case 7: // total_compressed_size
        setTotalCompressedSize(reader.readI64());
        break;
      case 9: // data_page_offset
        setDataPageOffset(reader.readI64());
        break;
      case 10: // index_page_offset
        reader.readI64();
        break;
      case 11: // dictionary_page_offset
        setDictionaryPageOffset(reader.readI64());
        break;
      default:
        reader.skipField(typeId);
    }
  }
}

/// Remote Parquet file reader using HTTP Range requests.
class ParquetReader {
  ParquetReader({
    http.Client? client,
    this.userAgent = 'dataset-inspector/2.4.6 (flutter)',
  }) : _client = client ?? http.Client();

  final http.Client _client;
  final String userAgent;

  final Map<String, ParquetFileMetadata> _metadataCache = {};

  /// Read Parquet file metadata from a remote URL.
  Future<ParquetFileMetadata> readMetadata(Uri url, {String? token}) async {
    final cacheKey = url.toString();
    if (_metadataCache.containsKey(cacheKey)) {
      return _metadataCache[cacheKey]!;
    }

    // First, get file size with a HEAD request or small range request
    final fileSize = await _getFileSize(url, token);
    if (fileSize < 12) {
      throw const FormatException('Parquet file too small');
    }

    // Read last 8 bytes: 4 bytes footer length + 4 bytes magic "PAR1"
    final tail = await _rangeRequest(url, fileSize - 8, fileSize - 1, token);
    if (tail.length < 8) {
      throw const FormatException('Failed to read Parquet footer');
    }

    // Verify magic
    if (tail[4] != _parquetMagic[0] ||
        tail[5] != _parquetMagic[1] ||
        tail[6] != _parquetMagic[2] ||
        tail[7] != _parquetMagic[3]) {
      throw const FormatException('Invalid Parquet file (bad magic)');
    }

    final footerLen = _readU32Le(tail, 0);
    if (footerLen > fileSize - 8 || footerLen > 64 * 1024 * 1024) {
      throw const FormatException('Invalid Parquet footer length');
    }

    // Read footer (Thrift-encoded FileMetaData)
    final footerStart = fileSize - 8 - footerLen;
    final footer = await _rangeRequest(url, footerStart, fileSize - 9, token);

    final metadata = parseFileMetadata(footer);
    _metadataCache[cacheKey] = metadata;

    AppLogger.info(
      'Parquet metadata: ${metadata.numRows} rows, ${metadata.rowGroups.length} row groups, '
      '${metadata.leafColumns.length} columns',
      tag: 'parquet',
    );

    return metadata;
  }

  /// Read raw bytes from a column chunk.
  Future<Uint8List> readColumnChunk(
    Uri url,
    ParquetColumnChunk chunk, {
    String? token,
  }) async {
    final start = chunk.startOffset;
    final end = start + chunk.totalCompressedSize - 1;
    return _rangeRequest(url, start, end, token);
  }

  /// Read all column chunks for a row group.
  Future<Map<String, Uint8List>> readRowGroupColumns(
    Uri url,
    ParquetRowGroup rowGroup, {
    String? token,
  }) async {
    final result = <String, Uint8List>{};

    for (final chunk in rowGroup.columns) {
      final data = await readColumnChunk(url, chunk, token: token);
      result[chunk.columnName] = data;
    }

    return result;
  }

  Future<int> _getFileSize(Uri url, String? token) async {
    final headers = <String, String>{
      HttpHeaders.userAgentHeader: userAgent,
    };
    if (token != null && token.isNotEmpty) {
      headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    }

    // Try HEAD request first
    try {
      final response = await _client.head(url, headers: headers);
      final contentLength = response.headers['content-length'];
      if (contentLength != null) {
        final size = int.tryParse(contentLength);
        if (size != null && size > 0) return size;
      }
    } catch (_) {}

    // Fallback: use Range request to get Content-Range
    headers[HttpHeaders.rangeHeader] = 'bytes=0-0';
    final response = await _client.get(url, headers: headers);
    final contentRange = response.headers['content-range'];
    if (contentRange != null) {
      // Format: bytes 0-0/12345
      final parts = contentRange.split('/');
      if (parts.length == 2) {
        final size = int.tryParse(parts[1]);
        if (size != null && size > 0) return size;
      }
    }

    throw const FormatException('Unable to determine file size');
  }

  Future<Uint8List> _rangeRequest(Uri url, int start, int end, String? token) async {
    final headers = <String, String>{
      HttpHeaders.rangeHeader: 'bytes=$start-$end',
      HttpHeaders.userAgentHeader: userAgent,
    };
    if (token != null && token.isNotEmpty) {
      headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    }

    final response = await _client.get(url, headers: headers);
    if (response.statusCode != 200 && response.statusCode != 206) {
      throw Exception('HTTP ${response.statusCode} from $url');
    }

    return response.bodyBytes;
  }

  int _readU32Le(Uint8List data, int offset) {
    return data[offset] |
        (data[offset + 1] << 8) |
        (data[offset + 2] << 16) |
        (data[offset + 3] << 24);
  }
}
