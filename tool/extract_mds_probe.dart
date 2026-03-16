import 'dart:convert';
import 'dart:io';

import 'package:dataset_inspector/state/viewer_state.dart';
import 'package:flutter/widgets.dart';
import 'package:path/path.dart' as p;

class _Args {
  _Args({
    required this.source,
    required this.shardName,
    required this.outputDir,
    required this.offset,
    required this.limit,
    required this.audioFieldIndex,
    required this.textFieldIndex,
    required this.idFieldIndex,
  });

  final String source;
  final String shardName;
  final String outputDir;
  final int offset;
  final int limit;
  final int audioFieldIndex;
  final int textFieldIndex;
  final int idFieldIndex;
}

Never _usage(String message) {
  stderr.writeln(message);
  stderr.writeln('');
  stderr.writeln('Usage:');
  stderr.writeln('  dart run tool/extract_mds_probe.dart \\');
  stderr.writeln('    --source remote://host/path/to/mds_shards \\');
  stderr.writeln('    --shard shard.00000.mds.zstd \\');
  stderr.writeln('    --output-dir /abs/path/out \\');
  stderr.writeln(
      '    [--offset 0] [--limit 16] [--audio-field 0] [--text-field 4] [--id-field 1]');
  exit(2);
}

_Args _parseArgs(List<String> args) {
  String? source;
  String? shardName;
  String? outputDir;
  var offset = 0;
  var limit = 16;
  var audioFieldIndex = 0;
  var textFieldIndex = 4;
  var idFieldIndex = 1;

  for (var i = 0; i < args.length; i += 1) {
    final arg = args[i];
    String? value() {
      if (i + 1 >= args.length) {
        _usage('Missing value for $arg');
      }
      i += 1;
      return args[i];
    }

    switch (arg) {
      case '--source':
        source = value();
      case '--shard':
        shardName = value();
      case '--output-dir':
        outputDir = value();
      case '--offset':
        offset = int.parse(value()!);
      case '--limit':
        limit = int.parse(value()!);
      case '--audio-field':
        audioFieldIndex = int.parse(value()!);
      case '--text-field':
        textFieldIndex = int.parse(value()!);
      case '--id-field':
        idFieldIndex = int.parse(value()!);
      default:
        _usage('Unknown argument: $arg');
    }
  }

  if (source == null || source.trim().isEmpty) {
    _usage('Missing required --source');
  }
  if (shardName == null || shardName.trim().isEmpty) {
    _usage('Missing required --shard');
  }
  if (outputDir == null || outputDir.trim().isEmpty) {
    _usage('Missing required --output-dir');
  }
  if (limit < 1) {
    _usage('--limit must be >= 1');
  }

  return _Args(
    source: source.trim(),
    shardName: shardName.trim(),
    outputDir: outputDir.trim(),
    offset: offset,
    limit: limit,
    audioFieldIndex: audioFieldIndex,
    textFieldIndex: textFieldIndex,
    idFieldIndex: idFieldIndex,
  );
}

String _joinRemote(String base, String child) {
  final trimmedBase =
      base.endsWith('/') ? base.substring(0, base.length - 1) : base;
  final trimmedChild = child.startsWith('/') ? child.substring(1) : child;
  return '$trimmedBase/$trimmedChild';
}

String _safeBaseName(int itemIndex) => itemIndex.toString().padLeft(6, '0');

Future<void> main(List<String> args) async {
  WidgetsFlutterBinding.ensureInitialized();
  final parsed = _parseArgs(args);
  final outDir = Directory(parsed.outputDir);
  await outDir.create(recursive: true);
  final audioDir = Directory(p.join(outDir.path, 'audio'));
  await audioDir.create(recursive: true);
  final manifestPath = p.join(outDir.path, 'manifest.jsonl');

  final state = ViewerState();
  try {
    await state.bootstrap();
    final added = await state.addSource(parsed.source, recordRecent: false);
    if (!added || state.activeDataset == null) {
      throw StateError('Failed to open source: ${parsed.source}');
    }
    final dataset = state.activeDataset!;
    final shardPath = _joinRemote(parsed.source, parsed.shardName);

    final rows = <Map<String, dynamic>>[];
    for (var itemIndex = parsed.offset;
        itemIndex < parsed.offset + parsed.limit;
        itemIndex += 1) {
      final prepared = await state.apiPrepareLocalDirectoryFieldFile(
        path: shardPath,
        itemIndex: itemIndex,
        fieldIndex: parsed.audioFieldIndex,
      );
      final ext = prepared.ext.trim().isEmpty ? 'bin' : prepared.ext.trim();
      final targetAudio =
          p.join(audioDir.path, '${_safeBaseName(itemIndex)}.$ext');
      await File(prepared.path).copy(targetAudio);

      final textPreview = await state.peekLocalDirectoryMdsField(
        shardPath: shardPath,
        itemIndex: itemIndex,
        fieldIndex: parsed.textFieldIndex,
      );
      final idPreview = await state.peekLocalDirectoryMdsField(
        shardPath: shardPath,
        itemIndex: itemIndex,
        fieldIndex: parsed.idFieldIndex,
      );

      final transcript = textPreview.previewText;
      rows.add(<String, dynamic>{
        'dataset_id': dataset.id,
        'source_input': dataset.sourceInput,
        'mode': dataset.mode.name,
        'shard_name': parsed.shardName,
        'item_index': itemIndex,
        'audio_path': targetAudio,
        'audio_ext': ext,
        'audio_size': prepared.size,
        'utt_id': idPreview.previewText,
        'transcript': transcript,
        'transcript_chars': transcript == null ? null : transcript.runes.length,
      });
    }

    final sink = File(manifestPath).openWrite();
    try {
      for (final row in rows) {
        sink.writeln(jsonEncode(row));
      }
    } finally {
      await sink.close();
    }

    final transcripts = rows
        .map((row) => row['transcript'])
        .whereType<String>()
        .where((text) => text.trim().isNotEmpty)
        .toList(growable: false);
    final transcriptChars = rows
        .map((row) => row['transcript_chars'])
        .whereType<int>()
        .toList(growable: false);
    final audioSizes = rows
        .map((row) => row['audio_size'])
        .whereType<int>()
        .toList(growable: false);

    stdout.writeln(
      jsonEncode(<String, dynamic>{
        'datasetId': dataset.id,
        'shardName': parsed.shardName,
        'recordCount': rows.length,
        'transcriptNonEmpty': transcripts.length,
        'transcriptMinChars': transcriptChars.isEmpty
            ? null
            : transcriptChars.reduce((a, b) => a < b ? a : b),
        'transcriptMaxChars': transcriptChars.isEmpty
            ? null
            : transcriptChars.reduce((a, b) => a > b ? a : b),
        'audioMinBytes': audioSizes.isEmpty
            ? null
            : audioSizes.reduce((a, b) => a < b ? a : b),
        'audioMaxBytes': audioSizes.isEmpty
            ? null
            : audioSizes.reduce((a, b) => a > b ? a : b),
        'audioDir': audioDir.path,
        'manifestPath': manifestPath,
      }),
    );
  } finally {
    state.dispose();
  }
}
