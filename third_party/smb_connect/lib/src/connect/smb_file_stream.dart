import 'dart:async';
import 'dart:math';
import 'dart:typed_data';

import 'package:smb_connect/src/connect/smb_file.dart';
import 'package:smb_connect/src/connect/smb_tree.dart';
import 'package:smb_connect/src/connect/impl/smb1/com/smb_com_blank_response.dart';
import 'package:smb_connect/src/connect/impl/smb1/com/smb_com_close.dart';
import 'package:smb_connect/src/connect/impl/smb1/com/smb_com_read_and_x.dart';
import 'package:smb_connect/src/connect/impl/smb1/com/smb_com_read_and_x_response.dart';
import 'package:smb_connect/src/connect/impl/smb2/create/smb2_close_request.dart';
import 'package:smb_connect/src/connect/impl/smb2/create/smb2_close_response.dart';
import 'package:smb_connect/src/connect/impl/smb2/io/smb2_read_request.dart';
import 'package:smb_connect/src/connect/impl/smb2/io/smb2_read_response.dart';
import 'package:smb_connect/src/connect/impl/smb2/nego/smb2_negotiate_response.dart';
import 'package:smb_connect/src/smb/request_param.dart';
import 'package:smb_connect/src/smb_constants.dart';

int openReadNextNum = 0;

/// Read buffer size: 1 MB.  The internal SMB2 READ request already caps at
/// ~64 KB per round-trip, but a large application-level buffer lets
/// [smbReadFromFile] fill it with multiple rounds before we yield, cutting
/// the number of Dart async stream events by ~250× vs the old 4 KB buffer.
const int _kReadBufferSize = 1024 * 1024; // 1 MB

Stream<Uint8List> smbOpenRead(
    SmbFile file, SmbTree tree, Uint8List? fileId, int fid, int start,
    [int? length]) async* {
  length = length ?? (file.size - start);
  var buffSize = min(length, _kReadBufferSize);
  var position = 0;
  Uint8List buff = Uint8List(buffSize);
  do {
    var remain = length - position;
    var readLen = min(buff.length, remain);
    var res = await smbReadFromFile(
        file, tree, fileId, fid, buff, position + start, 0, readLen);
    if (res <= 0) break;
    if (res == buff.length) {
      yield buff;
    } else {
      yield Uint8List.view(buff.buffer, 0, res);
    }
    position += res;
  } while (position < length);
  await smbCloseFile(file, tree, fileId, fid);
}

void readAsync(SmbFile file, SmbTree tree, Uint8List? fileId, int fid,
    int start, int? length, StreamController<Uint8List> controller) async {
  length ??= (file.size - start);
  var buffSize = min(length, _kReadBufferSize);
  var position = 0;
  Uint8List buff = Uint8List(buffSize);
  do {
    var remain = length - position;
    var readLen = min(buff.length, remain);
    var res = await smbReadFromFile(
        file, tree, fileId, fid, buff, start + position, 0, readLen);
    if (res <= 0) break;
    if (res == buff.length) {
      controller.add(buff);
    } else {
      controller.add(Uint8List.view(buff.buffer, 0, res));
    }
    position += res;
  } while (position < length);
  await smbCloseFile(file, tree, fileId, fid);
  await controller.close();
}

Stream<Uint8List> smbOpenRead2(
    SmbFile file, SmbTree tree, Uint8List? fileId, int fid, int start,
    [int? length]) {
  var controller = StreamController<Uint8List>.broadcast();
  readAsync(file, tree, fileId, fid, start, length, controller);
  return controller.stream;
}

Future<void> smbCloseFile(
    SmbFile file, SmbTree tree, Uint8List? fileId, int fid) async {
  if (tree.transport.isSMB2()) {
    Smb2CloseRequest closeReq =
        Smb2CloseRequest(tree.config, fileId: fileId, fileName: file.uncPath);
    closeReq.setCloseFlags(Smb2CloseResponse.SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB);
    tree.prepare(closeReq);
    await tree.transport.sendrecv(closeReq, params: {RequestParam.NO_RETRY});
  } else {
    var lastWriteTime = 0;
    var closeReq = SmbComClose(tree.config, fid, lastWriteTime);
    var closeResp = SmbComBlankResponse(tree.config);
    tree.prepare(closeReq);
    await tree.transport.sendrecv(closeReq,
        response: closeResp, params: {RequestParam.NO_RETRY});
  }
}

Future<int> smbReadFromFile(SmbFile file, SmbTree tree, Uint8List? fileId,
    int fid, Uint8List b, int position, int off, int len,
    {bool largeReadX = false}) async {
  int fp = position;
  int start = fp;
  int type = SmbConstants.TYPE_FILESYSTEM; //file.getType();

  SmbComReadAndXResponse response = SmbComReadAndXResponse(tree.config, b, off);
  int r, n;
  // Use negotiated maxReadSize when available (can be up to 8 MB with SMB3),
  // otherwise fall back to a safe default.
  int blockSize = 64936;
  final negotiated = tree.transport.getNegotiatedResponse();
  if (negotiated is Smb2NegotiateResponse) {
    final negotiatedMax = negotiated.maxReadSize;
    if (negotiatedMax > 0) blockSize = negotiatedMax;
  }
  // (type == SmbConstants.TYPE_FILESYSTEM) ? readSizeFile : readSize;
  do {
    r = len > blockSize ? blockSize : len;

    // try {
    if (tree.transport.isSMB2()) {
      Smb2ReadRequest request = Smb2ReadRequest(
        tree.config,
        fileId!,
        b,
        off,
        readLength: r,
        offset: type == SmbConstants.TYPE_NAMED_PIPE ? 0 : fp,
        remainingBytes: len - r,
      );
      // request.setOffset(type == SmbConstants.TYPE_NAMED_PIPE ? 0 : fp);
      // request.setReadLength(r);
      // request.setRemainingBytes(len - r);

      // try {
      tree.prepare(request);
      Smb2ReadResponse resp = await tree.transport
          .sendrecv(request, params: {RequestParam.NO_RETRY});
      n = resp.dataLength;
      // } catch (e) {
      //   //SmbException
      //   if (e.getNtStatus() == 0xC0000011) {
      //     // log.debug("Reached end of file", e);
      //     n = -1;
      //   } else {
      //     throw e;
      //   }
      // }
      if (n <= 0) {
        return ((fp - start) > 0 ? fp - start : -1);
      }
      fp += n;
      off += n;
      len -= n;
      continue;
    }

    SmbComReadAndX request =
        SmbComReadAndX(tree.config, fid, fp, r, andx: null);
    if (type == SmbConstants.TYPE_NAMED_PIPE) {
      request.minCount = 1024;
      request.maxCount = 1024;
      request.remaining = 1024;
    } else if (largeReadX) {
      request.maxCount = r & 0xFFFF;
      request.setOpenTimeout((r >> 16) & 0xFFFF);
    }
    tree.prepare(request);
    response = await tree.transport
        .sendrecv(request, response: response, params: {RequestParam.NO_RETRY});
    // th.send(request, response: response, params: {RequestParam.NO_RETRY});
    n = response.getDataLength();
    // } catch (se) {
    //   //SmbException
    //   if (type == SmbConstants.TYPE_NAMED_PIPE &&
    //       se.getNtStatus() == NtStatus.NT_STATUS_PIPE_BROKEN) {
    //     return -1;
    //   }
    //   throw seToIoe(se);
    // }
    if (n <= 0) {
      return ((fp - start) > 0 ? fp - start : -1);
    }
    fp += n;
    len -= n;
    response.adjustOffset(n);
  } while (len > 0 && n == r);
  return (fp - start);
}
