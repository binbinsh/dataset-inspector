/// Dart FFI bindings for libsmbclient (Samba).
///
/// Provides full SMB3 + multichannel file I/O at ~600+ MB/s throughput.
library libsmbclient_ffi;

export 'src/bindings.dart' show LibSmbClient, SmbcType;
export 'src/smb_client.dart' show SmbClient, SmbEntry;
