import 'dart:core';

enum RemoteHostType {
  samba,
  ssh,
  r2,
}

class SambaRemoteHostConfig {
  const SambaRemoteHostConfig({
    required this.host,
    required this.share,
    this.port = 445,
    this.basePath,
    this.username,
    this.password,
  });

  final String host;
  final String share;
  final int port;
  final String? basePath;
  final String? username;
  final String? password;

  SambaRemoteHostConfig copyWith({
    String? host,
    String? share,
    int? port,
    String? basePath,
    String? username,
    String? password,
  }) {
    return SambaRemoteHostConfig(
      host: host ?? this.host,
      share: share ?? this.share,
      port: port ?? this.port,
      basePath: basePath ?? this.basePath,
      username: username ?? this.username,
      password: password ?? this.password,
    );
  }

  Map<String, dynamic> toJson() {
    return <String, dynamic>{
      'host': host.trim(),
      'share': share.trim(),
      'port': port,
      if (basePath != null && basePath!.trim().isNotEmpty)
        'basePath': basePath!.trim(),
      if (username != null && username!.trim().isNotEmpty)
        'username': username!.trim(),
      if (password != null && password!.trim().isNotEmpty)
        'password': password!.trim(),
    };
  }

  static SambaRemoteHostConfig? fromJson(Map<String, dynamic>? json) {
    if (json == null) return null;
    final host = _readRequiredString(json, 'host');
    final share = _readRequiredString(json, 'share');
    if (host == null || share == null) return null;
    final port = _readInt(json['port']) ?? 445;
    return SambaRemoteHostConfig(
      host: host,
      share: share,
      port: port <= 0 ? 445 : port,
      basePath: _readNullableString(json, 'basePath'),
      username: _readNullableString(json, 'username'),
      password: _readNullableString(json, 'password'),
    );
  }

  static String? _readRequiredString(Map<String, dynamic> json, String key) {
    final value = _readNullableString(json, key);
    return (value == null || value.isEmpty) ? null : value;
  }

  static String? _readNullableString(Map<String, dynamic> json, String key) {
    final value = json[key];
    if (value == null) return null;
    final text = value.toString().trim();
    return text.isEmpty ? null : text;
  }

  static int? _readInt(dynamic value) {
    if (value is int) return value;
    if (value is num) return value.toInt();
    if (value is String) return int.tryParse(value.trim());
    return null;
  }

  @override
  bool operator ==(Object other) {
    if (other is! SambaRemoteHostConfig) return false;
    return host == other.host &&
        share == other.share &&
        port == other.port &&
        basePath == other.basePath &&
        username == other.username &&
        password == other.password;
  }

  @override
  int get hashCode => Object.hash(
        host,
        share,
        port,
        basePath,
        username,
        password,
      );
}

class SshRemoteHostConfig {
  const SshRemoteHostConfig({
    required this.host,
    required this.username,
    this.port = 22,
    this.basePath,
    this.password,
    this.privateKey,
    this.privateKeyPassphrase,
  });

  final String host;
  final String username;
  final int port;
  final String? basePath;
  final String? password;
  final String? privateKey;
  final String? privateKeyPassphrase;

  SshRemoteHostConfig copyWith({
    String? host,
    String? username,
    int? port,
    String? basePath,
    String? password,
    String? privateKey,
    String? privateKeyPassphrase,
  }) {
    return SshRemoteHostConfig(
      host: host ?? this.host,
      username: username ?? this.username,
      port: port ?? this.port,
      basePath: basePath ?? this.basePath,
      password: password ?? this.password,
      privateKey: privateKey ?? this.privateKey,
      privateKeyPassphrase: privateKeyPassphrase ?? this.privateKeyPassphrase,
    );
  }

  Map<String, dynamic> toJson() {
    return <String, dynamic>{
      'host': host.trim(),
      'username': username.trim(),
      'port': port,
      if (basePath != null && basePath!.trim().isNotEmpty)
        'basePath': basePath!.trim(),
      if (password != null && password!.trim().isNotEmpty)
        'password': password!.trim(),
      if (privateKey != null && privateKey!.trim().isNotEmpty)
        'privateKey': privateKey!.trim(),
      if (privateKeyPassphrase != null &&
          privateKeyPassphrase!.trim().isNotEmpty)
        'privateKeyPassphrase': privateKeyPassphrase!.trim(),
    };
  }

  static SshRemoteHostConfig? fromJson(Map<String, dynamic>? json) {
    if (json == null) return null;
    final host = _readRequiredString(json, 'host');
    final username = _readRequiredString(json, 'username');
    if (host == null || username == null) return null;
    final port = _readInt(json['port']) ?? 22;
    return SshRemoteHostConfig(
      host: host,
      username: username,
      port: port <= 0 ? 22 : port,
      basePath: _readNullableString(json, 'basePath'),
      password: _readNullableString(json, 'password'),
      privateKey: _readNullableString(json, 'privateKey'),
      privateKeyPassphrase: _readNullableString(json, 'privateKeyPassphrase'),
    );
  }

  static String? _readRequiredString(Map<String, dynamic> json, String key) {
    final value = _readNullableString(json, key);
    return (value == null || value.isEmpty) ? null : value;
  }

  static String? _readNullableString(Map<String, dynamic> json, String key) {
    final value = json[key];
    if (value == null) return null;
    final text = value.toString().trim();
    return text.isEmpty ? null : text;
  }

  static int? _readInt(dynamic value) {
    if (value is int) return value;
    if (value is num) return value.toInt();
    if (value is String) return int.tryParse(value.trim());
    return null;
  }

  @override
  bool operator ==(Object other) {
    if (other is! SshRemoteHostConfig) return false;
    return host == other.host &&
        username == other.username &&
        port == other.port &&
        basePath == other.basePath &&
        password == other.password &&
        privateKey == other.privateKey &&
        privateKeyPassphrase == other.privateKeyPassphrase;
  }

  @override
  int get hashCode => Object.hash(
        host,
        username,
        port,
        basePath,
        password,
        privateKey,
        privateKeyPassphrase,
      );
}

class R2RemoteHostConfig {
  const R2RemoteHostConfig({
    required this.endpoint,
    required this.bucket,
    required this.accessKeyId,
    required this.secretAccessKey,
    this.region = 'auto',
    this.basePrefix,
    this.useHttps = true,
  });

  final String endpoint;
  final String bucket;
  final String accessKeyId;
  final String secretAccessKey;
  final String region;
  final String? basePrefix;
  final bool useHttps;

  R2RemoteHostConfig copyWith({
    String? endpoint,
    String? bucket,
    String? accessKeyId,
    String? secretAccessKey,
    String? region,
    String? basePrefix,
    bool? useHttps,
  }) {
    return R2RemoteHostConfig(
      endpoint: endpoint ?? this.endpoint,
      bucket: bucket ?? this.bucket,
      accessKeyId: accessKeyId ?? this.accessKeyId,
      secretAccessKey: secretAccessKey ?? this.secretAccessKey,
      region: region ?? this.region,
      basePrefix: basePrefix ?? this.basePrefix,
      useHttps: useHttps ?? this.useHttps,
    );
  }

  Map<String, dynamic> toJson() {
    return <String, dynamic>{
      'endpoint': endpoint.trim(),
      'bucket': bucket.trim(),
      'accessKeyId': accessKeyId.trim(),
      'secretAccessKey': secretAccessKey.trim(),
      'region': region.trim().isEmpty ? 'auto' : region.trim(),
      if (basePrefix != null && basePrefix!.trim().isNotEmpty)
        'basePrefix': basePrefix!.trim(),
      'useHttps': useHttps,
    };
  }

  static R2RemoteHostConfig? fromJson(Map<String, dynamic>? json) {
    if (json == null) return null;
    final endpoint = _readRequiredString(json, 'endpoint');
    final bucket = _readRequiredString(json, 'bucket');
    final accessKeyId = _readRequiredString(json, 'accessKeyId');
    final secretAccessKey = _readRequiredString(json, 'secretAccessKey');
    if (endpoint == null ||
        bucket == null ||
        accessKeyId == null ||
        secretAccessKey == null) {
      return null;
    }
    return R2RemoteHostConfig(
      endpoint: endpoint,
      bucket: bucket,
      accessKeyId: accessKeyId,
      secretAccessKey: secretAccessKey,
      region: _readNullableString(json, 'region') ?? 'auto',
      basePrefix: _readNullableString(json, 'basePrefix'),
      useHttps: json['useHttps'] is bool ? json['useHttps'] as bool : true,
    );
  }

  static String? _readRequiredString(Map<String, dynamic> json, String key) {
    final value = _readNullableString(json, key);
    return (value == null || value.isEmpty) ? null : value;
  }

  static String? _readNullableString(Map<String, dynamic> json, String key) {
    final value = json[key];
    if (value == null) return null;
    final text = value.toString().trim();
    return text.isEmpty ? null : text;
  }

  @override
  bool operator ==(Object other) {
    if (other is! R2RemoteHostConfig) return false;
    return endpoint == other.endpoint &&
        bucket == other.bucket &&
        accessKeyId == other.accessKeyId &&
        secretAccessKey == other.secretAccessKey &&
        region == other.region &&
        basePrefix == other.basePrefix &&
        useHttps == other.useHttps;
  }

  @override
  int get hashCode => Object.hash(
        endpoint,
        bucket,
        accessKeyId,
        secretAccessKey,
        region,
        basePrefix,
        useHttps,
      );
}

class RemoteHostConfig {
  const RemoteHostConfig({
    required this.id,
    required this.label,
    required this.type,
    this.samba,
    this.ssh,
    this.r2,
  });

  final String id;
  final String label;
  final RemoteHostType type;
  final SambaRemoteHostConfig? samba;
  final SshRemoteHostConfig? ssh;
  final R2RemoteHostConfig? r2;

  bool get isValid {
    final normalizedId = id.trim();
    final normalizedLabel = label.trim();
    if (normalizedId.isEmpty || normalizedLabel.isEmpty) return false;
    switch (type) {
      case RemoteHostType.samba:
        return samba != null;
      case RemoteHostType.ssh:
        return ssh != null;
      case RemoteHostType.r2:
        return r2 != null;
    }
  }

  RemoteHostConfig copyWith({
    String? id,
    String? label,
    RemoteHostType? type,
    SambaRemoteHostConfig? samba,
    SshRemoteHostConfig? ssh,
    R2RemoteHostConfig? r2,
  }) {
    return RemoteHostConfig(
      id: id ?? this.id,
      label: label ?? this.label,
      type: type ?? this.type,
      samba: samba ?? this.samba,
      ssh: ssh ?? this.ssh,
      r2: r2 ?? this.r2,
    );
  }

  Map<String, dynamic> toJson() {
    return <String, dynamic>{
      'id': id.trim(),
      'label': label.trim(),
      'type': type.name,
      if (samba != null) 'samba': samba!.toJson(),
      if (ssh != null) 'ssh': ssh!.toJson(),
      if (r2 != null) 'r2': r2!.toJson(),
    };
  }

  static RemoteHostConfig? fromJson(Map<String, dynamic>? json) {
    if (json == null) return null;
    final id = _readNullableString(json, 'id');
    final label = _readNullableString(json, 'label');
    final typeRaw = _readNullableString(json, 'type');
    if (id == null || label == null || typeRaw == null) return null;
    final type = _parseType(typeRaw);
    if (type == null) return null;
    final samba = SambaRemoteHostConfig.fromJson(
      _readObject(json['samba']),
    );
    final ssh = SshRemoteHostConfig.fromJson(
      _readObject(json['ssh']),
    );
    final r2 = R2RemoteHostConfig.fromJson(
      _readObject(json['r2']),
    );
    final model = RemoteHostConfig(
      id: id,
      label: label,
      type: type,
      samba: samba,
      ssh: ssh,
      r2: r2,
    );
    return model.isValid ? model : null;
  }

  static Map<String, dynamic>? _readObject(dynamic value) {
    if (value is Map<String, dynamic>) return value;
    if (value is Map) return value.map((k, v) => MapEntry('$k', v));
    return null;
  }

  static String? _readNullableString(Map<String, dynamic> json, String key) {
    final value = json[key];
    if (value == null) return null;
    final text = value.toString().trim();
    return text.isEmpty ? null : text;
  }

  static RemoteHostType? _parseType(String value) {
    final normalized = value.trim().toLowerCase();
    return switch (normalized) {
      'samba' || 'smb' => RemoteHostType.samba,
      'ssh' || 'sftp' => RemoteHostType.ssh,
      'r2' || 'cloudflare-r2' || 'cloudflare_r2' => RemoteHostType.r2,
      _ => null,
    };
  }

  @override
  bool operator ==(Object other) {
    if (other is! RemoteHostConfig) return false;
    return id == other.id &&
        label == other.label &&
        type == other.type &&
        samba == other.samba &&
        ssh == other.ssh &&
        r2 == other.r2;
  }

  @override
  int get hashCode => Object.hash(id, label, type, samba, ssh, r2);
}
