import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../models/remote_host.dart';
import '../services/remote_dataset_service.dart';

class RemoteHostsPanel extends StatefulWidget {
  const RemoteHostsPanel({
    super.key,
    required this.hosts,
    required this.onSaveHosts,
    required this.onTestConnection,
    required this.onLoadCacheStats,
    required this.onClearCache,
    required this.remoteCacheQuotaMb,
    required this.onSaveRemoteCacheQuota,
    required this.onApplyRemoteCacheQuota,
  });

  final List<RemoteHostConfig> hosts;
  final Future<void> Function(List<RemoteHostConfig> hosts)? onSaveHosts;
  final Future<RemoteHostConnectionResult> Function(RemoteHostConfig host)?
      onTestConnection;
  final Future<RemoteCacheStats> Function(RemoteHostConfig? host)?
      onLoadCacheStats;
  final Future<void> Function(RemoteHostConfig? host)? onClearCache;
  final int? remoteCacheQuotaMb;
  final Future<void> Function(int? quotaMb)? onSaveRemoteCacheQuota;
  final Future<void> Function()? onApplyRemoteCacheQuota;

  @override
  State<RemoteHostsPanel> createState() => _RemoteHostsPanelState();
}

class _RemoteHostsPanelState extends State<RemoteHostsPanel> {
  late List<RemoteHostConfig> _hosts;

  final _labelController = TextEditingController();
  final _sambaHostController = TextEditingController();
  final _sambaPortController = TextEditingController(text: '445');
  final _sambaShareController = TextEditingController();
  final _sambaBasePathController = TextEditingController();
  final _sambaUserController = TextEditingController();
  final _sambaPasswordController = TextEditingController();
  final _sshHostController = TextEditingController();
  final _sshPortController = TextEditingController(text: '22');
  final _sshBasePathController = TextEditingController();
  final _sshUserController = TextEditingController();
  final _sshPasswordController = TextEditingController();
  final _sshPrivateKeyController = TextEditingController();
  final _sshPrivateKeyPassphraseController = TextEditingController();
  final _r2EndpointController = TextEditingController();
  final _r2BucketController = TextEditingController();
  final _r2AccessKeyController = TextEditingController();
  final _r2SecretKeyController = TextEditingController();
  final _r2RegionController = TextEditingController(text: 'auto');
  final _r2BasePrefixController = TextEditingController();
  final _r2CacheRootController = TextEditingController();
  final _quotaController = TextEditingController();

  RemoteHostType _type = RemoteHostType.samba;
  String? _editingHostId;
  bool _showEditor = false;
  bool _editorTesting = false;
  bool _editorSambaPasswordVisible = false;
  bool _editorSshPasswordVisible = false;
  bool _editorSshPassphraseVisible = false;
  bool _editorR2SecretVisible = false;
  bool _r2UseHttps = true;
  bool _savingHosts = false;
  String? _editorTestMessage;
  bool? _editorTestWritable;

  final _testingHostIds = <String>{};
  final _hostTestMessages = <String, String>{};

  bool _cacheLoading = false;
  bool _cacheBusy = false;
  bool _cacheInitialized = false;
  RemoteCacheStats? _cacheTotal;
  final _cachePerHost = <String, RemoteCacheStats>{};

  @override
  void initState() {
    super.initState();
    _hosts = _sanitizeHosts(widget.hosts);
    _syncQuotaText();
    WidgetsBinding.instance.addPostFrameCallback((_) {
      unawaited(_refreshCacheStats());
    });
  }

  @override
  void didUpdateWidget(covariant RemoteHostsPanel oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (!_savingHosts && !_showEditor && oldWidget.hosts != widget.hosts) {
      _hosts = _sanitizeHosts(widget.hosts);
    }
    if (oldWidget.remoteCacheQuotaMb != widget.remoteCacheQuotaMb) {
      _syncQuotaText();
    }
    if (oldWidget.hosts != widget.hosts && _cacheInitialized) {
      unawaited(_refreshCacheStats());
    }
  }

  @override
  void dispose() {
    _labelController.dispose();
    _sambaHostController.dispose();
    _sambaPortController.dispose();
    _sambaShareController.dispose();
    _sambaBasePathController.dispose();
    _sambaUserController.dispose();
    _sambaPasswordController.dispose();
    _sshHostController.dispose();
    _sshPortController.dispose();
    _sshBasePathController.dispose();
    _sshUserController.dispose();
    _sshPasswordController.dispose();
    _sshPrivateKeyController.dispose();
    _sshPrivateKeyPassphraseController.dispose();
    _r2EndpointController.dispose();
    _r2BucketController.dispose();
    _r2AccessKeyController.dispose();
    _r2SecretKeyController.dispose();
    _r2RegionController.dispose();
    _r2BasePrefixController.dispose();
    _r2CacheRootController.dispose();
    _quotaController.dispose();
    super.dispose();
  }

  void _syncQuotaText() {
    final value = widget.remoteCacheQuotaMb;
    _quotaController.text = value == null || value <= 0 ? '' : '$value';
  }

  Future<void> _copyToClipboard(String text) async {
    final content = text.trim();
    if (content.isEmpty) return;
    await Clipboard.setData(ClipboardData(text: content));
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(content: Text('Copied to clipboard.')),
    );
  }

  String? _validateEditor() {
    if (_labelController.text.trim().isEmpty) {
      return 'Display name is required.';
    }
    if (_type == RemoteHostType.samba) {
      if (_sambaHostController.text.trim().isEmpty) {
        return 'Samba host is required.';
      }
      if (_sambaShareController.text.trim().isEmpty) {
        return 'Samba share is required.';
      }
      return null;
    }
    if (_type == RemoteHostType.ssh) {
      if (_sshHostController.text.trim().isEmpty) {
        return 'SSH host is required.';
      }
      if (_sshUserController.text.trim().isEmpty) {
        return 'SSH username is required.';
      }
      if (_sshPasswordController.text.trim().isEmpty &&
          _sshPrivateKeyController.text.trim().isEmpty) {
        return 'SSH password or private key is required.';
      }
      return null;
    }
    if (_r2EndpointController.text.trim().isEmpty) {
      return 'R2 endpoint is required.';
    }
    if (_r2BucketController.text.trim().isEmpty) {
      return 'R2 bucket is required.';
    }
    if (_r2AccessKeyController.text.trim().isEmpty ||
        _r2SecretKeyController.text.trim().isEmpty) {
      return 'R2 access key and secret key are required.';
    }
    return null;
  }

  RemoteHostConfig? _buildHostFromEditor({
    required String hostId,
    bool showErrors = true,
  }) {
    final validationError = _validateEditor();
    if (validationError != null) {
      if (showErrors && mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text(validationError)),
        );
      }
      return null;
    }
    final id = hostId.trim();
    final label = _labelController.text.trim();
    if (_type == RemoteHostType.samba) {
      final port = int.tryParse(_sambaPortController.text.trim()) ?? 445;
      return RemoteHostConfig(
        id: id,
        label: label,
        type: RemoteHostType.samba,
        samba: SambaRemoteHostConfig(
          host: _sambaHostController.text.trim(),
          share: _sambaShareController.text.trim(),
          port: port <= 0 ? 445 : port,
          basePath: _emptyToNull(_sambaBasePathController.text),
          username: _emptyToNull(_sambaUserController.text),
          password: _emptyToNull(_sambaPasswordController.text),
        ),
      );
    }
    if (_type == RemoteHostType.ssh) {
      final port = int.tryParse(_sshPortController.text.trim()) ?? 22;
      return RemoteHostConfig(
        id: id,
        label: label,
        type: RemoteHostType.ssh,
        ssh: SshRemoteHostConfig(
          host: _sshHostController.text.trim(),
          username: _sshUserController.text.trim(),
          port: port <= 0 ? 22 : port,
          basePath: _emptyToNull(_sshBasePathController.text),
          password: _emptyToNull(_sshPasswordController.text),
          privateKey: _emptyToNull(_sshPrivateKeyController.text),
          privateKeyPassphrase:
              _emptyToNull(_sshPrivateKeyPassphraseController.text),
        ),
      );
    }
    return RemoteHostConfig(
      id: id,
      label: label,
      type: RemoteHostType.r2,
      r2: R2RemoteHostConfig(
        endpoint: _r2EndpointController.text.trim(),
        bucket: _r2BucketController.text.trim(),
        accessKeyId: _r2AccessKeyController.text.trim(),
        secretAccessKey: _r2SecretKeyController.text.trim(),
        region: _emptyToValue(_r2RegionController.text, 'auto'),
        basePrefix: _emptyToNull(_r2BasePrefixController.text),
        cacheRoot: _emptyToNull(_r2CacheRootController.text),
        useHttps: _r2UseHttps,
      ),
    );
  }

  void _startAddHost() {
    setState(() {
      _editingHostId = null;
      _showEditor = true;
      _type = RemoteHostType.samba;
      _editorTesting = false;
      _editorTestMessage = null;
      _editorTestWritable = null;
      _editorSambaPasswordVisible = false;
      _editorSshPasswordVisible = false;
      _editorSshPassphraseVisible = false;
      _editorR2SecretVisible = false;
      _r2UseHttps = true;
      _labelController.clear();
      _sambaHostController.clear();
      _sambaPortController.text = '445';
      _sambaShareController.clear();
      _sambaBasePathController.clear();
      _sambaUserController.clear();
      _sambaPasswordController.clear();
      _sshHostController.clear();
      _sshPortController.text = '22';
      _sshBasePathController.clear();
      _sshUserController.clear();
      _sshPasswordController.clear();
      _sshPrivateKeyController.clear();
      _sshPrivateKeyPassphraseController.clear();
      _r2EndpointController.clear();
      _r2BucketController.clear();
      _r2AccessKeyController.clear();
      _r2SecretKeyController.clear();
      _r2RegionController.text = 'auto';
      _r2BasePrefixController.clear();
      _r2CacheRootController.clear();
    });
  }

  void _startEditHost(RemoteHostConfig host) {
    setState(() {
      _editingHostId = host.id.trim();
      _showEditor = true;
      _type = host.type;
      _editorTesting = false;
      _editorTestMessage = null;
      _editorTestWritable = null;
      _editorSambaPasswordVisible = false;
      _editorSshPasswordVisible = false;
      _editorSshPassphraseVisible = false;
      _editorR2SecretVisible = false;

      _labelController.text = host.label;

      final samba = host.samba;
      _sambaHostController.text = samba?.host ?? '';
      _sambaPortController.text = (samba?.port ?? 445).toString();
      _sambaShareController.text = samba?.share ?? '';
      _sambaBasePathController.text = samba?.basePath ?? '';
      _sambaUserController.text = samba?.username ?? '';
      _sambaPasswordController.text = samba?.password ?? '';

      final ssh = host.ssh;
      _sshHostController.text = ssh?.host ?? '';
      _sshPortController.text = (ssh?.port ?? 22).toString();
      _sshBasePathController.text = ssh?.basePath ?? '';
      _sshUserController.text = ssh?.username ?? '';
      _sshPasswordController.text = ssh?.password ?? '';
      _sshPrivateKeyController.text = ssh?.privateKey ?? '';
      _sshPrivateKeyPassphraseController.text =
          ssh?.privateKeyPassphrase ?? '';

      final r2 = host.r2;
      _r2EndpointController.text = r2?.endpoint ?? '';
      _r2BucketController.text = r2?.bucket ?? '';
      _r2AccessKeyController.text = r2?.accessKeyId ?? '';
      _r2SecretKeyController.text = r2?.secretAccessKey ?? '';
      _r2RegionController.text = r2?.region ?? 'auto';
      _r2BasePrefixController.text = r2?.basePrefix ?? '';
      _r2CacheRootController.text = r2?.cacheRoot ?? '';
      _r2UseHttps = r2?.useHttps ?? true;
    });
  }

  Future<void> _saveHostFromEditor() async {
    final editingId = _editingHostId?.trim();
    final resolvedId = editingId != null && editingId.isNotEmpty
        ? editingId
        : _generateHostIdFromLabel(_labelController.text);
    final host = _buildHostFromEditor(hostId: resolvedId);
    if (host == null) return;

    final nextHosts = List<RemoteHostConfig>.from(_hosts);

    if (editingId != null && editingId.isNotEmpty) {
      final index = nextHosts.indexWhere((item) => item.id.trim() == editingId);
      if (index >= 0) {
        nextHosts[index] = host;
      } else {
        nextHosts.add(host);
      }
    } else {
      nextHosts.add(host);
    }

    await _persistHosts(nextHosts);
    if (!mounted) return;
    setState(() {
      _showEditor = false;
      _editingHostId = null;
      _editorTestMessage = null;
      _editorTestWritable = null;
    });
    unawaited(_refreshCacheStats());
  }

  Future<void> _deleteHost(RemoteHostConfig host) async {
    final id = host.id.trim();
    final next = List<RemoteHostConfig>.from(_hosts)
      ..removeWhere((item) => item.id.trim() == id);
    await _persistHosts(next);
    if (!mounted) return;
    setState(() {
      _hostTestMessages.remove(id);
      _cachePerHost.remove(id);
    });
    unawaited(_refreshCacheStats());
  }

  Future<void> _persistHosts(List<RemoteHostConfig> nextHosts) async {
    final sanitizedHosts = _sanitizeHosts(nextHosts);
    final saver = widget.onSaveHosts;
    if (saver == null) {
      setState(() {
        _hosts = sanitizedHosts;
      });
      return;
    }
    setState(() {
      _savingHosts = true;
    });
    try {
      await saver(sanitizedHosts);
      if (!mounted) return;
      setState(() {
        _hosts = sanitizedHosts;
      });
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text(error.toString())),
      );
    } finally {
      if (mounted) {
        setState(() {
          _savingHosts = false;
        });
      }
    }
  }

  Future<void> _testEditorConnection() async {
    final tester = widget.onTestConnection;
    if (tester == null || _editorTesting) return;
    final editingId = _editingHostId?.trim();
    final resolvedId = editingId != null && editingId.isNotEmpty
        ? editingId
        : _generateHostIdFromLabel(_labelController.text);
    final host = _buildHostFromEditor(hostId: resolvedId);
    if (host == null) return;
    setState(() {
      _editorTesting = true;
    });
    try {
      final result = await tester(host);
      if (!mounted) return;
      setState(() {
        _editorTestMessage = result.message;
        _editorTestWritable = result.writable;
      });
    } catch (error) {
      if (!mounted) return;
      setState(() {
        _editorTestWritable = false;
        _editorTestMessage = error.toString();
      });
    } finally {
      if (mounted) {
        setState(() {
          _editorTesting = false;
        });
      }
    }
  }

  Future<void> _testHost(RemoteHostConfig host) async {
    final tester = widget.onTestConnection;
    final id = host.id.trim();
    if (tester == null || id.isEmpty || _testingHostIds.contains(id)) return;
    setState(() {
      _testingHostIds.add(id);
    });
    try {
      final result = await tester(host);
      if (!mounted) return;
      setState(() {
        _hostTestMessages[id] = result.message;
      });
    } catch (error) {
      if (!mounted) return;
      setState(() {
        _hostTestMessages[id] = error.toString();
      });
    } finally {
      if (mounted) {
        setState(() {
          _testingHostIds.remove(id);
        });
      }
    }
  }

  String _generateHostIdFromLabel(String label) {
    var base = label.trim().toLowerCase();
    base = base.replaceAll(RegExp(r'[^a-z0-9]+'), '-');
    base = base.replaceAll(RegExp(r'-{2,}'), '-');
    base = base.replaceAll(RegExp(r'^-+|-+$'), '');
    if (base.isEmpty) {
      base = 'remote-host';
    }

    final existing = <String>{
      for (final host in _hosts)
        if (host.id.trim().isNotEmpty) host.id.trim().toLowerCase(),
    };
    if (!existing.contains(base)) {
      return base;
    }

    var index = 2;
    while (true) {
      final candidate = '$base-$index';
      if (!existing.contains(candidate)) {
        return candidate;
      }
      index += 1;
    }
  }

  Future<void> _refreshCacheStats() async {
    final loader = widget.onLoadCacheStats;
    if (loader == null || _cacheLoading) return;
    setState(() {
      _cacheLoading = true;
    });
    try {
      final total = await loader(null);
      final perHost = <String, RemoteCacheStats>{};
      for (final host in _hosts) {
        final id = host.id.trim();
        if (id.isEmpty) continue;
        perHost[id] = await loader(host);
      }
      if (!mounted) return;
      setState(() {
        _cacheInitialized = true;
        _cacheTotal = total;
        _cachePerHost
          ..clear()
          ..addAll(perHost);
      });
    } catch (_) {
      // Ignore cache refresh failures in settings UI.
    } finally {
      if (mounted) {
        setState(() {
          _cacheLoading = false;
        });
      }
    }
  }

  Future<void> _saveAndApplyQuota() async {
    final saver = widget.onSaveRemoteCacheQuota;
    final apply = widget.onApplyRemoteCacheQuota;
    if (saver == null || apply == null || _cacheBusy) return;

    final text = _quotaController.text.trim();
    final parsed = text.isEmpty ? null : int.tryParse(text);
    if (text.isNotEmpty && (parsed == null || parsed <= 0)) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
            content: Text('Quota must be a positive integer in MB.')),
      );
      return;
    }

    setState(() {
      _cacheBusy = true;
    });
    try {
      await saver(parsed);
      await apply();
      await _refreshCacheStats();
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Remote cache quota saved and applied.')),
      );
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text(error.toString())),
      );
    } finally {
      if (mounted) {
        setState(() {
          _cacheBusy = false;
        });
      }
    }
  }

  Future<void> _clearCache(RemoteHostConfig? host) async {
    final clearer = widget.onClearCache;
    if (clearer == null || _cacheBusy) return;
    setState(() {
      _cacheBusy = true;
    });
    try {
      await clearer(host);
      await _refreshCacheStats();
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text(error.toString())),
      );
    } finally {
      if (mounted) {
        setState(() {
          _cacheBusy = false;
        });
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    final textTheme = Theme.of(context).textTheme;

    return SingleChildScrollView(
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Expanded(
                child: Text(
                  'Remote Hosts',
                  style: textTheme.titleSmall?.copyWith(
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ),
              FilledButton.icon(
                onPressed: _savingHosts ? null : _startAddHost,
                icon: const Icon(Icons.add, size: 16),
                label: const Text('Add Host'),
              ),
            ],
          ),
          const SizedBox(height: 8),
          Text(
            'Manage Samba, SSH/SFTP, and Cloudflare R2 hosts directly here. Changes are saved immediately.',
            style: textTheme.bodySmall?.copyWith(
              color: scheme.onSurface.withValues(alpha: 0.65),
            ),
          ),
          const SizedBox(height: 12),
          if (_showEditor) _buildEditorCard(context),
          _buildHostList(context),
          const SizedBox(height: 14),
          Divider(height: 1, color: scheme.outlineVariant),
          const SizedBox(height: 14),
          _buildCacheManager(context),
        ],
      ),
    );
  }

  Widget _buildEditorCard(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    return Container(
      margin: const EdgeInsets.only(bottom: 12),
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        border: Border.all(color: scheme.outlineVariant),
        borderRadius: BorderRadius.circular(10),
        color: scheme.surfaceContainerLowest,
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Expanded(
                child: Text(
                  _editingHostId == null
                      ? 'Add Remote Host'
                      : 'Edit Remote Host',
                  style: Theme.of(context).textTheme.titleSmall,
                ),
              ),
              TextButton(
                onPressed: () {
                  setState(() {
                    _showEditor = false;
                    _editingHostId = null;
                  });
                },
                child: const Text('Cancel'),
              ),
            ],
          ),
          const SizedBox(height: 8),
          TextField(
            controller: _labelController,
            decoration: const InputDecoration(
              labelText: 'Display name',
              hintText: 'Thai Samba NAS',
              helperText: 'Host ID is auto-generated internally.',
            ),
          ),
          const SizedBox(height: 10),
          SegmentedButton<RemoteHostType>(
            segments: const [
              ButtonSegment(
                value: RemoteHostType.samba,
                icon: Icon(Icons.storage_outlined),
                label: Text('Samba'),
              ),
              ButtonSegment(
                value: RemoteHostType.ssh,
                icon: Icon(Icons.terminal_outlined),
                label: Text('SSH/SFTP'),
              ),
              ButtonSegment(
                value: RemoteHostType.r2,
                icon: Icon(Icons.cloud_outlined),
                label: Text('Cloudflare R2'),
              ),
            ],
            selected: <RemoteHostType>{_type},
            onSelectionChanged: (selection) {
              if (selection.isEmpty) return;
              setState(() {
                _type = selection.first;
              });
            },
          ),
          const SizedBox(height: 12),
          if (_type == RemoteHostType.samba) ...[
            TextField(
              controller: _sambaHostController,
              decoration: const InputDecoration(labelText: 'Host'),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sambaPortController,
              keyboardType: TextInputType.number,
              decoration:
                  const InputDecoration(labelText: 'Port', hintText: '445'),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sambaShareController,
              decoration: const InputDecoration(labelText: 'Share'),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sambaBasePathController,
              decoration: const InputDecoration(
                labelText: 'Base path (optional)',
                hintText: 'datasets',
              ),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sambaUserController,
              decoration:
                  const InputDecoration(labelText: 'Username (optional)'),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sambaPasswordController,
              obscureText: !_editorSambaPasswordVisible,
              decoration: InputDecoration(
                labelText: 'Password (optional)',
                suffixIcon: IconButton(
                  onPressed: () {
                    setState(() {
                      _editorSambaPasswordVisible =
                          !_editorSambaPasswordVisible;
                    });
                  },
                  icon: Icon(
                    _editorSambaPasswordVisible
                        ? Icons.visibility_off
                        : Icons.visibility,
                    size: 18,
                  ),
                ),
              ),
            ),
          ] else if (_type == RemoteHostType.ssh) ...[
            TextField(
              controller: _sshHostController,
              decoration: const InputDecoration(labelText: 'Host'),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sshPortController,
              keyboardType: TextInputType.number,
              decoration:
                  const InputDecoration(labelText: 'Port', hintText: '22'),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sshUserController,
              decoration: const InputDecoration(labelText: 'Username'),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sshPasswordController,
              obscureText: !_editorSshPasswordVisible,
              decoration: InputDecoration(
                labelText: 'Password (optional)',
                hintText: 'Provide password or private key',
                suffixIcon: IconButton(
                  onPressed: () {
                    setState(() {
                      _editorSshPasswordVisible = !_editorSshPasswordVisible;
                    });
                  },
                  icon: Icon(
                    _editorSshPasswordVisible
                        ? Icons.visibility_off
                        : Icons.visibility,
                    size: 18,
                  ),
                ),
              ),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sshPrivateKeyController,
              minLines: 3,
              maxLines: 6,
              decoration: const InputDecoration(
                labelText: 'Private key PEM (optional)',
                hintText: '-----BEGIN OPENSSH PRIVATE KEY-----',
              ),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sshPrivateKeyPassphraseController,
              obscureText: !_editorSshPassphraseVisible,
              decoration: InputDecoration(
                labelText: 'Key passphrase (optional)',
                suffixIcon: IconButton(
                  onPressed: () {
                    setState(() {
                      _editorSshPassphraseVisible =
                          !_editorSshPassphraseVisible;
                    });
                  },
                  icon: Icon(
                    _editorSshPassphraseVisible
                        ? Icons.visibility_off
                        : Icons.visibility,
                    size: 18,
                  ),
                ),
              ),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _sshBasePathController,
              decoration: const InputDecoration(
                labelText: 'Base path (optional)',
                hintText: '/datasets',
              ),
            ),
          ] else ...[
            TextField(
              controller: _r2EndpointController,
              decoration: const InputDecoration(
                labelText: 'Endpoint',
                hintText: 'xxx.r2.cloudflarestorage.com',
              ),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _r2BucketController,
              decoration: const InputDecoration(labelText: 'Bucket'),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _r2AccessKeyController,
              decoration: const InputDecoration(labelText: 'Access key ID'),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _r2SecretKeyController,
              obscureText: !_editorR2SecretVisible,
              decoration: InputDecoration(
                labelText: 'Secret access key',
                suffixIcon: IconButton(
                  onPressed: () {
                    setState(() {
                      _editorR2SecretVisible = !_editorR2SecretVisible;
                    });
                  },
                  icon: Icon(
                    _editorR2SecretVisible
                        ? Icons.visibility_off
                        : Icons.visibility,
                    size: 18,
                  ),
                ),
              ),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _r2RegionController,
              decoration:
                  const InputDecoration(labelText: 'Region', hintText: 'auto'),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _r2BasePrefixController,
              decoration: const InputDecoration(
                labelText: 'Base prefix (optional)',
                hintText: 'gigaspeech2/th',
              ),
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _r2CacheRootController,
              decoration: const InputDecoration(
                labelText: 'Local cache root (optional)',
                hintText: '~/.dataset-inspector/remote-cache/r2',
              ),
            ),
            const SizedBox(height: 8),
            SwitchListTile.adaptive(
              contentPadding: EdgeInsets.zero,
              title: const Text('Use HTTPS'),
              value: _r2UseHttps,
              onChanged: (value) {
                setState(() {
                  _r2UseHttps = value;
                });
              },
            ),
          ],
          if ((_editorTestMessage ?? '').trim().isNotEmpty) ...[
            const SizedBox(height: 8),
            Container(
              width: double.infinity,
              padding: const EdgeInsets.all(10),
              decoration: BoxDecoration(
                border: Border.all(color: scheme.outlineVariant),
                borderRadius: BorderRadius.circular(8),
              ),
              child: Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Expanded(
                    child: SelectableText(
                      _editorTestWritable == null
                          ? _editorTestMessage!
                          : '${_editorTestWritable! ? "Writable" : "Read-only"} · ${_editorTestMessage!}',
                      style: Theme.of(context).textTheme.bodySmall,
                    ),
                  ),
                  IconButton(
                    tooltip: 'Copy error log',
                    onPressed: () => _copyToClipboard(
                      _editorTestWritable == null
                          ? _editorTestMessage!
                          : '${_editorTestWritable! ? "Writable" : "Read-only"} · ${_editorTestMessage!}',
                    ),
                    icon: const Icon(Icons.copy_outlined, size: 16),
                  ),
                ],
              ),
            ),
          ],
          const SizedBox(height: 10),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              TextButton.icon(
                onPressed: _editorTesting || widget.onTestConnection == null
                    ? null
                    : _testEditorConnection,
                icon: _editorTesting
                    ? const SizedBox(
                        width: 14,
                        height: 14,
                        child: CircularProgressIndicator(strokeWidth: 2),
                      )
                    : const Icon(Icons.network_check_outlined, size: 16),
                label: const Text('Test Connection'),
              ),
              FilledButton(
                onPressed: _savingHosts ? null : _saveHostFromEditor,
                child: const Text('Save Host'),
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildHostList(BuildContext context) {
    if (_hosts.isEmpty) {
      return Container(
        width: double.infinity,
        padding: const EdgeInsets.all(12),
        decoration: BoxDecoration(
          border:
              Border.all(color: Theme.of(context).colorScheme.outlineVariant),
          borderRadius: BorderRadius.circular(10),
        ),
        child: const Text('No remote hosts configured.'),
      );
    }
    return Container(
      decoration: BoxDecoration(
        border: Border.all(color: Theme.of(context).colorScheme.outlineVariant),
        borderRadius: BorderRadius.circular(10),
      ),
      child: ListView.separated(
        shrinkWrap: true,
        physics: const NeverScrollableScrollPhysics(),
        itemCount: _hosts.length,
        separatorBuilder: (_, __) => Divider(
          height: 1,
          color: Theme.of(context).colorScheme.outlineVariant,
        ),
        itemBuilder: (context, index) {
          final host = _hosts[index];
          final id = host.id.trim();
          final testMessage = (_hostTestMessages[id] ?? '').trim();
          return Padding(
            padding: const EdgeInsets.symmetric(vertical: 3),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                ListTile(
                  dense: true,
                  leading: Icon(
                    switch (host.type) {
                      RemoteHostType.samba => Icons.storage_outlined,
                      RemoteHostType.ssh => Icons.terminal_outlined,
                      RemoteHostType.r2 => Icons.cloud_outlined,
                    },
                  ),
                  title: Text(host.label),
                  subtitle: Text(
                    _describeHost(host),
                    maxLines: 2,
                    overflow: TextOverflow.ellipsis,
                  ),
                  trailing: Wrap(
                    spacing: 4,
                    children: [
                      IconButton(
                        tooltip: 'Test connection',
                        onPressed: widget.onTestConnection == null ||
                                _testingHostIds.contains(id)
                            ? null
                            : () => _testHost(host),
                        icon: _testingHostIds.contains(id)
                            ? const SizedBox(
                                width: 14,
                                height: 14,
                                child:
                                    CircularProgressIndicator(strokeWidth: 2),
                              )
                            : const Icon(Icons.network_check_outlined,
                                size: 18),
                      ),
                      IconButton(
                        tooltip: 'Edit',
                        onPressed:
                            _savingHosts ? null : () => _startEditHost(host),
                        icon: const Icon(Icons.edit_outlined, size: 18),
                      ),
                      IconButton(
                        tooltip: 'Delete',
                        onPressed:
                            _savingHosts ? null : () => _deleteHost(host),
                        icon: const Icon(Icons.delete_outline, size: 18),
                      ),
                    ],
                  ),
                ),
                if (testMessage.isNotEmpty)
                  Padding(
                    padding:
                        const EdgeInsets.only(left: 72, right: 12, bottom: 6),
                    child: Row(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Expanded(
                          child: SelectableText(
                            testMessage,
                            style: Theme.of(context)
                                .textTheme
                                .labelSmall
                                ?.copyWith(
                                  color: Theme.of(context)
                                      .colorScheme
                                      .onSurfaceVariant,
                                ),
                          ),
                        ),
                        IconButton(
                          tooltip: 'Copy error log',
                          onPressed: () => _copyToClipboard(testMessage),
                          icon: const Icon(Icons.copy_outlined, size: 16),
                        ),
                      ],
                    ),
                  ),
              ],
            ),
          );
        },
      ),
    );
  }

  Widget _buildCacheManager(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    final total = _cacheTotal;
    final textTheme = Theme.of(context).textTheme;

    TableCell cell(
      String text, {
      bool header = false,
      TextAlign textAlign = TextAlign.left,
      Widget? trailing,
    }) {
      final textWidget = Text(
        text,
        textAlign: textAlign,
        maxLines: 1,
        overflow: TextOverflow.ellipsis,
        style: header
            ? textTheme.labelMedium?.copyWith(fontWeight: FontWeight.w600)
            : textTheme.bodyMedium,
      );
      final child = trailing ?? textWidget;
      return TableCell(
        verticalAlignment: TableCellVerticalAlignment.middle,
        child: Padding(
          padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
          child: child,
        ),
      );
    }

    final tableRows = <TableRow>[
      TableRow(
        decoration: BoxDecoration(
          color: scheme.surfaceContainerLow,
          border: Border(
            bottom: BorderSide(color: scheme.outlineVariant),
          ),
        ),
        children: [
          cell('Scope', header: true),
          cell('Size', header: true),
          cell('Files', header: true),
          cell('Action', header: true, textAlign: TextAlign.right),
        ],
      ),
      TableRow(
        decoration: BoxDecoration(
          border: Border(
            bottom: BorderSide(color: scheme.outlineVariant),
          ),
        ),
        children: [
          cell('All Hosts'),
          cell(total == null ? '--' : _formatBytes(total.totalBytes)),
          cell(total == null ? '--' : '${total.fileCount}'),
          cell(
            '',
            trailing: Align(
              alignment: Alignment.centerRight,
              child: TextButton(
                onPressed: widget.onClearCache == null || _cacheBusy
                    ? null
                    : () => _clearCache(null),
                child: const Text('Clear All'),
              ),
            ),
          ),
        ],
      ),
      for (final host in _hosts)
        TableRow(
          decoration: BoxDecoration(
            border: Border(
              bottom: BorderSide(color: scheme.outlineVariant),
            ),
          ),
          children: [
            cell(host.label),
            cell(
              _cachePerHost[host.id.trim()] == null
                  ? 'No cache'
                  : _formatBytes(_cachePerHost[host.id.trim()]!.totalBytes),
            ),
            cell(
              _cachePerHost[host.id.trim()] == null
                  ? '--'
                  : '${_cachePerHost[host.id.trim()]!.fileCount}',
            ),
            cell(
              '',
              trailing: Align(
                alignment: Alignment.centerRight,
                child: TextButton(
                  onPressed: widget.onClearCache == null || _cacheBusy
                      ? null
                      : () => _clearCache(host),
                  child: const Text('Clear'),
                ),
              ),
            ),
          ],
        ),
      TableRow(
        children: [
          cell('Quota (MB)'),
          cell(
            '',
            trailing: SizedBox(
              width: 120,
              child: TextField(
                controller: _quotaController,
                keyboardType: TextInputType.number,
                decoration: const InputDecoration(
                  isDense: true,
                  border: OutlineInputBorder(),
                  contentPadding: EdgeInsets.symmetric(
                    horizontal: 8,
                    vertical: 8,
                  ),
                ),
              ),
            ),
          ),
          cell(''),
          cell(
            '',
            trailing: Align(
              alignment: Alignment.centerRight,
              child: FilledButton.icon(
                onPressed: widget.onSaveRemoteCacheQuota == null ||
                        widget.onApplyRemoteCacheQuota == null ||
                        _cacheBusy
                    ? null
                    : _saveAndApplyQuota,
                icon: const Icon(Icons.playlist_add_check_outlined, size: 16),
                label: const Text('Save & Apply'),
              ),
            ),
          ),
        ],
      ),
    ];

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          children: [
            Expanded(
              child: Text(
                'Cache Manager',
                style: Theme.of(context).textTheme.titleSmall?.copyWith(
                      fontWeight: FontWeight.w600,
                    ),
              ),
            ),
            TextButton.icon(
              onPressed:
                  _cacheLoading ? null : () => unawaited(_refreshCacheStats()),
              icon: const Icon(Icons.refresh, size: 16),
              label: const Text('Refresh'),
            ),
          ],
        ),
        const SizedBox(height: 6),
        Text(
          'Save & Apply updates quota and enforces it immediately, clearing old cache files as needed.',
          style: Theme.of(context).textTheme.bodySmall?.copyWith(
                color: scheme.onSurface.withValues(alpha: 0.65),
              ),
        ),
        const SizedBox(height: 10),
        Container(
          width: double.infinity,
          padding: const EdgeInsets.all(12),
          decoration: BoxDecoration(
            border: Border.all(color: scheme.outlineVariant),
            borderRadius: BorderRadius.circular(10),
            color: scheme.surfaceContainerLowest,
          ),
          child: _cacheLoading
              ? const Center(
                  child: SizedBox(
                    width: 18,
                    height: 18,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  ),
                )
              : ClipRRect(
                  borderRadius: BorderRadius.circular(8),
                  child: Table(
                    columnWidths: const <int, TableColumnWidth>{
                      0: FlexColumnWidth(2.5),
                      1: FlexColumnWidth(1.2),
                      2: FlexColumnWidth(0.9),
                      3: FlexColumnWidth(1.1),
                    },
                    defaultVerticalAlignment: TableCellVerticalAlignment.middle,
                    children: tableRows,
                  ),
                ),
        ),
      ],
    );
  }

  String _describeHost(RemoteHostConfig host) {
    switch (host.type) {
      case RemoteHostType.samba:
        final samba = host.samba;
        if (samba == null) return 'Invalid Samba config';
        final base = samba.basePath?.trim();
        if (base == null || base.isEmpty) {
          return '//${samba.host}:${samba.port}/${samba.share}';
        }
        return '//${samba.host}:${samba.port}/${samba.share}/$base';
      case RemoteHostType.ssh:
        final ssh = host.ssh;
        if (ssh == null) return 'Invalid SSH config';
        final base = ssh.basePath?.trim();
        final suffix = (base == null || base.isEmpty) ? '' : '/$base';
        return 'sftp://${ssh.username}@${ssh.host}:${ssh.port}$suffix';
      case RemoteHostType.r2:
        final r2 = host.r2;
        if (r2 == null) return 'Invalid R2 config';
        final prefix = r2.basePrefix?.trim();
        if (prefix == null || prefix.isEmpty) {
          return '${r2.endpoint}/${r2.bucket}';
        }
        return '${r2.endpoint}/${r2.bucket}/$prefix';
    }
  }

  String? _emptyToNull(String value) {
    final trimmed = value.trim();
    return trimmed.isEmpty ? null : trimmed;
  }

  String _emptyToValue(String value, String fallback) {
    final trimmed = value.trim();
    return trimmed.isEmpty ? fallback : trimmed;
  }

  String _formatBytes(int bytes) {
    if (bytes < 1024) return '$bytes B';
    const units = <String>['KB', 'MB', 'GB', 'TB'];
    var value = bytes.toDouble();
    var unitIndex = -1;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex += 1;
    }
    final fixed =
        value >= 100 ? value.toStringAsFixed(0) : value.toStringAsFixed(1);
    return '$fixed ${units[unitIndex]}';
  }

  List<RemoteHostConfig> _sanitizeHosts(Iterable<RemoteHostConfig> hosts) {
    return hosts.toList(growable: false);
  }
}
