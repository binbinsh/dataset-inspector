import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../models/remote_host.dart';
import '../services/remote_dataset_service.dart';
import '../utils/app_fonts.dart';
import '../utils/dialog_action_styles.dart';

const Color _kDialogBorderColor = Color(0xFFE0E0E0);
const Color _kDialogInputFillColor = Color(0xFFF7F7F7);

ThemeData _dialogTheme(BuildContext context) {
  final theme = Theme.of(context);
  final baseBorder = OutlineInputBorder(
    borderRadius: BorderRadius.circular(10),
    borderSide: const BorderSide(color: _kDialogBorderColor),
  );
  return theme.copyWith(
    inputDecorationTheme: theme.inputDecorationTheme.copyWith(
      filled: true,
      fillColor: _kDialogInputFillColor,
      border: baseBorder,
      enabledBorder: baseBorder,
      contentPadding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
      focusedBorder: baseBorder.copyWith(
        borderSide: BorderSide(
          color: theme.colorScheme.primary.withValues(alpha: 0.65),
          width: 1.2,
        ),
      ),
    ),
  );
}

RoundedRectangleBorder _dialogShape() {
  return RoundedRectangleBorder(
    borderRadius: BorderRadius.circular(12),
    side: const BorderSide(color: _kDialogBorderColor),
  );
}

TextStyle _dialogTitleStyle(BuildContext context) {
  return AppFonts.flexTextStyle(Theme.of(context).textTheme.titleMedium)
      .copyWith(fontWeight: FontWeight.w700);
}

Future<List<RemoteHostConfig>?> showRemoteHostsSettingsDialog({
  required BuildContext context,
  required List<RemoteHostConfig> initialHosts,
  required Future<RemoteHostConnectionResult> Function(RemoteHostConfig host)
      onTestConnection,
}) async {
  final hosts = List<RemoteHostConfig>.from(initialHosts);
  final testingHosts = <String>{};
  final hostTestMessages = <String, String>{};
  return showDialog<List<RemoteHostConfig>>(
    context: context,
    builder: (dialogContext) {
      return Theme(
        data: _dialogTheme(dialogContext),
        child: StatefulBuilder(
          builder: (context, setState) {
            Future<void> addHost() async {
              final created = await showRemoteHostEditorDialog(
                context: context,
                onTestConnection: onTestConnection,
                existingHostIds: hosts.map((host) => host.id),
              );
              if (created == null) return;
              final duplicateIndex = hosts
                  .indexWhere((item) => item.id.trim() == created.id.trim());
              if (duplicateIndex >= 0) {
                hosts[duplicateIndex] = created;
              } else {
                hosts.add(created);
              }
              setState(() {});
            }

            Future<void> editHost(RemoteHostConfig host) async {
              final edited = await showRemoteHostEditorDialog(
                context: context,
                initial: host,
                onTestConnection: onTestConnection,
                existingHostIds: hosts.map((item) => item.id),
              );
              if (edited == null) return;
              final index =
                  hosts.indexWhere((item) => item.id.trim() == host.id.trim());
              if (index >= 0) {
                hosts[index] = edited;
              } else {
                hosts.add(edited);
              }
              setState(() {});
            }

            Future<void> testHost(RemoteHostConfig host) async {
              final id = host.id.trim();
              if (id.isEmpty || testingHosts.contains(id)) return;
              testingHosts.add(id);
              setState(() {});
              try {
                final result = await onTestConnection(host);
                hostTestMessages[id] = result.message;
              } catch (error) {
                hostTestMessages[id] = error.toString();
              } finally {
                testingHosts.remove(id);
                setState(() {});
              }
            }

            return AlertDialog(
              backgroundColor: Colors.white,
              surfaceTintColor: Colors.transparent,
              shape: _dialogShape(),
              titleTextStyle: _dialogTitleStyle(context),
              title: const Text('Settings · Remote Hosts'),
              content: SizedBox(
                width: 720,
                height: 420,
                child: hosts.isEmpty
                    ? const Center(
                        child: Text(
                          'No remote hosts configured.\nClick "Add Host" to create one.',
                          textAlign: TextAlign.center,
                        ),
                      )
                    : ListView.separated(
                        itemCount: hosts.length,
                        separatorBuilder: (_, __) => const Divider(height: 1),
                        itemBuilder: (context, index) {
                          final host = hosts[index];
                          final subtitle = switch (host.type) {
                            RemoteHostType.samba => _describeSambaHost(host),
                            RemoteHostType.ssh => _describeSshHost(host),
                            RemoteHostType.r2 => _describeR2Host(host),
                          };
                          final testMessage =
                              (hostTestMessages[host.id.trim()] ?? '').trim();
                          return Padding(
                            padding: const EdgeInsets.symmetric(vertical: 4),
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                ListTile(
                                  dense: true,
                                  title: Text(host.label),
                                  subtitle: Text(
                                    subtitle,
                                    maxLines: 2,
                                    overflow: TextOverflow.ellipsis,
                                  ),
                                  leading: Icon(
                                    switch (host.type) {
                                      RemoteHostType.samba =>
                                        Icons.storage_outlined,
                                      RemoteHostType.ssh =>
                                        Icons.terminal_outlined,
                                      RemoteHostType.r2 => Icons.cloud_outlined,
                                    },
                                  ),
                                  trailing: Wrap(
                                    spacing: 6,
                                    children: [
                                      IconButton(
                                        tooltip: 'Test connection',
                                        onPressed: testingHosts
                                                .contains(host.id.trim())
                                            ? null
                                            : () => testHost(host),
                                        icon: testingHosts
                                                .contains(host.id.trim())
                                            ? const SizedBox(
                                                width: 16,
                                                height: 16,
                                                child:
                                                    CircularProgressIndicator(
                                                  strokeWidth: 2,
                                                ),
                                              )
                                            : const Icon(
                                                Icons.network_check_outlined,
                                                size: 18,
                                              ),
                                      ),
                                      IconButton(
                                        tooltip: 'Edit',
                                        onPressed: () => editHost(host),
                                        icon: const Icon(
                                          Icons.edit_outlined,
                                          size: 18,
                                        ),
                                      ),
                                      IconButton(
                                        tooltip: 'Delete',
                                        onPressed: () {
                                          hosts.removeAt(index);
                                          setState(() {});
                                        },
                                        icon: const Icon(
                                          Icons.delete_outline,
                                          size: 18,
                                        ),
                                      ),
                                    ],
                                  ),
                                ),
                                if (testMessage.isNotEmpty)
                                  Padding(
                                    padding: const EdgeInsets.only(
                                      left: 72,
                                      right: 12,
                                      top: 2,
                                    ),
                                    child: Row(
                                      crossAxisAlignment:
                                          CrossAxisAlignment.start,
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
                                          onPressed: () => _copyToClipboard(
                                            context,
                                            testMessage,
                                          ),
                                          icon: const Icon(
                                            Icons.copy_outlined,
                                            size: 16,
                                          ),
                                        ),
                                      ],
                                    ),
                                  ),
                              ],
                            ),
                          );
                        },
                      ),
              ),
              actions: [
                OutlinedButton.icon(
                  style: buildDialogSecondaryButtonStyle(context),
                  onPressed: addHost,
                  icon: const Icon(Icons.add),
                  label: const Text('Add Host'),
                ),
                OutlinedButton(
                  style: buildDialogSecondaryButtonStyle(context),
                  onPressed: () => Navigator.of(dialogContext).pop(),
                  child: const Text('Cancel'),
                ),
                FilledButton(
                  style: buildDialogPrimaryButtonStyle(context),
                  onPressed: () {
                    Navigator.of(dialogContext).pop(hosts);
                  },
                  child: const Text('Save'),
                ),
              ],
            );
          },
        ),
      );
    },
  );
}

Future<RemoteHostConfig?> showRemoteHostEditorDialog({
  required BuildContext context,
  required Future<RemoteHostConnectionResult> Function(RemoteHostConfig host)
      onTestConnection,
  Iterable<String> existingHostIds = const <String>[],
  RemoteHostConfig? initial,
}) async {
  final labelController = TextEditingController(text: initial?.label ?? '');
  var type = initial?.type ?? RemoteHostType.samba;

  final sambaHostController = TextEditingController(
    text: initial?.samba?.host ?? '',
  );
  final sambaPortController = TextEditingController(
    text: (initial?.samba?.port ?? 445).toString(),
  );
  final sambaShareController = TextEditingController(
    text: initial?.samba?.share ?? '',
  );
  final sambaBasePathController = TextEditingController(
    text: initial?.samba?.basePath ?? '',
  );
  final sambaUserController = TextEditingController(
    text: initial?.samba?.username ?? '',
  );
  final sambaPasswordController = TextEditingController(
    text: initial?.samba?.password ?? '',
  );

  final sshHostController = TextEditingController(
    text: initial?.ssh?.host ?? '',
  );
  final sshPortController = TextEditingController(
    text: (initial?.ssh?.port ?? 22).toString(),
  );
  final sshBasePathController = TextEditingController(
    text: initial?.ssh?.basePath ?? '',
  );
  final sshUserController = TextEditingController(
    text: initial?.ssh?.username ?? '',
  );
  final sshPasswordController = TextEditingController(
    text: initial?.ssh?.password ?? '',
  );
  final sshPrivateKeyController = TextEditingController(
    text: initial?.ssh?.privateKey ?? '',
  );
  final sshPrivateKeyPassphraseController = TextEditingController(
    text: initial?.ssh?.privateKeyPassphrase ?? '',
  );

  final r2EndpointController = TextEditingController(
    text: initial?.r2?.endpoint ?? '',
  );
  final r2BucketController = TextEditingController(
    text: initial?.r2?.bucket ?? '',
  );
  final r2AccessKeyController = TextEditingController(
    text: initial?.r2?.accessKeyId ?? '',
  );
  final r2SecretKeyController = TextEditingController(
    text: initial?.r2?.secretAccessKey ?? '',
  );
  final r2RegionController = TextEditingController(
    text: initial?.r2?.region ?? 'auto',
  );
  final r2BasePrefixController = TextEditingController(
    text: initial?.r2?.basePrefix ?? '',
  );
  var r2UseHttps = initial?.r2?.useHttps ?? true;
  var obscureSambaPassword = true;
  var obscureSshPassword = true;
  var obscureSshPassphrase = true;
  var obscureR2Secret = true;
  var testingConnection = false;
  String? connectionMessage;
  bool? connectionWritable;

  final result = await showDialog<RemoteHostConfig>(
    context: context,
    builder: (dialogContext) {
      return Theme(
        data: _dialogTheme(dialogContext),
        child: StatefulBuilder(
          builder: (context, setState) {
            String? validate() {
              if (labelController.text.trim().isEmpty) {
                return 'Display name is required.';
              }
              if (type == RemoteHostType.samba) {
                if (sambaHostController.text.trim().isEmpty) {
                  return 'Samba host is required.';
                }
                if (sambaShareController.text.trim().isEmpty) {
                  return 'Samba share is required.';
                }
                return null;
              }
              if (type == RemoteHostType.ssh) {
                if (sshHostController.text.trim().isEmpty) {
                  return 'SSH host is required.';
                }
                if (sshUserController.text.trim().isEmpty) {
                  return 'SSH username is required.';
                }
                if (sshPasswordController.text.trim().isEmpty &&
                    sshPrivateKeyController.text.trim().isEmpty) {
                  return 'SSH password or private key is required.';
                }
                return null;
              }
              if (r2EndpointController.text.trim().isEmpty) {
                return 'R2 endpoint is required.';
              }
              if (r2BucketController.text.trim().isEmpty) {
                return 'R2 bucket is required.';
              }
              if (r2AccessKeyController.text.trim().isEmpty ||
                  r2SecretKeyController.text.trim().isEmpty) {
                return 'R2 access key and secret key are required.';
              }
              return null;
            }

            RemoteHostConfig? buildHost() {
              final error = validate();
              if (error != null) {
                ScaffoldMessenger.of(context).showSnackBar(
                  SnackBar(content: Text(error)),
                );
                return null;
              }
              final id = initial?.id.trim().isNotEmpty == true
                  ? initial!.id.trim()
                  : _generateHostIdFromLabel(
                      labelController.text,
                      existingHostIds: existingHostIds,
                    );
              final label = labelController.text.trim();
              if (type == RemoteHostType.samba) {
                final port =
                    int.tryParse(sambaPortController.text.trim()) ?? 445;
                return RemoteHostConfig(
                  id: id,
                  label: label,
                  type: RemoteHostType.samba,
                  samba: SambaRemoteHostConfig(
                    host: sambaHostController.text.trim(),
                    share: sambaShareController.text.trim(),
                    port: port <= 0 ? 445 : port,
                    basePath: _emptyToNull(sambaBasePathController.text),
                    username: _emptyToNull(sambaUserController.text),
                    password: _emptyToNull(sambaPasswordController.text),
                  ),
                );
              }
              if (type == RemoteHostType.ssh) {
                final port = int.tryParse(sshPortController.text.trim()) ?? 22;
                return RemoteHostConfig(
                  id: id,
                  label: label,
                  type: RemoteHostType.ssh,
                  ssh: SshRemoteHostConfig(
                    host: sshHostController.text.trim(),
                    username: sshUserController.text.trim(),
                    port: port <= 0 ? 22 : port,
                    basePath: _emptyToNull(sshBasePathController.text),
                    password: _emptyToNull(sshPasswordController.text),
                    privateKey: _emptyToNull(sshPrivateKeyController.text),
                    privateKeyPassphrase:
                        _emptyToNull(sshPrivateKeyPassphraseController.text),
                  ),
                );
              }
              return RemoteHostConfig(
                id: id,
                label: label,
                type: RemoteHostType.r2,
                r2: R2RemoteHostConfig(
                  endpoint: r2EndpointController.text.trim(),
                  bucket: r2BucketController.text.trim(),
                  accessKeyId: r2AccessKeyController.text.trim(),
                  secretAccessKey: r2SecretKeyController.text.trim(),
                  region: _emptyToValue(r2RegionController.text, 'auto'),
                  basePrefix: _emptyToNull(r2BasePrefixController.text),
                  useHttps: r2UseHttps,
                ),
              );
            }

            Future<void> runConnectionTest() async {
              final host = buildHost();
              if (host == null || testingConnection) return;
              setState(() {
                testingConnection = true;
              });
              try {
                final result = await onTestConnection(host);
                if (!context.mounted) return;
                setState(() {
                  connectionWritable = result.writable;
                  connectionMessage = result.message;
                });
              } catch (error) {
                if (!context.mounted) return;
                setState(() {
                  connectionWritable = false;
                  connectionMessage = error.toString();
                });
              } finally {
                if (context.mounted) {
                  setState(() {
                    testingConnection = false;
                  });
                }
              }
            }

            return AlertDialog(
              backgroundColor: Colors.white,
              surfaceTintColor: Colors.transparent,
              shape: _dialogShape(),
              titleTextStyle: _dialogTitleStyle(context),
              title: Text(
                initial == null ? 'Add Remote Host' : 'Edit Remote Host',
              ),
              content: SizedBox(
                width: 640,
                child: SingleChildScrollView(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      TextField(
                        controller: labelController,
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
                        selected: <RemoteHostType>{type},
                        onSelectionChanged: (selection) {
                          if (selection.isEmpty) return;
                          setState(() {
                            type = selection.first;
                          });
                        },
                      ),
                      const SizedBox(height: 12),
                      if (type == RemoteHostType.samba) ...[
                        TextField(
                          controller: sambaHostController,
                          decoration: const InputDecoration(labelText: 'Host'),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sambaPortController,
                          keyboardType: TextInputType.number,
                          decoration: const InputDecoration(
                            labelText: 'Port',
                            hintText: '445',
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sambaShareController,
                          decoration: const InputDecoration(labelText: 'Share'),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sambaBasePathController,
                          decoration: const InputDecoration(
                            labelText: 'Base path (optional)',
                            hintText: 'datasets',
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sambaUserController,
                          decoration: const InputDecoration(
                            labelText: 'Username (optional)',
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sambaPasswordController,
                          obscureText: obscureSambaPassword,
                          decoration: InputDecoration(
                            labelText: 'Password (optional)',
                            suffixIcon: IconButton(
                              icon: Icon(
                                obscureSambaPassword
                                    ? Icons.visibility
                                    : Icons.visibility_off,
                              ),
                              onPressed: () {
                                setState(() {
                                  obscureSambaPassword = !obscureSambaPassword;
                                });
                              },
                            ),
                          ),
                        ),
                      ] else if (type == RemoteHostType.ssh) ...[
                        TextField(
                          controller: sshHostController,
                          decoration: const InputDecoration(labelText: 'Host'),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sshPortController,
                          keyboardType: TextInputType.number,
                          decoration: const InputDecoration(
                            labelText: 'Port',
                            hintText: '22',
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sshUserController,
                          decoration:
                              const InputDecoration(labelText: 'Username'),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sshPasswordController,
                          obscureText: obscureSshPassword,
                          decoration: InputDecoration(
                            labelText: 'Password (optional)',
                            hintText: 'Provide password or private key',
                            suffixIcon: IconButton(
                              icon: Icon(
                                obscureSshPassword
                                    ? Icons.visibility
                                    : Icons.visibility_off,
                              ),
                              onPressed: () {
                                setState(() {
                                  obscureSshPassword = !obscureSshPassword;
                                });
                              },
                            ),
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sshPrivateKeyController,
                          minLines: 3,
                          maxLines: 6,
                          decoration: const InputDecoration(
                            labelText: 'Private key PEM (optional)',
                            hintText: '-----BEGIN OPENSSH PRIVATE KEY-----',
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sshPrivateKeyPassphraseController,
                          obscureText: obscureSshPassphrase,
                          decoration: InputDecoration(
                            labelText: 'Key passphrase (optional)',
                            suffixIcon: IconButton(
                              icon: Icon(
                                obscureSshPassphrase
                                    ? Icons.visibility
                                    : Icons.visibility_off,
                              ),
                              onPressed: () {
                                setState(() {
                                  obscureSshPassphrase = !obscureSshPassphrase;
                                });
                              },
                            ),
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: sshBasePathController,
                          decoration: const InputDecoration(
                            labelText: 'Base path (optional)',
                            hintText: '/datasets',
                          ),
                        ),
                      ] else ...[
                        TextField(
                          controller: r2EndpointController,
                          decoration: const InputDecoration(
                            labelText: 'Endpoint',
                            hintText: 'xxx.r2.cloudflarestorage.com',
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: r2BucketController,
                          decoration:
                              const InputDecoration(labelText: 'Bucket'),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: r2AccessKeyController,
                          decoration:
                              const InputDecoration(labelText: 'Access key ID'),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: r2SecretKeyController,
                          obscureText: obscureR2Secret,
                          decoration: InputDecoration(
                            labelText: 'Secret access key',
                            suffixIcon: IconButton(
                              icon: Icon(
                                obscureR2Secret
                                    ? Icons.visibility
                                    : Icons.visibility_off,
                              ),
                              onPressed: () {
                                setState(() {
                                  obscureR2Secret = !obscureR2Secret;
                                });
                              },
                            ),
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: r2RegionController,
                          decoration: const InputDecoration(
                            labelText: 'Region',
                            hintText: 'auto',
                          ),
                        ),
                        const SizedBox(height: 8),
                        TextField(
                          controller: r2BasePrefixController,
                          decoration: const InputDecoration(
                            labelText: 'Base prefix (optional)',
                            hintText: 'gigaspeech2/th',
                          ),
                        ),
                        const SizedBox(height: 8),
                        SwitchListTile.adaptive(
                          contentPadding: EdgeInsets.zero,
                          title: const Text('Use HTTPS'),
                          value: r2UseHttps,
                          onChanged: (value) {
                            setState(() {
                              r2UseHttps = value;
                            });
                          },
                        ),
                      ],
                      if ((connectionMessage ?? '').trim().isNotEmpty) ...[
                        const SizedBox(height: 12),
                        Container(
                          width: double.infinity,
                          padding: const EdgeInsets.all(10),
                          decoration: BoxDecoration(
                            color: _kDialogInputFillColor,
                            border: Border.all(color: _kDialogBorderColor),
                            borderRadius: BorderRadius.circular(8),
                          ),
                          child: Row(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Expanded(
                                child: SelectableText(
                                  connectionWritable == null
                                      ? connectionMessage!
                                      : '${connectionWritable! ? "Writable" : "Read-only"} · ${connectionMessage!}',
                                  style: Theme.of(context).textTheme.bodySmall,
                                ),
                              ),
                              IconButton(
                                tooltip: 'Copy error log',
                                onPressed: () => _copyToClipboard(
                                  context,
                                  connectionWritable == null
                                      ? connectionMessage!
                                      : '${connectionWritable! ? "Writable" : "Read-only"} · ${connectionMessage!}',
                                ),
                                icon: const Icon(Icons.copy_outlined, size: 16),
                              ),
                            ],
                          ),
                        ),
                      ],
                    ],
                  ),
                ),
              ),
              actions: [
                OutlinedButton.icon(
                  style: buildDialogSecondaryButtonStyle(context),
                  onPressed: testingConnection ? null : runConnectionTest,
                  icon: testingConnection
                      ? const SizedBox(
                          width: 16,
                          height: 16,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Icon(Icons.network_check_outlined, size: 16),
                  label: const Text('Test Connection'),
                ),
                OutlinedButton(
                  style: buildDialogSecondaryButtonStyle(context),
                  onPressed: () => Navigator.of(dialogContext).pop(),
                  child: const Text('Cancel'),
                ),
                FilledButton(
                  style: buildDialogPrimaryButtonStyle(context),
                  onPressed: () {
                    final host = buildHost();
                    if (host == null) return;
                    Navigator.of(dialogContext).pop(host);
                  },
                  child: const Text('Save'),
                ),
              ],
            );
          },
        ),
      );
    },
  );

  labelController.dispose();
  sambaHostController.dispose();
  sambaPortController.dispose();
  sambaShareController.dispose();
  sambaBasePathController.dispose();
  sambaUserController.dispose();
  sambaPasswordController.dispose();
  sshHostController.dispose();
  sshPortController.dispose();
  sshBasePathController.dispose();
  sshUserController.dispose();
  sshPasswordController.dispose();
  sshPrivateKeyController.dispose();
  sshPrivateKeyPassphraseController.dispose();
  r2EndpointController.dispose();
  r2BucketController.dispose();
  r2AccessKeyController.dispose();
  r2SecretKeyController.dispose();
  r2RegionController.dispose();
  r2BasePrefixController.dispose();
  return result;
}

Future<void> _copyToClipboard(BuildContext context, String text) async {
  final content = text.trim();
  if (content.isEmpty) return;
  await Clipboard.setData(ClipboardData(text: content));
  if (!context.mounted) return;
  ScaffoldMessenger.of(context).showSnackBar(
    const SnackBar(content: Text('Copied to clipboard.')),
  );
}

String _describeSambaHost(RemoteHostConfig host) {
  final samba = host.samba;
  if (samba == null) return 'Invalid Samba host';
  return '//${samba.host}:${samba.port}/${samba.share}';
}

String _describeSshHost(RemoteHostConfig host) {
  final ssh = host.ssh;
  if (ssh == null) return 'Invalid SSH host';
  final base = ssh.basePath?.trim();
  return 'sftp://${ssh.username}@${ssh.host}:${ssh.port}'
      '${base == null || base.isEmpty ? '' : '/$base'}';
}

String _describeR2Host(RemoteHostConfig host) {
  final r2 = host.r2;
  if (r2 == null) return 'Invalid R2 host';
  final prefix = r2.basePrefix?.trim();
  return '${r2.endpoint}/${r2.bucket}'
      '${prefix == null || prefix.isEmpty ? '' : ' · prefix: $prefix'}';
}

String? _emptyToNull(String value) {
  final trimmed = value.trim();
  return trimmed.isEmpty ? null : trimmed;
}

String _generateHostIdFromLabel(
  String label, {
  Iterable<String> existingHostIds = const <String>[],
}) {
  var base = label.trim().toLowerCase();
  base = base.replaceAll(RegExp(r'[^a-z0-9]+'), '-');
  base = base.replaceAll(RegExp(r'-{2,}'), '-');
  base = base.replaceAll(RegExp(r'^-+|-+$'), '');
  if (base.isEmpty) {
    base = 'remote-host';
  }

  final existing = <String>{
    for (final id in existingHostIds)
      if (id.trim().isNotEmpty) id.trim().toLowerCase(),
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

String _emptyToValue(String value, String fallback) {
  final trimmed = value.trim();
  return trimmed.isEmpty ? fallback : trimmed;
}
