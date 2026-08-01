import 'dart:convert';
import 'dart:io';

import 'package:montycat/montycat.dart';
import 'package:test/test.dart';

Future<List<Map<String, dynamic>>> captureCommands(
  Future<void> Function(Engine engine) run,
) async {
  final commands = <Map<String, dynamic>>[];
  final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final subscription = server.listen((socket) {
    socket
        .cast<List<int>>()
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen((line) {
          commands.add(jsonDecode(line) as Map<String, dynamic>);
          socket.write('{"status":true}\n');
        });
  });
  try {
    final engine = Engine(
      host: InternetAddress.loopbackIPv4.address,
      port: server.port,
      username: 'owner',
      password: 'secret',
      store: 'orders',
    );
    await run(engine);
    return commands;
  } finally {
    await subscription.cancel();
    await server.close();
  }
}

void main() {
  test('parses valid URIs and rejects malformed URIs', () {
    final engine = Engine.fromUri(
      'montycat://alice:secret@db.example:12777/orders',
    );
    expect(engine.host, 'db.example');
    expect(engine.port, 12777);
    expect(engine.username, 'alice');
    expect(engine.password, 'secret');
    expect(engine.store, 'orders');
    expect(
      Engine.fromUri('montycat://alice:secret@db.example:12777').store,
      isNull,
    );
    expect(
      () => Engine.fromUri('https://db.example:12777'),
      throwsFormatException,
    );
    expect(
      () => Engine.fromUri('montycat://db.example:12777'),
      throwsFormatException,
    );
  });

  test('builds store, owner, access, and semantic commands', () async {
    final commands = await captureCommands((engine) async {
      await engine.createStore();
      await engine.removeStore();
      await engine.createOwner('alice', 'pw');
      await engine.removeOwner('alice');
      await engine.listOwners();
      await engine.grantTo(
        'alice',
        Permission.read,
        keyspaces: ['events', 'users'],
      );
      await engine.revokeFrom('alice', Permission.write, keyspaces: ['events']);
      await engine.enableSemanticSearch(
        model: SemanticModel.bgeSmall,
        field: 'body',
        store: 'catalog',
        keyspace: 'products',
      );
      await engine.disableSemanticSearch(
        dropVectors: true,
        store: 'catalog',
        keyspace: 'products',
      );
      try {
        await engine.getSemanticStatus(
          store: 'catalog',
          keyspace: 'products',
        );
      } on StateError {
        // The command-capture server intentionally returns no payload.
      }
      try {
        await engine.reembedSemanticSearch(
          model: SemanticModel.bgeBase,
          field: 'description',
          store: 'catalog',
          keyspace: 'products',
        );
      } on StateError {
        // The command-capture server intentionally returns no payload.
      }
    });

    expect(commands.map((query) => query['raw']).toList(), [
      ['create-store', 'store', 'orders'],
      ['remove-store', 'store', 'orders'],
      ['create-owner', 'username', 'alice', 'password', 'pw'],
      ['remove-owner', 'username', 'alice'],
      ['list-owners'],
      [
        'grant-to',
        'owner',
        'alice',
        'permission',
        'read',
        'store',
        'orders',
        'keyspaces',
        'events',
        'users',
      ],
      [
        'revoke-from',
        'owner',
        'alice',
        'permission',
        'write',
        'store',
        'orders',
        'keyspaces',
        'events',
      ],
      [
        'enable-semantic-search',
        'model',
        'bge-small',
        'field',
        'body',
        'store',
        'catalog',
        'keyspace',
        'products',
      ],
      [
        'disable-semantic-search',
        'drop-vectors',
        'store',
        'catalog',
        'keyspace',
        'products',
      ],
      [
        'get-semantic-status',
        'store',
        'catalog',
        'keyspace',
        'products',
      ],
      [
        'reembed-semantic-search',
        'model',
        'bge-base',
        'field',
        'description',
        'store',
        'catalog',
        'keyspace',
        'products',
      ],
    ]);
    expect(commands.first['credentials'], ['owner', 'secret']);

    final engine = Engine(
      host: 'localhost',
      port: 12777,
      username: 'owner',
      password: 'secret',
    );
    expect(
      engine.enableSemanticSearch(keyspace: 'products'),
      throwsArgumentError,
    );
    expect(
      engine.disableSemanticSearch(keyspace: 'products'),
      throwsArgumentError,
    );
    expect(engine.createStore(), throwsArgumentError);
  });

  test(
    'builds governance commands and omits provision keyspace scope',
    () async {
      final commands = await captureCommands((engine) async {
        await engine.policyView(owner: 'alice', store: 'catalog');
        await engine.policyHistory(
          owner: 'alice',
          store: 'catalog',
          keyspace: 'products',
        );
        await engine.policyExplain(
          capability: PolicyCapability.manageSemantic,
          store: 'catalog',
          owner: 'alice',
          keyspace: 'products',
          keyspaceType: PolicyKeyspaceType.persistent,
          model: SemanticModel.bgeSmall,
        );
        final mutations = [
          engine.policyGrant,
          engine.policyRevoke,
          engine.policyDeny,
          engine.policyRemoveDenial,
          engine.policyPreviewGrant,
          engine.policyPreviewRevoke,
        ];
        for (final mutation in mutations) {
          await mutation(
            owner: 'alice',
            capability: PolicyCapability.provisionKeyspace,
            store: 'catalog',
            keyspace: 'ignored',
            types: [PolicyKeyspaceType.persistent],
            models: [SemanticModel.bgeSmall],
          );
        }
        await engine.policyValidate('rules: []', format: PolicyFormat.yaml);
        await engine.policyPlan('rules: []', format: PolicyFormat.yaml);
        await engine.policyApply('rules: []', format: PolicyFormat.yaml);
        await engine.policyExport(format: PolicyFormat.yml);
      });

      expect(commands[0]['raw'], [
        'policy-view',
        'owner',
        'alice',
        'store',
        'catalog',
      ]);
      expect(commands[1]['raw'], [
        'policy-history',
        'owner',
        'alice',
        'store',
        'catalog',
        'keyspace',
        'products',
      ]);
      expect(commands[2]['raw'], [
        'policy-explain',
        'capability',
        'manage-semantic',
        'store',
        'catalog',
        'owner',
        'alice',
        'keyspace',
        'products',
        'type',
        'persistent',
        'model',
        'bge-small',
      ]);
      final operations = [
        'policy-grant',
        'policy-revoke',
        'policy-deny',
        'policy-remove-denial',
        'policy-preview-grant',
        'policy-preview-revoke',
      ];
      for (var index = 0; index < operations.length; index++) {
        expect(commands[index + 3]['raw'], [
          operations[index],
          'owner',
          'alice',
          'capability',
          'provision-keyspace',
          'store',
          'catalog',
          'types',
          'persistent',
          'models',
          'bge-small',
        ]);
      }
      expect(commands.skip(9).map((query) => query['raw']).toList(), [
        ['policy-validate', 'format', 'yaml', 'document', 'rules: []'],
        ['policy-plan', 'format', 'yaml', 'document', 'rules: []'],
        ['policy-apply', 'format', 'yaml', 'document', 'rules: []'],
        ['policy-export', 'format', 'yml'],
      ]);
    },
  );

  test('builds operator commands', () async {
    final commands = await captureCommands((engine) async {
      await engine.getStructureAvailable();
      await engine.enableWaitForIndex();
      await engine.disableWaitForIndex();
      await engine.enableReports();
      await engine.disableReports();
      await engine.allowSubscriptions();
      await engine.restrictSubscriptions();
      await engine.queueDepths();
      await engine.nodeInfo();
      await engine.setSnapshotRate(5);
      await engine.setExpirationCheckRate(10);
    });
    expect(commands.map((query) => query['raw']).toList(), [
      ['get-structure-available', 'store', 'orders'],
      ['enable-wait-for-index'],
      ['disable-wait-for-index'],
      ['enable-reports'],
      ['disable-reports'],
      ['allow-subscriptions'],
      ['restrict-subscriptions'],
      ['queue-depths'],
      ['node-info'],
      ['snapshot-rate', '5'],
      ['expiration-check', '10'],
    ]);
  });
}
