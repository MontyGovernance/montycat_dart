import 'package:montycat/src/utils.dart' show recursiveParseJson;
import 'package:montycat/montycat.dart'
    show Engine, PolicyCapability, PolicyKeyspaceType, SemanticModel;
import 'package:test/test.dart';

void main() {
  test('preserves governance denial details', () {
    const error =
        "Governance permission denied: capability 'manage-schema' on store 'orders', keyspace 'events'";
    final response =
        recursiveParseJson(
              '{"status":false,"payload":null,"error":${_jsonString(error)}}',
            )
            as Map;
    expect(response['error'], error);
  });

  test('preserves creator revocations in policy views', () {
    final response =
        recursiveParseJson(
              '{"status":true,"payload":{"owned_keyspaces":[{"revoked_creator_capabilities":["manage-schema"]}]},"error":null}',
            )
            as Map;
    expect(
      response['payload']['owned_keyspaces'][0]['revoked_creator_capabilities'],
      ['manage-schema'],
    );
  });

  test('policy qualifiers are restricted to their matching capabilities', () {
    final engine = Engine(
      host: 'localhost',
      port: 21210,
      username: 'owner',
      password: 'password',
    );

    expect(
      engine.policyGrant(
        owner: 'alice',
        capability: PolicyCapability.manageSchema,
        store: 'catalog',
        models: [SemanticModel.bgeSmall],
      ),
      throwsArgumentError,
    );
    expect(
      engine.policyGrant(
        owner: 'alice',
        capability: PolicyCapability.manageSnapshots,
        store: 'catalog',
        types: [PolicyKeyspaceType.persistent],
      ),
      throwsArgumentError,
    );
    expect(
      engine.policyExplain(
        capability: PolicyCapability.manageSchema,
        store: 'catalog',
        model: SemanticModel.bgeSmall,
      ),
      throwsArgumentError,
    );
  });
}

String _jsonString(String value) =>
    '"${value.replaceAll(r'\', r'\\').replaceAll('"', r'\"')}"';
