import 'package:montycat/src/utils.dart' show recursiveParseJson;
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
}

String _jsonString(String value) =>
    '"${value.replaceAll(r'\', r'\\').replaceAll('"', r'\"')}"';
