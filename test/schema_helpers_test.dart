import 'dart:convert';

import 'package:montycat/montycat.dart';
import 'package:montycat/src/functions/generic.dart';
import 'package:montycat/src/tools.dart' show Limit;
import 'package:montycat/src/utils.dart' show isU128, recursiveParseJson;
import 'package:test/test.dart';

class QueryContext {
  String username = 'owner';
  String password = 'secret';
  String keyspace = 'events';
  String store = 'orders';
  bool persistent = true;
  bool distributed = false;
}

void main() {
  test('serializes timestamps, pointers, limits, and enum wire names', () {
    expect(const Timestamp(start: 'a', end: 'b').serialize(), {
      'range_timestamp': ['a', 'b'],
    });
    expect(const Timestamp(after: 'a').serialize(), {'after_timestamp': 'a'});
    expect(const Timestamp(before: 'b').serialize(), {'before_timestamp': 'b'});
    expect(const Timestamp(timestamp: 'now').serialize(), 'now');
    expect(const Timestamp().serialize, throwsArgumentError);
    expect(const Pointer(keyspace: 'events', key: 'root').serialize(), [
      'events',
      'root',
    ]);
    expect(const Limit(start: 2, stop: 5).serialize(), {'start': 2, 'stop': 5});
    expect(Permission.all.toString(), 'all');
    expect(SemanticModel.bgeSmall.wireName, 'bge-small');
  });

  test('validates and serializes dynamic schemas', () {
    final schema = makeSchema(
      'Event',
      {
        'name': 'launch',
        'count': 2,
        'note': null,
        'parent': const Pointer(keyspace: 'events', key: 'root'),
        'createdAt': const Timestamp(timestamp: 'now'),
      },
      {
        'name': const FieldType(String),
        'count': const FieldType(int),
        'note': const FieldType(String, nullable: true),
        'parent': const FieldType(Pointer),
        'createdAt': const FieldType(Timestamp),
      },
    );
    expect(schema.toString(), 'Event');
    expect(schema.serialize(), {
      'name': 'launch',
      'count': 2,
      'note': null,
      'pointers': {
        'parent': ['events', 'root'],
      },
      'timestamps': {'createdAt': 'now'},
      'schema': 'Event',
    });

    expect(
      () => makeSchema('Bad', {}, {'name': const FieldType(String)}),
      throwsArgumentError,
    );
    expect(
      () => makeSchema('Bad', {'name': 2}, {'name': const FieldType(String)}),
      throwsArgumentError,
    );
    expect(
      () => makeSchema(
        'Bad',
        {'name': 'ok', 'extra': true},
        {'name': const FieldType(String)},
      ),
      throwsArgumentError,
    );
  });

  test('parses nested JSON while preserving u128 strings', () {
    const huge = '340282366920938463463374607431768211455';
    final parsed =
        recursiveParseJson('{"nested":"[1,{\\"ok\\":true}]","id":"$huge"}')
            as Map;
    expect(parsed['nested'], [
      1,
      {'ok': true},
    ]);
    expect(parsed['id'], huge);
    expect(isU128(huge), isTrue);
    expect(isU128('123'), isFalse);
  });

  test('hashes keys and processes pointer search criteria', () {
    expect(convertCustomKeys(['a', 2]), [
      convertCustomKey('a'),
      convertCustomKey(2),
    ]);
    expect(convertCustomKeysValues({'a': 1}), {convertCustomKey('a'): 1});
    expect(
      handleTimestampsAndPointers({
        'created': const Timestamp(after: 'now'),
        'parent': const Pointer(keyspace: 'events', key: 'root'),
        'active': true,
      }),
      {
        'created': {'after_timestamp': 'now'},
        'active': true,
        'pointers': {
          'parent': ['events', 'root'],
        },
      },
    );
    expect(
      modifyPointers({
        'pointers': {
          'parent': ['events', 'custom'],
        },
      }),
      {
        'pointers': {
          'parent': ['events', convertCustomKey('custom')],
        },
      },
    );
  });

  test('serializes semantic query fields and rejects mixed schemas', () {
    final bytes = convertToBinaryQuery(
      cls: QueryContext(),
      command: 'semantic_search',
      key: '7',
      value: {
        'schema': 'Event',
        'parent': const Pointer(keyspace: 'events', key: 'root'),
      },
      semanticQuery: 'launch',
      minScore: 0.7,
      semanticFilter: {'created': const Timestamp(after: 'now')},
      waitForIndex: true,
    );
    final query = jsonDecode(utf8.decode(bytes)) as Map<String, dynamic>;
    expect(query['schema'], 'Event');
    expect(query['search_criteria'], 'launch');
    expect(query['min_score'], 0.7);
    expect(query['wait_for_index'], isTrue);
    expect(jsonDecode(query['value']), {
      'parent': ['events', 'root'],
    });
    expect(jsonDecode(query['semantic_filter']), {
      'created': {'after_timestamp': 'now'},
    });

    expect(
      () => convertToBinaryQuery(
        cls: QueryContext(),
        command: 'insert_bulk',
        bulkValues: [
          {'schema': 'One', 'value': 1},
          {'schema': 'Two', 'value': 2},
        ],
      ),
      throwsException,
    );
  });

  test('KV rejects incomplete requests before networking', () async {
    final keyspace = KeyspaceInMemory(keyspace: 'events');
    expect(keyspace.getValue(), throwsException);
    expect(keyspace.deleteKey(), throwsArgumentError);
    expect(keyspace.deleteKey(key: '1', customKey: 'one'), throwsArgumentError);
    expect(keyspace.deleteBulk(), throwsArgumentError);
    expect(keyspace.getBulk(), throwsArgumentError);
    expect(keyspace.updateBulk(), throwsArgumentError);
    expect(keyspace.semanticSearchGetKeys('   '), throwsArgumentError);
    expect(
      keyspace.semanticSearchGetKeysWhere('launch', {}),
      throwsArgumentError,
    );
    expect(
      keyspace.semanticSearchGetValuesWhere('launch', {}),
      throwsArgumentError,
    );
    expect(
      keyspace.listAllDependingKeys(key: '1', customKey: 'one'),
      throwsArgumentError,
    );
    expect(
      keyspace.enforceSchema(schemaMetadata: {}, schemaName: 'Empty'),
      throwsException,
    );
  });
}
