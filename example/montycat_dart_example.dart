import 'package:montycat_dart/source.dart' show DynamicSchema, Engine, KeyspaceInMemory, KeyspacePersistent, Pointer, Pointer, Timestamp, Schema, Timestamp, makeSchema;

class User extends Schema {
  User(super.kwargs);

  static String get schemaName => 'User';

  static Map<String, Type> get schemaMetadata => {
        'name': String,
        'age': int,
        'email': String,
        'dep': Pointer,
        'created': Timestamp
      };

  @override
  Map<String, Type> metadata() => schemaMetadata;
}

class Deps extends Schema {
  Deps(super.kwargs);

  static String get schemaName => 'Deps';

  static Map<String, Type> get schemaMetadata => {
        'name': String,
        'employees': int
      };

  @override
  Map<String, Type> metadata() => schemaMetadata;
}

class NEWSCHEMA extends Schema {

  NEWSCHEMA(super.kwargs);

  static String get schemaName => 'NEWSCHEMA';

  static Map<String, Type> get schemaMetadata => {
        'field1': String,
        'field2': int,
        'field3': String,
        'field4': Map<String, dynamic>,
        'field5': List<String>
      };

  @override
  Map<String, Type> metadata() => schemaMetadata;
}

Future<void> main() async {

  Engine engine = Engine(
    host: '127.0.0.1',
    port: 21210,
    username: 'EugeneAndMonty',
    password: '12345',
    store: 'DEFAULT_STORE'
  );

  Engine engine1 = Engine.fromUri('montycat://EugeneAndMonty:12345@127.0.0.1:21210/DEFAULT_STORE1');

  KeyspaceInMemory inMemoryKeyspace = KeyspaceInMemory(keyspace: 'inmemory_keyspace1');
  inMemoryKeyspace.connectEngine(engine);

  KeyspaceInMemory inMemoryKeyspaceRelated = KeyspaceInMemory(keyspace: 'inmemory_keyspace_related');
  inMemoryKeyspaceRelated.connectEngine(engine);

  KeyspacePersistent persistentKeyspace = KeyspacePersistent(keyspace: 'persistent_keyspace1');
  persistentKeyspace.connectEngine(engine1);

  print(await inMemoryKeyspace.createKeyspace());
  print(await persistentKeyspace.createKeyspace());
  print(await inMemoryKeyspaceRelated.createKeyspace());

  print(await inMemoryKeyspace.enforceSchema(schema: User.schemaMetadata, schemaName: User.schemaName));
  print(await inMemoryKeyspaceRelated.enforceSchema(schema: Deps.schemaMetadata, schemaName: Deps.schemaName));
  print(await inMemoryKeyspaceRelated.enforceSchema(schema: NEWSCHEMA.schemaMetadata, schemaName: NEWSCHEMA.schemaName));

  var department = Deps({
    'name': 'Engineering',
    'employees': 12
  });

  print(await inMemoryKeyspaceRelated.insertValue(value: department.serialize()));

  List<dynamic> users = [];

  for (num i = 0; i < 100; i++) {
    users.add(User({
      'name': 'User $i',
      'age': 20 + i,
      'email': 'user$i@example.com',
      'dep': Pointer(keyspace: inMemoryKeyspaceRelated.keyspace, key: '295521135830941005163436314849878064088'),
      'created': Timestamp(timestamp: DateTime.now().toUtc().toString())
    }).serialize());
  }

  print(await inMemoryKeyspace.insertBulk(bulkValues: users));

  var keys = await inMemoryKeyspace.lookupKeysWhere(schema: User.schemaName, limit: [0, 10], searchCriteria: {'created': Timestamp(after: '2023-01-01')});

  if (keys['status']) {

    Map<String, dynamic> bulkKV = {};

    if (keys['payload'].length > 0) {
      for (int i = 0; i < (keys['payload'] as List).length; i++) {
        bulkKV[(keys['payload'] as List)[i]] = {'name': 'Denis', 'dep': Pointer(keyspace: 'inmemory_keyspace_related', key: '295521135830941005163436314849878064088')};
      }
    }

    // convert to list of Strings
    var bulk = (keys['payload'] as List).map((e) => e.toString()).toList();

    print(await inMemoryKeyspace.updateBulk(bulkKeysValues: bulkKV));
    print(await inMemoryKeyspace.getBulk(bulkKeys: bulk));

    print(await inMemoryKeyspace.lookupKeysWhere(schema: User.schemaName));

    //print(await inMemoryKeyspace.deleteBulk(bulkKeys: bulk));


  }


}