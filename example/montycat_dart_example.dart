import 'package:montycat_dart/source.dart' show Engine, KeyspaceInMemory, KeyspacePersistent, Schema, makeSchema;

class User extends Schema {
  User(super.kwargs);

  static Map<String, Type> get schemaMetadata => {
        'name': String,
        'age': int,
        'email': String,
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

  KeyspaceInMemory inMemoryKeyspace = KeyspaceInMemory(keyspace: 'test_keyspace');
  inMemoryKeyspace.connectEngine(engine);

  KeyspacePersistent persistentKeyspace = KeyspacePersistent(keyspace: 'persistent_keyspace');
  persistentKeyspace.connectEngine(engine1);

  User user = User({
    'name': 'John Doe',
    'age': 30,
    'email': 'john.doe@example.com',
  });

  var hint = {
    'name': String,
    'age': int,
    'email': String,
  };

  var usr = makeSchema("USR", {'name': 'John Doe', 'age': 30, 'email': 'john.doe@example.com'}, hint);
  print(usr.serialize());
  print(('FIELD TYPES', user.metadata()));

  print(user.serialize());

  print(await engine1.createStore());
  print(await engine1.getStructureAvailable());
  print(await inMemoryKeyspace.createKeyspace());

  print(await inMemoryKeyspace.insertValue(value: {
    'name': 'John Doe',
    'age': 30,
    'email': 'john.doe@example.com',
  }));

  print(await inMemoryKeyspace.enforceSchema(User.schemaMetadata));

  print(await inMemoryKeyspace.insertBulk(bulkValues: [
    {
      'name': 'Alice',
      'age': 25,
      'email': 'alice@example.com',
    },
    {
      'name': 'Bob',
      'age': 28,
      'email': 'bob@example.com',
    }
  ]));
  var keys = await inMemoryKeyspace.getKeys();

  if (keys['status'] && keys['payload'] != null) {
    print(keys);
    // List<String> stringList = keys['payload'].cast<String>();
    // print(await inMemoryKeyspace.getBulk(bulkKeys: stringList));
    // print(await inMemoryKeyspace.updateValue(key: stringList[3], filters: {
    //   'age': 31
    // }));
  }

  print(await inMemoryKeyspace.lookupKeysWhere(searchCriteria: {
    'age': 31
  }));

  print(await inMemoryKeyspace.lookupValuesWhere(searchCriteria: {
    'age': 31
  }));

  print(await inMemoryKeyspace.insertCustomKeyValue(customKey: 'BaileyJay', value: {
    'name': 'Bailey Jay',
    'age': 29,
    'email': 'bailey.jay@example.com',
  }));

  print(await inMemoryKeyspace.getValue(customKey: 'BaileyJay'));
  print(await inMemoryKeyspace.deleteKey(customKey: 'BaileyJay'));

}