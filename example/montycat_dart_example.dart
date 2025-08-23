import 'package:montycat_dart/source.dart' show KeyspaceInMemory, KeyspacePersistent, Engine;

void main() {

  Engine engine = Engine(
    host: '127.0.0.1',
    port: 21210,
    username: 'user',
    password: 'pass',
    store: 'default_store'
  );

  KeyspaceInMemory testKeyspace = KeyspaceInMemory(keyspace: 'test_keyspace');
  testKeyspace.connectEngine(engine);

  KeyspacePersistent persistentKeyspace = KeyspacePersistent(keyspace: 'persistent_keyspace');
  persistentKeyspace.connectEngine(engine);

  Map<String, Type> schema = {
    'name': String,
    'age': int,
    'email': String,
  };

  testKeyspace.enforceSchema(schema);
  persistentKeyspace.enforceSchema(schema);

}