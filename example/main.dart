import 'dart:async';
import 'package:montycat/montycat.dart'
    show
        Engine,
        KeyspaceInMemory,
        KeyspacePersistent,
        Timestamp,
        Schema,
        FieldType;

class Customer extends Schema {
  Customer(super.kwargs);

  static String get schemaName => 'Customer';

  static Map<String, FieldType> get schemaMetadata => {
    'name': FieldType(String),
    'age': FieldType(int, nullable: true),
    'email': FieldType(String, nullable: true),
  };

  @override
  Map<String, FieldType> metadata() => schemaMetadata;
}

class Orders extends Schema {
  Orders(super.kwargs);

  static String get schemaName => 'Orders';

  static Map<String, FieldType> get schemaMetadata => {
    'date': FieldType(Timestamp),
    'quantity': FieldType(int),
    'customer': FieldType(String),
  };

  @override
  Map<String, FieldType> metadata() => schemaMetadata;
}

Future<void> main() async {
  Engine engine = Engine(
    host: '127.0.0.1',
    port: 21210,
    username: 'USER',
    password: '12345',
    store: 'Company',
  );

  KeyspaceInMemory customers = KeyspaceInMemory(keyspace: 'customers');
  KeyspacePersistent production = KeyspacePersistent(keyspace: 'production');

  customers.connectEngine(engine);
  production.connectEngine(engine);

  final customersCreated = await customers.createKeyspace();
  final productionCreated = await production.createKeyspace();

  print("Keyspaces created: $customersCreated, $productionCreated");

  // ---IN-MEMORY KEYSPACE--- //

  var customer = Customer({'name': 'Alice Smith', 'age': 28, 'email': null});

  var custInsert = await customers.insertValue(value: customer.serialize());

  print(custInsert);
  //{status: true, payload: 29095364578528255816148465894650046051, error: null}

  var custFetched = await customers.getValue(
    key: '29095364578528255816148465894650046051',
  );

  print(custFetched);
  //{status: true, payload: {name: Alice Smith, age: 28, email: alice.smith@example.com}, error: null}

  var custUpdate = await customers.updateValue(
    key: '29095364578528255816148465894650046051',
    updates: {'age': 29},
  );

  print(custUpdate);
  //{status: true, payload: null, error: null}

  var custDelete = await customers.deleteKey(
    key: '29095364578528255816148465894650046051',
  );

  print(custDelete);
  //{status: true, payload: null, error: null}

  var custVerifyKeys = await customers.getKeys();

  print(custVerifyKeys);
  //{status: true, payload: [], error: null}

  // ---PERSISTENT KEYSPACE--- //

  var order = Orders({
    'date': Timestamp(timestamp: DateTime.now().toUtc().toString()),
    'quantity': 3,
    'customer': 'Name',
  });

  var prodInsert = await production.insertValue(value: order.serialize());

  print(prodInsert);
  //{status: true, payload: 30442970696809394303186116932586352271, error: null}

  var prodFetched = await production.getValue(
    key: '30442970696809394303186116932586352271',
  );

  print(prodFetched);
  //{status: true, payload: {date: 2025-10-05T12:34:56.789Z, quantity: 3, customer: Name}, error: null}

  var prodUpdate = await production.updateValue(
    key: '30442970696809394303186116932586352271',
    updates: {'quantity': 10},
  );

  print(prodUpdate);
  //{status: true, payload: null, error: null}

  var prodLookup = await production.lookupValuesWhere(
    searchCriteria: {'quantity': 10, 'date': Timestamp(after: '2025-10-01')},
    keyIncluded: true,
    schema: Orders.schemaName,
  );

  print(prodLookup);
  //{status: true, payload: [{__key__: 30442970696809394303186116932586352271, __value__: {date: 2025-10-05T12:34:56.789Z, quantity: 10, customer: Name}}], error: null}
}
