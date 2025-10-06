import 'package:montycat/source.dart'
    show
        DynamicSchema,
        Engine,
        KeyspaceInMemory,
        KeyspacePersistent,
        Pointer,
        Pointer,
        Timestamp,
        Schema,
        Timestamp,
        Permission,
        makeSchema,
        SubscriptionHandle;

import 'dart:async';

class User extends Schema {
  User(super.kwargs);

  static String get schemaName => 'User';

  static Map<String, Type> get schemaMetadata => {
    'name': String,
    'age': int,
    'email': String,
    'dep': Pointer,
    'created': Timestamp,
  };

  @override
  Map<String, Type> metadata() => schemaMetadata;
}

class Deps extends Schema {
  Deps(super.kwargs);

  static String get schemaName => 'Deps';

  static Map<String, Type> get schemaMetadata => {
    'name': String,
    'employees': int,
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
    'field5': List<String>,
  };

  @override
  Map<String, Type> metadata() => schemaMetadata;
}

class SchemaRAND extends DynamicSchema {
  SchemaRAND(super.fields, super.hints, super.name);
}

Future<void> main() async {
  // Engine engine = Engine(
  //   host: '127.0.0.1',
  //   port: 21210,
  //   username: 'EugeneAndMonty',
  //   password: '12345',
  //   store: 'DEFAULT_STORE',
  // );

  Map<String, Type> _hints = {'name': String, 'age': int, 'email': String};

  SchemaRAND value = SchemaRAND(
    {'name': 'John Doe', 'age': 30, 'email': 'john.doe@example.com'},
    _hints,
    'UserSchema',
  );

  SchemaRAND val2 = SchemaRAND(
    {'name': 'Jane Doe', 'age': 25, 'email': 'jane.doe@example.com'},
    _hints,
    'UserSchema',
  );

  print(value.serialize());
  print(value.schema);
  print(value.metadata());

  var another = makeSchema(
    'AnotherSchema',
    {
      'field1': 'value1',
      'field2': 42,
      'field3': ['item1', 'item2'],
    },
    {'field1': String, 'field2': int, 'field3': List<String>},
  );

  print(another.serialize());
  print(another.schema);
  print(another.metadata());

  Engine engine1 = Engine.fromUri(
    'montycat://EUGENE:12345@127.0.0.1:21210/departments',
  );

  engine1.createOwner('<owner username>', '<owner password>');
  engine1.removeOwner('<owner username>');

  engine1.grantTo('<owner username>', Permission.all);

  await engine1.getStructureAvailable();

  KeyspacePersistent production = KeyspacePersistent(keyspace: 'production');
  KeyspaceInMemory staging = KeyspaceInMemory(keyspace: 'staging');

  staging.updateValue(
    key: '135113904380619923677927799672778281922',
    updates: {'name': 'Updated Name', 'age': 35},
    expireSec: 3600,
  );

  production.connectEngine(engine1);
  production.enforceSchema(schema: value.metadata(), schemaName: value.schema);
  production.listAllSchemasInKeyspace();

  production.enforceSchema(
    schema: User.schemaMetadata,
    schemaName: User.schemaName,
  );

  production.updateCacheAndCompression();
  // // production.enforceSchema(schema: value.schema, schemaName: value.);

  void callback(dynamic message) {
    print("Callback received: $message");
    print(message["payload"]?["__key__"]);
  }

  var resss = await production.lookupValuesWhere(
    pointersMetadata: true,
    schema: User.schemaName,
    limit: [0, 10],
    searchCriteria: {'created': Timestamp(after: '2023-01-01')},
  );

  var valll = {
    'name': 'James',
    'dep': Pointer(
      keyspace: production.keyspace,
      key: '135113904380619923677927799672778281922',
    ),
  };

  var retr = await production.getBulk(
    bulkCustomKeys: [],
    bulkKeys: [],
    limit: [0, 10],
  );

  await production.getKeys(volumes: ['0'], limit: [0, 10], latestVolume: true);

  var sub = await production.subscribe(
    key: null,
    customKey: null,
    callback: callback,
  );

  if (sub is SubscriptionHandle) {
    Timer(Duration(seconds: 100), () {
      sub.stop();
      print("Subscription stopped");
    });
  }

  final get = await production.insertValue(value: value.serialize());
  print(await production.getValue(key: get['payload']));

  await production.lookupKeysWhere(
    searchCriteria: {'created': Timestamp(after: '2023-01-01')},
  );

  await production.insertBulk(bulkValues: [val2.serialize()]);
  await production.getKeys();

  // KeyspaceInMemory inMemoryKeyspace = KeyspaceInMemory(
  //   keyspace: 'inmemory_keyspace1',
  // );
  // inMemoryKeyspace.connectEngine(engine);

  // KeyspaceInMemory inMemoryKeyspaceRelated = KeyspaceInMemory(
  //   keyspace: 'inmemory_keyspace_related',
  // );
  // inMemoryKeyspaceRelated.connectEngine(engine);

  // KeyspacePersistent persistentKeyspace = KeyspacePersistent(
  //   keyspace: 'persistent_keyspace1',
  // );
  // persistentKeyspace.connectEngine(engine1);

  // print(await inMemoryKeyspace.createKeyspace());
  // print(await persistentKeyspace.createKeyspace());
  // print(await inMemoryKeyspaceRelated.createKeyspace());

  // print(
  //   await inMemoryKeyspace.enforceSchema(
  //     schema: User.schemaMetadata,
  //     schemaName: User.schemaName,
  //   ),
  // );
  // print(
  //   await inMemoryKeyspaceRelated.enforceSchema(
  //     schema: Deps.schemaMetadata,
  //     schemaName: Deps.schemaName,
  //   ),
  // );
  // print(
  //   await inMemoryKeyspaceRelated.enforceSchema(
  //     schema: NEWSCHEMA.schemaMetadata,
  //     schemaName: NEWSCHEMA.schemaName,
  //   ),
  // );

  // var department = Deps({'name': 'Engineering', 'employees': 12});

  // print(
  //   await inMemoryKeyspaceRelated.insertValue(value: department.serialize()),
  // );

  // List<dynamic> users = [];

  // for (num i = 0; i < 100; i++) {
  //   users.add(
  //     User({
  //       'name': 'User $i',
  //       'age': 20 + i,
  //       'email': 'user$i@example.com',
  //       'dep': Pointer(
  //         keyspace: inMemoryKeyspaceRelated.keyspace,
  //         key: '135113904380619923677927799672778281922',
  //       ),
  //       'created': Timestamp(timestamp: DateTime.now().toUtc().toString()),
  //     }).serialize(),
  //   );
  // }

  // print(await inMemoryKeyspace.insertBulk(bulkValues: users));
  // print("PERS");
  // print(
  //   await persistentKeyspace.insertBulk(
  //     bulkValues: [
  //       {'user': 'user1@example.com'},
  //       {'user': 'user2@example.com'},
  //     ],
  //   ),
  // );

  // var keys = await inMemoryKeyspace.lookupKeysWhere(
  //   schema: User.schemaName,
  //   limit: [0, 10],
  //   searchCriteria: {'created': Timestamp(after: '2023-01-01')},
  // );

  var search = await production.lookupValuesWhere(
    schema: User.schemaName,
    limit: [0, 10],
    searchCriteria: {'created': Timestamp(after: '2023-01-01')},
  );

  await production.listAllDependingKeys(
    key: '135113904380619923677927799672778281922',
  );

  // if (keys['status']) {
  //   Map<String, dynamic> bulkKV = {};

  //   if (keys['payload'].length > 0) {
  //     for (int i = 0; i < (keys['payload'] as List).length; i++) {
  //       bulkKV[(keys['payload'] as List)[i]] = {
  //         'name': 'James',
  //         'dep': Pointer(
  //           keyspace: 'inmemory_keyspace_related',
  //           key: '135113904380619923677927799672778281922',
  //         ),
  //       };
  //     }
  //   }

  //   // convert to list of Strings
  //   var bulk = (keys['payload'] as List).map((e) => e.toString()).toList();

  //   print(await inMemoryKeyspace.updateBulk(bulkKeysValues: bulkKV));
  //   print(await inMemoryKeyspace.getBulk(bulkKeys: bulk, keyIncluded: true));

  //   print(await inMemoryKeyspace.lookupKeysWhere(schema: User.schemaName));

  //   print(await inMemoryKeyspaceRelated.getLen());
  //   print(await engine1.getStructureAvailable());
  //   print(await engine.getStructureAvailable());

  //   print("LATEST");
  //   print(await inMemoryKeyspace.getKeys(volumes: ['0']));
  //   print(await inMemoryKeyspace.deleteBulk(bulkKeys: bulk));
  // }
}
