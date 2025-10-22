import 'dart:convert';
import 'dart:typed_data';
import 'package:montycat/source.dart';

import '../tools.dart' show Limit, Pointer, Timestamp;
import '../utils.dart' show sendData;
import '../functions/generic.dart'
    show convertCustomKey, convertToBinaryQuery, convertCustomKeysValues;

/// Abstract base class for interacting with the MontyCat database.
/// Provides methods for connection handling, schema enforcement,
/// and CRUD-like operations (single, bulk, lookup).
///
abstract class KV {
  /// The last executed command (e.g. "get_value", "update_bulk").
  String command = "";

  /// Store name (database/store identifier).
  String? store;

  /// Database server host.
  String host = "";

  /// Username for authentication.
  String username = "";

  /// Password for authentication.
  String password = "";

  /// Server port number.
  int port = 0;

  /// Whether to use TLS for the connection.
  bool useTls = false;

  /// Serialized representation of query limits (`start`, `stop`).
  Map<String, int> limitOutput = {};

  /// Logical partition inside the store.
  String get keyspace;
  set keyspace(String value);

  /// Whether the keyspace is distributed across nodes.
  bool get distributed;

  set distributed(bool? value) {
    distributed = value ?? false;
  }

  /// Whether the keyspace is persistent on disk.
  bool get persistent;

  set persistent(bool value) {
    persistent = value;
  }

  /// Sends a query to the server.
  Future<dynamic> runQuery(
    String host,
    int port,
    Uint8List query, {
    void Function(dynamic)? callback,
    bool useTls = false,
  }) async {
    return await sendData(
      host,
      port,
      query,
      callback: callback,
      useTls: useTls,
    );
  }

  /// Connects to the database engine using an [Engine] object.
  /// Copies connection parameters from the [Engine].
  ///
  /// Example:
  ///
  /// ```dart
  /// final engine = Engine(
  ///   host: 'localhost',
  ///   port: 1234,
  ///   username: 'admin',
  ///   password: 'secret',
  ///   store: 'mystore',
  /// );
  /// ```
  ///
  /// Then connect the keyspace:
  ///
  /// ```dart
  /// await keyspace.connectEngine(engine);
  /// ```
  ///
  void connectEngine(Engine engine) {
    host = engine.host;
    port = engine.port;
    username = engine.username;
    password = engine.password;
    store = engine.store;
    useTls = engine.useTls;
  }

  /// Enforces a schema for the current keyspace.
  ///
  /// Converts Dart [Type]s to database-supported types and sends an
  /// 'enforce-schema' query.
  ///
  /// Throws an [ArgumentError] if [schema] is empty.
  ///
  /// Example:
  ///
  ///
  ///```dart
  ///
  ///class Orders extends Schema {
  ///   Orders(super.kwargs);
  ///   static String get schemaName => 'Orders';
  ///   static Map<String, Type> get schemaMetadata => {
  ///     'date': Timestamp,
  ///     'quantity': int,
  ///     'customer': String,
  ///   };
  ///   @override
  ///   Map<String, Type> metadata() => schemaMetadata;
  /// }
  ///
  /// await keyspace.enforceSchema(
  /// schema: Orders.schemaMetadata,
  /// schemaName: Orders.schemaName,
  /// );
  ///```
  ///
  Future<dynamic> enforceSchema({
    required Map<String, Type> schema,
    required String schemaName,
  }) async {
    if (schema.isEmpty) {
      throw Exception('No schema provided for enforcement');
    }

    /// Converts a Dart [Type] into a MontyCat schema type string.
    String parseType(Type fieldType) {
      final typeStr = fieldType.toString();

      if (fieldType == String) {
        return 'String';
      } else if (fieldType == int) {
        return 'Number';
      } else if (fieldType == double) {
        return 'Float';
      } else if (fieldType == bool) {
        return 'Boolean';
      } else if (typeStr.startsWith('List<') || fieldType == List) {
        return 'Array';
      } else if (typeStr.startsWith('Map<') || fieldType == Map) {
        return 'Object';
      } else if (fieldType == Pointer) {
        return 'Pointer';
      } else if (fieldType == Timestamp) {
        return 'Timestamp';
      } else {
        throw TypeError();
      }
    }

    final schemaTypes = <String, String>{};
    for (var entry in schema.entries) {
      schemaTypes[entry.key] = parseType(entry.value);
    }

    final queryMap = {
      'raw': [
        'enforce-schema',
        'store',
        store,
        'keyspace',
        keyspace,
        'persistent',
        persistent ? 'y' : 'n',
        'schema_name',
        schemaName,
        'schema_content',
        jsonEncode(schemaTypes).toString(),
      ],
      'credentials': [username, password],
    };

    final queryBytes = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, queryBytes, useTls: useTls);
  }

  /// Removes a previously enforced schema from the keyspace.
  /// Throws an [ArgumentError] if [schema] is empty.
  /// 
  /// Example:
  /// 
  /// ```dart
  /// await keyspace.removeEnforcedSchema('Orders');
  /// ```
  ///
  Future<dynamic> removeEnforcedSchema(String schema) async {
    final queryMap = {
      'raw': [
        'remove-enforced-schema',
        'store',
        store,
        'keyspace',
        keyspace,
        'persistent',
        persistent ? 'y' : 'n',
        'schema_name',
        schema,
      ],
      'credentials': [username, password],
    };

    final queryBytes = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, queryBytes, useTls: useTls);
  }

  /// Retrieves a single value by [key] or [customKey].
  /// Optionally fetches associated pointers.
  /// - [withPointers]: If true, includes pointer values.
  /// - [keyIncluded]: If true, includes the key in the response.
  /// - [pointersMetadata]: If true, includes pointer metadata instead of values.
  /// Throws an [ArgumentError] if no valid key is provided.
  /// 
  /// Example:
  /// 
  /// ```dart
  /// final result = await keyspace.getValue(
  ///   key: 'some_key',
  ///   keyIncluded: true,
  /// );
  /// ```
  ///
  Future<dynamic> getValue({
    String? key,
    String? customKey,
    bool withPointers = false,
    bool keyIncluded = false,
    bool pointersMetadata = false,
  }) async {
    if (pointersMetadata && withPointers) {
      throw ArgumentError(
        "You select both pointers value and pointers metadata. Choose one.",
      );
    }

    if (customKey != null && customKey.isNotEmpty) {
      key = convertCustomKey(customKey);
    }

    if (key == null || key.isEmpty) {
      throw Exception('No key provided');
    }

    command = 'get_value';

    final query = convertToBinaryQuery(
      cls: this,
      key: key,
      withPointers: withPointers,
      keyIncluded: keyIncluded,
      pointersMetadata: pointersMetadata,
    );

    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Deletes a single key from the store.
  /// Throws an [ArgumentError] if no valid key is provided.
  /// 
  /// Example:
  /// 
  /// ```dart
  /// await keyspace.deleteKey(key: 'some_key');
  /// ```
  ///
  Future<dynamic> deleteKey({String? key, String? customKey}) async {
    if (key != null && customKey != null) {
      throw ArgumentError("Provide either 'key' or 'customKey', not both.");
    }

    if (customKey != null && customKey.isNotEmpty) {
      key = convertCustomKey(customKey);
    }

    if (key == null || key.isEmpty) {
      throw ArgumentError("No key provided");
    }

    command = "delete_key";

    Uint8List query = convertToBinaryQuery(cls: this, key: key);

    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Deletes multiple keys in one query.
  /// Throws an [ArgumentError] if no valid keys are provided.
  /// Combines [bulkKeys] and [bulkCustomKeys] into a single list.
  ///
  /// Example:
  ///
  /// ```dart
  /// final keysToDelete = ['key1', 'key2'];
  /// await keyspace.deleteBulk(bulkKeys: keysToDelete);
  /// ```
  ///
  Future<dynamic> deleteBulk({
    List<String> bulkKeys = const [],
    List<String> bulkCustomKeys = const [],
  }) async {
    if (bulkCustomKeys.isNotEmpty) {
      bulkKeys = [...bulkKeys, ...bulkCustomKeys];
    }

    if (bulkKeys.isEmpty) {
      throw ArgumentError("No keys provided for deletion.");
    }

    command = "delete_bulk";

    final query = convertToBinaryQuery(cls: this, bulkKeys: bulkKeys);

    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Fetches multiple values at once.
  /// Supports [limit] as `[start, stop]` and optional [withPointers].
  /// - [keyIncluded]: If true, includes the key in the response.
  /// - [pointersMetadata]: If true, includes pointer metadata instead of values.
  /// Throws an [ArgumentError] if no valid keys are provided.
  /// Throws an [ArgumentError] if [limit] is not a list of two integers.
  /// Combines [bulkKeys] and [bulkCustomKeys] into a single list.
  /// 
  /// Example:
  ///
  /// ```dart
  /// final keysToFetch = ['key1', 'key2'];
  /// final result = await keyspace.getBulk(bulkKeys: keysToFetch);
  /// ```
  ///
  Future<dynamic> getBulk({
    List<String> bulkKeys = const [],
    List<String> bulkCustomKeys = const [],
    List<int> limit = const [],
    bool withPointers = false,
    bool keyIncluded = false,
    bool pointersMetadata = false,
  }) async {
    if (pointersMetadata && withPointers) {
      throw ArgumentError(
        "You select both pointers value and pointers metadata. Choose one.",
      );
    }

    if (bulkCustomKeys.isNotEmpty) {
      bulkKeys = [...bulkKeys, ...bulkCustomKeys];
    }

    if (bulkKeys.isEmpty) {
      throw ArgumentError("No keys provided for retrieval.");
    }

    command = "get_bulk";

    if (limit.length == 2) {
      limitOutput = Limit(start: limit[0], stop: limit[1]).serialize();
    } else if (limit.isNotEmpty && limit.length != 2) {
      throw ArgumentError(
        "Limit must be a list of two integers [start, stop].",
      );
    }

    final query = convertToBinaryQuery(
      cls: this,
      bulkKeys: bulkKeys,
      withPointers: withPointers,
      keyIncluded: keyIncluded,
      pointersMetadata: pointersMetadata,
    );

    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Updates multiple key-value pairs at once.
  /// Converts [bulkCustomKeysValues] automatically before sending.
  /// Throws an [ArgumentError] if no valid key-value pairs are provided.
  /// Throws an [ArgumentError] if [bulkKeysValues] is empty.
  /// Combines [bulkKeysValues] and [bulkCustomKeysValues] into a single map.
  /// The [bulkKeysValues] map contains key-value pairs to update.
  /// For example: bulkKeysValues = {'key1': {'field1': 'newValue'}, 'key2': {'field2': 42}}
  ///
  /// Example:
  ///
  /// ```dart
  /// final updates = {
  /// 'key1': {'field1': 'newValue'},
  /// 'key2': {'field2': 42},
  /// };
  ///
  /// await keyspace.updateBulk(bulkKeysValues: updates);
  /// ```
  ///
  Future<dynamic> updateBulk({
    Map<String, dynamic> bulkKeysValues = const {},
    Map<String, dynamic> bulkCustomKeysValues = const {},
  }) async {
    if (bulkKeysValues.isEmpty && bulkCustomKeysValues.isEmpty) {
      throw Exception("No key-value pairs provided for update.");
    }

    Map<String, dynamic> finalMap = Map.from(bulkKeysValues);

    if (bulkCustomKeysValues.isNotEmpty) {
      final converted = convertCustomKeysValues(bulkCustomKeysValues);
      finalMap.addAll(converted);
    }
    command = "update_bulk";
    final query = convertToBinaryQuery(cls: this, bulkKeysValues: finalMap);
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Looks up keys matching search criteria.
  /// Supports [limit] as `[start, stop]`.
  /// - [schema]: Optional schema name to filter results.
  /// - [searchCriteria]: Map of fields and values to match.
  /// Throws an [ArgumentError] if [limit] is not a list of two integers.
  /// Throws an [ArgumentError] if [searchCriteria] is empty.
  /// Throws an [ArgumentError] if [schema] is not a valid string.
  ///
  /// Example:
  ///
  /// ```dart
  /// final result = await keyspace.lookupKeysWhere(
  ///   schema: Orders.schemaName,
  ///  searchCriteria: {'customer': 'Alice Smith'},
  /// );
  /// ```
  ///
  Future<dynamic> lookupKeysWhere({
    List<int> limit = const [],
    String? schema,
    Map<String, dynamic> searchCriteria = const {},
  }) async {
    command = "lookup_keys";

    if (limit.length == 2) {
      limitOutput = Limit(start: limit[0], stop: limit[1]).serialize();
    } else if (limit.isNotEmpty && limit.length != 2) {
      throw ArgumentError(
        "Limit must be a list of two integers [start, stop].",
      );
    }

    final query = convertToBinaryQuery(
      cls: this,
      searchCriteria: searchCriteria,
      schema: schema,
    );

    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Looks up values matching search criteria.
  /// Supports [limit] as `[start, stop]` and optional [withPointers].
  /// - [schema]: Optional schema name to filter results.
  /// - [searchCriteria]: Map of fields and values to match.
  /// - [keyIncluded]: If true, includes the key in the response.
  /// - [pointersMetadata]: If true, includes pointer metadata instead of values.
  /// Throws an [ArgumentError] if [limit] is not a list of two integers.
  /// Throws an [ArgumentError] if [searchCriteria] is empty.
  /// Throws an [ArgumentError] if [schema] is not a valid string.
  ///
  /// Example:
  ///
  /// ```dart
  /// final result = await keyspace.lookupValuesWhere(
  /// schema: Orders.schemaName,
  /// searchCriteria: {'customer': 'Alice Smith'},
  /// );
  /// ```
  ///
  Future<dynamic> lookupValuesWhere({
    List<int> limit = const [],
    String? schema,
    Map<String, dynamic> searchCriteria = const {},
    bool withPointers = false,
    bool keyIncluded = false,
    bool pointersMetadata = false,
  }) async {
    if (pointersMetadata && withPointers) {
      throw ArgumentError(
        "You select both pointers value and pointers metadata. Choose one.",
      );
    }

    command = "lookup_values";

    if (limit.length == 2) {
      limitOutput = Limit(start: limit[0], stop: limit[1]).serialize();
    } else if (limit.isNotEmpty && limit.length != 2) {
      throw ArgumentError(
        "Limit must be a list of two integers [start, stop].",
      );
    }

    final query = convertToBinaryQuery(
      cls: this,
      searchCriteria: searchCriteria,
      withPointers: withPointers,
      schema: schema,
      keyIncluded: keyIncluded,
      pointersMetadata: pointersMetadata,
    );

    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Lists all keys that depend on the given key.
  /// Throws an [ArgumentError] if [key] is not a valid string.
  /// Throws an [ArgumentError] if [customKey] is not a valid string.
  ///
  /// Example:
  ///
  /// ```dart
  /// final result = await keyspace.listAllDependingKeys(key: 'my_key');
  /// ```
  ///
  Future<dynamic> listAllDependingKeys({String? key, String? customKey}) async {
    if (customKey != null && customKey.isNotEmpty) {
      key = convertCustomKey(customKey);
    }

    if (key == null || key.isEmpty) {
      throw ArgumentError("No key provided");
    }

    command = "list_all_depending_keys";

    final query = convertToBinaryQuery(key: key, cls: this);
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Returns the number of keys in the current keyspace.
  ///
  /// Example:
  ///
  /// ```dart
  /// final length = await keyspace.getLen();
  /// ```
  ///
  Future<dynamic> getLen() async {
    command = "get_len";
    final query = convertToBinaryQuery(cls: this);
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Returns all enforced schemas in the keyspace.
  ///
  /// Example:
  ///  ```dart
  /// final schemas = await keyspace.listAllSchemasInKeyspace();
  /// ```
  ///
  Future<dynamic> listAllSchemasInKeyspace() async {
    command = "list_all_schemas_in_keyspace";
    final query = convertToBinaryQuery(cls: this);
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Removes the entire keyspace from the store.
  ///
  /// Example:
  ///
  /// ```dart
  /// await keyspace.removeKeyspace();
  /// ```
  ///
  Future<dynamic> removeKeyspace() async {
    final queryMap = {
      'raw': [
        'remove-keyspace',
        'store',
        store,
        'keyspace',
        keyspace,
        'persistent',
        persistent ? 'y' : 'n',
      ],
      'credentials': [username, password],
    };

    final queryBytes = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, queryBytes, useTls: useTls);
  }

  /// Prints the current connection and keyspace properties.
  ///
  /// Example:
  ///
  /// ```dart
  /// keyspace.showProperties();
  /// ```
  ///
  showProperties() {
    var map = <String, dynamic>{
      'host': host,
      'port': port,
      'username': username,
      'password': password,
      'store': store,
      'keyspace': keyspace,
      'persistent': persistent,
    };
    print(map);
  }
}
