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
  Future<dynamic> runQuery(String host, int port, Uint8List query, {void Function(dynamic)? callback}) async {
    return await sendData(host, port, query, callback: callback);
  }

  /// Connects to the database engine using an [Engine] object.
  void connectEngine(Engine engine) {
    host = engine.host;
    port = engine.port;
    username = engine.username;
    password = engine.password;
    store = engine.store;
  }

  /// Enforces a schema for the current keyspace.
  /// 
  /// Converts Dart [Type]s to database-supported types and sends an
  /// "enforce-schema" query.
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
        'store', store,
        'keyspace', keyspace,
        'persistent', persistent ? 'y' : 'n',
        'schema_name', schemaName,
        'schema_content', jsonEncode(schemaTypes).toString(),
      ],
      'credentials': [username, password],
    };

    final queryBytes = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, queryBytes);
  }

  /// Removes a previously enforced schema from the keyspace.
  Future<dynamic> removeEnforcedSchema(String schema) async {
    final queryMap = {
      'raw': [
        'remove-enforced-schema',
        'store', store,
        'keyspace', keyspace,
        'persistent', persistent ? 'y' : 'n',
        'schema_name', schema,
      ],
      'credentials': [username, password],
    };

    final queryBytes = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, queryBytes);
  }

  /// Retrieves a single value by [key] or [customKey].
  /// Optionally fetches associated pointers.
  Future<dynamic> getValue({
    String? key,
    String? customKey,
    bool withPointers = false,
    bool keyIncluded = false,
    bool pointersMetadata = false,
  }) async {

    if (pointersMetadata && withPointers) {
      throw ArgumentError(
          "You select both pointers value and pointers metadata. Choose one.");
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

    return await runQuery(host, port, query);
  }

  /// Deletes a single key from the store.
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

    return await runQuery(host, port, query);
  }

  /// Deletes multiple keys in one query.
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

    return await runQuery(host, port, query);
  }

  /// Fetches multiple values at once.
  /// 
  /// Supports [limit] as `[start, stop]` and optional [withPointers].
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
          "You select both pointers value and pointers metadata. Choose one.");
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
      throw ArgumentError("Limit must be a list of two integers [start, stop].");
    }

    final query = convertToBinaryQuery(
      cls: this,
      bulkKeys: bulkKeys,
      withPointers: withPointers,
      keyIncluded: keyIncluded,
      pointersMetadata: pointersMetadata,
    );

    return await runQuery(host, port, query);
  }

  /// Updates multiple key-value pairs at once.
  /// 
  /// Converts [bulkCustomKeysValues] automatically before sending.
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
    return await runQuery(host, port, query);
  }

  /// Looks up keys matching search criteria.
  Future<dynamic> lookupKeysWhere({
    List<int> limit = const [],
    String? schema,
    Map<String, dynamic> searchCriteria = const {},
  }) async {
    command = "lookup_keys";

    if (limit.length == 2) {
      limitOutput = Limit(start: limit[0], stop: limit[1]).serialize();
    } else if (limit.isNotEmpty && limit.length != 2) {
      throw ArgumentError("Limit must be a list of two integers [start, stop].");
    }

    final query = convertToBinaryQuery(
      cls: this,
      searchCriteria: searchCriteria,
      schema: schema,
    );

    return await runQuery(host, port, query);
  }

  /// Looks up values matching search criteria.
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
          "You select both pointers value and pointers metadata. Choose one.");
    }

    command = "lookup_values";

    if (limit.length == 2) {
      limitOutput = Limit(start: limit[0], stop: limit[1]).serialize();
    } else if (limit.isNotEmpty && limit.length != 2) {
      throw ArgumentError("Limit must be a list of two integers [start, stop].");
    }

    final query = convertToBinaryQuery(
      cls: this,
      searchCriteria: searchCriteria,
      withPointers: withPointers,
      schema: schema,
      keyIncluded: keyIncluded,
      pointersMetadata: pointersMetadata,
    );

    return await runQuery(host, port, query);
  }

  /// Lists all keys that depend on the given key.
  Future<dynamic> listAllDependingKeys({
    String? key,
    String? customKey,
  }) async {
    if (customKey != null && customKey.isNotEmpty) {
      key = convertCustomKey(customKey);
    }

    if (key == null || key.isEmpty) {
      throw ArgumentError("No key provided");
    }

    command = "list_all_depending_keys";

    final query = convertToBinaryQuery(key: key);
    return await runQuery(host, port, query);
  }

  /// Returns the number of keys in the current keyspace.
  Future<dynamic> getLen() async {
    command = "get_len";
    final query = convertToBinaryQuery(cls: this);
    return await runQuery(host, port, query);
  }

  /// Returns all enforced schemas in the keyspace.
  Future<dynamic> listAllSchemasInKeyspace() async {
    command = "list_all_schemas_in_keyspace";
    final query = convertToBinaryQuery(cls: this);
    return await runQuery(host, port, query);
  }

  /// Removes the entire keyspace from the store.
  Future<dynamic> removeKeyspace() async {
    final queryMap = {
      'raw': [
        'remove-keyspace',
        'store', store,
        'keyspace', keyspace,
        'persistent', persistent ? 'y' : 'n',
      ],
      'credentials': [username, password],
    };

    final queryBytes = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, queryBytes);
  }

  /// Prints the current connection and keyspace properties.
  showProperties() async {
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
