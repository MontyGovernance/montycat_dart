import 'dart:convert';
import 'dart:typed_data';
import 'package:montycat_dart/source.dart';

import '../tools.dart' show Limit, Pointer, Timestamp;
import '../utils.dart' show sendData;
import '../functions/generic.dart'
    show convertCustomKey, convertToBinaryQuery, convertCustomKeysValues;

abstract class KV {

  String command = "";
  String? store;
  String host = "";
  String username = "";
  String password = "";
  int port = 0;
  Map<String, int> limitOutput = {};

  String get keyspace;
  set keyspace(String value);

  bool get distributed;

  set distributed(bool? value) {
    distributed = value ?? false;
  }

  bool get persistent;

  set persistent(bool value) {
    persistent = value;
  }

  Future<dynamic> runQuery(String host, int port, Uint8List query) async {
    return await sendData(host, port, query);
  }

  void connectEngine(Engine engine) {
    host = engine.host;
    port = engine.port;
    username = engine.username;
    password = engine.password;
    store = engine.store;
  }

  Future<dynamic> enforceSchema({required Map<String, Type> schema, required String schemaName}) async {

    if (schema.isEmpty) {
      throw Exception('No schema provided for enforcement');
    }

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

  // Static method equivalent to Python's @classmethod
  Future<dynamic> getValue({
    String? key,
    String? customKey,
    bool withPointers = false,
  }) async {
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
    );

    return await runQuery(host, port, query);
  }

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

  Future<dynamic> getBulk({
    List<String> bulkKeys = const [],
    List<String> bulkCustomKeys = const [],
    List<int> limit = const [],
    bool withPointers = false,
  }) async {
    if (bulkCustomKeys.isNotEmpty) {
      bulkKeys = [...bulkKeys, ...bulkCustomKeys];
    }

    if (bulkKeys.isEmpty) {
      throw ArgumentError("No keys provided for retrieval.");
    }

    command = "get_bulk";

    // check limit
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
    );

    return await runQuery(host, port, query);
  }

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

  Future<dynamic> lookupKeysWhere({
    List<int> limit = const [],
    String? schema,
    Map<String, dynamic> searchCriteria = const {},
  }) async {

    command = "lookup_keys";

    // check limit
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
      schema: schema
    );

    return await runQuery(host, port, query);
  }

  Future<dynamic> lookupValuesWhere({
    List<int> limit = const [],
    String? schema,
    Map<String, dynamic> searchCriteria = const {},
    bool withPointers = false,
  }) async {

    command = "lookup_values";

    // check limit
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
      schema: schema
    );

    return await runQuery(host, port, query);
  }

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

  Future<dynamic> getLen() async {
    command = "get_len";
    final query = convertToBinaryQuery(cls: this);
    return await runQuery(host, port, query);
  }

  Future<dynamic> listAllSchemasInKeyspace() async {
    command = "list_all_schemas_in_keyspace";
    final query = convertToBinaryQuery(cls: this);
    return await runQuery(host, port, query);
  }

  Future<dynamic> removeKeyspace() async {
    final queryMap ={
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