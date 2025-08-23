import 'dart:convert';
import 'dart:typed_data';
import 'package:montycat_dart/source.dart';

import '../tools.dart' show Limit, Permission, Pointer, Timestamp;
import '../engine.dart' show Engine;
import 'package:xxhash/xxhash.dart';
import '../utils.dart' show sendData;
import '../functions/generic.dart' show convertCustomKey, convertToBinaryQuery, handlePointersForUpdate;

class KV {

  String command = "";
  String? store;
  String host = "";
  String username = "";
  String password = "";
  int port = 0;
  String keyspace = "";
  bool persistent = false;
  Map<String, int> limitOutput = {};

  Future<Uint8List> _runQuery(String host, int port, Uint8List query) async {
    return await sendData(host, port, query);
  }

  void connectEngine(Engine engine) {
    host = engine.host;
    port = engine.port;
    username = engine.username;
    password = engine.password;
    store = engine.store;
  }

  Future<Uint8List> enforceSchema(Map<String, Type> schema) async {
    if (schema.isEmpty) {
      throw Exception('No schema provided for enforcement');
    }

    String parseType(Type fieldType) {
      if (fieldType == String) {
        return 'String';
      } else if (fieldType == int) {
        return 'Number';
      } else if (fieldType == double) {
        return 'Float';
      } else if (fieldType == bool) {
        return 'Boolean';
      } else if (fieldType == List) {
        return 'Array';
      } else if (fieldType == Map) {
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

    final query = jsonEncode({
      'raw': [
        'enforce-schema',
        'store',
        store,
        'keyspace',
        keyspace,
        'persistent',
        persistent ? 'y' : 'n',
        'schema_name',
        schema.toString(),
        'schema_content',
        jsonEncode(schemaTypes),
      ],
      'credentials': [username, password],
    });

    final queryBytes = Uint8List.fromList(jsonEncode(query).codeUnits);
    return await _runQuery(host, port, queryBytes);
  }

  Future<Uint8List> removeEnforcedSchema(String schema) async {

    final query = jsonEncode({
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
    });

    final queryBytes = Uint8List.fromList(jsonEncode(query).codeUnits);
    return await _runQuery(host, port, queryBytes);
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

    return await _runQuery(host, port, query);

  }

    Future<dynamic> deleteKey({String? key, String? customKey}) async {

    if (customKey != null && customKey.isNotEmpty) {
      key = customKey;
    }

    if (key == null || key.isEmpty) {
      throw ArgumentError("No key provided");
    }

    command = "delete_key";

    Uint8List query = convertToBinaryQuery(
      cls: this,
      key: key,
    );

    return await _runQuery(host, port, query);
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

    final query = convertToBinaryQuery(
      cls: this,
      bulkKeys: bulkKeys,
    );

    return await _runQuery(host, port, query);
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
    throw ArgumentError("Limit must be a list of two integers [start, stop].");
  }

  final query = convertToBinaryQuery(
    cls: this,
    bulkKeys: bulkKeys,
    withPointers: withPointers,
  );

  return await _runQuery(host, port, query);
}


}
