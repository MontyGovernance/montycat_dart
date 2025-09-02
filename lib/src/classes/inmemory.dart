import '../classes/kv.dart';
import 'dart:convert';
import 'dart:typed_data';
import '../functions/generic.dart'
    show convertCustomKey, convertToBinaryQuery;

class KeyspaceInMemory extends KV {

  String _keyspace;
  bool _distributed = false;

  KeyspaceInMemory({required String keyspace}) : _keyspace = keyspace;

  @override
  String get keyspace => _keyspace;

  @override
  set keyspace(String value) {
    _keyspace = value;
  }

  @override
  bool get distributed => _distributed;

  @override
  set distributed(bool? value) {
    _distributed = value ?? false;
  }

  @override
  bool get persistent => false;

  Future<dynamic> doSnapshotsForKeyspace() async {

    if (persistent) {
      throw Exception("Snapshots can only be taken for in-memory keyspaces");
    }

    final queryMap = {
      "raw": [
        "do-snapshots-for-keyspace",
        "store", store,
        "keyspace", keyspace,
      ],
      "credentials": [username, password]
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query);
  }

  Future<dynamic> cleanSnapshotsForKeyspace() async {

    if (persistent) {
      throw Exception("Snapshots can only be taken for in-memory keyspaces");
    }

    final queryMap = {
      "raw": [
        "clean-snapshots-for-keyspace",
        "store", store,
        "keyspace", keyspace,
      ],
      "credentials": [username, password]
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query);
  }

  Future<dynamic> stopSnapshotsForKeyspace() async {

    if (persistent) {
      throw Exception("Snapshots can only be taken for in-memory keyspaces");
    }

    final queryMap = {
      "raw": [
        "stop-snapshots-for-keyspace",
        "store", store,
        "keyspace", keyspace,
      ],
      "credentials": [username, password]
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query);
  }

  Future<dynamic> createKeyspace() async {
    final queryMap = {
      "raw": [
        "create-keyspace",
        "store", store,
        "keyspace", keyspace,
        "persistent", persistent ? "y" : "n",
        "distributed", distributed ? "y" : "n"
      ],
      "credentials": [username, password]
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query);
  }

  Future<dynamic> getKeys() async {
    command = "get_keys";
    final query = convertToBinaryQuery(cls: this);
    return await runQuery(host, port, query);
  }

  Future<dynamic> insertBulk({required List bulkValues, int expireSec = 0}) async {

    if (bulkValues.isEmpty) {
      throw ArgumentError("No values provided for bulk insertion.");
    }

    command = "insert_bulk";
    final query = convertToBinaryQuery(cls: this, bulkValues: bulkValues, expireSec: expireSec);
    return await runQuery(host, port, query);
  }

  Future<dynamic> updateValue({String? key, String? customKey, int expireSec = 0, Map<String, dynamic>? filters}) async {

    if (customKey != null && customKey.isNotEmpty) {
      key = convertCustomKey(customKey);
    }

    if (filters == null || filters.isEmpty) {
      throw ArgumentError("No filters provided");
    }
    if (key == null || key.isEmpty) {
      throw ArgumentError("No key provided");
    }

    command = "update_value";

    final query = convertToBinaryQuery(cls: this, key: key, value: filters, expireSec: expireSec);
    return await runQuery(host, port, query);
  }

  Future<dynamic> insertValue({required dynamic value, int expireSec = 0}) async {
    if (value.isEmpty) {
      throw ArgumentError("No value provided for insertion.");
    }

    command = "insert_value";

    final query = convertToBinaryQuery(cls: this, value: value, expireSec: expireSec);

    return await runQuery(host, port, query);
  }

  Future<dynamic> insertCustomKeyValue({required String customKey, required dynamic value, int expireSec = 0}) async {
    if (value.isEmpty) {
      throw ArgumentError("No value provided for insertion.");
    }
    if (customKey.isEmpty) {
      throw ArgumentError("No custom key provided for insertion.");
    }

    final customKeyConverted = convertCustomKey(customKey);
    command = "insert_custom_key_value";

    final query = convertToBinaryQuery(cls: this, key: customKeyConverted, value: value, expireSec: expireSec);
    return await runQuery(host, port, query);
  }

  Future<dynamic> insertCustomKey({required String customKey, int expireSec = 0}) async {
    if (customKey.isEmpty) {
      throw ArgumentError("No custom key provided for insertion.");
    }

    final customKeyConverted = convertCustomKey(customKey);
    command = "insert_custom_key";

    final query = convertToBinaryQuery(cls: this, key: customKeyConverted, expireSec: expireSec);
    return await runQuery(host, port, query);
  }

}
