import 'package:montycat_dart/src/tools.dart';

import '../classes/kv.dart';
import 'dart:convert';
import 'dart:typed_data';
import '../functions/generic.dart'
    show convertCustomKey, convertToBinaryQuery;

class KeyspacePersistent extends KV {

  String _keyspace;
  bool _distributed = false;

  KeyspacePersistent({required String keyspace}) : _keyspace = keyspace;

  int? cache;
  bool? compression;

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
  bool get persistent => true;

  @override
  set persistent(bool? value) {
    super.persistent = value ?? true;
  }

  Future<dynamic> insertCustomKey({required String customKey}) async {
    if (customKey.isEmpty) {
      throw ArgumentError("No custom key provided for insertion.");
    }

    final customKeyConverted = convertCustomKey(customKey);
    command = "insert_custom_key";

    final query = convertToBinaryQuery(cls: this, key: customKeyConverted);
    return await runQuery(host, port, query);
  }

  Future<dynamic> insertCustomKeyValue({required String customKey, required dynamic value}) async {
    if (value.isEmpty) {
      throw ArgumentError("No value provided for insertion.");
    }
    if (customKey.isEmpty) {
      throw ArgumentError("No custom key provided for insertion.");
    }

    final customKeyConverted = convertCustomKey(customKey);
    command = "insert_custom_key_value";

    final query = convertToBinaryQuery(cls: this, key: customKeyConverted, value: value);
    return await runQuery(host, port, query);
  }

  Future<dynamic> insertValue({required dynamic value}) async {
    if (value.isEmpty) {
      throw ArgumentError("No value provided for insertion.");
    }

    command = "insert_value";

    final query = convertToBinaryQuery(cls: this, value: value);
    return await runQuery(host, port, query);
  }

  Future<dynamic> updateValue({String? key, String? customKey, Map<String, dynamic>? filters}) async {

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

    final query = convertToBinaryQuery(cls: this, key: key, value: filters);
    return await runQuery(host, port, query);
  }

  Future<dynamic> getKeys({ List<int> limit = const [] }) async {
    command = "get_keys";

    // check limit
    if (limit.length == 2) {
      limitOutput = Limit(start: limit[0], stop: limit[1]).serialize();
    } else if (limit.isNotEmpty && limit.length != 2) {
      throw ArgumentError(
        "Limit must be a list of two integers [start, stop].",
      );
    }

    final query = convertToBinaryQuery(cls: this);
    return await runQuery(host, port, query);
  }

  Future<dynamic> insertBulk({required List bulkValues}) async {

    if (bulkValues.isEmpty) {
      throw ArgumentError("No values provided for bulk insertion.");
    }

    command = "insert_bulk";
    final query = convertToBinaryQuery(cls: this, bulkValues: bulkValues);
    return await runQuery(host, port, query);
  }

  Future<dynamic> createKeyspace() async {
    final queryMap = {
      "raw": [
        "create-keyspace",
        "store", store,
        "keyspace", keyspace,
        "persistent", persistent ? "y" : "n",
        "distributed", distributed ? "y" : "n",
        "cache", cache ?? "0",
        "compression", compression == true ? "y" : "n"
      ],
      "credentials": [username, password]
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query);
  }

    Future<dynamic> updateCacheAndCompression() async {

    if (!persistent) {
      throw Exception("Cache and compression settings can only be updated for persistent keyspaces.");
    }

    final queryMap = {
      "raw": [
        "update-cache-compression",
        "store", store,
        "keyspace", keyspace,
        "cache", cache ?? "0",
        "compression", compression == true ? "y" : "n"
      ],
      "credentials": [username, password]
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query);
  }

}
