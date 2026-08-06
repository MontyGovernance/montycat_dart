import 'package:montycat/src/tools.dart';
import '../classes/kv.dart';
import 'dart:convert';
import 'dart:typed_data';
import '../functions/generic.dart' show convertCustomKey, convertToBinaryQuery;

/// Represents a persistent keyspace within MontyCat.
///
/// A persistent keyspace stores data on disk (not just in memory).
/// Supports features like cache and compression settings, along with
/// CRUD operations on keys and values.
///
/// Example:
///
/// ```dart
/// final keyspace = KeyspacePersistent(keyspace: 'my_persistent_keyspace');
/// keyspace.cache = 1024; // Set cache size
/// keyspace.compression = true; // Enable compression
/// await keyspace.createKeyspace(); // Create the keyspace on the server
/// ```
///
class KeyspacePersistent extends KV {
  String _keyspace;
  bool _distributed = false;

  /// Create a new persistent keyspace instance.
  ///
  /// [keyspace] is the name of the keyspace.
  KeyspacePersistent({required String keyspace}) : _keyspace = keyspace;

  /// Keyspace name getter.
  @override
  String get keyspace => _keyspace;

  /// Keyspace name setter.
  @override
  set keyspace(String value) {
    _keyspace = value;
  }

  /// Whether the keyspace is distributed.
  @override
  bool get distributed => _distributed;

  @override
  set distributed(bool? value) {
    _distributed = value ?? false;
  }

  /// Persistent flag is always true for this class.
  @override
  bool get persistent => true;

  @override
  set persistent(bool? value) {
    super.persistent = value ?? true;
  }

  /// Insert a custom key into the keyspace.
  /// Throws an [ArgumentError] if [customKey] is empty.
  ///
  /// Example:
  ///
  /// ```dart
  /// await keyspace.insertCustomKey(customKey: 'my_custom_key');
  /// ```
  ///
  Future<dynamic> insertCustomKey({
    required String customKey,
    bool? waitForIndex,
  }) async {
    if (customKey.isEmpty) {
      throw ArgumentError("No custom key provided for insertion.");
    }

    final customKeyConverted = convertCustomKey(customKey);
    final query = convertToBinaryQuery(
      cls: this,
      command: "insert_custom_key",
      key: customKeyConverted,
      waitForIndex: waitForIndex,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Insert a custom key-value pair into the keyspace.
  /// [vector] optionally supplies a precomputed vector for the inserted value.
  /// Throws an [ArgumentError] if [customKey] or [value] is empty.
  ///
  /// Example:
  ///
  /// ```dart
  /// await keyspace.insertCustomKeyValue(customKey: 'my_custom_key', value: 'my_value');
  /// ```
  ///
  Future<dynamic> insertCustomKeyValue({
    required String customKey,
    required dynamic value,
    List<double>? vector,
    bool? waitForIndex,
  }) async {
    if (value.isEmpty) {
      throw ArgumentError("No value provided for insertion.");
    }
    if (customKey.isEmpty) {
      throw ArgumentError("No custom key provided for insertion.");
    }

    final customKeyConverted = convertCustomKey(customKey);
    final query = convertToBinaryQuery(
      cls: this,
      command: "insert_custom_key_value",
      key: customKeyConverted,
      value: value,
      semanticVector: vector,
      waitForIndex: waitForIndex,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Insert a value (auto-generated key will be used).
  /// [vector] optionally supplies a precomputed vector for the inserted value.
  /// Throws an [ArgumentError] if [value] is empty.
  ///
  /// Example:
  ///
  /// ```dart
  /// await keyspace.insertValue(value: 'my_value');
  /// ```
  ///
  Future<dynamic> insertValue({
    required dynamic value,
    List<double>? vector,
    bool? waitForIndex,
  }) async {
    if (value.isEmpty) {
      throw ArgumentError("No value provided for insertion.");
    }

    final query = convertToBinaryQuery(
      cls: this,
      command: "insert_value",
      value: value,
      semanticVector: vector,
      waitForIndex: waitForIndex,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Update a value in the keyspace, using a key or custom key and filters.
  /// [vector] optionally replaces the stored semantic vector.
  /// Throws an [ArgumentError] if no filters or key are provided.
  /// If [customKey] is provided, it will be used instead of [key].
  /// The [filters] map contains the fields to update and their new values.
  /// For example: filters = {'field1': 'newValue', 'field2': 42}
  ///
  /// Example:
  ///
  /// ```dart
  /// await keyspace.updateValue(
  /// key: 'my_key',
  /// updates: {'field1': 'newValue', 'field2': 42},
  /// ```
  ///
  Future<dynamic> updateValue({
    String? key,
    String? customKey,
    Map<String, dynamic>? updates,
    List<double>? vector,
    bool? waitForIndex,
  }) async {
    if (customKey != null && customKey.isNotEmpty) {
      key = convertCustomKey(customKey);
    }

    if (updates == null || updates.isEmpty) {
      throw ArgumentError("No updates provided");
    }
    if (key == null || key.isEmpty) {
      throw ArgumentError("No key provided");
    }

    final query = convertToBinaryQuery(
      cls: this,
      command: "update_value",
      key: key,
      value: updates,
      semanticVector: vector,
      waitForIndex: waitForIndex,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Get all keys in the keyspace with optional [limit].
  /// If [latestVolume] is true, only the latest volume is queried.
  /// If [volumes] is provided, only those volumes are queried.
  /// Throws an [ArgumentError] if neither [latestVolume] nor [volumes] are provided or limit is not set.
  /// Throws an [ArgumentError] if [limit] is not a list of two integers.
  ///
  /// Example:
  ///
  /// ```dart
  /// final keys = await keyspace.getKeys(
  ///   limit: [0, 10],
  ///   latestVolume: true,
  /// );
  /// ```
  ///
  Future<dynamic> getKeys({
    List<int> limit = const [],
    List<String> volumes = const [],
    bool latestVolume = false,
  }) async {
    if (!latestVolume &&
        volumes.isEmpty &&
        limit.isEmpty &&
        (limit.isEmpty ||
            limit.length != 2 ||
            (limit[0] == 0 && limit[1] == 0))) {
      throw ArgumentError(
        "Please provide volumes/latest volume or valid limit range.",
      );
    }

    Map<String, int> limitOutput = {};
    if (limit.length == 2) {
      limitOutput = Limit(start: limit[0], stop: limit[1]).serialize();
    } else if (limit.isNotEmpty && limit.length != 2) {
      throw ArgumentError(
        "Limit must be a list of two integers [start, stop].",
      );
    }

    final query = convertToBinaryQuery(
      cls: this,
      command: "get_keys",
      limitOutput: limitOutput,
      volumes: volumes,
      latestVolume: latestVolume,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Insert multiple values at once.
  /// [vectors] optionally supplies precomputed vectors paired by position.
  /// Throws an [ArgumentError] if [bulkValues] is empty.
  ///
  /// Example:
  ///
  /// ```dart
  /// final result = await keyspace.insertBulk(
  ///   bulkValues: ['value1', 'value2', 'value3'],
  /// );
  /// ```
  ///
  Future<dynamic> insertBulk({
    required List bulkValues,
    List<List<double>> vectors = const [],
    bool? waitForIndex,
  }) async {
    if (bulkValues.isEmpty) {
      throw ArgumentError("No values provided for bulk insertion.");
    }

    final query = convertToBinaryQuery(
      cls: this,
      command: "insert_bulk",
      bulkValues: bulkValues,
      semanticVectorList: vectors,
      waitForIndex: waitForIndex,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Creates a new keyspace with the current configuration.
  /// Throws an [ArgumentError] if [store] or [keyspace] is empty.
  ///
  /// Example:
  ///
  /// ```dart
  /// await keyspace.createKeyspace();
  /// ```
  ///
  Future<dynamic> createKeyspace({int? cache, bool? compression}) async {
    if (store == null || store!.isEmpty) {
      throw ArgumentError("Store name cannot be empty.");
    }

    var cacheValue = cache != null ? cache.toString() : "0";

    final queryMap = {
      "raw": [
        "create-keyspace",
        "store",
        store,
        "keyspace",
        keyspace,
        "persistent",
        persistent ? "y" : "n",
        "distributed",
        distributed ? "y" : "n",
        "cache",
        cacheValue,
        "compression",
        compression == true ? "y" : "n",
      ],
      "credentials": [username, password],
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Update cache and compression settings for this keyspace.
  /// Throws an [ArgumentError] if [cache] or [compression] is empty.
  ///
  /// Example:
  ///
  /// ```dart
  /// await keyspace.updateCacheAndCompression();
  /// ```
  ///
  Future<dynamic> updateCacheAndCompression({
    int? cache,
    bool? compression,
  }) async {
    final queryMap = {
      "raw": [
        "update-cache-compression",
        "store",
        store,
        "keyspace",
        keyspace,
        "cache",
        cache != null ? cache.toString() : "0",
        "compression",
        compression == true ? "y" : "n",
      ],
      "credentials": [username, password],
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query, useTls: useTls);
  }
}
