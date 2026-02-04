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

  /// Cache size for this keyspace (null = not set).
  int? cache;

  /// Compression flag (true = enabled, false = disabled, null = not set).
  bool? compression;

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

  /// Subscribe to changes on a specific key or custom key.
  /// If [callback] is provided, it will be called on updates.
  /// If no key is provided, subscribes to all changes in the keyspace.
  /// If [customKey] is provided, it will be used instead of [key].
  /// Note: Each subscription increments the port by 1.
  /// Make sure to manage ports accordingly.
  ///
  /// Example:
  ///
  /// ```dart
  /// await keyspace.subscribe(
  /// key: 'my_key',
  /// callback: (data) {
  ///  print('Received update: $data');
  /// });
  /// ```
  ///
  Future<dynamic> subscribe({
    String? key,
    String? customKey,
    void Function(dynamic)? callback,
  }) async {
    if (customKey != null && customKey.isNotEmpty) {
      key = convertCustomKey(customKey);
    }

    var usePort = port + 1;

    var queryObj = {
      "subscribe": true,
      "store": store,
      "keyspace": keyspace,
      "username": username,
      "password": password,
      "key": key,
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryObj)));

    return await runQuery(
      host,
      usePort,
      query,
      callback: callback,
      useTls: useTls,
    );
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
  Future<dynamic> insertCustomKey({required String customKey}) async {
    if (customKey.isEmpty) {
      throw ArgumentError("No custom key provided for insertion.");
    }

    final customKeyConverted = convertCustomKey(customKey);
    command = "insert_custom_key";

    final query = convertToBinaryQuery(cls: this, key: customKeyConverted);
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Insert a custom key-value pair into the keyspace.
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
  }) async {
    if (value.isEmpty) {
      throw ArgumentError("No value provided for insertion.");
    }
    if (customKey.isEmpty) {
      throw ArgumentError("No custom key provided for insertion.");
    }

    final customKeyConverted = convertCustomKey(customKey);
    command = "insert_custom_key_value";

    final query = convertToBinaryQuery(
      cls: this,
      key: customKeyConverted,
      value: value,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Insert a value (auto-generated key will be used).
  /// Throws an [ArgumentError] if [value] is empty.
  ///
  /// Example:
  ///
  /// ```dart
  /// await keyspace.insertValue(value: 'my_value');
  /// ```
  ///
  Future<dynamic> insertValue({required dynamic value}) async {
    if (value.isEmpty) {
      throw ArgumentError("No value provided for insertion.");
    }

    command = "insert_value";

    final query = convertToBinaryQuery(cls: this, value: value);
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Update a value in the keyspace, using a key or custom key and filters.
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

    command = "update_value";

    final query = convertToBinaryQuery(cls: this, key: key, value: updates);
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Get all keys in the keyspace with optional [limit].
  /// If [latestVolume] is true, only the latest volume is queried.
  /// If [volumes] is provided, only those volumes are queried.
  /// Throws an [ArgumentError] if both [latestVolume] and [volumes] are set.
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
    if (latestVolume && volumes.isNotEmpty) {
      throw ArgumentError(
        "Select either latest volume or volumes list, not both.",
      );
    }

    command = "get_keys";

    // Check limit
    if (limit.length == 2) {
      limitOutput = Limit(start: limit[0], stop: limit[1]).serialize();
    } else if (limit.isNotEmpty && limit.length != 2) {
      throw ArgumentError(
        "Limit must be a list of two integers [start, stop].",
      );
    }

    final query = convertToBinaryQuery(
      cls: this,
      volumes: volumes,
      latestVolume: latestVolume,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Insert multiple values at once.
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
  Future<dynamic> insertBulk({required List bulkValues}) async {
    if (bulkValues.isEmpty) {
      throw ArgumentError("No values provided for bulk insertion.");
    }

    command = "insert_bulk";
    final query = convertToBinaryQuery(cls: this, bulkValues: bulkValues);
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
  Future<dynamic> createKeyspace() async {
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
        cache ?? "0",
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
  Future<dynamic> updateCacheAndCompression() async {
    if (!persistent) {
      throw Exception(
        "Cache and compression settings can only be updated for persistent keyspaces.",
      );
    }

    final queryMap = {
      "raw": [
        "update-cache-compression",
        "store",
        store,
        "keyspace",
        keyspace,
        "cache",
        cache ?? "0",
        "compression",
        compression == true ? "y" : "n",
      ],
      "credentials": [username, password],
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query, useTls: useTls);
  }
}
