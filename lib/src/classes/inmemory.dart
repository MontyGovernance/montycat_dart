import '../classes/kv.dart';
import 'dart:convert';
import 'dart:typed_data';
import '../functions/generic.dart' show convertCustomKey, convertToBinaryQuery;

/// In-memory implementation of a keyspace.
/// Provides high-performance, non-persistent storage with optional
/// distributed configuration. This class supports snapshots, bulk
/// operations, and fine-grained value manipulation.
class KeyspaceInMemory extends KV {
  String _keyspace;
  bool _distributed = false;

  /// Creates a new in-memory keyspace with the given [keyspace] name.
  KeyspaceInMemory({required String keyspace}) : _keyspace = keyspace;

  /// The name of the keyspace.
  @override
  String get keyspace => _keyspace;

  /// Updates the name of the keyspace.
  @override
  set keyspace(String value) {
    _keyspace = value;
  }

  /// Whether this keyspace is distributed across nodes.
  @override
  bool get distributed => _distributed;

  /// Enables or disables distribution for this keyspace.
  @override
  set distributed(bool? value) {
    _distributed = value ?? false;
  }

  /// Always `false` for this implementation, as in-memory
  /// keyspaces are non-persistent by definition.
  @override
  bool get persistent => false;

  /// Initiates snapshots for this keyspace.
  /// Snapshots are only supported for in-memory keyspaces. Throws
  /// an [Exception] if invoked on a persistent keyspace.
  Future<dynamic> doSnapshotsForKeyspace() async {
    if (persistent) {
      throw Exception("Snapshots can only be taken for in-memory keyspaces");
    }

    final queryMap = {
      "raw": [
        "do-snapshots-for-keyspace",
        "store",
        store,
        "keyspace",
        keyspace,
      ],
      "credentials": [username, password],
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Cleans all snapshots associated with this keyspace.
  ///
  /// Snapshots are only supported for in-memory keyspaces. Throws
  /// an [Exception] if invoked on a persistent keyspace.
  Future<dynamic> cleanSnapshotsForKeyspace() async {
    if (persistent) {
      throw Exception("Snapshots can only be taken for in-memory keyspaces");
    }

    final queryMap = {
      "raw": [
        "clean-snapshots-for-keyspace",
        "store",
        store,
        "keyspace",
        keyspace,
      ],
      "credentials": [username, password],
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Stops ongoing snapshots for this keyspace.
  ///
  /// Snapshots are only supported for in-memory keyspaces. Throws
  /// an [Exception] if invoked on a persistent keyspace.
  Future<dynamic> stopSnapshotsForKeyspace() async {
    if (persistent) {
      throw Exception("Snapshots can only be taken for in-memory keyspaces");
    }

    final queryMap = {
      "raw": [
        "stop-snapshots-for-keyspace",
        "store",
        store,
        "keyspace",
        keyspace,
      ],
      "credentials": [username, password],
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Creates a new keyspace with the current configuration.
  /// Throws an [ArgumentError] if [store] or [keyspace] is empty.
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
      ],
      "credentials": [username, password],
    };

    final query = Uint8List.fromList(utf8.encode(jsonEncode(queryMap)));
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Retrieves all keys stored in this keyspace.
  /// If [latestVolume] is true, only the latest volume is queried.
  /// If [volumes] is provided, only those volumes are queried.
  /// Throws an [ArgumentError] if both [latestVolume] and [volumes]
  Future<dynamic> getKeys({
    List<String> volumes = const [],
    bool latestVolume = false,
  }) async {
    if (latestVolume && volumes.isNotEmpty) {
      throw ArgumentError(
        "Select either latest volume or volumes list, not both.",
      );
    }

    command = "get_keys";
    final query = convertToBinaryQuery(
      cls: this,
      volumes: volumes,
      latestVolume: latestVolume,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Inserts multiple values into the keyspace in a single bulk operation.
  /// - [bulkValues]: List of values to insert.
  /// - [expireSec]: Optional expiration time in seconds.
  /// Throws [ArgumentError] if [bulkValues] is empty.
  Future<dynamic> insertBulk({
    required List bulkValues,
    int expireSec = 0,
  }) async {
    if (bulkValues.isEmpty) {
      throw ArgumentError("No values provided for bulk insertion.");
    }

    command = "insert_bulk";
    final query = convertToBinaryQuery(
      cls: this,
      bulkValues: bulkValues,
      expireSec: expireSec,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Updates a value in the keyspace for the given [key] or [customKey].
  /// - [updates] must contain the fields to update.
  /// - [expireSec] optionally sets a new expiration time.
  /// Throws [ArgumentError] if [updates] is empty or if no valid key is provided.
  /// If [customKey] is provided, it will be used instead of [key].
  /// The [updates] map contains the fields to update and their new values.
  /// Example: updates = {'field1': 'newValue', 'field2': 42}
  Future<dynamic> updateValue({
    String? key,
    String? customKey,
    int expireSec = 0,
    Map<String, dynamic>? updates,
  }) async {
    if (customKey != null && customKey.isNotEmpty) {
      key = convertCustomKey(customKey);
    }

    if (updates == null || updates.isEmpty) {
      throw ArgumentError("No filters provided");
    }
    if (key == null || key.isEmpty) {
      throw ArgumentError("No key provided");
    }

    command = "update_value";
    final query = convertToBinaryQuery(
      cls: this,
      key: key,
      value: updates,
      expireSec: expireSec,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Inserts a single [value] into the keyspace.
  /// - [expireSec]: Optional expiration time in seconds.
  /// Throws [ArgumentError] if [value] is empty.
  Future<dynamic> insertValue({
    required dynamic value,
    int expireSec = 0,
  }) async {
    if (value.isEmpty) {
      throw ArgumentError("No value provided for insertion.");
    }

    command = "insert_value";
    final query = convertToBinaryQuery(
      cls: this,
      value: value,
      expireSec: expireSec,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Inserts a value under a specified [customKey].
  /// - [customKey] must not be empty.
  /// - [expireSec]: Optional expiration time in seconds.
  /// Throws [ArgumentError] if [value] or [customKey] is empty.
  Future<dynamic> insertCustomKeyValue({
    required String customKey,
    required dynamic value,
    int expireSec = 0,
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
      expireSec: expireSec,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }

  /// Inserts an empty entry under a specified [customKey].
  /// - [customKey] must not be empty.
  /// - [expireSec]: Optional expiration time in seconds.
  /// Throws [ArgumentError] if [customKey] is empty.
  Future<dynamic> insertCustomKey({
    required String customKey,
    int expireSec = 0,
  }) async {
    if (customKey.isEmpty) {
      throw ArgumentError("No custom key provided for insertion.");
    }

    final customKeyConverted = convertCustomKey(customKey);
    command = "insert_custom_key";
    final query = convertToBinaryQuery(
      cls: this,
      key: customKeyConverted,
      expireSec: expireSec,
    );
    return await runQuery(host, port, query, useTls: useTls);
  }
}
