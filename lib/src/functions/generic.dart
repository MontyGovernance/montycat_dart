import 'package:hashlib/hashlib.dart' show xxh32code;
import 'dart:convert';
import '../tools.dart' show Pointer, Timestamp;
import 'dart:typed_data' show Uint8List;

/// Converts any key to a hashed string using XXH32.
String convertCustomKey(dynamic key) {
  final keyStr = key.toString();
  var digest = xxh32code(keyStr);
  return digest.toString();
}

/// Converts a list of keys to their hashed representations.
List<String> convertCustomKeys(List<dynamic> keys) {
  return keys.map((key) => convertCustomKey(key)).toList();
}

/// Converts a map of keys to hashed keys while keeping values intact.
Map<String, dynamic> convertCustomKeysValues(Map<String, dynamic> keysValues) {
  return {
    for (var entry in keysValues.entries)
      convertCustomKey(entry.key): entry.value,
  };
}

/// Recursively processes pointers and timestamps in a value map.
///
/// - Converts Pointer objects using `serialize()`.
/// - Converts Timestamp objects using `serialize()`.
/// - Processes nested 'pointers' entries to ensure keys are hashed or stringified.
Map<String, dynamic> modifyPointers(dynamic value) {
  try {
    final updatedValue = Map<String, dynamic>.from(value);
    for (var entry in updatedValue.entries) {
      if (entry.value is Pointer) {
        updatedValue[entry.key] = (entry.value as Pointer).serialize();
      }
      if (entry.value is Timestamp) {
        updatedValue[entry.key] = (entry.value as Timestamp).serialize();
      }
    }

    // Process nested pointers if present
    if (updatedValue.containsKey('pointers') &&
        updatedValue['pointers'] is Map<String, dynamic>) {
      final pointers = Map<String, dynamic>.from(updatedValue['pointers']);
      for (var entry in pointers.entries) {
        final v = entry.value as List<dynamic>;
        final keyspace = v[0] as String;
        final rawKey = v[1];
        String processedKey;

        if (rawKey is int ||
            (rawKey is String && RegExp(r'^\d+$').hasMatch(rawKey))) {
          processedKey = rawKey.toString();
        } else {
          processedKey = convertCustomKey(rawKey);
        }
        pointers[entry.key] = [keyspace, processedKey];
      }
      updatedValue['pointers'] = pointers;
    }
    return updatedValue;
  } catch (e) {
    throw Exception('Error processing pointers: $e');
  }
}

/// Converts query parameters to a serialized Uint8List to send over the network.
///
/// Handles:
/// - Keys, values, bulk operations
/// - Pointers and Timestamps
/// - Optional schema, limits, search criteria
Uint8List convertToBinaryQuery({
  required dynamic cls,
  required String command,
  Map<String, int> limitOutput = const {},
  String? key,
  Map<String, dynamic>? searchCriteria,
  dynamic value,
  int expireSec = 0,
  List<dynamic>? bulkValues,
  List<String>? bulkKeys,
  Map<String, dynamic>? bulkKeysValues,
  bool withPointers = false,
  String? schema,
  List<String>? volumes,
  bool latestVolume = false,
  bool keyIncluded = false,
  bool pointersMetadata = false,
  String? semanticQuery,
  double? minScore,
  bool? waitForIndex,
}) {
  searchCriteria = searchCriteria ?? {};
  value = value ?? {};
  bulkValues = bulkValues ?? [];
  bulkKeys = bulkKeys ?? [];
  bulkKeysValues = bulkKeysValues ?? {};

  if (value.isNotEmpty && value is Map<String, dynamic>) {
    value = modifyPointers(value);
  }

  if (bulkValues.isNotEmpty && bulkValues.first is Map<String, dynamic>) {
    final schemas = bulkValues.map((item) => item['schema'] as String?).toSet();
    if (schemas.length > 1) {
      throw Exception('Bulk values should fit only one schema');
    }
    schema = schemas.first;
    bulkValues =
        bulkValues.map((item) {
          if (item is! Map<String, dynamic>) {
            return item;
          }
          final filtered = Map<String, dynamic>.from(item)..remove('schema');
          return modifyPointers(filtered);
        }).toList();
  }

  if (bulkKeysValues.isNotEmpty) {
    bulkKeysValues = {
      for (var entry in bulkKeysValues.entries)
        if (entry.value is Map<String, dynamic>)
          entry.key: modifyPointers(entry.value),
    };
  }

  if (bulkKeys.isNotEmpty) {
    bulkKeys = bulkKeys.map((k) => k.toString()).toList();
  }

  if (value is Map<String, dynamic> && value.containsKey('schema')) {
    schema = value['schema'] as String?;
    value = Map<String, dynamic>.from(value)..remove('schema');
  }

  searchCriteria = handleTimestampsAndPointers(searchCriteria);

  final queryDict = {
    'schema': schema,
    'username': cls.username,
    'password': cls.password,
    'keyspace': cls.keyspace,
    'store': cls.store,
    'persistent': cls.persistent,
    'distributed': cls.distributed,
    'limit_output': limitOutput,
    'key': key?.toString(),
    'value': jsonEncode(value),
    'command': command,
    'expire': expireSec,
    'bulk_values': bulkValues.map((v) => jsonEncode(v)).toList(),
    'bulk_keys': bulkKeys,
    'bulk_keys_values': {
      for (var entry in bulkKeysValues.entries)
        entry.key: jsonEncode(entry.value),
    },
    // `semantic_search` sends the raw query text (the engine trims it as a
    // plain string); every other command sends a JSON-encoded filter map.
    'search_criteria': semanticQuery ?? jsonEncode(searchCriteria),
    'with_pointers': withPointers,
    'volumes': volumes ?? [],
    'latest_volume': latestVolume,
    'key_included': keyIncluded,
    'pointers_metadata': pointersMetadata,
  };

  // Only `semantic_search` honors min_score; omit it otherwise so the wire is
  // unchanged for existing commands (the server defaults the field to null).
  if (minScore != null) {
    queryDict['min_score'] = minScore;
  }

  // Per-request wait_for_index override for persistent writes; omit when null
  // so the server falls back to its DB-wide default (existing wire unchanged).
  if (waitForIndex != null) {
    queryDict['wait_for_index'] = waitForIndex;
  }

  return Uint8List.fromList(jsonEncode(queryDict).codeUnits);
}

/// Processes search criteria for Timestamps and Pointers.
///
/// - Converts Timestamp objects to serialized format
/// - Converts Pointer objects to serialized format
/// - Collects all pointers in a nested 'pointers' key
Map<String, dynamic> handleTimestampsAndPointers(
  Map<String, dynamic> searchCriteria,
) {
  final pointers = <String, dynamic>{};
  final result = <String, dynamic>{};

  for (var entry in searchCriteria.entries) {
    if (entry.value is Timestamp) {
      result[entry.key] = (entry.value as Timestamp).serialize();
    } else if (entry.value is Pointer) {
      pointers[entry.key] = (entry.value as Pointer).serialize();
    } else {
      result[entry.key] = entry.value;
    }
  }

  if (pointers.isNotEmpty) {
    result['pointers'] = pointers;
  }

  return result;
}
