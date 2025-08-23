import 'package:xxhash/xxhash.dart';
import 'dart:convert';
import '../tools.dart' show Pointer, Timestamp;
import 'dart:typed_data' show Uint8List;

String convertCustomKey(dynamic key) {
  final keyStr = key.toString();
  var bytes = utf8.encode(keyStr); // data being hashed
  var digest = xxh32.convert(bytes);
  return digest.toString();
}

List<String> convertCustomKeys(List<dynamic> keys) {
  return keys.map((key) => convertCustomKey(key)).toList();
}

Map<String, dynamic> handlePointersForUpdate(Map<String, dynamic> value) {
  final updatedValue = Map<String, dynamic>.from(value);
  for (var entry in updatedValue.entries) {
    if (entry.value is Pointer) {
      updatedValue[entry.key] = (entry.value as Pointer).serialize();
    }
  }
  return updatedValue;

}

Map<String, dynamic> convertCustomKeysValues(Map<String, dynamic> keysValues) {
  return {
    for (var entry in keysValues.entries)
      convertCustomKey(entry.key): entry.value,
  };
}

Map<String, dynamic> modifyPointers(Map<String, dynamic> value) {
  try {
    final updatedValue = Map<String, dynamic>.from(value);
    for (var entry in updatedValue.entries) {
      if (entry.value is Pointer) {
        updatedValue[entry.key] = (entry.value as Pointer).serialize();
      }
    }

    if (updatedValue.containsKey('pointers') &&
        updatedValue['pointers'] is Map<String, dynamic>) {
      final pointers = Map<String, dynamic>.from(updatedValue['pointers']);
      for (var entry in pointers.entries) {
        final v = entry.value as List<dynamic>;
        final keyspace = v[0] as String;
        final rawKey = v[1];
        String processedKey;
        if (rawKey is int || (rawKey is String && RegExp(r'^\d+$').hasMatch(rawKey))) {
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

Uint8List convertToBinaryQuery({
  dynamic cls,
  String? key,
  Map<String, dynamic>? searchCriteria,
  dynamic value,
  int expireSec = 0,
  List<Map<String, dynamic>>? bulkValues,
  List<String>? bulkKeys,
  Map<String, dynamic>? bulkKeysValues,
  bool withPointers = false,
}) {

  searchCriteria = searchCriteria ?? {};
  value = value ?? {};
  bulkValues = bulkValues ?? [];
  bulkKeys = bulkKeys ?? [];
  bulkKeysValues = bulkKeysValues ?? {};

  if (value.isNotEmpty) {
    value = modifyPointers(value);
  }

  String? schema;
  if (bulkValues.isNotEmpty) {
    final schemas = bulkValues.map((item) => item['schema'] as String?).toSet();
    if (schemas.length > 1) {
      throw Exception('Bulk values should fit only one schema');
    }
    schema = schemas.first;
    bulkValues = bulkValues.map((item) {
      final filtered = Map<String, dynamic>.from(item)..remove('schema');
      return modifyPointers(filtered);
    }).toList();
  }

  if (bulkKeysValues.isNotEmpty) {
    bulkKeysValues = {
      for (var entry in bulkKeysValues.entries)
        entry.key: modifyPointers(entry.value),
    };
  }

  if (bulkKeys.isNotEmpty) {
    bulkKeys = bulkKeys.map((k) => k.toString()).toList();
  }

  if (value.containsKey('schema')) {
    schema = value['schema'] as String?;
    value = Map<String, dynamic>.from(value)..remove('schema');
  }

  searchCriteria = handleTimestampsAndPointers(searchCriteria);
  value = handleTimestampsAndPointers(value);

  final queryDict = {
    'schema': schema,
    'username': cls.username, // Placeholder; replace with cls.username if available
    'password': cls.password, // Placeholder
    'keyspace': cls.keyspace, // Placeholder
    'store': cls.store, // Placeholder
    'persistent': cls.persistent, // Placeholder
    'distributed': cls.distributed, // Placeholder
    'limit_output': cls.limitOutput, // Placeholder
    'key': key?.toString(),
    'value': jsonEncode(value),
    'command': cls.command, // Placeholder
    'expire': expireSec,
    'bulk_values': bulkValues.map((v) => jsonEncode(v)).toList(),
    'bulk_keys': bulkKeys,
    'bulk_keys_values': {
      for (var entry in bulkKeysValues.entries)
        entry.key: jsonEncode(entry.value),
    },
    'search_criteria': jsonEncode(searchCriteria),
    'with_pointers': withPointers,
  };

  return Uint8List.fromList(jsonEncode(queryDict).codeUnits);
}

Map<String, dynamic> handleTimestampsAndPointers(Map<String, dynamic> searchCriteria) {
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
