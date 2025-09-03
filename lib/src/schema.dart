import 'tools.dart';

abstract class Schema {
  final Map<String, dynamic> _fields = {};
  late final String schema;

  Schema(Map<String, dynamic> kwargs) {
    final hints = metadata();
    schema = runtimeType.toString();
    // Assign provided values
    kwargs.forEach((key, value) {
      _fields[key] = value;
    });

    // Ensure all declared fields exist
    for (final entry in hints.entries) {
      if (!_fields.containsKey(entry.key)) {
        _fields[entry.key] = null;
      }
    }

    checkMissingFields(hints);
    checkExtraFields(hints);
    validateTypes(hints);

  }

  /// Each subclass must override to declare its expected field types.
  Map<String, dynamic> metadata();

  Map<String, dynamic> serialize() => Map<String, dynamic>.from(_fields);

  void checkMissingFields(Map<String, dynamic> hints) {
    for (final entry in hints.entries) {
      final key = entry.key;
      if (_fields[key] == null) {
        throw ArgumentError("Missing required field: '$key'");
      }
    }
  }

  void checkExtraFields(Map<String, dynamic> hints) {
    final defined = hints.keys.toSet();
    for (final key in _fields.keys) {
      if (!defined.contains(key)) {
        throw ArgumentError("Unexpected field '$key' found in the instance.");
      }
    }
  }

  void validateTypes(Map<String, dynamic> hints) {
    final Map<String, dynamic> pointers = {};
    final Map<String, dynamic> timestamps = {};

    hints.forEach((attribute, expectedType) {
      final actualValue = _fields[attribute];

      // Handle Pointer
      if (expectedType == Pointer) {
        if (actualValue is! Pointer && actualValue != null) {
          throw ArgumentError(
              "Attribute '$attribute' should be Pointer, got ${actualValue.runtimeType}");
        }
        if (actualValue != null) {
          pointers[attribute] = actualValue.serialize();
          _fields.remove(attribute);
        }
      }

      // Handle Timestamp
      else if (expectedType == Timestamp) {
        if (actualValue is! Timestamp && actualValue != null) {
          throw ArgumentError(
              "Attribute '$attribute' should be Timestamp, got ${actualValue.runtimeType}");
        }
        if (actualValue != null) {
          timestamps[attribute] = actualValue.serialize();
          _fields.remove(attribute);
        }
      }

      else if (expectedType is List<Type>) {
        final ok = actualValue == null ||
            expectedType.any((t) => actualValue.runtimeType == t);
        if (!ok) {
          throw ArgumentError(
              "Attribute '$attribute' should be one of $expectedType, got ${actualValue.runtimeType}");
        }
      }

      // Normal type
      else {
        if (actualValue != null && actualValue.runtimeType != expectedType) {
          throw ArgumentError(
              "Attribute '$attribute' should be $expectedType, got ${actualValue.runtimeType}");
        }
      }
    });

    if (pointers.isNotEmpty) _fields['pointers'] = pointers;
    if (timestamps.isNotEmpty) _fields['timestamps'] = timestamps;
    // add schema
    _fields['schema'] = schema;
  }

  @override
  String toString() => schema;
  String toJson() => serialize().toString();
}

  // final userSchema = DynamicSchema({
  //   'name': 'Alice',
  //   'age': 30,
  //   'email': 'alice@example.com',
  // }, {
  //   'name': String,
  //   'age': int,
  //   'email': String,
  // });
class DynamicSchema extends Schema {
  final Map<String, Type> _hints;

  DynamicSchema(super.kwargs, this._hints);

  @override
  Map<String, Type> metadata() => _hints;
}

Schema makeSchema(String name, Map<String, dynamic> fields, Map<String, Type> hints) {
  return DynamicSchema(fields, hints);
}