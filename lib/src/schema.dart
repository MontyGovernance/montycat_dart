import 'tools.dart';

/// Base abstract class for defining Montycat schemas.
///
/// Handles:
/// - Field initialization
/// - Field type validation
/// - Pointer and Timestamp serialization
/// - Extra and missing field checks
abstract class Schema {
  final Map<String, dynamic> _fields = {};
  late final String schema;

  /// Constructor accepts a map of field values.
  /// Performs type validation and prepares pointers/timestamps.
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

  /// Subclasses must override this method to define expected field types.
  Map<String, dynamic> metadata();

  /// Returns a serializable map of field values
  Map<String, dynamic> serialize() => Map<String, dynamic>.from(_fields);

  /// Checks that no required fields are missing
  void checkMissingFields(Map<String, dynamic> hints) {
    for (final entry in hints.entries) {
      final key = entry.key;
      if (_fields[key] == null) {
        throw ArgumentError("Missing required field: '$key'");
      }
    }
  }

  /// Checks for any fields not defined in metadata
  void checkExtraFields(Map<String, dynamic> hints) {
    final defined = hints.keys.toSet();
    for (final key in _fields.keys) {
      if (!defined.contains(key)) {
        throw ArgumentError("Unexpected field '$key' found in the instance.");
      }
    }
  }

  /// Validates field types and serializes Pointers and Timestamps
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

      // Handle lists of types
      else if (expectedType is List<Type>) {
        final ok = actualValue == null ||
            expectedType.any((t) => actualValue.runtimeType == t);
        if (!ok) {
          throw ArgumentError(
            "Attribute '$attribute' should be one of $expectedType, got ${actualValue.runtimeType}");
        }
      }

      // Normal type check
      else {
        if (actualValue != null && actualValue.runtimeType != expectedType) {
          throw ArgumentError(
            "Attribute '$attribute' should be $expectedType, got ${actualValue.runtimeType}");
        }
      }
    });

    if (pointers.isNotEmpty) _fields['pointers'] = pointers;
    if (timestamps.isNotEmpty) _fields['timestamps'] = timestamps;

    // Include schema name
    _fields['schema'] = schema;
  }

  @override
  String toString() => schema;

  /// Returns serialized fields as a string (JSON-like)
  String toJson() => serialize().toString();
}

/// A dynamic schema implementation, where field types are provided at runtime
class DynamicSchema extends Schema {
  final Map<String, Type> _hints;

  DynamicSchema(super.kwargs, this._hints);

  @override
  Map<String, Type> metadata() => _hints;
}

/// Helper function to create a schema dynamically
Schema makeSchema(
  String name,
  Map<String, dynamic> fields,
  Map<String, Type> hints
) {
  return DynamicSchema(fields, hints);
}
