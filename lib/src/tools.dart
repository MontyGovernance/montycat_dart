/// Enum for permission levels in Montycat.
/// Used in grant and revoke operations.
///
enum Permission {
  read,
  write,
  all;

  @override
  String toString() {
    return name; // returns "read", "write", or "all"
  }
}

/// A class for handling timestamp conditions.
///
/// Supports single timestamps, ranges, or before/after conditions.
///
//// Used in schema definitions and queries.
///
class Timestamp {
  final String? timestamp; // Single timestamp
  final String? start; // Range start
  final String? end; // Range end
  final String? after; // After condition
  final String? before; // Before condition

  const Timestamp({
    this.timestamp,
    this.start,
    this.end,
    this.after,
    this.before,
  });

  /// Serializes the timestamp into a map or string for Montycat queries.
  /// Throws [ArgumentError] if the configuration is invalid.
  ///
  /// Returns a dynamic object representing the serialized timestamp.
  ///
  dynamic serialize() {
    if (start != null && end != null) {
      return {
        "range_timestamp": [start, end],
      };
    } else if (after != null) {
      return {"after_timestamp": after};
    } else if (before != null) {
      return {"before_timestamp": before};
    } else if (timestamp != null) {
      return timestamp;
    }
    throw ArgumentError("Invalid timestamp configuration");
  }
}

/// A simple class representing a reference pointer.
///
/// Pointers refer to a key in another keyspace.
///
class Pointer {
  final String keyspace;
  final dynamic key;

  const Pointer({required this.keyspace, required this.key});

  /// Serializes the pointer as `[keyspace, key]`.
  List<dynamic> serialize() {
    return [keyspace, key];
  }
}

/// A class representing pagination limits.
///
/// Used for queries with `start` and `stop` bounds.
///
class Limit {
  final int start;
  final int stop;

  const Limit({this.start = 0, this.stop = 0});

  /// Serializes the limit into a map: `{"start": start, "stop": stop}`.
  Map<String, int> serialize() {
    return {"start": start, "stop": stop};
  }
}
