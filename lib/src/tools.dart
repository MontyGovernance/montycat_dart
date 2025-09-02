/// Enum for permission levels.
enum Permission {
  read,
  write,
  all;

  @override
  String toString() {
    return name; // returns "read", "write", "all"
  }
}

/// A class for handling timestamp conditions.
class Timestamp {
  final dynamic timestamp; // Single timestamp
  final dynamic start;     // Range start
  final dynamic end;       // Range end
  final dynamic after;     // After condition
  final dynamic before;    // Before condition

  const Timestamp({
    this.timestamp,
    this.start,
    this.end,
    this.after,
    this.before,
  });

  Map<String, dynamic> serialize() {
    if (start != null && end != null) {
      return {"range_timestamp": [start, end]};
    } else if (after != null) {
      return {"after_timestamp": after};
    } else if (before != null) {
      return {"before_timestamp": before};
    } else if (timestamp != null) {
      return {"timestamp": timestamp};
    }
    throw ArgumentError("Invalid timestamp configuration");
  }
}

/// A simple class representing a reference pointer.
class Pointer {
  final String keyspace;
  final dynamic key;

  const Pointer(this.keyspace, this.key);

  List<dynamic> serialize() {
    // returns [keyspace, key]
    return [keyspace, key];
  }
}

/// A class for pagination limits.
class Limit {
  final int start;
  final int stop;

  const Limit({this.start = 0, this.stop = 0});

  Map<String, int> serialize() {
    return {"start": start, "stop": stop};
  }
}

