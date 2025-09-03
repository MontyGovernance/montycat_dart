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
  final String? timestamp; // Single timestamp
  final String? start;     // Range start
  final String? end;       // Range end
  final String? after;     // After condition
  final String? before;    // Before condition

  const Timestamp({
    this.timestamp,
    this.start,
    this.end,
    this.after,
    this.before,
  });

  dynamic serialize() {
    if (start != null && end != null) {
      return {"range_timestamp": [start, end]};
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
class Pointer {
  final String keyspace;
  final dynamic key;

const Pointer({required this.keyspace, required this.key});

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

