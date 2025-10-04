import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'dart:typed_data';

/// Handle for managing active subscriptions.
/// Allows stopping the subscription and closing the socket.
/// When stopped, no further callbacks will be invoked.
/// Used in subscribe queries.
class SubscriptionHandle {
  final Socket _socket;
  bool _stopped = false;

  SubscriptionHandle(this._socket);

  void stop() {
    _stopped = true;
    _socket.destroy();
  }

  bool get stopped => _stopped;
}

/// Sends data asynchronously to a remote server and handles the response.
///
/// Args:
///   - [host]: The server's hostname or IP address.
///   - [port]: The server's port.
///   - [query]: The serialized data to be sent as Uint8List.
///   - [callback]: An optional function to handle subscription responses.
///
/// Returns:
///   A Future containing the server's parsed response for non-subscribe queries,
///   or null for subscribe queries after the connection closes.
///
/// Throws:
///   - [TimeoutException]: If the operation exceeds the 120-second timeout.
///   - [SocketException]: If the server refuses the connection or resets it.
Future<dynamic> sendData(
  String host,
  int port,
  Uint8List query, {
  void Function(dynamic)? callback,
}) async {
  try {
    var socket = await Socket.connect(host, port, timeout: Duration(seconds: 10));
    var queryStr = utf8.decode(query, allowMalformed: true);
    bool isSubscribe = queryStr.contains("subscribe");

    socket.add([...query, 10]);
    await socket.flush();

    var lines = socket
        .cast<List<int>>()
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .asBroadcastStream();

    if (isSubscribe) {

      final handle = SubscriptionHandle(socket);

      lines.listen((line) {
        if (!handle.stopped && callback != null) {
          try {
            var parsed = recursiveParseJson(line.trim());
            callback(parsed);
          } catch (e) {
            print("Error occurred: $e");
            callback("Failed to parse: $line");
          }
        }
      });

      return handle;
    } else {
      var line = await lines.first.timeout(Duration(seconds: 120));
      await socket.close();
      return recursiveParseJson(line.trim());
    }
  } on SocketException catch (e) {
    return "SocketError: $e (address: $host, port: $port)";
  } on TimeoutException catch (e) {
    return "TimeoutError: $e (address: $host, port: $port)";
  } catch (e) {
    return "Error: $e";
  }
}


/// Recursively parses nested JSON strings in the provided data.
///
/// Keeps u128 values (strings with >16 digits) as strings.
///
/// Args:
///   - [data]: A Dart object that may contain JSON strings, including nested structures.
///
/// Keeps u128 as string as Dart can't handle u128 natively
///
/// Returns:
///   A fully parsed Dart object with all nested JSON strings converted, except for u128 values.
dynamic recursiveParseJson(dynamic data) {
  if (data is String) {
    if (isU128(data)) {
      return data;
    }
    try {
      var parsed = json.decode(data);
      return recursiveParseJson(parsed);
    } on FormatException {
      return data;
    }
  } else if (data is Map) {
    return data.map((key, value) => MapEntry(key, recursiveParseJson(value)));
  } else if (data is List) {
    return data.map(recursiveParseJson).toList();
  } else {
    return data;
  }
}

/// Checks if the given string is a u128 value (digits only, length > 16).
///
/// Args:
///   - [value]: A string to check.
///
/// Returns:
///   True if the string is a u128 value, false otherwise.
bool isU128(String value) {
  return value.length > 16 && RegExp(r'^\d+$').hasMatch(value);
}
