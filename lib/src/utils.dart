import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'dart:typed_data';

import 'pool.dart' show PoolConfig, PooledConnection, getPool;

/// Handle for managing active subscriptions.
/// Allows stopping the subscription and closing the socket.
/// When stopped, no further callbacks will be invoked.
/// Used in subscribe queries.
///
class SubscriptionHandle {
  final Socket _socket;
  bool _stopped = false;

  SubscriptionHandle(this._socket);

  /// Stops the subscription and closes the socket connection.
  /// After calling this, no further callbacks will be invoked.
  ///
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
///
Future<dynamic> sendData(
  String host,
  int port,
  Uint8List query, {
  void Function(dynamic)? callback,
  bool useTls = false,
  PoolConfig? poolConfig,
}) async {
  // A subscription is the call that supplies a callback — never inferred from
  // the payload. Searching the request for "subscribe" misread any record whose
  // value merely contained that word, routing it into the streaming branch,
  // which never completes its future.
  //
  // Subscriptions are never pooled (contract §5): they are long-lived, stream
  // many responses to one request, and live on the `port + 1` subscription port.
  if (callback == null && poolConfig != null) {
    return _pooledRequest(host, port, query, useTls, poolConfig);
  }

  try {
    late Socket socket;

    if (useTls) {
      socket = await SecureSocket.connect(
        host,
        port,
        onBadCertificate: (X509Certificate cert) => true, // Accept self-signed
      ).timeout(const Duration(seconds: 10));
    } else {
      socket = await Socket.connect(
        host,
        port,
        timeout: const Duration(seconds: 10),
      );
    }

    final bool isSubscribe = callback != null;

    socket.add([...query, 10]);
    await socket.flush();

    var lines =
        socket
            .cast<List<int>>()
            .transform(utf8.decoder)
            .transform(const LineSplitter())
            .asBroadcastStream();

    if (isSubscribe) {
      final handle = SubscriptionHandle(socket);

      lines.listen((line) {
        if (!handle.stopped) {
          try {
            var parsed = recursiveParseJson(line.trim());
            callback(parsed);
          } catch (e) {
            callback("Failed to parse: $line");
          }
        }
      });

      return handle;
    } else {
      var line = await lines.first.timeout(const Duration(seconds: 120));
      await socket.close();
      return recursiveParseJson(line.trim());
    }
  } on SocketException catch (e) {
    print("SocketException: $e (address: $host, port: $port)");
    return e;
  } on TimeoutException catch (e) {
    print("TimeoutException: $e (address: $host, port: $port)");
    return e;
  } catch (e) {
    print("Unexpected error: $e (address: $host, port: $port)");
    return e;
  }
}

/// Open one connection and wrap it with its framing chain for pooling.
Future<PooledConnection> _openPooled(
  String host,
  int port,
  bool useTls,
) async {
  final Socket socket =
      useTls
          ? await SecureSocket.connect(
            host,
            port,
            onBadCertificate: (X509Certificate cert) => true,
          ).timeout(const Duration(seconds: 10))
          : await Socket.connect(
            host,
            port,
            timeout: const Duration(seconds: 10),
          );
  return PooledConnection(socket);
}

/// Request/response over a pooled connection.
///
/// Errors are returned rather than thrown, matching what this client has always
/// done — changing that would break every existing caller.
Future<dynamic> _pooledRequest(
  String host,
  int port,
  Uint8List query,
  bool useTls,
  PoolConfig poolConfig,
) async {
  // The key is read here, at request time. `useTls` is mutable on the engine
  // after construction, so caching it at connectEngine time would let a TLS
  // engine reuse a plaintext connection.
  final pool = getPool(host, port, useTls, poolConfig)!;

  final leased = await pool.checkout();
  if (leased != null) {
    try {
      final line = await leased.request(query, const Duration(seconds: 120));
      await pool.checkin(leased);
      return recursiveParseJson(line.trim());
    } catch (e) {
      // Never retry here: the engine may have applied the write and only the
      // response was lost. These commands are not idempotent and the wire has
      // no request IDs, so replaying would duplicate data (contract §4). A
      // genuinely stale connection is caught by `checkout`, before anything is
      // sent, so this path is a real failure rather than a routine one.
      await leased.close();
      return e;
    }
  }

  PooledConnection connection;
  try {
    connection = await _openPooled(host, port, useTls);
  } catch (e) {
    print("Connection error: $e (address: $host, port: $port)");
    return e;
  }

  try {
    final line = await connection.request(query, const Duration(seconds: 120));
    await pool.checkin(connection);
    return recursiveParseJson(line.trim());
  } catch (e) {
    await connection.close();
    return e;
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
///
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
///
bool isU128(String value) {
  return value.length > 16 && RegExp(r'^\d+$').hasMatch(value);
}
