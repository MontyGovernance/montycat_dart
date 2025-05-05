import 'utils.dart' show sendData;
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

/// The Engine class provides methods to interact with a Montycat server.
/// It allows you to create and delete stores, and retrieve system structure.
/// The class is initialized with the server's host, port, username, password,
/// and an optional store name.
///
/// Usage:
/// ```dart
/// final engine = Engine(
///  host: '127.0.0.1
/// port: 21210,
/// username: 'user',
/// password: 'pass',
/// store: 'store',
/// );
/// ```
/// The class also provides a factory constructor to create an instance from a URI string:
/// ```dart
/// final engine = Engine.fromUri('montycat://127.0.0.1/21210/username/password[/store]');
///```
/// The store name is optional.
///
class Engine {
  const Engine({
    required this.host,
    required this.port,
    required this.username,
    required this.password,
    this.store,
  });

  static const Set<String> validPermissions = {'read', 'write', 'all'};

  final String host;
  final int port;
  final String username;
  final String password;
  final String? store;

  /// Creates an Engine instance from a URI string in the format:
  /// `montycat://host/port/username/password[/store]`
  ///
  /// The URI must start with `montycat://` and contain the following parts:
  /// - `host`: The hostname or IP address of the Montycat server.
  /// - `port`: The port number of the Montycat server.
  /// - `username`: The username for authentication.
  /// - `password`: The password for authentication.
  /// - `store`: (Optional) The name of the store.
  ///
  /// Throws [FormatException] if the URI is invalid.
  factory Engine.fromUri(String uri) {
    const prefix = 'montycat://';
    if (!uri.startsWith(prefix)) {
      throw FormatException("URI must use 'montycat://' protocol");
    }

    final parts = uri.substring(prefix.length).split('/');
    if (parts.length != 4 && parts.length != 5) {
      throw FormatException("Missing or extra parts in URI");
    }

    final host = parts[0];
    final portStr = parts[1];
    final username = parts[2];
    final password = parts[3];
    final store = parts.length == 5 ? parts[4] : null;

    if ([host, portStr, username, password].any((e) => e.isEmpty)) {
      throw FormatException(
        "Host, port, username, and password must be non-empty",
      );
    }

    final port = int.tryParse(portStr);
    if (port == null) {
      throw FormatException("Port must be an integer");
    }

    return Engine(
      host: host,
      port: port,
      username: username,
      password: password,
      store: store,
    );
  }

  Future<dynamic> _executeQuery(List<dynamic> command) async {
    Map<String, dynamic> query = {
      "raw": command,
      "superowner_credentials": [username, password],
    };
    String queryJson = jsonEncode(query);
    Uint8List queryBytes = utf8.encode(queryJson);

    return await sendData(host, port, queryBytes);
  }

  Future<dynamic> createStore({bool persistent = false}) async {
    if (store == null) {
      throw ArgumentError("Store name must be specified");
    }

    print("Creating store: $store");

    return await _executeQuery([
      "create-store",
      "store",
      store,
      "persistent",
      persistent ? "y" : "n",
    ]);
  }

  Future<dynamic> deleteStore({bool persistent = false}) async {
    return await _executeQuery([
      "delete-store",
      "store",
      store,
      persistent ? "y" : "n",
    ]);
  }

  Future<dynamic> getSystemStructure() async {
    return await _executeQuery(["get-available-structure"]);
  }
}
