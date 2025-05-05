import 'utils.dart' show sendData;
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

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
