import 'utils.dart' show sendData;
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

/// The Engine class provides methods to interact with a Montycat server.
/// It allows you to create and delete stores, manage owners, grant/revoke
/// permissions, and retrieve system structure.
///
/// Args:
///   host: The hostname of the Montycat server.
///   port: The port number of the Montycat server.
///   username: The username for authentication.
///   password: The password for authentication.
///   store: The name of the store to operate on (optional).
///
class Engine {

  Engine({
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
  late String? store;

  /// Creates an Engine instance from a URI string in the format:
  /// `montycat://host/port/username/password[/store]`
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
      throw FormatException("Host, port, username, and password must be non-empty");
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
      "credentials": [username, password],
    };
    String queryJson = jsonEncode(query);
    Uint8List queryBytes = utf8.encode(queryJson);
    return await sendData(host, port, queryBytes);
  }

  // ---------------- Store Management ----------------

  /// Creates a new store.
  ///
  /// Args:
  ///   persistent: Whether the store should be persistent (default: false).
  /// 
  Future<dynamic> createStore({bool persistent = false}) async {
    if (store == null) {
      throw ArgumentError("Store name must be specified");
    }
    return await _executeQuery([
      "create-store",
      "store",
      store,
      "persistent",
      persistent ? "y" : "n",
    ]);
  }

  Future<dynamic> removeStore({bool persistent = false}) async {
    if (store == null) {
      throw ArgumentError("Store name must be specified");
    }
    return await _executeQuery([
      "remove-store",
      "store",
      store,
      "persistent",
      persistent ? "y" : "n",
    ]);
  }

  // ---------------- Permissions ----------------

  Future<dynamic> grantTo(
    String owner,
    String permission, {
    List<String>? keyspaces,
  }) async {
    if (!validPermissions.contains(permission)) {
      throw ArgumentError(
          "Invalid permission: $permission. Valid: $validPermissions");
    }

    if (store == null) {
      throw ArgumentError("Store must be specified");
    }

    final command = [
      "grant-to",
      "owner",
      owner,
      "permission",
      permission,
      "store",
      store!,
    ];

    if (keyspaces != null && keyspaces.isNotEmpty) {
      command.add("keyspaces");
      command.addAll(keyspaces);
    }

    return await _executeQuery(command);
  }

  Future<dynamic> revokeFrom(
    String owner,
    String permission, {
    List<String>? keyspaces,
  }) async {
    if (!validPermissions.contains(permission)) {
      throw ArgumentError(
          "Invalid permission: $permission. Valid: $validPermissions");
    }

    if (store == null) {
      throw ArgumentError("Store must be specified");
    }

    final command = [
      "revoke-from",
      "owner",
      owner,
      "permission",
      permission,
      "store",
      store!,
    ];

    if (keyspaces != null && keyspaces.isNotEmpty) {
      command.add("keyspaces");
      command.addAll(keyspaces);
    }

    return await _executeQuery(command);
  }

  // ---------------- Owner Management ----------------

  Future<dynamic> createOwner(String owner, String password) async {
    return await _executeQuery([
      "create-owner",
      "username",
      owner,
      "password",
      password,
    ]);
  }

  Future<dynamic> removeOwner(String owner) async {
    return await _executeQuery([
      "remove-owner",
      "username",
      owner,
    ]);
  }

  Future<dynamic> listOwners() async {
    return await _executeQuery(["list-owners"]);
  }

  // ---------------- Structure ----------------

  Future<dynamic> getStructureAvailable() async {
    return await _executeQuery(["get-structure-available"]);
  }
}
