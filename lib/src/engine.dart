import 'utils.dart' show sendData;
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'tools.dart' show Permission;

/// The `Engine` class provides methods to interact with a Montycat server.
///
/// It allows you to:
/// - Create and delete stores
/// - Manage owners
/// - Grant or revoke permissions
/// - Retrieve the system structure
///
/// Required parameters:
/// - `host`: Montycat server hostname
/// - `port`: Port number of the Montycat server
/// - `username`: Authentication username
/// - `password`: Authentication password
/// - `store`: Optional store name to operate on
class Engine {
  Engine({
    required this.host,
    required this.port,
    required this.username,
    required this.password,
    this.store,
    this.useTls = false,
  });

  final String host;
  final int port;
  final String username;
  final String password;
  late String? store;
  late bool useTls;

  /// Creates an `Engine` instance from a URI string.
  ///
  /// Expected URI format:
  /// `montycat://username:password@host:port[/store]`
  /// - `username`: Authentication username
  /// - `password`: Authentication password
  /// - `host`: Montycat server hostname
  /// - `port`: Port number of the Montycat server
  /// - `store`: Optional store name to operate on
  /// Throws a [FormatException] if the URI is invalid or missing required components.
  factory Engine.fromUri(String uri) {
    final parsed = Uri.parse(uri);

    if (parsed.scheme != 'montycat') {
      throw FormatException("URI must use 'montycat://' protocol");
    }

    final username =
        parsed.userInfo.isNotEmpty ? parsed.userInfo.split(':').first : null;
    final password =
        parsed.userInfo.contains(':') ? parsed.userInfo.split(':').last : null;

    final host = parsed.host;
    final port = parsed.port;
    final store =
        parsed.pathSegments.isNotEmpty ? parsed.pathSegments.first : null;

    if (username == null || password == null || host.isEmpty || port == 0) {
      throw FormatException(
        "Invalid URI: missing username, password, host, or port",
      );
    }

    return Engine(
      host: host,
      port: port,
      username: username,
      password: password,
      store: store,
      useTls: false,
    );
  }

  /// Internal helper to execute any Montycat command.
  Future<dynamic> _executeQuery(List<dynamic> command) async {
    Map<String, dynamic> query = {
      "raw": command,
      "credentials": [username, password],
    };
    String queryJson = jsonEncode(query);
    Uint8List queryBytes = utf8.encode(queryJson);
    return await sendData(host, port, queryBytes, useTls: useTls);
  }

  /// Creates a new store on the Montycat server.
  /// Throws an [ArgumentError] if the store name is not specified.
  Future<dynamic> createStore() async {
    if (store == null) throw ArgumentError("Store name must be specified");
    return await _executeQuery(["create-store", "store", store]);
  }

  /// Removes an existing store from the Montycat server.
  /// Throws an [ArgumentError] if the store name is not specified.
  Future<dynamic> removeStore() async {
    if (store == null) throw ArgumentError("Store name must be specified");
    return await _executeQuery(["remove-store", "store", store]);
  }

  /// Grants a permission to an owner.
  /// Optional `keyspaces` can be specified to limit scope.
  Future<dynamic> grantTo(
    String owner,
    Permission permission, {
    List<String>? keyspaces,
  }) async {
    if (store == null) throw ArgumentError("Store must be specified");

    final List<String> command = [
      "grant-to",
      "owner",
      owner,
      "permission",
      permission.toString(),
      "store",
      store!,
    ];

    if (keyspaces != null && keyspaces.isNotEmpty) {
      command.add("keyspaces");
      command.addAll(keyspaces);
    }

    return await _executeQuery(command);
  }

  /// Revokes a permission from an owner.
  /// Optional `keyspaces` can be specified to limit scope.
  Future<dynamic> revokeFrom(
    String owner,
    Permission permission, {
    List<String>? keyspaces,
  }) async {

    if (store == null) throw ArgumentError("Store must be specified");

    final List<String> command = [
      "revoke-from",
      "owner",
      owner,
      "permission",
      permission.toString(),
      "store",
      store!,
    ];

    if (keyspaces != null && keyspaces.isNotEmpty) {
      command.add("keyspaces");
      command.addAll(keyspaces);
    }

    return await _executeQuery(command);
  }

  /// Creates a new owner with the given username and password.
  Future<dynamic> createOwner(String owner, String password) async {
    return await _executeQuery([
      "create-owner",
      "username",
      owner,
      "password",
      password,
    ]);
  }

  /// Removes an existing owner.
  Future<dynamic> removeOwner(String owner) async {
    return await _executeQuery(["remove-owner", "username", owner]);
  }

  /// Lists all existing owners in the system.
  Future<dynamic> listOwners() async {
    return await _executeQuery(["list-owners"]);
  }

  /// Retrieves the system structure available on the server.
  Future<dynamic> getStructureAvailable() async {
    final storePart = store != null ? ["store", store!] : [];
    return await _executeQuery(["get-structure-available", ...storePart]);
  }
}
