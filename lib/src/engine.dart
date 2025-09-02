import 'utils.dart' show sendData;
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'tools.dart' show Permission;

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

  // static const Set<String> validPermissions ={'read', 'write', 'all'};

  final String host; 
  final int port;
  final String username;
  final String password;
  late String? store;

  /// Creates an Engine instance from a URI string in the format:
  /// `montycat://username:password@host:port[/store]`
  factory Engine.fromUri(String uri) {
    final parsed = Uri.parse(uri);

    if (parsed.scheme != 'montycat') {
      throw FormatException("URI must use 'montycat://' protocol");
    }

    final username = parsed.userInfo.isNotEmpty
        ? parsed.userInfo.split(':').first
        : null;
    final password = parsed.userInfo.contains(':')
        ? parsed.userInfo.split(':').last
        : null;

    final host = parsed.host;
    final port = parsed.port;
    final store = parsed.pathSegments.isNotEmpty ? parsed.pathSegments.first : null;

    if (username == null || password == null || host.isEmpty || port == 0) {
      throw FormatException("Invalid URI: missing username, password, host, or port");
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
    // print('Sending query: $queryJson');
    return await sendData(host, port, queryBytes);
  }

  // ---------------- Store Management ----------------

  /// Creates a new store.
  ///
  /// Args:
  ///   persistent: Whether the store should be persistent (default: false).
  ///
  Future<dynamic> createStore() async {
    if (store == null) {
      throw ArgumentError("Store name must be specified");
    }
    return await _executeQuery([
      "create-store",
      "store",
      store,
    ]);
  }

  Future<dynamic> removeStore() async {
    if (store == null) {
      throw ArgumentError("Store name must be specified");
    }
    return await _executeQuery([
      "remove-store",
      "store",
      store,
    ]);
  }

  // ---------------- Permissions ----------------

  Future<dynamic> grantTo(
    String owner,
    Permission permission, {
    List<String>? keyspaces,
  }) async {

    if (store == null) {
      throw ArgumentError("Store must be specified");
    }

    final List<String> command = [
      "grant-to",
      "owner", owner,
      "permission", permission.toString(),
      "store", store!,
    ];

    if (keyspaces != null && keyspaces.isNotEmpty) {
      command.add("keyspaces");
      command.addAll(keyspaces);
    }

    return await _executeQuery(command);
  }

  Future<dynamic> revokeFrom(
    String owner,
    Permission permission, {
    List<String>? keyspaces,
  }) async {

    if (store == null) {
      throw ArgumentError("Store must be specified");
    }

    final List<String> command = [
      "revoke-from",
      "owner", owner,
      "permission", permission.toString(),
      "store", store!,
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
      "username", owner,
      "password", password,
    ]);
  }

  Future<dynamic> removeOwner(String owner) async {
    return await _executeQuery(["remove-owner", "username", owner]);
  }

  Future<dynamic> listOwners() async {
    return await _executeQuery(["list-owners"]);
  }

  // ---------------- Structure ----------------

  Future<dynamic> getStructureAvailable() async {
    return await _executeQuery(["get-structure-available"]);
  }

}
