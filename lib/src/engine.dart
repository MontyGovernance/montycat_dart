import 'utils.dart' show sendData;
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'tools.dart'
    show Permission, PolicyCapability, PolicyKeyspaceType, SemanticModel, PolicyFormat;

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
/// - `useTls`: Whether to use TLS for the connection (default is false)
///
/// Example:
///
/// ```dart
/// final engine = Engine(
///   host: 'localhost',
///   port: 1234,
///   username: 'admin',
///   password: 'secret',
///   store: 'mystore',
/// );
/// ```
///
/// Or using a URI:
///
/// ```dart
/// final engine = Engine.fromUri('montycat://admin:secret@localhost:1234/mystore');
/// ```
///
/// This class handles communication with the Montycat server and executes commands as needed.
///
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
  ///
  /// `montycat://username:password@host:port[/store]`
  ///
  /// - `username`: Authentication username
  /// - `password`: Authentication password
  /// - `host`: Montycat server hostname
  /// - `port`: Port number of the Montycat server
  /// - `store`: Optional store name to operate on
  /// - `useTls`: Whether to use TLS for the connection (default is false)
  ///
  /// Throws a [FormatException] if the URI is invalid or missing required components.
  ///
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
  ///
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
  ///
  Future<dynamic> createStore() async {
    if (store == null) throw ArgumentError("Store name must be specified");
    return await _executeQuery(["create-store", "store", store]);
  }

  /// Removes an existing store from the Montycat server.
  /// Throws an [ArgumentError] if the store name is not specified.
  ///
  Future<dynamic> removeStore() async {
    if (store == null) throw ArgumentError("Store name must be specified");
    return await _executeQuery(["remove-store", "store", store]);
  }

  /// Grants a permission to an owner.
  /// Optional `keyspaces` can be specified to limit scope.
  ///
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
  ///
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
  ///
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
  ///
  Future<dynamic> removeOwner(String owner) async {
    return await _executeQuery(["remove-owner", "username", owner]);
  }

  /// Lists all existing owners in the system.
  ///
  Future<dynamic> listOwners() async {
    return await _executeQuery(["list-owners"]);
  }

  /// Retrieves the system structure available on the server.
  ///
  Future<dynamic> getStructureAvailable() async {
    final storePart = store != null ? ["store", store!] : [];
    return await _executeQuery(["get-structure-available", ...storePart]);
  }

  /// Enables semantic (vector similarity) search.
  ///
  /// Without [store], this is DB-wide: it flips the whole database on, sets the
  /// default embedding model and field, and enrolls every existing keyspace that
  /// has no semantic config yet (each gets a background backfill so its existing
  /// items become searchable). The chosen model is downloaded on demand on first
  /// enable, so this call may take a while the first time.
  ///
  /// With [store], it is scoped: only that store's un-enrolled keyspaces are
  /// enrolled and backfilled; the DB-wide switch and default model/field are left
  /// untouched. Use this to (re-)enable one store without re-embedding the entire
  /// database.
  ///
  /// - [model]: The embedding model key to use by default. One of 'minilm',
  ///   'bge-small', 'bge-base', 'e5-small'. Null uses the server default
  ///   ('bge-small').
  /// - [field]: The JSON field of each value to embed. Null embeds the whole value.
  /// - [store]: Restrict enrollment/backfill to this store only. If the DB-wide
  ///   switch is off, a scoped enable enrolls but nothing embeds until a DB-wide
  ///   enable.
  ///
  Future<dynamic> enableSemanticSearch({
    SemanticModel? model,
    String? field,
    String? store,
    String? keyspace,
  }) async {
    if (keyspace != null && store == null) {
      throw ArgumentError("A store is required when keyspace is specified");
    }
    final List<dynamic> command = ["enable-semantic-search"];
    if (model != null) command.addAll(["model", model.wireName]);
    if (field != null) command.addAll(["field", field]);
    if (store != null) command.addAll(["store", store]);
    if (keyspace != null) command.addAll(["keyspace", keyspace]);
    return await _executeQuery(command);
  }

  /// Disables semantic search.
  ///
  /// Without [store], this is DB-wide: embedding and semantic queries stop across
  /// the whole database; stored vectors are kept by default so re-enabling
  /// resumes without a full re-embed.
  ///
  /// With [store], it is scoped: only that store's keyspaces are unenrolled
  /// (their configs and resident graphs dropped); the DB-wide switch and all
  /// other stores are left untouched. This is the surgical way to reset one
  /// store's semantic state instead of nuking and re-backfilling the whole
  /// database.
  ///
  /// - [dropVectors]: If true, also clear stored vectors — every keyspace's
  ///   DB-wide, or the scoped store's when [store] is set. Required before
  ///   switching to a different embedding model.
  /// - [store]: Restrict the disable to this store only. Null disables DB-wide.
  ///
  Future<dynamic> disableSemanticSearch({
    bool dropVectors = false,
    String? store,
    String? keyspace,
  }) async {
    if (keyspace != null && store == null) {
      throw ArgumentError("A store is required when keyspace is specified");
    }
    final List<dynamic> command = ["disable-semantic-search"];
    if (dropVectors) command.add("drop-vectors");
    if (store != null) command.addAll(["store", store]);
    if (keyspace != null) command.addAll(["keyspace", keyspace]);
    return await _executeQuery(command);
  }

  Future<dynamic> policyView({String? owner, String? store}) async {
    final command = <dynamic>["policy-view"];
    if (owner != null) command.addAll(["owner", owner]);
    if (store != null) command.addAll(["store", store]);
    return await _executeQuery(command);
  }

  Future<dynamic> policyHistory({
    String? owner,
    String? store,
    String? keyspace,
  }) async {
    final command = <dynamic>["policy-history"];
    if (owner != null) command.addAll(["owner", owner]);
    if (store != null) command.addAll(["store", store]);
    if (keyspace != null) command.addAll(["keyspace", keyspace]);
    return await _executeQuery(command);
  }

  Future<dynamic> policyExplain({
    required PolicyCapability capability,
    required String store,
    String? owner,
    String? keyspace,
    PolicyKeyspaceType? keyspaceType,
    SemanticModel? model,
  }) async {
    final command = <dynamic>[
      "policy-explain",
      "capability",
      capability.wireName,
      "store",
      store,
    ];
    if (owner != null) command.addAll(["owner", owner]);
    if (keyspace != null && capability != PolicyCapability.provisionKeyspace) {
      command.addAll(["keyspace", keyspace]);
    }
    if (keyspaceType != null) command.addAll(["type", keyspaceType.wireName]);
    if (model != null) command.addAll(["model", model.wireName]);
    return await _executeQuery(command);
  }

  Future<dynamic> _policyMutation(
    String operation, {
    required String owner,
    required PolicyCapability capability,
    required String store,
    String? keyspace,
    List<PolicyKeyspaceType> types = const [],
    List<SemanticModel> models = const [],
  }) async {
    final command = <dynamic>[
      operation,
      "owner",
      owner,
      "capability",
      capability.wireName,
      "store",
      store,
    ];
    if (keyspace != null && capability != PolicyCapability.provisionKeyspace) {
      command.addAll(["keyspace", keyspace]);
    }
    if (types.isNotEmpty) {
      command.addAll(["types", ...types.map((type) => type.wireName)]);
    }
    if (models.isNotEmpty) {
      command.addAll(["models", ...models.map((model) => model.wireName)]);
    }
    return await _executeQuery(command);
  }

  Future<dynamic> policyGrant({
    required String owner,
    required PolicyCapability capability,
    required String store,
    String? keyspace,
    List<PolicyKeyspaceType> types = const [],
    List<SemanticModel> models = const [],
  }) => _policyMutation(
    "policy-grant",
    owner: owner,
    capability: capability,
    store: store,
    keyspace: keyspace,
    types: types,
    models: models,
  );

  Future<dynamic> policyRevoke({
    required String owner,
    required PolicyCapability capability,
    required String store,
    String? keyspace,
    List<PolicyKeyspaceType> types = const [],
    List<SemanticModel> models = const [],
  }) => _policyMutation(
    "policy-revoke",
    owner: owner,
    capability: capability,
    store: store,
    keyspace: keyspace,
    types: types,
    models: models,
  );

  Future<dynamic> policyDeny({
    required String owner,
    required PolicyCapability capability,
    required String store,
    String? keyspace,
    List<PolicyKeyspaceType> types = const [],
    List<SemanticModel> models = const [],
  }) => _policyMutation(
    "policy-deny",
    owner: owner,
    capability: capability,
    store: store,
    keyspace: keyspace,
    types: types,
    models: models,
  );

  Future<dynamic> policyRemoveDenial({
    required String owner,
    required PolicyCapability capability,
    required String store,
    String? keyspace,
    List<PolicyKeyspaceType> types = const [],
    List<SemanticModel> models = const [],
  }) => _policyMutation(
    "policy-remove-denial",
    owner: owner,
    capability: capability,
    store: store,
    keyspace: keyspace,
    types: types,
    models: models,
  );

  Future<dynamic> policyPreviewGrant({
    required String owner,
    required PolicyCapability capability,
    required String store,
    String? keyspace,
    List<PolicyKeyspaceType> types = const [],
    List<SemanticModel> models = const [],
  }) => _policyMutation(
    "policy-preview-grant",
    owner: owner,
    capability: capability,
    store: store,
    keyspace: keyspace,
    types: types,
    models: models,
  );

  Future<dynamic> policyPreviewRevoke({
    required String owner,
    required PolicyCapability capability,
    required String store,
    String? keyspace,
    List<PolicyKeyspaceType> types = const [],
    List<SemanticModel> models = const [],
  }) => _policyMutation(
    "policy-preview-revoke",
    owner: owner,
    capability: capability,
    store: store,
    keyspace: keyspace,
    types: types,
    models: models,
  );

  Future<dynamic> _policyManifest(
    String operation,
    String document,
    PolicyFormat format,
  ) {
    return _executeQuery([operation, "format", format.wireName, "document", document]);
  }

  Future<dynamic> policyValidate(String document, {PolicyFormat format = PolicyFormat.json}) =>
      _policyManifest("policy-validate", document, format);
  Future<dynamic> policyPlan(String document, {PolicyFormat format = PolicyFormat.json}) =>
      _policyManifest("policy-plan", document, format);
  Future<dynamic> policyApply(String document, {PolicyFormat format = PolicyFormat.json}) =>
      _policyManifest("policy-apply", document, format);
  Future<dynamic> policyExport({PolicyFormat format = PolicyFormat.json}) =>
      _executeQuery(["policy-export", "format", format.wireName]);

  /// Enables the DB-wide "wait for index" default.
  ///
  /// Writes block until their secondary indexes are updated before returning,
  /// so a write is immediately visible to index-backed reads (e.g.
  /// [KV.lookupValuesWhere]) at the cost of higher write latency. Requires
  /// superowner credentials.
  ///
  Future<dynamic> enableWaitForIndex() async {
    return await _executeQuery(["enable-wait-for-index"]);
  }

  /// Disables the DB-wide "wait for index" default.
  ///
  /// Writes return as soon as the data is committed and indexing happens
  /// asynchronously in the background (lower write latency; index-backed reads
  /// may briefly lag). This is the default behavior. Requires superowner
  /// credentials.
  ///
  Future<dynamic> disableWaitForIndex() async {
    return await _executeQuery(["disable-wait-for-index"]);
  }

  /// Enable server-side operation reporting (logging). Requires superowner credentials.
  Future<dynamic> enableReports() async {
    return await _executeQuery(["enable-reports"]);
  }

  /// Disable server-side operation reporting (logging). Requires superowner credentials.
  Future<dynamic> disableReports() async {
    return await _executeQuery(["disable-reports"]);
  }

  /// Allow clients to open keyspace subscriptions DB-wide. Requires superowner credentials.
  Future<dynamic> allowSubscriptions() async {
    return await _executeQuery(["allow-subscriptions"]);
  }

  /// Restrict (disallow) keyspace subscriptions DB-wide. Requires superowner credentials.
  Future<dynamic> restrictSubscriptions() async {
    return await _executeQuery(["restrict-subscriptions"]);
  }

  /// Sample the current depth of every background task queue (index, timer,
  /// counting) — an observability probe for whether the background runners are
  /// keeping up with the write rate. Requires superowner credentials.
  ///
  /// The response payload maps `"index" | "timer" | "counting"` to per-queue
  /// depth maps.
  Future<dynamic> queueDepths() async {
    return await _executeQuery(["queue-depths"]);
  }

  /// Fetch the current DB-wide node settings so UIs can show live state instead
  /// of blind Enable/Disable toggles. Requires superowner credentials. Read-only
  /// mirror of the individual setters ([enableReports], [enableWaitForIndex],
  /// [allowSubscriptions], [setSnapshotRate], [setExpirationCheckRate]).
  ///
  /// The response payload is a JSON object:
  /// `{ reports, wait_for_index, subscriptions, snapshot_rate, expiration_check_rate }`.
  /// `expiration_check_rate` is the check period in whole seconds — the same value
  /// you pass to [setExpirationCheckRate] (stored as-is, like the snapshot rate).
  Future<dynamic> nodeInfo() async {
    return await _executeQuery(["node-info"]);
  }

  /// Set the server-wide snapshot rate. Requires superowner credentials.
  ///
  /// - [rate]: the snapshot rate value (server-defined units).
  Future<dynamic> setSnapshotRate(int rate) async {
    return await _executeQuery(["snapshot-rate", rate.toString()]);
  }

  /// Set how often the server scans for expired keys. Requires superowner credentials.
  ///
  /// - [rate]: the check period in whole seconds (e.g. `rate = 10` → a scan every
  ///   10 seconds). Stored as-is, like the snapshot rate. Defaults to 1 second.
  Future<dynamic> setExpirationCheckRate(int rate) async {
    return await _executeQuery(["expiration-check", rate.toString()]);
  }
}
