/// Connection pooling for request/response traffic.
///
/// Implements the client half of
/// `montycat_semantic/CLIENT_CONNECTION_POOLING_CONTRACT.md`. The rules that
/// shape this library:
///
/// - **§3** — pooling by `(host, port, tls)` is safe: credentials travel in
///   every request payload and the engine re-authenticates per request, so a
///   pooled connection carries no identity and may serve different users.
/// - **§4** — never replay a request after a read failure; the engine may have
///   applied it already. Stale connections are caught before use.
/// - **§5** — subscriptions are never pooled.
/// - **§6** — pooling is opt-in and bounded; an idle pooled connection still
///   holds a server permit.
/// - **§7** — the framing chain travels with the connection.
///
/// The pool lives in a library-level registry rather than on the `Engine`,
/// because `connectEngine` copies six fields onto the keyspace instance and
/// drops the engine. Keyspace state here is *per-instance*, which makes a
/// per-instance pool especially wrong: a Flutter app building a keyspace per
/// screen or per rebuild would create a pool per instance, and instance
/// lifetime is not something this client controls.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// How many idle connections to keep, and how long to keep them.
///
/// Defaults are deliberately conservative. An idle pooled connection still
/// holds one of the engine's connection permits (`num_workers * 200`, of which
/// the main listener receives 35%), so a large pool spread across many client
/// processes can starve the server while mostly idle. Raise these only after
/// measuring with the `queueDepths` command under realistic load.
///
/// On mobile, prefer a **short** [idleTimeout]. iOS and Android kill sockets
/// when an app is backgrounded, so every pooled connection is dead on resume
/// and the cost of discovering that is a user-visible round trip.
class PoolConfig {
  /// Maximum idle connections retained per target. Never unbounded.
  final int maxIdle;

  /// Discard an idle connection older than this.
  ///
  /// Keep it shorter than any server or firewall idle reaper so the client
  /// drops a connection before the peer does.
  final Duration idleTimeout;

  const PoolConfig({
    this.maxIdle = 8,
    this.idleTimeout = const Duration(seconds: 30),
  });

  @override
  String toString() =>
      'PoolConfig(maxIdle: $maxIdle, idleTimeout: $idleTimeout)';
}

/// One socket plus the framing chain that must travel with it.
///
/// The decoder and [LineSplitter] are built **once**, here, rather than per
/// request. `LineSplitter` holds a partial trailing line in its own internal
/// buffer, so tearing the chain down between requests would drop it — which is
/// exactly the byte loss contract §7 forbids on a pooled connection.
class PooledConnection {
  final Socket socket;
  late final StreamSubscription<String> _subscription;

  /// Frames that arrived before anyone asked for them.
  final List<String> _buffered = <String>[];
  Completer<String>? _pending;
  bool _dead = false;

  PooledConnection(this.socket) {
    _subscription = socket
        .cast<List<int>>()
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen(
          _onLine,
          onError: _onError,
          onDone: _onDone,
          cancelOnError: true,
        );
  }

  void _onLine(String line) {
    final pending = _pending;
    if (pending != null && !pending.isCompleted) {
      _pending = null;
      pending.complete(line);
      return;
    }
    _buffered.add(line);
  }

  void _onError(Object error) => _fail(error);

  void _onDone() => _fail(const SocketException('connection closed by peer'));

  void _fail(Object error) {
    _dead = true;
    final pending = _pending;
    _pending = null;
    if (pending != null && !pending.isCompleted) {
      pending.completeError(error);
    }
  }

  /// Send one request and complete with the raw text of exactly one frame.
  ///
  /// Parsing is the caller's job, which keeps this library transport-only.
  Future<String> request(Uint8ListLike query, Duration timeout) {
    if (_dead || _pending != null) {
      return Future.error(const SocketException('connection is not available'));
    }

    // A frame may already be waiting if the peer wrote ahead of us.
    if (_buffered.isNotEmpty) {
      return Future.error(
        const SocketException('connection has unconsumed data'),
      );
    }

    final completer = Completer<String>();
    _pending = completer;

    try {
      socket.add([...query, 10]);
    } catch (e) {
      _pending = null;
      _dead = true;
      return Future.error(e);
    }

    return socket
        .flush()
        .then((_) => completer.future)
        .timeout(
          timeout,
          onTimeout: () {
            _pending = null;
            _dead = true;
            socket.destroy();
            throw TimeoutException('Operation timed out', timeout);
          },
        );
  }

  /// Is this connection usable for a fresh exchange?
  ///
  /// Checked before use rather than relying on the write failing: writing to a
  /// peer-closed socket normally succeeds, and the request would then see the
  /// stream close — indistinguishable from "the engine applied the write and
  /// the response was lost", which contract §4 forbids retrying.
  ///
  /// Unlike the Rust and Python clients no polling is needed: `onDone` and
  /// `onError` on the stream subscription report a dead peer for free.
  bool get isHealthy =>
      !_dead &&
      _pending == null &&
      // Leftover frames mean a previous response was never consumed, which
      // would desynchronise the next caller's read.
      _buffered.isEmpty;

  Future<void> close() async {
    _dead = true;
    try {
      await _subscription.cancel();
    } catch (_) {
      /* already gone */
    }
    try {
      socket.destroy();
    } catch (_) {
      /* already gone */
    }
  }
}

/// Minimal structural alias so this library does not import `dart:typed_data`
/// solely for a parameter type.
typedef Uint8ListLike = List<int>;

class _IdleEntry {
  final PooledConnection connection;
  final DateTime idleSince;
  _IdleEntry(this.connection, this.idleSince);
}

/// A bounded set of idle connections for one `(host, port, useTls)` target.
class ConnectionPool {
  final PoolConfig config;
  final List<_IdleEntry> _idle = <_IdleEntry>[];

  ConnectionPool(this.config);

  /// Idle connections currently held. Test and diagnostic use.
  int get idleLength => _idle.length;

  /// Take a healthy idle connection, discarding any that aged out or died.
  Future<PooledConnection?> checkout() async {
    while (_idle.isNotEmpty) {
      final entry = _idle.removeLast();
      final agedOut =
          DateTime.now().difference(entry.idleSince) >= config.idleTimeout;
      if (agedOut || !entry.connection.isHealthy) {
        await entry.connection.close();
        continue;
      }
      return entry.connection;
    }
    return null;
  }

  /// Return a healthy connection. Callers must never return one that errored,
  /// timed out, or carried a subscription.
  Future<void> checkin(PooledConnection connection) async {
    if (!connection.isHealthy || _idle.length >= config.maxIdle) {
      await connection.close();
      return;
    }
    _idle.add(_IdleEntry(connection, DateTime.now()));
  }

  /// Drain and close every idle connection.
  Future<void> close() async {
    final entries = List<_IdleEntry>.from(_idle);
    _idle.clear();
    for (final entry in entries) {
      await entry.connection.close();
    }
  }
}

// One pool per target. `useTls` is part of the key: a plaintext and a TLS
// connection to the same address are not interchangeable, and `useTls` is
// mutable on the engine after construction — so the key must be read at request
// time, never cached at connectEngine time.
final Map<String, ConnectionPool> _pools = <String, ConnectionPool>{};

String _keyFor(String host, int port, bool useTls) => '$host:$port:$useTls';

/// The pool for this target, creating it on first use.
///
/// Returns `null` when no config is supplied, which is how pooling stays
/// opt-in: the caller then connects per request exactly as before.
ConnectionPool? getPool(
  String host,
  int port,
  bool useTls,
  PoolConfig? config,
) {
  if (config == null) return null;
  return _pools.putIfAbsent(
    _keyFor(host, port, useTls),
    () => ConnectionPool(config),
  );
}

/// Drain every pool.
///
/// Call before exit, and from a connectivity listener: switching Wi-Fi to
/// cellular invalidates every pooled connection, and dropping them proactively
/// is cheaper than discovering it one failed request at a time.
Future<void> closeAllPools() async {
  final pools = List<ConnectionPool>.from(_pools.values);
  _pools.clear();
  for (final pool in pools) {
    await pool.close();
  }
}
