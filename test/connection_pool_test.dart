/// Connection pooling behaviour.
///
/// Covers the required matrix in
/// `montycat_semantic/CLIENT_CONNECTION_POOLING_CONTRACT.md` §9. Stub servers
/// throughout; no live engine required.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:montycat/montycat.dart';
// `getPool` is internal to the package; reached directly so the registry keying
// can be asserted without opening sockets.
import 'package:montycat/src/pool.dart' show getPool;
import 'package:test/test.dart';

const ok = '{"status":true,"payload":null,"error":null}\n';

/// A newline-framed stub that serves many requests per connection.
class StubServer {
  late ServerSocket _server;
  int accepts = 0;
  final List<Socket> _live = <Socket>[];

  /// Per-connection response builder. Returning null closes without replying.
  final String? Function(int connection, int served)? responder;

  /// Serve this many requests on connection [i], then hang up.
  final int Function(int connection)? closeAfter;

  StubServer({this.responder, this.closeAfter});

  int get port => _server.port;

  Future<void> start() async {
    _server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    _server.listen((socket) {
      final index = accepts++;
      var served = 0;
      _live.add(socket);
      socket
          .cast<List<int>>()
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen(
            (_) {
              final body = responder?.call(index, served) ?? ok;
              served++;
              // ignore: unnecessary_null_comparison
              if (responder != null && responder!(index, served - 1) == null) {
                socket.destroy();
                return;
              }
              socket.write(body);
              if (closeAfter != null && served >= closeAfter!(index)) {
                socket.close();
              }
            },
            onError: (_) {},
            cancelOnError: true,
          );
    });
  }

  Future<void> stop() async {
    // Drain client-side pools first: a pooled connection stays open by design,
    // so closing the server while the pool holds one would hang the test.
    await closeAllPools();
    for (final socket in _live) {
      try {
        socket.destroy();
      } catch (_) {}
    }
    await _server.close();
  }
}

Engine engineFor(int port, {PoolConfig? pool}) => Engine(
  host: '127.0.0.1',
  port: port,
  username: 'owner',
  password: 'secret',
  store: 'orders',
  pool: pool,
);

void main() {
  tearDown(() async => closeAllPools());

  test('pooling is off by default and opens a connection per request', () async {
    final stub = StubServer();
    await stub.start();
    try {
      final engine = engineFor(stub.port);
      for (var i = 0; i < 5; i++) {
        await engine.listOwners();
      }
      expect(stub.accepts, 5, reason: 'unpooled engine must not reuse connections');
    } finally {
      await stub.stop();
    }
  });

  test('sequential requests reuse one pooled connection', () async {
    final stub = StubServer();
    await stub.start();
    try {
      final engine = engineFor(stub.port, pool: const PoolConfig());
      for (var i = 0; i < 10; i++) {
        await engine.listOwners();
      }
      expect(stub.accepts, 1, reason: '10 requests should share one connection');
    } finally {
      await stub.stop();
    }
  });

  test('two engines against one server share a pool', () async {
    // The registry is keyed by (host, port, useTls), so a second engine — or a
    // second keyspace instance, which is the real Flutter hazard — reuses the
    // same connections instead of opening its own.
    final stub = StubServer();
    await stub.start();
    try {
      final a = engineFor(stub.port, pool: const PoolConfig());
      final b = engineFor(stub.port, pool: const PoolConfig());
      await a.listOwners();
      await b.listOwners();
      expect(stub.accepts, 1, reason: 'the second engine opened its own connection');
    } finally {
      await stub.stop();
    }
  });

  test('connections older than the idle timeout are discarded', () async {
    final stub = StubServer();
    await stub.start();
    try {
      final engine = engineFor(
        stub.port,
        pool: const PoolConfig(idleTimeout: Duration(milliseconds: 50)),
      );
      await engine.listOwners();
      expect(stub.accepts, 1);
      await Future<void>.delayed(const Duration(milliseconds: 120));
      await engine.listOwners();
      expect(stub.accepts, 2, reason: 'an expired connection was reused');
    } finally {
      await stub.stop();
    }
  });

  test('a server-closed idle connection is replaced without replaying', () async {
    // The stale-socket case. The mechanism is not a failing write — writing to a
    // peer-closed socket normally succeeds. Dart surfaces the close through the
    // stream's onDone, so the connection is known dead before it is handed out.
    final stub = StubServer(closeAfter: (i) => i == 0 ? 1 : 1 << 30);
    await stub.start();
    try {
      final engine = engineFor(stub.port, pool: const PoolConfig());
      final first = await engine.listOwners();
      expect(first, isA<Map>());

      await Future<void>.delayed(const Duration(milliseconds: 80));

      final second = await engine.listOwners();
      expect(second, isA<Map>(), reason: 'stale connection not replaced cleanly: $second');
      expect(stub.accepts, 2, reason: 'expected exactly one fresh connection');
    } finally {
      await stub.stop();
    }
  });

  test('no bytes leak between two requests on one pooled connection', () async {
    final stub = StubServer(
      responder: (_, served) => '{"status":true,"payload":"response-$served"}\n',
    );
    await stub.start();
    try {
      final engine = engineFor(stub.port, pool: const PoolConfig());
      final first = await engine.listOwners() as Map;
      final second = await engine.listOwners() as Map;
      expect(stub.accepts, 1, reason: 'the requests did not share a connection');
      expect(first['payload'], 'response-0');
      expect(second['payload'], 'response-1');
    } finally {
      await stub.stop();
    }
  });

  test('the LineSplitter partial line survives across two pooled requests', () async {
    // The failure this client is most exposed to: `LineSplitter` holds a partial
    // trailing line in its own buffer, so rebuilding the chain per request would
    // drop it. The stub answers the first request in two writes, splitting the
    // JSON mid-token, then answers the second normally.
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    var accepts = 0;
    try {
      server.listen((socket) {
        accepts++;
        var served = 0;
        socket
            .cast<List<int>>()
            .transform(utf8.decoder)
            .transform(const LineSplitter())
            .listen((_) async {
              if (served == 0) {
                socket.write('{"status":true,"pay');
                await socket.flush();
                await Future<void>.delayed(const Duration(milliseconds: 20));
                socket.write('load":"split"}\n');
              } else {
                socket.write('{"status":true,"payload":"second"}\n');
              }
              served++;
            }, onError: (_) {}, cancelOnError: true);
      });

      final engine = engineFor(server.port, pool: const PoolConfig());
      final first = await engine.listOwners() as Map;
      final second = await engine.listOwners() as Map;

      expect(first['payload'], 'split', reason: 'a split frame was not reassembled');
      expect(second['payload'], 'second', reason: 'the second exchange was corrupted');
      expect(accepts, 1, reason: 'the connection was not reused');

    } finally {
      await closeAllPools();
      await server.close();
    }
  });

  test('a response larger than one chunk is reassembled', () async {
    final filler = 'x' * (600 * 1024);
    final stub = StubServer(
      responder: (_, _) => '{"status":true,"payload":"$filler"}\n',
    );
    await stub.start();
    try {
      final engine = engineFor(stub.port, pool: const PoolConfig());
      final result = await engine.listOwners() as Map;
      expect((result['payload'] as String).length, 600 * 1024,
          reason: 'large response was truncated');
    } finally {
      await stub.stop();
    }
  });

  test('closeAllPools drains idle connections', () async {
    final stub = StubServer();
    await stub.start();
    try {
      final engine = engineFor(stub.port, pool: const PoolConfig());
      await engine.listOwners();
      await closeAllPools();
      // A drained pool means the next request must reconnect.
      await engine.listOwners();
      expect(stub.accepts, 2, reason: 'closeAllPools left a connection in use');
    } finally {
      await stub.stop();
    }
  });

  test('tls is part of the registry key', () async {
    // A plaintext and a TLS connection to one address are not interchangeable,
    // and `useTls` is mutable on the engine after construction — so the key is
    // read at request time rather than cached at connectEngine time.
    await closeAllPools();
    const config = PoolConfig();
    final plain = getPool('127.0.0.1', 21210, false, config);
    final secure = getPool('127.0.0.1', 21210, true, config);
    final plainAgain = getPool('127.0.0.1', 21210, false, config);

    expect(identical(plain, secure), isFalse,
        reason: 'a TLS engine would reuse a plaintext connection');
    expect(identical(plain, plainAgain), isTrue,
        reason: 'the same target must resolve to the same pool');
  });

  test('no config means no pool', () {
    expect(getPool('127.0.0.1', 21210, false, null), isNull);
  });
}
