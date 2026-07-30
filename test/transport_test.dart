import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:montycat/montycat.dart' show SubscriptionHandle;
import 'package:montycat/src/utils.dart' show sendData;
import 'package:test/test.dart';

Uint8List _query(String json) => Uint8List.fromList(utf8.encode(json));

void main() {
  test('sendData frames the request and parses the response envelope', () async {
    const hugeKey = '340282366920938463463374607431768211455';
    final received = Completer<List<int>>();

    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((socket) {
      socket.listen((data) {
        if (!received.isCompleted) received.complete(data);
        socket.write(
          '{"status":true,"payload":[{"__key__":"$hugeKey","__score__":0.78}],"error":null}\n',
        );
        socket.close();
      }, onError: (_) {});
    });

    final response = await sendData(
      '127.0.0.1',
      server.port,
      _query('{"raw":["list-owners"],"credentials":["owner","secret"]}'),
    );
    await server.close();

    // The client delimits every request with a trailing newline.
    final sent = await received.future;
    expect(sent.last, 10);
    expect(
      utf8.decode(sent.sublist(0, sent.length - 1)),
      '{"raw":["list-owners"],"credentials":["owner","secret"]}',
    );

    expect(response['status'], isTrue);
    expect(response['error'], isNull);
    // Keys are u128 on the server and must stay strings, not become doubles.
    expect(response['payload'][0]['__key__'], hugeKey);
    expect(response['payload'][0]['__score__'], 0.78);
  });

  test('subscription mode returns a handle and stop() ends delivery', () async {
    final events = <dynamic>[];
    final firstDelivered = Completer<void>();
    late SubscriptionHandle handle;

    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((socket) {
      socket.listen((_) {
        // One burst of three frames. Delivery of the later two is decided by the
        // handle's stopped flag, not by timing, so this cannot race on a slow runner.
        for (var i = 1; i <= 3; i++) {
          socket.write('{"status":true,"payload":{"event":$i},"error":null}\n');
        }
      }, onError: (_) {});
    });

    handle =
        await sendData(
              '127.0.0.1',
              server.port,
              _query('{"subscribe":true,"keyspace":"events","key":null}'),
              callback: (data) {
                events.add(data);
                // Stop on the first frame; the rest of the burst must be dropped.
                handle.stop();
                if (!firstDelivered.isCompleted) firstDelivered.complete();
              },
            )
            as SubscriptionHandle;

    await firstDelivered.future;
    expect(handle.stopped, isTrue);
    expect(events.single['payload']['event'], 1);

    await server.close();
  });

  // Regression: subscription mode used to be detected by searching the
  // serialized request for "subscribe", so a record whose value merely
  // contained that word was answered with a SubscriptionHandle instead of the
  // response envelope — and its socket was never closed.
  test(
    'a value containing "subscribe" is not treated as a subscription',
    () async {
      final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      server.listen((socket) {
        socket.listen((_) {
          socket.write('{"status":true,"payload":"1","error":null}\n');
          socket.close();
        }, onError: (_) {});
      });

      for (final note in [
        'hello',
        'please subscribe',
        'subscribe',
        'unsubscribe',
      ]) {
        final response = await sendData(
          '127.0.0.1',
          server.port,
          _query(
            '{"command":"insert_value","value":"{\\"note\\":\\"$note\\"}"}',
          ),
        );

        expect(
          response,
          isA<Map>(),
          reason: '"$note" must return the response envelope, not a handle',
        );
        expect(response['status'], isTrue, reason: 'note: $note');
      }

      await server.close();
    },
  );

  test('sendData surfaces connection failures instead of throwing', () async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final port = server.port;
    await server.close();

    final result = await sendData(
      '127.0.0.1',
      port,
      _query('{"raw":["ping"]}'),
    );
    expect(result, isA<SocketException>());
  });
}
