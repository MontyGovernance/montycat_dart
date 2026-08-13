import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:montycat/montycat.dart' show Engine, KeyspaceInMemory, Pointer;
import 'package:montycat/src/functions/generic.dart' show convertToBinaryQuery;
import 'package:test/test.dart';

/// Text the client must carry intact. `nfdSmaland` is the load-bearing one: in
/// decomposed form the ring above is U+030A, whose low byte is 0x0A — the very
/// byte that frames a request.
const samples = <String, String>{
  'ascii': 'plain',
  'accents': 'café',
  'cyrillic': 'Привет',
  'cjk': '日本語',
  'arabic': 'مرحبا',
  'hebrew': 'שלום',
  'greek': 'Καλημέρα',
  'thai': 'สวัสดี',
  'emoji': 'a🐱b',
  'zwj emoji': '👩‍💻',
  'nfdSmaland': 'Småland',
  'mixed': 'id-42 · Привет · 日本 · 🐱',
};

KeyspaceInMemory keyspaceFor(String store) {
  final keyspace = KeyspaceInMemory(keyspace: 'события');
  keyspace.connectEngine(
    Engine(
      host: '127.0.0.1',
      port: 21210,
      username: 'øwner',
      password: 'sécret',
      store: store,
    ),
  );
  return keyspace;
}

void main() {
  group('convertToBinaryQuery emits UTF-8', () {
    test('every sample survives a byte round-trip', () {
      for (final entry in samples.entries) {
        final bytes = convertToBinaryQuery(
          cls: keyspaceFor('магазин'),
          command: 'insert',
          value: {'text': entry.value},
        );

        // `utf8.decode` without allowMalformed throws on invalid sequences,
        // which is exactly what `.codeUnits` used to produce.
        final decoded = json.decode(utf8.decode(bytes)) as Map<String, dynamic>;
        final value = json.decode(decoded['value'] as String);

        expect(
          value['text'],
          entry.value,
          reason: 'sample "${entry.key}" did not round-trip',
        );
      }
    });

    test('the keyspace, store and credentials round-trip too', () {
      final bytes = convertToBinaryQuery(
        cls: keyspaceFor('магазин'),
        command: 'get_len',
      );
      final decoded = json.decode(utf8.decode(bytes)) as Map<String, dynamic>;

      expect(decoded['keyspace'], 'события');
      expect(decoded['store'], 'магазин');
      expect(decoded['username'], 'øwner');
      expect(decoded['password'], 'sécret');
    });

    test('search criteria round-trip, including pointers', () {
      final bytes = convertToBinaryQuery(
        cls: keyspaceFor('магазин'),
        command: 'lookup_keys',
        searchCriteria: {
          'имя': 'Привет',
          'ref': const Pointer(keyspace: 'пользователи', key: 'ключ'),
        },
      );
      final decoded = json.decode(utf8.decode(bytes)) as Map<String, dynamic>;
      final criteria = json.decode(decoded['search_criteria'] as String);

      expect(criteria['имя'], 'Привет');
      expect(criteria['pointers']['ref'], ['пользователи', 'ключ']);
    });

    test('bulk values round-trip', () {
      final bytes = convertToBinaryQuery(
        cls: keyspaceFor('магазин'),
        command: 'insert_bulk',
        bulkValues: [
          {'text': 'Привет'},
          {'text': '日本語'},
        ],
      );
      final decoded = json.decode(utf8.decode(bytes)) as Map<String, dynamic>;
      final bulk =
          (decoded['bulk_values'] as List)
              .map((v) => json.decode(v as String)['text'])
              .toList();

      expect(bulk, ['Привет', '日本語']);
    });

    test('a semantic query round-trips as raw text', () {
      final bytes = convertToBinaryQuery(
        cls: keyspaceFor('магазин'),
        command: 'semantic_search',
        semanticQuery: 'что такое 日本語 ?',
      );
      final decoded = json.decode(utf8.decode(bytes)) as Map<String, dynamic>;
      expect(decoded['search_criteria'], 'что такое 日本語 ?');
    });

    test('ASCII payloads are byte-identical to the old encoding', () {
      // The fix must not move the wire for data that already worked.
      final bytes = convertToBinaryQuery(
        cls: KeyspaceInMemory(keyspace: 'events')..connectEngine(
          Engine(
            host: '127.0.0.1',
            port: 21210,
            username: 'owner',
            password: 'secret',
            store: 'shop',
          ),
        ),
        command: 'insert',
        value: {'text': 'plain ascii'},
      );
      final asJson = utf8.decode(bytes);
      expect(bytes, asJson.codeUnits);
    });
  });

  group('request framing', () {
    test('no sample smuggles the frame delimiter into the body', () {
      for (final entry in samples.entries) {
        final bytes = convertToBinaryQuery(
          cls: keyspaceFor('магазин'),
          command: 'insert',
          value: {'text': entry.value},
        );

        // `sendData` appends 0x0A to delimit the request, so a 0x0A anywhere in
        // the body splits it. UTF-8 continuation bytes are all >= 0x80, which
        // is what makes this safe; truncated UTF-16 was not.
        expect(
          bytes.contains(10),
          isFalse,
          reason: 'sample "${entry.key}" injected a newline into the frame',
        );
      }
    });
  });

  group('end to end over a socket', () {
    test('a Cyrillic insert arrives intact and in one frame', () async {
      final received = Completer<List<int>>();
      final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      server.listen((socket) {
        socket.listen((data) {
          if (!received.isCompleted) received.complete(data);
          socket.write('{"status":true,"payload":null,"error":null}\n');
          socket.close();
        }, onError: (_) {});
      });

      final keyspace = KeyspaceInMemory(keyspace: 'события');
      keyspace.connectEngine(
        Engine(
          host: '127.0.0.1',
          port: server.port,
          username: 'owner',
          password: 'secret',
          store: 'магазин',
        ),
      );
      await keyspace.insertValue(value: {'текст': 'Привет, 世界 🐱'});
      await server.close();

      final sent = await received.future;
      expect(sent.last, 10, reason: 'request must end with the delimiter');
      expect(
        sent.sublist(0, sent.length - 1).contains(10),
        isFalse,
        reason: 'the body must not contain the delimiter',
      );

      final decoded =
          json.decode(utf8.decode(sent.sublist(0, sent.length - 1)))
              as Map<String, dynamic>;
      expect(json.decode(decoded['value'] as String), {
        'текст': 'Привет, 世界 🐱',
      });
      expect(decoded['keyspace'], 'события');
      expect(decoded['store'], 'магазин');
    });
  });
}
