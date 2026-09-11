/// Certificate verification: what it accepts, what it refuses, what it leaves
/// alone.
///
/// The fixtures are a real self-signed pair generated the way the engine's
/// `init-self-tls` generates one — same subject, same `localhost` /
/// `127.0.0.1` / `::1` SANs — so the handshake tests exercise the certificate
/// shape operators actually deploy, including the part that makes hostname
/// checking the wrong question to ask.
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:hashlib/hashlib.dart' show sha256;
import 'package:montycat/montycat.dart';
import 'package:montycat/src/utils.dart' show sendData;
import 'package:test/test.dart';

final String fixtures = '${Directory.current.path}/test/fixtures';
final String certificate = '$fixtures/cert.pem';
final String privateKey = '$fixtures/key.pem';
final String otherCertificate = '$fixtures/other_cert.pem';

String fingerprintOf(String path) {
  final String pem = File(path).readAsStringSync();
  final int start = pem.indexOf('-----BEGIN CERTIFICATE-----');
  final int end = pem.indexOf('-----END CERTIFICATE-----');
  final String body = pem
      .substring(start + '-----BEGIN CERTIFICATE-----'.length, end)
      .replaceAll(RegExp(r'\s'), '');
  return sha256.convert(Uint8List.fromList(base64.decode(body))).hex();
}

/// One TLS listener answering one JSON line, the way the engine does.
Future<SecureServerSocket> engineLikeListener() async {
  final SecurityContext context =
      SecurityContext()
        ..useCertificateChain(certificate)
        ..usePrivateKey(privateKey);

  final SecureServerSocket server = await SecureServerSocket.bind(
    '127.0.0.1',
    0,
    context,
  );

  server.listen((SecureSocket socket) {
    socket.listen(
      (_) {
        socket.add(
          utf8.encode('{"status":true,"payload":"ok","error":null}\n'),
        );
      },
      onError: (_) {},
      cancelOnError: true,
    );
  }, onError: (_) {});

  return server;
}

void main() {
  group('the default stays where it was', () {
    test('no settings at all means encryption without checking', () {
      // Every existing caller passes only `useTls`. Turning verification on for
      // them would break every deployment running the engine's own self-signed
      // certificate, which is every default deployment.
      final engine = Engine(
        host: '127.0.0.1',
        port: 21210,
        username: 'user',
        password: 'password',
        useTls: true,
      );

      expect(engine.tls, isNull);
    });

    test('settings appear only when something was asked for', () {
      final engine = Engine(
        host: '127.0.0.1',
        port: 21210,
        username: 'user',
        password: 'password',
        useTls: true,
        certificatePath: certificate,
      );

      expect(engine.tls, isNotNull);
      expect(engine.tls!.pinned, isTrue);
    });
  });

  group('turning verification on', () {
    test('a pin implies verification', () {
      // Nobody should have to pass two arguments to say one thing.
      final settings = TlsSettings(certificatePath: certificate);

      expect(settings.verification, isTrue);
      expect(settings.pinned, isTrue);
    });

    test('verification without a pin defers to the platform', () {
      // The proxy-with-a-real-certificate case: nothing to compare against, so
      // the ordinary rules apply — a chain to a public root, and a hostname
      // that matches.
      final settings = TlsSettings(verification: true);

      expect(settings.pinned, isFalse);
      expect(settings.defersToPlatform, isTrue);
    });

    test('a pin does not defer to the platform', () {
      // The engine's certificate names localhost, 127.0.0.1 and ::1 only. An
      // operator pointing at a LAN address would fail hostname verification
      // with nothing actually wrong, and the comparison has already answered
      // the question that matters.
      expect(TlsSettings(certificatePath: certificate).defersToPlatform, isFalse);
    });

    test('a fingerprint is accepted in the shape openssl prints it', () {
      final String digest = fingerprintOf(certificate);
      final StringBuffer colonSeparated = StringBuffer();
      for (int index = 0; index < digest.length; index += 2) {
        if (index > 0) colonSeparated.write(':');
        colonSeparated.write(digest.substring(index, index + 2));
      }

      final settings = TlsSettings(
        certificateFingerprint: colonSeparated.toString().toUpperCase(),
      );

      expect(settings.pinned, isTrue);
      // Same certificate, whichever way it was named.
      expect(
        settings.poolKey(),
        equals(TlsSettings(certificatePath: certificate).poolKey()),
      );
    });

    test('a fingerprint that cannot be one is refused', () {
      for (final String value in ['', 'not-a-fingerprint', 'ab99cf', 'z' * 64]) {
        expect(
          () => TlsSettings(certificateFingerprint: value),
          throwsA(isA<ArgumentError>()),
          reason: 'accepted $value',
        );
      }
    });
  });

  group('combinations that cannot mean anything', () {
    test('a pin with verification switched off is a contradiction', () {
      expect(
        () => TlsSettings(verification: false, certificatePath: certificate),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('verifying a plaintext connection is refused', () {
      // Ignoring this quietly would leave someone believing a connection is
      // checked when it is not even encrypted.
      expect(
        () => TlsSettings(certificatePath: certificate).assertUsableWith(false),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('a fingerprint that disagrees with the file is refused', () {
      expect(
        () => TlsSettings(
          certificatePath: certificate,
          certificateFingerprint: fingerprintOf(otherCertificate),
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('an unreadable certificate fails where it was configured', () {
      expect(
        () => TlsSettings(certificatePath: '/nonexistent/montycat/cert.pem'),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('a file that is not a certificate is named as such', () {
      final File junk = File(
        '${Directory.systemTemp.path}/montycat-not-a-cert-$pid.pem',
      )..writeAsStringSync('just some text\n');

      try {
        expect(
          () => TlsSettings(certificatePath: junk.path),
          throwsA(isA<ArgumentError>()),
        );
      } finally {
        junk.deleteSync();
      }
    });
  });

  group('against a real listener', () {
    late SecureServerSocket server;

    setUp(() async {
      server = await engineLikeListener();
    });

    tearDown(() async {
      await server.close();
      await closeAllPools();
    });

    test('the expected certificate completes a handshake', () async {
      final response = await sendData(
        '127.0.0.1',
        server.port,
        Uint8List.fromList(utf8.encode('{"raw":["version"],"credentials":[]}')),
        useTls: true,
        tls: TlsSettings(certificatePath: certificate),
      );

      expect(response, isA<Map>());
      expect((response as Map)['status'], isTrue);
    });

    test('a different certificate is refused', () async {
      // Errors are returned rather than thrown on this client, which is what
      // every other connection failure does here too.
      final response = await sendData(
        '127.0.0.1',
        server.port,
        Uint8List.fromList(utf8.encode('{"raw":["version"],"credentials":[]}')),
        useTls: true,
        tls: TlsSettings(certificatePath: otherCertificate),
      );

      expect(response, isA<TlsVerificationException>());
      // The message has to name what actually arrived: a regenerated
      // certificate is the common cause, and the operator needs the new value
      // in order to update the pin.
      expect(
        (response as TlsVerificationException).message,
        contains(fingerprintOf(certificate)),
      );
    });

    test('a fingerprint pin reaches the same verdict as a file pin', () async {
      final good = await sendData(
        '127.0.0.1',
        server.port,
        Uint8List.fromList(utf8.encode('{"raw":["version"],"credentials":[]}')),
        useTls: true,
        tls: TlsSettings(certificateFingerprint: fingerprintOf(certificate)),
      );
      expect((good as Map)['status'], isTrue);

      final bad = await sendData(
        '127.0.0.1',
        server.port,
        Uint8List.fromList(utf8.encode('{"raw":["version"],"credentials":[]}')),
        useTls: true,
        tls: TlsSettings(
          certificateFingerprint: fingerprintOf(otherCertificate),
        ),
      );
      expect(bad, isA<TlsVerificationException>());
    });

    test('an unverified connection still reaches a self-signed engine', () async {
      // The default path, and the reason it is still the default.
      final response = await sendData(
        '127.0.0.1',
        server.port,
        Uint8List.fromList(utf8.encode('{"raw":["version"],"credentials":[]}')),
        useTls: true,
      );

      expect((response as Map)['status'], isTrue);
    });

    test('a pin is enforced on pooled connections too', () async {
      // Two code paths open sockets here. A pin enforced on one and forgotten
      // on the other would be worse than no pin, because it would look like it
      // was working.
      final response = await sendData(
        '127.0.0.1',
        server.port,
        Uint8List.fromList(utf8.encode('{"raw":["version"],"credentials":[]}')),
        useTls: true,
        tls: TlsSettings(certificatePath: otherCertificate),
        poolConfig: const PoolConfig(),
      );

      expect(response, isA<TlsVerificationException>());
    });
  });
}
