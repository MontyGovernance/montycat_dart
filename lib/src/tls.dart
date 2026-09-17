/// Client-side TLS trust: what this client requires of the engine that answers.
///
/// The CLI that ships with the engine can pin the engine's certificate exactly,
/// because it reads the very file the listener loaded — same package, same
/// host. A client library has no such luxury. It runs somewhere else entirely,
/// and the only trust material it has is whatever the operator copied over.
///
/// So verification is opt-in, and accepts whichever form of that material the
/// operator actually has:
///
/// - [TlsSettings.certificatePath] — the engine's certificate, copied to the
///   client host.
/// - [TlsSettings.certificateFingerprint] — its SHA-256 digest, which travels
///   in an environment variable and needs no file. Read one with:
///
///   ```sh
///   openssl x509 -in server.crt -noout -fingerprint -sha256
///   ```
///
/// - neither, with `verification: true` — the platform trust store with
///   ordinary hostname checking, for an engine behind a proxy holding a
///   certificate from a real CA.
///
/// Both pinning forms compare the certificate the engine presents against the
/// one expected, byte for byte, and skip hostname checking: the engine's
/// self-signed certificate carries only `localhost`, `127.0.0.1` and `::1` as
/// subject alternative names unless it was regenerated with
/// `init-self-tls dns/ip`, so requiring a hostname match would reject a
/// perfectly good certificate for the wrong reason. Identity is already
/// answered exactly by the comparison.
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:hashlib/hashlib.dart' show sha256;

const String _beginCertificate = '-----BEGIN CERTIFICATE-----';
const String _endCertificate = '-----END CERTIFICATE-----';

/// The engine presented a certificate other than the expected one.
///
/// Thrown during connection setup, so it surfaces the way any other connection
/// failure does on this client — `sendData` returns it rather than throwing it
/// on to the caller.
class TlsVerificationException implements Exception {
  TlsVerificationException(this.message);

  final String message;

  @override
  String toString() => 'TlsVerificationException: $message';
}

/// Accepts a SHA-256 fingerprint in any of the shapes tools print it in.
///
/// `openssl` emits colon-separated uppercase, some dashboards emit bare
/// lowercase, and a value pasted from either should work without the operator
/// having to reformat it.
String normalizeFingerprint(
  String value, [
  String source = 'certificateFingerprint',
]) {
  final String cleaned = value
      .trim()
      .replaceAll(':', '')
      .replaceAll(' ', '')
      .replaceAll('-', '')
      .toLowerCase();

  final bool hexadecimal = RegExp(r'^[0-9a-f]{64}$').hasMatch(cleaned);
  if (!hexadecimal) {
    throw ArgumentError.value(
      value,
      source,
      'must be a SHA-256 fingerprint (64 hex characters, optionally '
          'colon-separated)',
    );
  }

  return cleaned;
}

/// The first certificate in a PEM file.
///
/// A file may hold a chain. The engine presents its leaf first, so the leaf is
/// what a pin compares against.
Uint8List _firstCertificateDer(String pem, String source) {
  final int start = pem.indexOf(_beginCertificate);
  final int end =
      start == -1
          ? -1
          : pem.indexOf(_endCertificate, start + _beginCertificate.length);

  if (start == -1 || end == -1) {
    throw ArgumentError('no PEM certificate found in $source');
  }

  final String body = pem.substring(start + _beginCertificate.length, end);

  try {
    return Uint8List.fromList(
      base64.decode(body.replaceAll(RegExp(r'\s'), '')),
    );
  } on FormatException catch (error) {
    throw ArgumentError('could not parse the certificate at $source: $error');
  }
}

/// What this client requires of the engine's certificate.
///
/// Built for you by `Engine`; construct one directly only when calling
/// `sendData` yourself.
///
/// Reading and parsing happen here rather than at first request, so a
/// misconfiguration fails where it was written.
class TlsSettings {
  /// Creates trust requirements.
  ///
  /// [verification] left null means "whatever the other arguments imply": on
  /// when a pin is given, off otherwise — which keeps existing TLS callers
  /// working unchanged. Passing `false` alongside a pin is a contradiction and
  /// throws.
  ///
  /// Throws [ArgumentError] if the combination cannot mean anything coherent,
  /// or if the certificate file cannot be read or parsed.
  factory TlsSettings({
    bool? verification,
    String? certificatePath,
    String? certificateFingerprint,
  }) {
    final bool pinned =
        certificatePath != null || certificateFingerprint != null;

    if (verification == false && pinned) {
      throw ArgumentError(
        'verification: false contradicts certificatePath / '
        'certificateFingerprint. Drop the pin to connect without verification, '
        'or drop the argument to verify against the pin.',
      );
    }

    String? expectedFingerprint;
    if (certificateFingerprint != null) {
      expectedFingerprint = normalizeFingerprint(certificateFingerprint);
    }

    Uint8List? expectedDer;
    if (certificatePath != null) {
      final File file = File(certificatePath);
      final String pem;
      try {
        pem = file.readAsStringSync();
      } on FileSystemException catch (error) {
        throw ArgumentError(
          'could not read certificatePath $certificatePath: ${error.message}',
        );
      }

      expectedDer = _firstCertificateDer(pem, certificatePath);
      final String loaded = sha256.convert(expectedDer).hex();

      // Both forms given: they must agree, or the operator believes something
      // about this connection that is not true.
      if (expectedFingerprint != null && expectedFingerprint != loaded) {
        throw ArgumentError(
          'certificateFingerprint does not match the certificate at '
          '$certificatePath (that file is $loaded)',
        );
      }

      expectedFingerprint = loaded;
    }

    return TlsSettings._(
      verification: verification ?? pinned,
      certificatePath: certificatePath,
      expectedDer: expectedDer,
      expectedFingerprint: expectedFingerprint,
    );
  }

  TlsSettings._({
    required this.verification,
    required this.certificatePath,
    required Uint8List? expectedDer,
    required String? expectedFingerprint,
  }) : _expectedDer = expectedDer,
       _expectedFingerprint = expectedFingerprint;

  /// Whether the engine's certificate is checked at all.
  final bool verification;

  /// The pinned certificate's location, when it came from a file.
  final String? certificatePath;

  final Uint8List? _expectedDer;
  final String? _expectedFingerprint;

  /// Is identity decided by comparison rather than by a trust store?
  bool get pinned => _expectedFingerprint != null;

  /// Rejects requirements that cannot be met on a plaintext connection.
  ///
  /// Ignoring this quietly would leave someone believing a connection is
  /// checked when it is not even encrypted.
  void assertUsableWith(bool useTls) {
    if (!useTls && verification) {
      throw ArgumentError(
        'certificate verification requires TLS; pass useTls: true as well '
        '(there is nothing to verify on a plaintext connection)',
      );
    }
  }

  /// The part of a pool's identity that trust decides.
  ///
  /// Connections with different trust requirements are not interchangeable, so
  /// two engines pointing at one address with different pins must not share
  /// pooled connections.
  String poolKey() => '$verification:${_expectedFingerprint ?? ''}';

  /// Should the handshake itself decide, rather than this class?
  ///
  /// True only for the trust-store path, where the platform's own chain and
  /// hostname checks are exactly what is wanted.
  bool get defersToPlatform => verification && !pinned;

  /// Checks the certificate the engine presented against the pin.
  ///
  /// A no-op unless pinning: the other modes were already decided during the
  /// handshake.
  void verifyPeer(X509Certificate? presented) {
    if (!pinned) return;

    if (presented == null) {
      throw TlsVerificationException(
        'the engine presented no certificate to compare against the pin',
      );
    }

    final Uint8List der = Uint8List.fromList(presented.der);
    final Uint8List? expected = _expectedDer;

    if (expected != null) {
      if (_sameBytes(der, expected)) return;
    } else if (sha256.convert(der).hex() == _expectedFingerprint) {
      return;
    }

    // Naming what actually arrived is what makes this fixable: the usual cause
    // is a regenerated certificate, not an attack, and the operator needs the
    // new value in order to update the pin.
    throw TlsVerificationException(
      'the engine presented a certificate that is not the expected one '
      '(expected $_expectedFingerprint, got ${sha256.convert(der).hex()}). '
      'Update the pin if the engine\'s certificate was regenerated, or check '
      'what is listening on this port.',
    );
  }

  static bool _sameBytes(Uint8List left, Uint8List right) {
    if (left.length != right.length) return false;
    for (int index = 0; index < left.length; index++) {
      if (left[index] != right[index]) return false;
    }
    return true;
  }

  @override
  String toString() =>
      pinned
          ? 'TlsSettings(pinned: $_expectedFingerprint)'
          : 'TlsSettings(verification: $verification)';
}
