import 'package:montycat_dart/source.dart';

void main() {
  final engine = Engine(
    host: 'localhost',
    port: 21210,
    username: 'user',
    password: 'pass',
  );

  print('Engine created with host: ${engine.host}, port: ${engine.port}');
  print('Username: ${engine.username}');
  print('Password: ${engine.password}');
  print('Store: ${engine.store ?? "No store specified"}');
  print('Valid permissions: ${Engine.validPermissions}');
}
