import '../classes/kv.dart';

class KeyspacePersistent extends KV {
  KeyspacePersistent({
    required this.keyspace
  });

  String keyspace;
  bool persistent = true;
  bool distributed = false;
}
