import '../classes/kv.dart';

class KeyspaceInMemory extends KV {
  KeyspaceInMemory({
    required this.keyspace
  });

  String keyspace;
  bool persistent = false;
  bool distributed = false;
}
