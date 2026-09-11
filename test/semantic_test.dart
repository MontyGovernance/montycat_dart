import 'package:montycat/montycat.dart';
import 'package:test/test.dart';

void main() {
  test('parses semantic status and keyspace readback', () {
    final status = SemanticStatus.fromJson({
      'globally_enabled': true,
      'reloading': true,
      'indexing': {
        'live_queue': 3,
        'backfill_queue': 2,
        'backfill_in_flight': 1,
      },
      'default_model': 'bge-small',
      'default_field': null,
      'keyspaces': {
        'catalog/products': {
          'enrolled': true,
          'model': 'bge-base',
          'dimensions': 768,
          'field': 'description',
          'persistent': true,
          'backfill_pending': true,
        },
      },
    });
    final keyspace = status.keyspace('catalog', 'products');
    expect(status.globallyEnabled, isTrue);
    expect(status.reloading, isTrue);
    expect(status.indexing.liveQueue, 3);
    expect(status.indexing.backfillQueue, 2);
    expect(status.indexing.backfillInFlight, 1);
    expect(status.defaultModel, 'bge-small');
    expect(keyspace?.model, 'bge-base');
    expect(keyspace?.dimensions, 768);
    expect(keyspace?.backfillPending, isTrue);
  });

  test('parses re-embed result', () {
    final result = SemanticReembedResult.fromJson({
      'scope': 'catalog/products',
      'changed': true,
      'previous_model': 'bge-small',
      'model': 'bge-base',
      'dimensions': 768,
      'field': null,
      'backfill_started': true,
    });
    expect(result.previousModel, 'bge-small');
    expect(result.model, 'bge-base');
    expect(result.backfillStarted, isTrue);
  });
}
