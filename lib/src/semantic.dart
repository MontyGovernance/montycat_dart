class SemanticKeyspaceStatus {
  final bool enrolled;
  final String model;
  final String source;
  final String? embeddingSpace;
  final int dimensions;
  final String? field;
  final bool persistent;
  final bool backfillPending;

  const SemanticKeyspaceStatus({
    required this.enrolled,
    required this.model,
    required this.source,
    required this.embeddingSpace,
    required this.dimensions,
    required this.field,
    required this.persistent,
    required this.backfillPending,
  });

  factory SemanticKeyspaceStatus.fromJson(Map<dynamic, dynamic> json) {
    return SemanticKeyspaceStatus(
      enrolled: json['enrolled'] == true,
      model: json['model']?.toString() ?? '',
      source: json['source']?.toString() ?? 'onboard',
      embeddingSpace: json['embedding_space']?.toString(),
      dimensions: (json['dimensions'] as num?)?.toInt() ?? 0,
      field: json['field']?.toString(),
      persistent: json['persistent'] == true,
      backfillPending: json['backfill_pending'] == true,
    );
  }
}

/// Current semantic indexing work reported by the engine.
class SemanticIndexingStatus {
  final int liveQueue;
  final int backfillQueue;
  final int backfillInFlight;

  const SemanticIndexingStatus({
    required this.liveQueue,
    required this.backfillQueue,
    required this.backfillInFlight,
  });

  factory SemanticIndexingStatus.fromJson(Map<dynamic, dynamic>? json) {
    return SemanticIndexingStatus(
      liveQueue: (json?['live_queue'] as num?)?.toInt() ?? 0,
      backfillQueue: (json?['backfill_queue'] as num?)?.toInt() ?? 0,
      backfillInFlight: (json?['backfill_in_flight'] as num?)?.toInt() ?? 0,
    );
  }
}

class SemanticStatus {
  final bool globallyEnabled;
  final bool reloading;
  final SemanticIndexingStatus indexing;
  final String defaultModel;
  final String? defaultField;
  final Map<String, SemanticKeyspaceStatus> keyspaces;

  const SemanticStatus({
    required this.globallyEnabled,
    required this.reloading,
    required this.indexing,
    required this.defaultModel,
    required this.defaultField,
    required this.keyspaces,
  });

  factory SemanticStatus.fromJson(Map<dynamic, dynamic> json) {
    final keyspaces = <String, SemanticKeyspaceStatus>{};
    final rawKeyspaces = json['keyspaces'];
    if (rawKeyspaces is Map) {
      rawKeyspaces.forEach((key, value) {
        if (value is Map) {
          keyspaces[key.toString()] = SemanticKeyspaceStatus.fromJson(value);
        }
      });
    }
    return SemanticStatus(
      globallyEnabled: json['globally_enabled'] == true,
      reloading: json['reloading'] == true,
      indexing: SemanticIndexingStatus.fromJson(
        json['indexing'] is Map ? json['indexing'] : null,
      ),
      defaultModel: json['default_model']?.toString() ?? '',
      defaultField: json['default_field']?.toString(),
      keyspaces: keyspaces,
    );
  }

  SemanticKeyspaceStatus? keyspace(String store, String keyspace) =>
      keyspaces['$store/$keyspace'];
}

class SemanticReembedResult {
  final String scope;
  final bool changed;
  final String previousModel;
  final String model;
  final int dimensions;
  final String? field;
  final bool backfillStarted;

  const SemanticReembedResult({
    required this.scope,
    required this.changed,
    required this.previousModel,
    required this.model,
    required this.dimensions,
    required this.field,
    required this.backfillStarted,
  });

  factory SemanticReembedResult.fromJson(Map<dynamic, dynamic> json) {
    return SemanticReembedResult(
      scope: json['scope']?.toString() ?? '',
      changed: json['changed'] == true,
      previousModel: json['previous_model']?.toString() ?? '',
      model: json['model']?.toString() ?? '',
      dimensions: (json['dimensions'] as num?)?.toInt() ?? 0,
      field: json['field']?.toString(),
      backfillStarted: json['backfill_started'] == true,
    );
  }
}
