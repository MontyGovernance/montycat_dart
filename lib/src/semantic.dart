class SemanticKeyspaceStatus {
  final bool enrolled;
  final String model;
  final int dimensions;
  final String? field;
  final bool persistent;
  final bool backfillPending;

  const SemanticKeyspaceStatus({
    required this.enrolled,
    required this.model,
    required this.dimensions,
    required this.field,
    required this.persistent,
    required this.backfillPending,
  });

  factory SemanticKeyspaceStatus.fromJson(Map<dynamic, dynamic> json) {
    return SemanticKeyspaceStatus(
      enrolled: json['enrolled'] == true,
      model: json['model']?.toString() ?? '',
      dimensions: (json['dimensions'] as num?)?.toInt() ?? 0,
      field: json['field']?.toString(),
      persistent: json['persistent'] == true,
      backfillPending: json['backfill_pending'] == true,
    );
  }
}

class SemanticStatus {
  final bool globallyEnabled;
  final String defaultModel;
  final String? defaultField;
  final Map<String, SemanticKeyspaceStatus> keyspaces;

  const SemanticStatus({
    required this.globallyEnabled,
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
