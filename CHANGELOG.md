## 1.1.3 - 2026-07-31

Adds a way to read the server's real semantic configuration, and a safe way to
change an enrolled keyspace's embedding model. Additive — upgrading from 1.1.2
requires no code changes.

### Added

- `Engine.getSemanticStatus({store, keyspace})` returns the server's actual
  semantic settings rather than what the caller assumed: the DB-wide switch and
  default model, plus each enrolled keyspace's model, dimensions, field,
  storage type, and whether a backfill is still pending.
- `Engine.reembedSemanticSearch({model, field, store, keyspace})` atomically
  drops one keyspace's vectors, records the new configuration, and starts a
  complete backfill. It reports the previous model alongside the new one, so a
  caller can confirm what it replaced.
- `SemanticStatus`, `SemanticKeyspaceStatus`, and `SemanticReembedResult` are
  exported from `package:montycat/montycat.dart`. The two new methods return
  these types instead of an untyped map, and throw `StateError` when the server
  reports failure. `SemanticStatus.keyspace(store, keyspace)` looks up one
  entry without building the `'store/keyspace'` key by hand.

### Changed

- Documented that `enableSemanticSearch` leaves an already-enrolled keyspace
  alone, and rejects an explicitly different model or field. It was never a way
  to switch models; `reembedSemanticSearch` is. Behavior is unchanged — only
  the documentation was misleading.
- Corrected the `disableSemanticSearch` docs: `dropVectors` is not "required
  before switching to a different embedding model". Use
  `reembedSemanticSearch`, which does not leave the keyspace unsearchable in
  between.

## 1.1.2 - 2026-07-29

Fixes misrouted requests whose payload contains the word `subscribe`.
**Upgrade from 1.1.1 is recommended.**

### Fixed

- **A request whose value contained the substring `subscribe` returned a
  `SubscriptionHandle` instead of the response envelope, and leaked its
  socket.** Subscription mode was detected by searching the serialized request
  for `subscribe`, so a call like
  `insertValue(value: {'note': 'please subscribe'})` took the streaming branch:
  it resolved to a handle rather than `{status, payload, error}`, so reading
  `res['status']` threw, and the socket was never closed. Any record mentioning
  the word was affected, `unsubscribe` included.

  A request is now a subscription because the caller supplied a `callback`.
  That was always the real distinction: `subscribe` is the only method that
  passes one. Intent is no longer inferred from user data.

  Present in every release before this one. The Rust and Python clients carry
  the same defect — where it hangs instead of mistyping — and are fixed in their
  matching releases; the Node client was already correct.

- Note for anyone calling `subscribe()` without a `callback`: that now performs
  a single request/response and returns the parsed envelope, rather than
  returning a handle that never fired. Such a call did nothing useful before.

## 1.1.1 - 2026-07-29

Documentation, tests, and CI only — no library code changed, so upgrading from
1.1.0 is optional.

### Added

- README sections for behavior that was previously undocumented: response shape
  (`{status, payload, error}` and u128 keys arriving as strings), real-time
  subscriptions with `SubscriptionHandle` and the `port + 1` subscription port,
  TLS via `useTls`, and owner/access management with `createOwner`, `grantTo`,
  `revokeFrom`, and `Permission`.
- Socket-level transport tests covering request framing, response envelope
  parsing, u128 key preservation, subscription delivery and `stop()`, and
  connection failures being returned rather than thrown.
- `ci.yml` workflow running analyze, `dart pub publish --dry-run`, and the test
  suite on Linux, macOS, and Windows against the stable and beta SDKs.
- Changelog link in the README.

### Changed

- The publish workflow now runs `dart analyze --fatal-infos` and `dart test`
  before publishing to pub.dev.

### Fixed

- The README installation snippet pinned `^1.0.10`, a version that predates the
  governance APIs documented further down the same file.

### Added

- Data-mesh governance policy APIs on `Engine`:
  - inspection: `policyView`, `policyHistory`, `policyExplain`, `policyExport`
  - mutation: `policyGrant`, `policyRevoke`, `policyDeny`, `policyRemoveDenial`
  - dry runs: `policyPreviewGrant`, `policyPreviewRevoke`
  - manifests: `policyValidate`, `policyPlan`, `policyApply`
- `PolicyCapability`, `PolicyKeyspaceType`, `SemanticModel`, and `PolicyFormat`,
  exported from the package root.
- Keyspace-scoped semantic enrollment and removal through `keyspace` on
  `enableSemanticSearch` and `disableSemanticSearch`.

### Changed

- Governance qualifiers are validated client-side before sending a command:
  - semantic models apply to `provisionKeyspace` and `manageSemantic`
  - storage types apply to `provisionKeyspace`, `removeKeyspace`, `manageSchema`,
    `manageAccess`, and `manageSemantic`; `manageSnapshots` is always in-memory
- `provisionKeyspace` is treated as a store-level capability, so its policy commands
  omit `keyspace`.

## 1.0.1
- Initial release.

## 1.0.2
- Fixed Dart Pub compliance issues.

## 1.0.3
- Published to pub.dev.

## 1.0.4
- Added support for nullable fields using built-in schema mechanics.

## 1.0.5
- Cache value fix
- Populate errors directly if connection is unsuccesfull

## 1.0.6
- Fixed empty string insertion
- Added documentation
- Added changelog
- Added GitHub link
- Fixed bulk custom keys conversion
- Added retrieval by volume in getBulk() function for both types of keyspaces

## 1.0.7
- Fixed connection timeout

## 1.0.8
- Fixed bulk write
- Fixed bulk read

## 1.0.9
- Stateless query refactor: removed shared `command` and `limitOutput` mutable fields from `KV` base class; both are now passed as explicit local parameters to `convertToBinaryQuery`, eliminating state-related bugs in concurrent usage.
- `subscribe` API unified: moved from `KeyspacePersistent`-only into the `KV` base class, making it available on both keyspace types. Added `subscriptionPort` parameter to override the default port. Added validation to reject providing both `key` and `customKey` simultaneously.
- `listAllDependingKeys`: added validation to reject providing both `key` and `customKey` simultaneously.
- `get_value`: removed mutual-exclusivity restriction between `withPointers` and `pointersMetadata` — both can now be used together.
- `createKeyspace` / `updateCacheAndCompression`: `cache` and `compression` moved from class-level fields to method parameters; `updateCacheAndCompression` no longer throws when called on a non-persistent keyspace.

## 1.0.10
- Added AI semantic (vector) search support for the Montycat **Semantic** server edition:
  - `enableSemanticSearch({model, field, store})` / `disableSemanticSearch({dropVectors, store})` — toggle semantic search DB-wide (or scoped to a single store) and choose the embedding model (`minilm`, `bge-small` (default), `bge-base`, `e5-small`).
  - `semanticSearchGetValues(query, {limit, minScore})` — retrieve records ranked by meaning, each returned with its key, similarity score, and value.
  - `semanticSearchGetKeys(query, {limit, minScore})` — lighter key-and-score results; use `minScore` to drop weak matches.

## 1.0.11
- **Semantic search response fields renamed to the dunder envelope** used everywhere else in the API. Each hit from `semanticSearchGetValues` is now `{__key__, __score__, __value__}` (was `{key, score, value}`); `semanticSearchGetKeys` returns `{__key__, __score__}`. This matches the `__key__`/`__value__` wrapper `lookupValuesWhere(keyIncluded: true)` already returns. **Wire-breaking** for code that read the old `key`/`score`/`value` field names.

## 1.0.12
- Added hybrid semantic search with `semanticSearchGetKeysWhere` and
  `semanticSearchGetValuesWhere`.
- Hybrid search applies metadata criteria as a hard AND pre-filter using the
  same field, timestamp, and pointer criteria as `lookupKeysWhere`; results
  remain ranked by cosine similarity.
- Added optional `minScore` filtering to hybrid search.
