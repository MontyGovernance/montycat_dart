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