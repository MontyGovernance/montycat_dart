# 💙 Montycat for Dart & Flutter — The AI-Native NoSQL Database with Semantic Search for RAG & Agents

### Abolish the two-database stack.

The official Dart & Flutter SDK for [Montycat](https://montygovernance.com) — a self-hosted **NoSQL + vector database** with AI **semantic search** forged into the core, built for **RAG and AI-agent memory**. One Rust engine, not a sprawl of services. **Your hardware. Your data. Your meaning.**

[![pub package](https://img.shields.io/pub/v/montycat.svg)](https://pub.dev/packages/montycat)
[![pub points](https://img.shields.io/pub/points/montycat.svg)](https://pub.dev/packages/montycat/score)
[![Docker Pulls](https://img.shields.io/docker/pulls/montygovernance/montycat)](https://hub.docker.com/r/montygovernance/montycat)
[![platform](https://img.shields.io/badge/platform-Flutter%20%7C%20Dart-blue.svg)](https://pub.dev/packages/montycat)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/MontyGovernance/montycat_dart/blob/master/LICENSE)

```dart
// Search your data by MEANING — no external APIs, no separate vector database.
// (already ON by default in the montycat-semantic server edition)
final hits = await production.semanticSearchGetValues('Show all Bluetooth devices', limit: [0, 5]);
// → [{__key__: 123..., __score__: 0.78, __value__: { name: 'Wireless Headphones' }}]
```

> ### 🧩 All-in-one. AI-native. **Zero external dependencies.**
> The vector-embedding engine runs **inside** the database — **no** separate vector DB, **no** embedding API, **no** API keys, **no** sidecar service. One engine, one binary, your hardware.

## What is Montycat?

For a generation we were told the price of intelligence was two systems: a database for your records, and a separate vector store — with its per-query bill — for their meaning. Montycat rejects that tax. It is a **self-hosted NoSQL + vector database**: one Rust-powered engine with semantic search built in, so **RAG, AI-agent memory, and vector search** live where your data already lives. No cloud lock-in. No ops headache. Decentralized by nature, ultra-fast, and natively async.

Think of it as an **open-source, self-hosted alternative to Pinecone, Weaviate, Chroma, Qdrant, and Redis** — a **vector database _and_ a NoSQL store in a single engine** — that feels native to Dart & Flutter across mobile, web, desktop, and server-side Dart.

## 🌐 More Than a Database — a Living Data Mesh

Montycat is not storage you query. It is a structured, reactive, high-performance data mesh you converse with — and every part of it belongs to you:

- **Hybrid Engine** — memory-speed in-memory operations and persistent durability in one place.
- **Domain-Oriented Keyspaces** — each keyspace is an independently owned data product, not a shared table.
- **Reactive Core** — native subscriptions for live apps and analytics.
- **Rust-Powered** — memory-safe, zero-cost abstractions, ultra-low latency.
- **One Clean API** — real-time subscriptions, hybrid storage, and structured data behind a single async surface, built for Dart & Flutter.

## ✨ Why Dart & Flutter Developers Defect to Montycat

- ⚡ **No More Waiting** — forget slow queries, bloated drivers, and ORM hell.
- 🗂️ **Domain-Oriented Data** — each keyspace is a product you own and control.
- 📡 **Live & Reactive** — dashboards, notifications, analytics: real-time is effortless.
- 🛡️ **Safe & Future-Proof** — a Rust engine, TLS, and memory-safe guarantees.
- 🌐 **Cross-Platform** — Flutter mobile, web, desktop, and server-side Dart. No hacks.

## 🔍 Example Use Cases

- **RAG pipelines & semantic retrieval** for LLM-powered Dart/Flutter apps
- **On-device AI agent / chatbot memory** that survives restarts
- **Semantic search & recommendations** — match intent, not keywords
- Real-time dashboards, notifications, and live collaborative apps
- Offline-first Flutter cache backed by a real engine
- Data products in a decentralized Mesh architecture

## 🚀 Get the Engine (30 seconds)

The client talks to a Montycat server. Fastest way — Docker, with AI semantic search built in:

```bash
docker run -d --name montycat \
  -p 21210:21210 -p 21211:21211 \
  -e MONTYCAT_SUPEROWNER="admin" \
  -e MONTYCAT_PASSWORD="change-me" \
  -v montycat_data:/var/lib/.montycat \
  montygovernance/montycat:semantic
```

Prefer the lean edition without the embedding engine? Use the `latest` tag. Prebuilt packages (apt, macOS, Windows) at **https://montygovernance.com**.

## 📦 Installation

Add `montycat` to your `pubspec.yaml`:

```yaml
dependencies:
  montycat: ^1.1.0
```

Then fetch packages:

```bash
dart pub get
# or for Flutter
flutter pub get
```

## Quick Start

```dart
import 'dart:async';
import 'package:montycat/montycat.dart'
    show
        Engine,
        KeyspaceInMemory,
        KeyspacePersistent,
        Timestamp,
        Schema,
        FieldType;

class Customer extends Schema {
  Customer(super.kwargs);

  static String get schemaName => 'Customer';

  static Map<String, FieldType> get schemaMetadata => {
    'name': FieldType(String),
    'age': FieldType(int, nullable: true),
    'email': FieldType(String, nullable: true),
  };

  @override
  Map<String, FieldType> metadata() => schemaMetadata;
}

class Orders extends Schema {
  Orders(super.kwargs);

  static String get schemaName => 'Orders';

  static Map<String, FieldType> get schemaMetadata => {
    'date': FieldType(Timestamp),
    'quantity': FieldType(int),
    'customer': FieldType(String),
  };

  @override
  Map<String, FieldType> metadata() => schemaMetadata;
}

Future<void> main() async {
  Engine engine = Engine(
    host: '127.0.0.1',
    port: 21210,
    username: 'USER',
    password: '12345',
    store: 'Company',
  );

  KeyspaceInMemory customers = KeyspaceInMemory(keyspace: 'customers');
  KeyspacePersistent production = KeyspacePersistent(keyspace: 'production');

  customers.connectEngine(engine);
  production.connectEngine(engine);

  final customersCreated = await customers.createKeyspace();
  final productionCreated = await production.createKeyspace();

  print("Keyspaces created: $customersCreated, $productionCreated");

  var customer = Customer({'name': 'Alice Smith', 'age': 28, 'email': null});

  var custInsert = await customers.insertValue(value: customer.serialize());
  print(custInsert);
  //{status: true, payload: 29095364578528255816148465894650046051, error: null}

  var custFetched = await customers.getValue(
    key: '30748150595091665781806646557034343545',
  );
  print(custFetched);
  //{status: true, payload: {name: Alice Smith, age: 28, email: alice.smith@example.com}, error: null}

  var custUpdate = await customers.updateValue(
    key: '30748150595091665781806646557034343545',
    updates: {'age': 29},
  );
  print(custUpdate);
  //{status: true, payload: null, error: null}

  var custDelete = await customers.deleteKey(
    key: '30748150595091665781806646557034343545',
  );
  print(custDelete);
  //{status: true, payload: null, error: null}

  var custVerifyKeys = await customers.getKeys();
  print(custVerifyKeys);
  //{status: true, payload: [], error: null}

  var order = Orders({
    'date': Timestamp(timestamp: DateTime.now().toUtc().toString()),
    'quantity': 3,
    'customer': 'Name',
  });

  var prodInsert = await production.insertValue(value: order.serialize());
  print(prodInsert);
  //{status: true, payload: 30442970696809394303186116932586352271, error: null}

  var prodFetched = await production.getValue(
    key: '30648912591862065620656997781578274575',
  );
  print(prodFetched);
  //{status: true, payload: {date: 2025-10-05T12:34:56.789Z, quantity: 3, customer: Name}, error: null}

  var prodUpdate = await production.updateValue(
    key: '30648912591862065620656997781578274575',
    updates: {'quantity': 10},
  );
  print(prodUpdate);
  //{status: true, payload: null, error: null}

  var prodLookup = await production.lookupValuesWhere(
    searchCriteria: {'quantity': 10, 'date': Timestamp(after: '2025-10-01')},
    keyIncluded: true,
    schema: Orders.schemaName,
  );
  print(prodLookup);
  //{status: true, payload: [{__key__: 30442970696809394303186116932586352271, __value__: {date: 2025-10-05T12:34:56.789Z, quantity: 10, customer: Name}}], error: null}
}
```

## 🧠 AI-Native Semantic Search — Vector Search Built Into Your Database

**Stop bolting a separate vector database onto your stack.** Montycat ranks your data by
*meaning*, not keywords — an embedded, on-device vector-embedding engine turns every write
into a searchable vector automatically. It's the retrieval layer for **RAG pipelines, AI
agents, semantic search, recommendation engines, and LLM-powered apps** — with **zero
external APIs, zero API keys, and zero extra infrastructure.**

- 🔎 **Semantic / vector search** — kNN similarity over on-device embeddings, not brittle keyword matches.
- 🤖 **Built for AI** — RAG, semantic retrieval, AI agents, recommendations, dedup, clustering.
- 🔒 **Private & free** — embeddings never leave your machine. No OpenAI/Cohere bill, no data egress.
- ⚡ **One system, not two** — your data *and* its vectors live in the same database. No sync jobs, no drift, no second service to run.
- 🚀 **Zero setup** — no index tuning, no pipeline: `enableSemanticSearch()` and you're ranking by meaning.

> **⚠️ Requires the semantic edition of the server — nothing to compile.** Semantic
> search runs an embedded ONNX vector-embedding engine that ships only in the
> **`montycat-semantic`** edition; the default lean `montycat` server does not include it.
> Get it the way that suits you — pull the **Docker image**
> (`montygovernance/montycat:semantic`), download the prebuilt **package**, or install
> `montycat-semantic` from the **apt repository**. The Dart client API is identical either
> way; just point it at a semantic-edition server (semantic search is enabled by default
> there, using the `bge-small` model).

The switch is DB-wide and already on in the semantic edition; every keyspace is embedded
in the background as data is written (the embedding model is downloaded on demand).

```dart
// Semantic search is ON by default in the montycat-semantic edition — just search.
// Rank stored items by meaning — two flavors:
//   getValues → each hit is {__key__, __score__, __value__}
//   getKeys   → each hit is {__key__, __score__} (lighter; fetch a page later with getBulk)
final hits = await production.semanticSearchGetValues('bulk order of blue widgets', limit: [0, 5]);
final keys = await production.semanticSearchGetKeys('bulk order of blue widgets', limit: [0, 5]);

// Optionally drop weak matches by cosine similarity (range [-1, 1]).
final strong = await production.semanticSearchGetKeys('bulk order of blue widgets', limit: [0, 5], minScore: 0.35);

// Read back the actual model and backfill state.
final semantic = await engine.getSemanticStatus(
  store: 'catalog',
  keyspace: 'products',
);
final productsStatus = semantic.keyspace('catalog', 'products');

// Enable an unenrolled keyspace with an explicit model.
await engine.enableSemanticSearch(
  model: SemanticModel.bgeBase,
  store: 'catalog',
  keyspace: 'products',
);

// Changing an enrolled keyspace's model is destructive. This atomic operation
// drops its old vectors and starts a complete backfill.
await engine.reembedSemanticSearch(
  model: SemanticModel.bgeBase,
  store: 'catalog',
  keyspace: 'products',
);

// turn it off (vectors are kept so re-enabling resumes instantly;
// pass dropVectors: true to also clear stored vectors)
await engine.disableSemanticSearch();
```

### Hybrid semantic search

Combine meaning-based ranking with structured metadata constraints. The filter
is a hard AND pre-filter, not a relevance boost; it supports the same criteria
shape as `lookupKeysWhere`.

```dart
final matchingKeys = await production.semanticSearchGetKeysWhere(
  'astronomy and outer space',
  {'category': 'space'},
  limit: [0, 5],
  minScore: 0.35,
);

final matchingValues = await production.semanticSearchGetValuesWhere(
  'astronomy and outer space',
  {'category': 'space'},
  limit: [0, 5],
);
// key hits:    {__key__, __score__}
// value hits:  {__key__, __score__, __value__}
```

## 📨 Response Shape

Every call resolves to the same envelope, so there is one thing to check everywhere:

```dart
// {status: true,  payload: <result>, error: null}
// {status: false, payload: null,     error: 'Governance permission denied: ...'}

final res = await customers.insertValue(value: customer.serialize());
if (res['status'] == true) print(res['payload']);
```

`payload` is `null` for commands that only acknowledge, the new key for inserts, and a
list for lookups and semantic searches. **Keys are u128 and always arrive as strings** —
never parse one into `int`, which silently truncates above 2^63. Invalid arguments throw
`ArgumentError` before anything touches the network; server-side failures come back in
`error` with `status: false`.

## 🔄 Connection Pooling

By default every request opens a socket, sends, reads one response, and closes. Reuse the
connection instead and the handshake disappears from every call after the first. The win
scales with how much of your latency is connection setup: large for a chatty service
issuing many small reads, larger over a network — where the handshake costs a full round
trip before the query is even sent — and larger again with TLS.

Pooling is opt-in. One new argument, and no call site changes:

```dart
import 'package:montycat/montycat.dart';

final engine = Engine(
  host: '127.0.0.1',
  port: 21210,
  username: 'USER',
  password: '12345',
  store: 'Company',
  pool: const PoolConfig(),        // ← the only new argument
);

customers.connectEngine(engine);
await customers.insertValue(value: customer.serialize());   // unchanged

await closeAllPools();             // before exit
```

Tune it if you need to:

```dart
pool: const PoolConfig(maxIdle: 4, idleTimeout: Duration(seconds: 15)),  // defaults: 8, 30s
```

**Pools are shared per `(host, port, useTls)`.** They live in a library-level registry, not
on the `Engine`. That matters more here than elsewhere: keyspace state is *per-instance*, so
a Flutter app creating a keyspace per screen or per rebuild would otherwise get a pool per
instance. The key is read at request time, so flipping `useTls` after `connectEngine` cannot
reuse a plaintext connection for a TLS engine.

### On Flutter and mobile

**Backgrounding kills pooled sockets.** iOS and Android close them when the app is
backgrounded, so every pooled connection is dead on resume. The client detects that before
reusing one and opens a fresh connection — but the cost is a user-visible round trip, so
prefer a short `idleTimeout` on mobile.

**Connectivity changes invalidate the pool.** Wi-Fi to cellular kills every pooled
connection. If your app already observes connectivity, call `closeAllPools()` on a change
rather than discovering it one failed request at a time.

Because of backgrounding, this client benefits least from pooling on mobile and most on
server-side Dart. Weight it accordingly.

**Keep `maxIdle` modest.** An idle pooled connection still holds one of the engine's
connection permits. Raise the defaults only after measuring with `queueDepths()`.

Subscriptions are never pooled — they are long-lived, stream many responses to one request,
and live on their own port.

## 📡 Real-Time Subscriptions

Subscribe to one key or to a whole keyspace and get pushed every change — the reactive
core behind live dashboards, notifications, and collaborative Flutter apps.

```dart
// Whole keyspace: omit both key and customKey.
final handle = await production.subscribe(
  callback: (event) => print('changed: $event'),
);

// Or watch a single key (customKey is hashed for you).
// Passing key and customKey together throws ArgumentError.
final oneKey = await production.subscribe(
  key: '30442970696809394303186116932586352271',
  callback: (event) => print('changed: $event'),
);

// Stop listening and close the socket. No callback fires after this.
handle.stop();
oneKey.stop();
```

`subscribe` returns a `SubscriptionHandle`; `handle.stopped` reports whether it is still
live. Subscriptions use the **subscription port**, which defaults to `port + 1` — that is
the second port (`21211`) published in the Docker command above. Override it with
`subscriptionPort:` if your deployment maps it elsewhere.

## 🔐 TLS

Set `useTls` to negotiate an encrypted connection. It applies to commands and
subscriptions alike:

```dart
final engine = Engine(
  host: '127.0.0.1',
  port: 21210,
  username: 'USER',
  password: '12345',
  store: 'Company',
  useTls: true,
);

// Engine.fromUri parses credentials but always starts in plaintext — opt in after:
final fromUri = Engine.fromUri('montycat://USER:12345@127.0.0.1:21210/Company')
  ..useTls = true;
```

> **Note.** The client accepts self-signed certificates, which is convenient for local
> and internal deployments but means the server identity is not verified. Terminate TLS
> at a trusted proxy if you need certificate pinning.

## 👥 Owners & Access

Governance policies below are written against *owners*, so create them first. A
superowner provisions an owner, then grants data access — optionally narrowed to
specific keyspaces:

```dart
import 'package:montycat/montycat.dart' show Permission;

await engine.createOwner('alice', 'alice-password');

await engine.grantTo('alice', Permission.read);                          // whole store
await engine.grantTo('alice', Permission.write, keyspaces: ['production']); // scoped

await engine.listOwners();

await engine.revokeFrom('alice', Permission.write, keyspaces: ['production']);
await engine.removeOwner('alice');
```

`Permission` is `read`, `write`, or `all`. `grantTo` and `revokeFrom` apply to the
engine's `store` and throw `ArgumentError` when it is unset. This governs **data
access**; to delegate *administrative* capabilities such as provisioning keyspaces or
managing schemas, see [Data-mesh governance](#data-mesh-governance-for-shared-and-multi-tenant-deployments)
at the end of this document.

## ⚡ Features in Action
- 🧠 AI Semantic & Vector Search: rank items by meaning with on-device embeddings — kNN vector search for **RAG, AI agents & LLM apps**, no external API.
- Async by Default: Full async/await support for all operations.
- Can be used as a cache option for Flutter apps
- Real-Time: Subscribe to keyspace events or key changes.
- Hybrid Storage: In-memory + persistent keyspaces.
- Schema Support: Optional runtime schema enforcement.
- Safe & Secure: Rust-powered engine with TLS.
- Flutter Compatible: Works seamlessly on mobile, desktop, and web.

## 🔗 Links

- 🌐 **Website & Docs** — https://montygovernance.com
- 📦 **pub.dev** — https://pub.dev/packages/montycat
- 🐳 **Docker Hub** — https://hub.docker.com/r/montygovernance/montycat
- 💻 **Source** — https://github.com/MontyGovernance/montycat_dart
- 📝 **Changelog** — [CHANGELOG.md](CHANGELOG.md)

## ❓ FAQ

- **Is Montycat a vector database or a NoSQL database?** Both — one engine. Store records and query them by *meaning* (vector / semantic search) or by key/schema, without running two systems.
- **Do I need OpenAI or an embedding API?** No. Embeddings run on-device in the `montycat-semantic` server. No API keys, no per-query bill, no data egress.
- **Is it a Pinecone / Weaviate / Chroma / Qdrant alternative?** Yes — self-hosted and open-source, with a NoSQL store built in.
- **Does it work with Flutter?** Yes — mobile, web, desktop, and server-side Dart, on the same async API.

## Data-mesh governance for shared and multi-tenant deployments

Delegate administration without giving every team full server control. Policies scope
authority to an owner and store, with optional keyspace, storage-type, and semantic-model
constraints. Platform teams can govern shared infrastructure while domain teams operate
the data products they own.

- Grant, revoke, or explicitly deny keyspace provisioning/removal, schema, semantic,
  snapshot, and access-management capabilities.
- Inspect effective permissions and policy history, or preview a grant/revoke before
  applying it.
- Validate, plan, apply, and export JSON or YAML policy manifests for repeatable
  infrastructure-as-code workflows.
- Constrain storage types for provisioning, removal, schema, access, and semantic
  management. Snapshot management is always in-memory, so it takes no storage-type
  qualifier.
- Constrain semantic models during keyspace provisioning and semantic management.

For example, a superowner can restrict what Alice may provision and separately delegate
semantic management for one keyspace:

```dart
import 'package:montycat/montycat.dart'
    show PolicyCapability, PolicyKeyspaceType, SemanticModel;

await engine.policyGrant(
  owner: 'alice',
  capability: PolicyCapability.provisionKeyspace,
  store: 'catalog',
  types: [PolicyKeyspaceType.inMemory, PolicyKeyspaceType.persistent],
  models: [SemanticModel.bgeSmall],
);
await engine.policyGrant(
  owner: 'alice',
  capability: PolicyCapability.manageSemantic,
  store: 'catalog',
  keyspace: 'products',
  types: [PolicyKeyspaceType.inMemory],
  models: [SemanticModel.bgeSmall],
);
await engine.policyView(owner: 'alice', store: 'catalog');
```

Use `policyExplain` to inspect an authorization decision and `policyHistory` to audit
changes. Superowners can manage policies directly with `policyGrant`, `policyRevoke`,
`policyDeny`, and `policyRemoveDenial`, or use `policyValidate`, `policyPlan`,
`policyApply`, and `policyExport` with JSON or YAML documents.
