# 🚀 Montycat Dart Client — the self-hosted NoSQL + vector database with built-in AI semantic search for RAG & AI agents (Dart & Flutter), powered by Rust

[![pub package](https://img.shields.io/pub/v/montycat.svg)](https://pub.dev/packages/montycat)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Changelog](https://img.shields.io/badge/changelog-1.0.9-blue.svg)](CHANGELOG.md)
[![GitHub](https://img.shields.io/badge/github-@montycat-blue.svg)](https://github.com/MontyGovernance/montycat_dart)

## Say goodbye to slow, bloated, legacy databases — and to bolting a separate vector DB onto your stack. Say hello to Montycat: the self-hosted, Rust-powered **NoSQL + vector database** with built-in semantic search — **RAG & AI-agent memory** that feels native to Dart & Flutter. No cloud lock-in, no ops headache.

## 🌐 Montycat Highlights

- Montycat is more than a database — it’s a living Data Mesh:
- Hybrid Engine: Combine memory-speed in-memory operations with persistent durability.
- Domain-Oriented Keyspaces: Each keyspace is an independently owned data product.
- Reactive Core: Native subscriptions for live apps and analytics.
- Rust-Powered: Memory-safe, zero-cost abstractions, ultra-low latency.
- With Montycat, you’re not just storing data — you’re interacting with a structured, reactive, high-performance data mesh.
- Montycat Dart client allows Dart & Flutter developers to interact with the Montycat NoSQL engine, a Rust-powered, ultra-fast, Data Mesh–native database. It combines real-time subscriptions, hybrid storage, and structured data support with a clean async API.

## ✨ Why Montycat Dart?

- ⚡ No More Waiting – Forget slow queries, bloated drivers, or ORM hell.
- 🗂️ Domain-Oriented Data – Each keyspace is a mini product you control.
- 📡 Live & Reactive – Dashboards, notifications, or analytics — real-time is effortless.
- 🛡️ Safe & Future-Proof – Rust engine + TLS + memory-safe guarantees.
- 🌐 Cross-Platform – Flutter mobile, web, desktop; server-side Dart; no hacks.

## Learn more about Montycat Engine at https://montygovernance.com

## 📦 Installation

Add `montycat_dart` to your `pubspec.yaml`:

```yaml
dependencies:
  montycat_dart: ^1.0.9
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
> Get it the way that suits you — pull the `montycat-semantic` **Docker image**, download
> the prebuilt **package**, or install from the **apt repository**. The Dart client API
> is identical either way; just point it at a `montycat-semantic` server (semantic search
> is enabled by default there, using the `bge-small` model).

Enable it once, DB-wide, on the engine; every keyspace is embedded in the background as
data is written (the embedding model is downloaded on first enable).

```dart
// Turn semantic search on for the whole database (model downloaded on first use).
// model: 'minilm' | 'bge-small' (default) | 'bge-base' | 'e5-small'
await engine.enableSemanticSearch();

// Rank stored items by meaning — two flavors:
//   getValues → each hit is {key, score, value}
//   getKeys   → each hit is {key, score} (lighter; fetch a page later with getBulk)
await production.semanticSearchGetValues('bulk order of blue widgets', limit: [0, 5]);
await production.semanticSearchGetKeys('bulk order of blue widgets', limit: [0, 5]);

// Optionally drop weak matches by cosine similarity (range [-1, 1]).
await production.semanticSearchGetKeys('bulk order of blue widgets', limit: [0, 5], minScore: 0.35);

// Turn it off (vectors are kept so re-enabling resumes instantly;
// pass dropVectors: true to also clear stored vectors).
await engine.disableSemanticSearch();
```

## ⚡ Features in Action
- 🧠 AI Semantic & Vector Search: rank items by meaning with on-device embeddings — kNN vector search for **RAG, AI agents & LLM apps**, no external API.
- Async by Default: Full async/await support for all operations.
- Can be used as a cache option for Flutter apps
- Real-Time: Subscribe to keyspace events or key changes.
- Hybrid Storage: In-memory + persistent keyspaces.
- Schema Support: Optional runtime schema enforcement.
- Safe & Secure: Rust-powered engine with TLS.
- Flutter Compatible: Works seamlessly on mobile, desktop, and web.
