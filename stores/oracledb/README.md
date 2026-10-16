# @mastra/oracledb

Oracle Database provider for Mastra, providing storage and vector similarity search with Oracle JSON, Oracle `VECTOR`, connection pooling, and transaction support.

## Installation

```bash
npm install @mastra/oracledb
```

## Usage

### Storage

```typescript
import { OracleStore } from '@mastra/oracledb';

const store = new OracleStore({
  id: 'oracle-store',
  user: process.env.ORACLE_DATABASE_USER,
  password: process.env.ORACLE_DATABASE_PASSWORD,
  connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
});

await store.init();
const memory = await store.getStore('memory');
if (!memory) throw new Error('Oracle memory store is not available');

// Create a thread
await memory.saveThread({
  thread: {
    id: 'thread-123',
    resourceId: 'resource-456',
    title: 'My Thread',
    metadata: { key: 'value' },
    createdAt: new Date(),
  },
});

// Add messages to thread
await memory.saveMessages({
  messages: [
    {
      id: 'msg-789',
      threadId: 'thread-123',
      role: 'user',
      content: { content: 'Hello' },
      resourceId: 'resource-456',
      createdAt: new Date(),
    },
  ],
});

// Query threads and messages
const savedThread = await memory.getThreadById({ threadId: 'thread-123' });
const { messages } = await memory.listMessages({ threadId: 'thread-123' });
```

## Documentation

`OracleStore` registers all 23 storage domains, including datasets, experiments, runtime tasks, versioned configuration, and opt-in Knowledge V2. `OracleFactoryStorage` adds application collections on the same pool. See the integration guide for schema activation, indexed-text limits, and concurrency costs. `OracleStoreVNext` explicitly enables advanced observability on its own connection. Optional read pools keep mutation checks and lifecycle reads on the writer.

`OracleVector` supports namespaces, dense and sparse vectors, half-precision compatibility, binary vectors, and exact or approximate search. Oracle score behavior remains the default; select `scoreMode: 'postgres'` explicitly when PostgreSQL-compatible Hamming normalization and thresholds are required. See the integration guide for format costs, search tuning, and schema deployment.

- [Oracle Database integration guide](https://mastra.ai/integrations/databases/oracledb)
- [Oracle Database vector reference](https://mastra.ai/reference/vectors/oracledb)

## Development tests

`pnpm test` runs unit tests. Database integrations require a disposable Oracle database and the `ORACLE_DATABASE_USER`, `ORACLE_DATABASE_PASSWORD`, and `ORACLE_DATABASE_CONNECT_STRING` environment variables.

From this package directory, prepare the test database and its HNSW vector memory:

```bash
docker compose up -d --wait
docker compose restart db
docker compose up -d --wait
pnpm test:storage-integration
ORACLE_REQUIRE_HNSW=true pnpm test:vector-integration
```

`ORACLE_REQUIRE_HNSW=true` makes unavailable vector memory a test failure. Without it, local runs can skip the HNSW test when vector memory hasn't been configured.

The storage suite measures batch message writes, history reads, concurrent working-memory updates, retention, and advanced observability. The vector suite compares deterministic approximate recall with exact search. Set `ORACLE_BENCHMARK_REPORT` to a JSON-lines file path to retain database versions, dataset sizes, latency, throughput, write/build times, and process memory measurements. The CI matrix uploads this report for each Oracle release. These measurements describe the test dataset and aren't a production latency guarantee.

The combined-store CI workflow runs both integration suites on pinned Oracle 23ai and 26ai images. Set `ORACLE_DATABASE_IMAGE` to choose a Compose image, and `ORACLE_DATABASE_PORT` or `ORACLE_DATABASE_CONTAINER_NAME` to isolate local test instances. `ORACLE_DATABASE_VERSION_PREFIX` optionally checks that the connected server matches the intended release.

## Changelog

See the [package changelog](https://github.com/mastra-ai/mastra/blob/main/stores/oracledb/CHANGELOG.md) for version history and release notes.

## Support

We have an [open community Discord](https://discord.gg/mastra-ai). Come and say hello and let us know if you have any questions or need any help getting things running.
