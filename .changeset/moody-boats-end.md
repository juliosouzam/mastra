---
'@mastra/oracledb': minor
---

Added atomic working-memory updates and retention for Oracle conversation data, workflow snapshots, and evaluation scores. Concurrent memory updates preserve each other's changes, and deleting a workflow run no longer lets a late result recreate it.

Completed stored agent configuration and organization/project filtering for scorer definitions. Added workflow thread filters, summary queries, and message reads without a total count. Fixed exported schemas to include workflow and memory ownership records.

Caller-supplied score IDs now replace the existing score, so retried evaluation writes don't create duplicates.

Added durable thread state, immutable skill file blobs, scoped tool-provider connection labels, and persisted workflow definitions. Exported schemas include these domains. Timestamp writes and retention cutoffs now preserve their instant across database session timezones.

Added versioned prompt blocks, MCP servers, workspaces and skills, plus favorites, channels, background tasks, notifications, and schedules. Dataset history and experiment results include tool mocks, retry identity, reviewer comments, scoped queries, and purge protection against late submissions. Knowledge V2 is opt-in and supports structural scopes, mentions, activity, schema inspection, and recoverable semantic indexing work.

Dataset selection and metric breakdowns support batches of dataset and experiment IDs while preserving tenant filters.

Added `OracleFactoryStorage` for application collections, atomic updates, transactions, additive schema evolution, and Oracle-backed Better Auth authentication through the existing custom-database capability. Long text is preserved. Sorting uses native comparisons with an application-memory fallback for values beyond Oracle's scalar representation; serializable callbacks use collection locks.

Added `OracleStoreVNext` for advanced observability on an explicit Oracle connection: metrics, feedback, trace/span/thread queries, aggregates, previews, commit-aware polling, and bounded retention. The observability user needs `EXECUTE ON SYS.DBMS_FLASHBACK`. Existing `OracleStore` behavior and tables remain available.

Span queries support Core's context and lineage filters while preserving older supported response contracts. Span events store time to first chunk, and initialization adds the field to existing tables without losing spans.

Added optional `readPool` / `readPoolManager` routing. Mutations, schema initialization, and lifecycle decisions use the writer even when the replica lags. Workflow run lookups, notification delivery checks, and running-task counts use committed writer state to prevent snapshot resets, repeated signals, and dispatch beyond concurrency limits.

Preserved large JSON values and authored keys through memory, configuration, and vector reads. Concurrent startup now reconciles migration records and publishes the Knowledge initialization marker atomically. Advanced query responses follow the installed Core preview schema, including Core 1.76, and consistent reads work immediately after schema creation.

Added vector namespaces, native sparse vectors, half-precision compatibility using FLOAT32 storage, and binary dimensions with byte padding. Deferred index configuration survives restarts, and search supports explicit HNSW/IVF effort controls. Existing scores remain the default; select `scoreMode: 'postgres'` for PostgreSQL-compatible Hamming normalization and thresholds.

Configure retention and apply it from your scheduler:

```ts
import { OracleStore } from '@mastra/oracledb';

const storage = new OracleStore({
  id: 'oracle-storage',
  user: process.env.ORACLE_DATABASE_USER,
  password: process.env.ORACLE_DATABASE_PASSWORD,
  connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
  retention: { memory: { messages: { maxAge: '30d' } } },
});

await storage.init();
await storage.prune({ maxBatches: 10 });

const memory = await storage.getStore('memory');
if (!memory) throw new Error('Memory storage is unavailable');
await memory.mergeResourceWorkingMemory({
  resourceId: 'customer-123',
  merge: existing => [existing, 'Prefers Portuguese.'].filter(Boolean).join('\n'),
});
```
