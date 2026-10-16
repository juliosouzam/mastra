import { randomUUID } from 'node:crypto';
import { appendFile } from 'node:fs/promises';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleVector } from '.';

// A reproducible regression workload, not an application latency guarantee.
// Compare approximate retrieval with Oracle's exact answers for the same stored FLOAT32 data.
describe.runIf(process.env.RUN_ORACLE_VECTOR_INTEGRATION === 'true')('Oracle approximate recall and latency', () => {
  let vector: OracleVector;
  let databaseVersion: string | undefined;
  beforeAll(async () => {
    vector = new OracleVector({
      id: 'oracle-recall-regression',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    const connection = await (await vector.getPool()).getConnection();
    try {
      databaseVersion = connection.oracleServerVersionString;
    } finally {
      await connection.close();
    }
  });
  afterAll(async () => {
    await vector?.disconnect();
  });

  for (const type of ['hnsw', 'ivf'] as const) {
    it(`checks ${type} recall@10 and query latency`, async context => {
      const indexName = `recall_${type}_${randomUUID().replaceAll('-', '')}`;
      let seed = 1729;
      const random = () => {
        seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
        return seed / 4294967296;
      };
      const centers = Array.from({ length: 16 }, () => Array.from({ length: 32 }, () => random() - 0.5));
      const sample = (cluster: number) => centers[cluster]!.map(value => value + 0.12 * (random() - 0.5));
      const vectors = Array.from({ length: 1024 }, (_, index) => sample(index % centers.length));
      const queries = Array.from({ length: 24 }, (_, index) => sample(index % centers.length));
      const exactMs: number[] = [];
      const approximateMs: number[] = [];
      const recalls: number[] = [];
      try {
        await vector.createIndex({
          indexName,
          dimension: 32,
          metric: 'cosine',
          buildIndex: false,
          indexConfig:
            type === 'hnsw'
              ? { type, accuracy: 95, hnsw: { neighbors: 16, efConstruction: 128 } }
              : { type, accuracy: 95, ivf: { neighborPartitions: 16 } },
        });
        const upsertStart = performance.now();
        await vector.upsert({ indexName, vectors, ids: vectors.map((_, index) => `item-${index}`) });
        const upsertMs = performance.now() - upsertStart;
        const buildStart = performance.now();
        try {
          await vector.buildIndex({ indexName });
        } catch (error) {
          if (
            type === 'hnsw' &&
            /VECTOR_MEMORY_SIZE|Vector Pool|ORA-51962/i.test(String(error)) &&
            process.env.ORACLE_REQUIRE_HNSW !== 'true'
          ) {
            context.skip('HNSW requires configured Oracle Vector Pool memory');
          }
          throw error;
        }
        const buildMs = performance.now() - buildStart;
        expect(await vector.getIndexStatus({ indexName })).toMatch(/^VALID\b/);
        // Warm the query paths before measuring.
        await vector.query({ indexName, queryVector: queries[0], queryMode: 'exact', topK: 10 });
        await vector.query({ indexName, queryVector: queries[0], queryMode: 'approx', topK: 10 });
        for (const queryVector of queries) {
          const exactStart = performance.now();
          const exact = await vector.query({ indexName, queryVector, queryMode: 'exact', topK: 10 });
          exactMs.push(performance.now() - exactStart);
          const approximateStart = performance.now();
          const approximate = await vector.query({
            indexName,
            queryVector,
            queryMode: 'approx',
            topK: 10,
            ...(type === 'hnsw' ? { ef: 128 } : { probes: 8 }),
          });
          approximateMs.push(performance.now() - approximateStart);
          expect(exact).toHaveLength(10);
          expect(approximate).toHaveLength(10);
          const expectedIds = new Set(exact.map(result => result.id));
          recalls.push(approximate.filter(result => expectedIds.has(result.id)).length / 10);
        }
        const recallAt10 = recalls.reduce((sum, recall) => sum + recall, 0) / recalls.length;
        const p95 = (values: number[]) => values.toSorted((a, b) => a - b)[Math.ceil(values.length * 0.95) - 1]!;
        const metrics = {
          databaseVersion,
          type,
          seed: 1729,
          vectors: 1024,
          dimension: 32,
          queries: queries.length,
          recallAt10,
          minRecallAt10: Math.min(...recalls),
          exactP95Ms: p95(exactMs),
          approximateP95Ms: p95(approximateMs),
          upsertMs,
          buildMs,
          rssBytes: process.memoryUsage().rss,
        };
        console.info('Oracle vector regression', JSON.stringify(metrics));
        if (process.env.ORACLE_BENCHMARK_REPORT) {
          await appendFile(process.env.ORACLE_BENCHMARK_REPORT, `${JSON.stringify(metrics)}\n`);
        }
        expect(recallAt10).toBeGreaterThanOrEqual(0.9);
        expect(Math.min(...recalls)).toBeGreaterThanOrEqual(0.8);
        expect(p95(approximateMs)).toBeLessThan(2000);
      } finally {
        await vector.deleteIndex({ indexName }).catch(() => undefined);
      }
    }, 60_000);
  }
});
