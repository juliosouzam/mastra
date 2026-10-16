import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleVector } from '.';
import type { OracleMetric, OracleVectorFormat } from '.';

describe.runIf(process.env.RUN_ORACLE_VECTOR_INTEGRATION === 'true')('Oracle HNSW vector formats', () => {
  let vector: OracleVector;

  beforeAll(() => {
    vector = new OracleVector({
      id: 'oracle-hnsw-formats',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
  });

  afterAll(async () => {
    await vector?.disconnect();
  });

  const cases: Array<{ format: OracleVectorFormat; metric: OracleMetric; vectors: number[][] }> = [
    {
      format: 'bit',
      metric: 'jaccard',
      vectors: [
        [1, 0, 1, 0, 0, 0, 0],
        [0, 1, 0, 1, 0, 0, 0],
      ],
    },
    {
      format: 'sparsevec',
      metric: 'cosine',
      vectors: [
        [1, 0, 0, 0, 0, 0.5],
        [0, 1, 0, 0, 0.5, 0],
      ],
    },
  ];

  for (const { format, metric, vectors } of cases) {
    it(`builds and queries ${format} with ${metric}`, async context => {
      const indexName = `hnsw_format_${randomUUID().replaceAll('-', '')}`;
      try {
        await vector.createIndex({
          indexName,
          dimension: vectors[0]!.length,
          vectorFormat: format,
          metric,
          indexConfig: { type: 'hnsw' },
          buildIndex: false,
        });
        await vector.upsert({ indexName, ids: ['nearest', 'other'], vectors });
        try {
          await vector.buildIndex({ indexName });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          if (/VECTOR_MEMORY_SIZE|Vector Pool|ORA-51962/i.test(message) && process.env.ORACLE_REQUIRE_HNSW !== 'true') {
            context.skip('HNSW requires configured Oracle Vector Pool memory');
          }
          throw error;
        }
        expect(await vector.getIndexStatus({ indexName })).toMatch(/^VALID\b/);
        const results = await vector.query({
          indexName,
          queryVector: vectors[0],
          queryMode: 'approx',
          ef: 64,
          topK: 1,
        });
        expect(results[0]?.id).toBe('nearest');
        await expect(
          vector.query({ indexName, queryVector: vectors[0], queryMode: 'approx', probes: 2 }),
        ).rejects.toThrow('probes requires an IVF index');
      } finally {
        await vector.deleteIndex({ indexName }).catch(() => undefined);
      }
    });
  }
});
