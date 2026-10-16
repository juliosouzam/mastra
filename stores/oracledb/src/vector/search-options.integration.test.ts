import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { OracleVector } from '.';

describe.runIf(process.env.RUN_ORACLE_VECTOR_INTEGRATION === 'true')('Oracle vector search effort', () => {
  const indexName = `search_effort_${randomUUID().replaceAll('-', '')}`;
  let vector: OracleVector;

  beforeAll(async () => {
    vector = new OracleVector({
      id: 'oracle-search-effort',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    await vector.createIndex({
      indexName,
      dimension: 3,
      buildIndex: false,
      indexConfig: { type: 'ivf', ivf: { neighborPartitions: 2 } },
    });
    await vector.upsert({
      indexName,
      ids: Array.from({ length: 32 }, (_, index) => `v${index}`),
      vectors: Array.from({ length: 32 }, (_, index) => [1, index / 32, 0.5]),
    });
    await vector.buildIndex({ indexName });
  });

  afterAll(async () => {
    await vector?.deleteIndex({ indexName });
    await vector?.disconnect();
  });

  it('passes IVF probes to Oracle and returns the closest vector', async () => {
    const pool = await vector.getPool();
    const connection = await pool.getConnection();
    const borrow = vi.spyOn(pool, 'getConnection').mockResolvedValue(connection);
    const execute = vi.spyOn(connection, 'execute');
    try {
      const result = await vector.query({
        indexName,
        queryVector: [1, 0, 0.5],
        topK: 1,
        queryMode: 'approx',
        probes: 2,
      });
      expect(result[0]?.id).toBe('v0');
      expect(execute.mock.calls.some(call => String(call[0]).includes('NEIGHBOR PARTITION PROBES 2'))).toBe(true);
    } finally {
      borrow.mockRestore();
      execute.mockRestore();
      await connection.close().catch(() => undefined);
    }
  });

  it('rejects HNSW search effort on an IVF index', async () => {
    await expect(vector.query({ indexName, queryVector: [1, 0, 0.5], ef: 64 })).rejects.toThrow(
      'ef requires an HNSW index',
    );
  });
});
