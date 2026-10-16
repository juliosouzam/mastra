import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleVector } from '.';

describe.runIf(process.env.RUN_ORACLE_VECTOR_INTEGRATION === 'true')('Oracle vector format compatibility', () => {
  let vector: OracleVector;

  beforeAll(() => {
    vector = new OracleVector({
      id: 'oracle-vector-formats',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
  });

  afterAll(async () => {
    await vector?.disconnect();
  });

  it('preserves PostgreSQL half-precision values in Oracle FLOAT32 storage', async () => {
    const indexName = `half_${randomUUID().replaceAll('-', '')}`;
    const input = [1.0001, 0.3333, 65519, 5.960464477539063e-8, 1.00048828125, 1.00146484375, -2.0001];
    // Values independently verified against pgvector 0.8.4 halfvec, expanded to float8 for comparison.
    const expected = [1, 0.333251953125, 65504, 5.960464477539063e-8, 1, 1.001953125, -2];
    try {
      await vector.createIndex({ indexName, dimension: 7, vectorFormat: 'halfvec', metric: 'euclidean' });
      await vector.upsert({ indexName, ids: ['half-vector'], vectors: [input] });
      const result = await vector.query({ indexName, queryVector: input, includeVector: true });
      expect(result).toHaveLength(1);
      expect(result[0]).toMatchObject({ id: 'half-vector', score: 1, vector: expected });
      expect(await vector.getIndexInfo({ indexName })).toMatchObject({
        dimension: 7,
        vectorFormat: 'halfvec',
        count: 1,
      });
    } finally {
      await vector.deleteIndex({ indexName });
    }
  });

  it('rejects half-precision overflow before changing existing vectors', async () => {
    const indexName = `half_range_${randomUUID().replaceAll('-', '')}`;
    try {
      await vector.createIndex({ indexName, dimension: 1, vectorFormat: 'halfvec', metric: 'euclidean' });
      await vector.upsert({ indexName, ids: ['original'], vectors: [[65519]], metadata: [{ source: 'original' }] });
      await expect(
        vector.upsert({ indexName, ids: ['overflow'], vectors: [[65520]], deleteFilter: { source: 'original' } }),
      ).rejects.toThrow(/half-precision/);
      const remaining = await vector.query({ indexName, queryVector: [65519], includeVector: true });
      expect(remaining).toHaveLength(1);
      expect(remaining[0]).toMatchObject({ id: 'original', vector: [65504] });
    } finally {
      await vector.deleteIndex({ indexName });
    }
  });

  it('stores and retrieves sparsevec values through the existing dense-vector contract', async () => {
    const indexName = `sparse_${randomUUID().replaceAll('-', '')}`;
    try {
      await vector.createIndex({ indexName, dimension: 5, vectorFormat: 'sparsevec', metric: 'euclidean' });
      await vector.upsert({ indexName, ids: ['sparse'], vectors: [[0, 1, 0, 2, 0]] });
      const result = await vector.query({ indexName, queryVector: [0, 1, 0, 2, 0], includeVector: true });
      expect(result[0]).toMatchObject({ id: 'sparse', vector: [0, 1, 0, 2, 0], score: 1 });
      await vector.updateVector({ indexName, id: 'sparse', update: { vector: [0, 3, 0, 0, 0] } });
      expect((await vector.query({ indexName, queryVector: [0, 3, 0, 0, 0], includeVector: true }))[0]).toMatchObject({
        vector: [0, 3, 0, 0, 0],
        score: 1,
      });
    } finally {
      await vector.deleteIndex({ indexName });
    }
  });

  it('accepts sparse inputs without allocating a dense embedding and uses the index dimension', async () => {
    const indexName = `sparse_input_${randomUUID().replaceAll('-', '')}`;
    try {
      await vector.createIndex({ indexName, dimension: 6, vectorFormat: 'sparsevec', metric: 'euclidean' });
      await vector.upsert({
        indexName,
        namespace: 'team-sparse',
        ids: ['sparse'],
        sparseVectors: [{ indices: [3, 1], values: [2, 1] }],
      });
      const result = await vector.query({
        indexName,
        namespace: 'team-sparse',
        sparseVector: { indices: [1, 3], values: [1, 2] },
        includeVector: true,
      });
      expect(result).toHaveLength(1);
      expect(result[0]).toMatchObject({ id: 'sparse', score: 1, vector: [0, 1, 0, 2, 0, 0] });
    } finally {
      await vector.deleteIndex({ indexName });
    }
  });

  it('preserves logical binary dimensions that are not multiples of eight', async () => {
    const indexName = `binary_${randomUUID().replaceAll('-', '')}`;
    try {
      await vector.createIndex({ indexName, dimension: 7, vectorFormat: 'bit', metric: 'hamming' });
      await vector.upsert({ indexName, ids: ['binary'], vectors: [[1, 0, 1, 0, 0, 1, 1]] });
      const result = await vector.query({ indexName, queryVector: [1, 0, 1, 0, 0, 1, 1], includeVector: true });
      expect(result[0]).toMatchObject({ id: 'binary', score: 1, vector: [1, 0, 1, 0, 0, 1, 1] });
      expect(await vector.getIndexInfo({ indexName })).toMatchObject({ dimension: 7, vectorFormat: 'bit' });
    } finally {
      await vector.deleteIndex({ indexName });
    }
  });

  it('supports zero sparse vectors for euclidean distance and rejects malformed sparse inputs', async () => {
    const indexName = `sparse_validation_${randomUUID().replaceAll('-', '')}`;
    try {
      await vector.createIndex({ indexName, dimension: 3, vectorFormat: 'sparsevec', metric: 'euclidean' });
      await vector.upsert({ indexName, ids: ['zero'], sparseVectors: [{ indices: [], values: [] }] });
      expect(
        (await vector.query({ indexName, sparseVector: { indices: [], values: [] }, includeVector: true }))[0],
      ).toMatchObject({ id: 'zero', vector: [0, 0, 0], score: 1 });
      for (const sparse of [
        { indices: [0, 0], values: [1, 2] },
        { indices: [0], values: [] },
        { indices: [-1], values: [1] },
        { indices: [3], values: [1] },
        { indices: [1], values: [NaN] },
      ]) {
        await expect(vector.upsert({ indexName, ids: ['invalid'], sparseVectors: [sparse] })).rejects.toThrow(
          /sparse/i,
        );
      }
      expect(await vector.describeIndex({ indexName })).toMatchObject({ count: 1 });
    } finally {
      await vector.deleteIndex({ indexName });
    }
  });

  it('offers PostgreSQL score semantics explicitly while retaining Oracle defaults', async () => {
    const indexName = `binary_scores_${randomUUID().replaceAll('-', '')}`;
    const queryVector = [1, 0, 0, 0, 0, 0, 0, 0];
    try {
      await vector.createIndex({ indexName, dimension: 8, vectorFormat: 'bit', metric: 'hamming' });
      await vector.upsert({ indexName, ids: ['different'], vectors: [[1, 1, 1, 1, 1, 0, 0, 0]] });
      const native = await vector.query({ indexName, queryVector, minScore: 0.2 });
      expect(native[0]?.score).toBeCloseTo(0.2, 12);
      const compatible = await vector.query({ indexName, queryVector, scoreMode: 'postgres' });
      expect(compatible[0]?.score).toBeCloseTo(0.5, 12);
      expect(await vector.query({ indexName, queryVector, scoreMode: 'postgres', minScore: 0.5 })).toEqual([]);
    } finally {
      await vector.deleteIndex({ indexName });
    }
  });

  it('applies a configured score mode to framework queries and normalizes logical binary dimensions', async () => {
    const indexName = `score_default_${randomUUID().replaceAll('-', '')}`;
    const compatible = new OracleVector({
      id: 'oracle-compatible-scores',
      pool: await vector.getPool(),
      defaultScoreMode: 'postgres',
    });
    const queryVector = [1, 0, 0, 0, 0, 0, 0];
    try {
      await vector.createIndex({ indexName, dimension: 7, vectorFormat: 'bit', metric: 'hamming' });
      await vector.upsert({ indexName, ids: ['different'], vectors: [[1, 1, 1, 1, 1, 0, 0]] });
      expect((await compatible.query({ indexName, queryVector }))[0]?.score).toBeCloseTo(0.42857142857142855, 12);
      expect((await compatible.query({ indexName, queryVector, scoreMode: 'native' }))[0]?.score).toBeCloseTo(0.2, 12);
    } finally {
      await compatible.disconnect();
      await vector.deleteIndex({ indexName });
    }
  });
});
