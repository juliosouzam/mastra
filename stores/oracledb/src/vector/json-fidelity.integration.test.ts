import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { OracleVector } from '.';

describe.runIf(process.env.RUN_ORACLE_VECTOR_INTEGRATION === 'true')('Oracle vector JSON fidelity', () => {
  let vector: OracleVector;
  const indexName = `json_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
  beforeAll(async () => {
    vector = new OracleVector({
      id: 'oracle-vector-json',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    await vector.createIndex({ indexName, dimension: 3, indexConfig: { type: 'none' } });
  });
  afterAll(async () => {
    await vector?.deleteIndex({ indexName });
    await vector?.disconnect();
  });
  it('preserves authored keys and large Unicode metadata in every query projection', async () => {
    const metadata = { ...JSON.parse('{"__proto__":{"authored":true}}'), text: 'Olá 🧩'.repeat(10_000), tag: 'json' };
    await vector.upsert({ indexName, vectors: [[1, 0, 0]], metadata: [metadata], ids: ['json-record'] });
    for (const options of [
      { filter: { tag: 'json' } },
      { queryVector: [1, 0, 0] },
      { queryVector: [1, 0, 0], minScore: 0.5 },
    ]) {
      expect((await vector.query({ indexName, topK: 1, ...options }))[0]?.metadata).toEqual(metadata);
    }
  });
});
