import { randomUUID } from 'node:crypto';
import oracledb from 'oracledb';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { tableNameForIndex } from './identifiers';
import { OracleVector } from '.';

describe.runIf(process.env.RUN_ORACLE_VECTOR_INTEGRATION === 'true')('Oracle vector namespaces', () => {
  const indexName = `namespace_${randomUUID().replaceAll('-', '')}`;
  let vector: OracleVector;

  beforeAll(async () => {
    vector = new OracleVector({
      id: 'oracle-namespace-integration',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
  });

  beforeEach(async () => {
    await vector.createIndex({ indexName, dimension: 3 });
  });

  afterEach(async () => {
    await vector?.deleteIndex({ indexName });
  });

  afterAll(async () => {
    await vector?.disconnect();
  });

  it('keeps identical vector IDs independent across namespaces and the default namespace', async () => {
    await vector.upsert({
      indexName,
      namespace: 'team-a',
      ids: ['shared-id'],
      vectors: [[1, 0, 0]],
      metadata: [{ team: 'a' }],
    });
    await vector.upsert({
      indexName,
      namespace: 'team-b',
      ids: ['shared-id'],
      vectors: [[0, 1, 0]],
      metadata: [{ team: 'b' }],
    });
    await vector.upsert({ indexName, ids: ['shared-id'], vectors: [[0, 0, 1]], metadata: [{ team: 'default' }] });

    const a = await vector.query({ indexName, namespace: 'team-a', queryVector: [1, 0, 0], includeVector: true });
    const b = await vector.query({
      indexName,
      namespace: 'team-b',
      filter: { team: { $exists: true } },
      includeVector: true,
    });
    const defaultNamespace = await vector.query({ indexName, queryVector: [0, 0, 1] });
    expect(a).toHaveLength(1);
    expect(a[0]).toMatchObject({ id: 'shared-id', metadata: { team: 'a' }, vector: [1, 0, 0] });
    expect(b).toHaveLength(1);
    expect(b[0]).toMatchObject({ id: 'shared-id', metadata: { team: 'b' }, vector: [0, 1, 0] });
    expect(defaultNamespace).toHaveLength(1);
    expect(defaultNamespace[0]).toMatchObject({ id: 'shared-id', metadata: { team: 'default' } });
  });

  it('updates only the requested namespace when IDs overlap', async () => {
    for (const namespace of ['team-a', 'team-b']) {
      await vector.upsert({
        indexName,
        namespace,
        ids: ['shared-id'],
        vectors: [[1, 0, 0]],
        metadata: [{ team: namespace }],
      });
    }

    await vector.updateVector({
      indexName,
      namespace: 'team-a',
      id: 'shared-id',
      update: { metadata: { updated: true } },
    });
    const untouched = await vector.query({ indexName, namespace: 'team-b', queryVector: [1, 0, 0] });
    expect(untouched[0]?.metadata).toEqual({ team: 'team-b' });
    const updated = await vector.query({ indexName, namespace: 'team-a', queryVector: [1, 0, 0] });
    expect(updated[0]?.metadata).toEqual({ updated: true });
  });

  it.each(['single', 'ids', 'filter', 'namespace'] as const)('isolates deletion by %s', async operation => {
    for (const namespace of ['team-a', 'team-b']) {
      await vector.upsert({
        indexName,
        namespace,
        ids: ['shared-id', 'second-id'],
        vectors: [
          [1, 0, 0],
          [0, 1, 0],
        ],
        metadata: [{ kind: 'remove' }, { kind: 'keep' }],
      });
    }
    const target = { indexName, namespace: 'team-a' };
    if (operation === 'single') await vector.deleteVector({ ...target, id: 'shared-id' });
    if (operation === 'ids') await vector.deleteVectors({ ...target, ids: ['shared-id'] });
    if (operation === 'filter') await vector.deleteVectors({ ...target, filter: { kind: 'remove' } });
    if (operation === 'namespace') await vector.deleteVectors(target);

    const untouched = await vector.query({ indexName, namespace: 'team-b', filter: { kind: { $exists: true } } });
    expect(untouched.map(result => result.id).sort()).toEqual(['second-id', 'shared-id']);
    const remaining = await vector.query({ indexName, namespace: 'team-a', filter: { kind: { $exists: true } } });
    expect(remaining.map(result => result.id)).toEqual(operation === 'namespace' ? [] : ['second-id']);
  });

  it('keeps legacy rows in the default namespace when opening an older index', async () => {
    await vector.upsert({ indexName, ids: ['legacy-id'], vectors: [[1, 0, 0]], metadata: [{ legacy: true }] });
    const config = {
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    };
    const connection = await oracledb.getConnection(config);
    try {
      // Recreate the previous physical layout as an upgrade fixture.
      const table = `"${tableNameForIndex(indexName)}"`;
      await connection.execute(`ALTER TABLE ${table} DROP PRIMARY KEY`);
      await connection.execute(`ALTER TABLE ${table} DROP COLUMN namespace`);
      await connection.execute(`ALTER TABLE ${table} ADD PRIMARY KEY (vector_id)`);
    } finally {
      await connection.close();
    }

    const reopened = new OracleVector({ id: 'oracle-legacy-namespace', ...config });
    const concurrent = new OracleVector({ id: 'oracle-legacy-namespace-concurrent', ...config });
    try {
      const results = await Promise.all(
        [reopened, concurrent].map(store => store.query({ indexName, queryVector: [1, 0, 0] })),
      );
      for (const result of results)
        expect(result).toEqual([expect.objectContaining({ id: 'legacy-id', metadata: { legacy: true } })]);
      await Promise.all([
        reopened.upsert({ indexName, namespace: 'team-a', ids: ['legacy-id'], vectors: [[0, 1, 0]] }),
        concurrent.upsert({ indexName, namespace: 'team-b', ids: ['legacy-id'], vectors: [[0, 0, 1]] }),
      ]);
      expect((await reopened.query({ indexName, queryVector: [1, 0, 0] }))[0]?.metadata).toEqual({ legacy: true });
    } finally {
      await Promise.all([reopened.disconnect(), concurrent.disconnect()]);
    }
  });

  it('keeps empty, Unicode, and SQL-like namespaces distinct', async () => {
    const namespaces = ['', '💡'.repeat(255), "team' OR 1=1 --", 'default'];
    for (const namespace of namespaces) {
      await vector.upsert({
        indexName,
        namespace,
        ids: ['shared-id'],
        vectors: [[1, 0, 0]],
        metadata: [{ namespace }],
      });
    }
    for (const namespace of namespaces) {
      const result = await vector.query({ indexName, namespace, queryVector: [1, 0, 0] });
      expect(result).toHaveLength(1);
      expect(result[0]?.metadata).toEqual({ namespace });
    }
    await vector.deleteVectors({ indexName, namespace: '' });
    expect(await vector.query({ indexName, namespace: '', queryVector: [1, 0, 0] })).toEqual([]);
    expect(await vector.query({ indexName, queryVector: [1, 0, 0] })).toHaveLength(1);
  });

  it('clears every namespace while retaining a usable index definition', async () => {
    for (const namespace of ['team-a', 'team-b']) {
      await vector.upsert({ indexName, namespace, ids: ['shared-id'], vectors: [[1, 0, 0]] });
    }
    await vector.truncateIndex({ indexName });
    expect(await vector.listIndexes()).toContain(indexName);
    expect(await vector.describeIndex({ indexName })).toMatchObject({ dimension: 3, count: 0 });
    await vector.upsert({ indexName, ids: ['new-id'], vectors: [[0, 1, 0]] });
    expect(await vector.describeIndex({ indexName })).toMatchObject({ dimension: 3, count: 1 });
  });

  it('exposes index details, the connection pool, and parameterized Oracle filters', async () => {
    await vector.upsert({ indexName, ids: ['document'], vectors: [[1, 0, 0]], metadata: [{ source: 'paper' }] });
    const info = await vector.getIndexInfo({ indexName });
    expect(info).toMatchObject({ indexName, dimension: 3, count: 1, metric: 'cosine', indexType: 'none' });
    const filter = vector.transformFilter({ source: 'paper' });
    expect(Object.values(filter.binds)).toEqual(['paper']);
    expect(filter.sql).not.toContain('paper');
    const connection = await (await vector.getPool()).getConnection();
    try {
      const result = await connection.execute<{ count: number }>(
        `SELECT COUNT(*) AS "count" FROM "${info.tableName}" ${filter.sql}`,
        filter.binds,
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      expect(result.rows?.[0]?.count).toBe(1);
    } finally {
      await connection.close();
    }
  });

  it('replaces matching vectors only inside the requested namespace', async () => {
    for (const namespace of ['team-a', 'team-b']) {
      await vector.upsert({
        indexName,
        namespace,
        ids: ['old'],
        vectors: [[1, 0, 0]],
        metadata: [{ source: 'document' }],
      });
    }
    await vector.upsert({
      indexName,
      namespace: 'team-a',
      ids: ['new'],
      vectors: [[0, 1, 0]],
      metadata: [{ source: 'document' }],
      deleteFilter: { source: 'document' },
    });
    expect(
      (await vector.query({ indexName, namespace: 'team-a', filter: { source: 'document' } })).map(row => row.id),
    ).toEqual(['new']);
    expect(
      (await vector.query({ indexName, namespace: 'team-b', filter: { source: 'document' } })).map(row => row.id),
    ).toEqual(['old']);
  });

  it.each(['current', 'legacy'] as const)(
    'performs no schema writes when opening a %s index with disableInit',
    async schema => {
      await vector.upsert({ indexName, ids: ['existing'], vectors: [[1, 0, 0]] });
      const pool = await oracledb.createPool({
        user: process.env.ORACLE_DATABASE_USER,
        password: process.env.ORACLE_DATABASE_PASSWORD,
        connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      });
      const connection = await pool.getConnection();
      if (schema === 'legacy') {
        const table = `"${tableNameForIndex(indexName)}"`;
        await connection.execute(`ALTER TABLE ${table} DROP PRIMARY KEY`);
        await connection.execute(`ALTER TABLE ${table} DROP COLUMN namespace`);
        await connection.execute(`ALTER TABLE ${table} ADD PRIMARY KEY (vector_id)`);
      }
      const borrow = vi.spyOn(pool, 'getConnection').mockResolvedValue(connection);
      const execute = vi.spyOn(connection, 'execute');
      const reader = new OracleVector({ id: 'oracle-managed-schema', pool, disableInit: true });
      try {
        const query = reader.query({ indexName, queryVector: [1, 0, 0] });
        if (schema === 'legacy') await expect(query).rejects.toThrow(/namespace schema.*initialization is disabled/);
        else await expect(query).resolves.toHaveLength(1);
        expect(
          execute.mock.calls.some(call => /\b(CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|MERGE)\s/i.test(String(call[0]))),
        ).toBe(false);
      } finally {
        borrow.mockRestore();
        execute.mockRestore();
        await connection.close().catch(() => undefined);
        await reader.disconnect();
        await pool.close(0);
      }
    },
  );
});
