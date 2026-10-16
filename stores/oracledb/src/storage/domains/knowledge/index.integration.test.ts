import { TABLE_KNOWLEDGE_ACCESS_STATE, TABLE_KNOWLEDGE_NODES } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { OracleStore } from '../..';
import { createKnowledgeStorageTests } from '../../../../../_test-utils/src/domains/knowledge';
import { OraclePoolManager } from '../../../shared/connection';
import { OracleDB } from '../../db';
import { KnowledgeOracle, getDefaultKnowledgeIndexDefinitions } from '.';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle Knowledge V2', () => {
  let poolManager: OraclePoolManager;
  let knowledge: KnowledgeOracle;
  beforeAll(async () => {
    poolManager = new OraclePoolManager({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    knowledge = new KnowledgeOracle({ poolManager, skipDefaultIndexes: true });
  });
  afterAll(async () => {
    await knowledge?.dangerouslyClearAll();
    await poolManager?.close();
  });
  createKnowledgeStorageTests(() => knowledge);

  it('serializes cross-pool identity creation and gives semantic work to only one current owner', async () => {
    const otherPool = new OraclePoolManager({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      poolMax: 1,
    });
    const other = new KnowledgeOracle({ poolManager: otherPool, skipDefaultIndexes: true });
    try {
      await knowledge.dangerouslyClearAll();
      const nodes = await Promise.all(
        Array.from({ length: 16 }, (_, i) =>
          (i % 2 ? knowledge : other).createNode({ name: 'One identity', kind: 'test', scope: ['org:concurrent'] }),
        ),
      );
      expect(new Set(nodes.map(node => node.id)).size).toBe(1);
      const [first, second] = await Promise.all([
        knowledge.claimSemanticOutbox({ workerId: 'first' }),
        other.claimSemanticOutbox({ workerId: 'second' }),
      ]);
      expect([...first, ...second]).toHaveLength(1);
      const claimed = [...first, ...second][0]!;
      const stale = new Date(claimed.claimedAt!.getTime() + 60_001);
      const retry = await other.claimSemanticOutbox({ workerId: 'retry', now: stale });
      expect(retry).toMatchObject([{ id: claimed.id, attempts: 2, claimedBy: 'retry' }]);
      await knowledge.completeSemanticOutbox({ ids: [claimed.id], workerId: claimed.claimedBy! });
      expect(await other.listSemanticOutbox({ status: 'processing' })).toHaveLength(1);
      await other.completeSemanticOutbox({ ids: [claimed.id], workerId: 'retry' });
      expect(await knowledge.listSemanticOutbox({ status: 'completed' })).toHaveLength(1);
    } finally {
      await otherPool.close();
    }
  });

  it('creates all default indexes and exposes the domain through the composite store', async () => {
    const indexed = new KnowledgeOracle({ poolManager });
    await indexed.init();
    const db = new OracleDB({ poolManager });
    const indexes = await db.manyOrNone<{ name: string }>(
      `SELECT index_name AS "name" FROM user_indexes WHERE table_name LIKE 'MASTRA_KNOWLEDGE_%'`,
    );
    expect(indexes.map(index => index.name)).toEqual(
      expect.arrayContaining(getDefaultKnowledgeIndexDefinitions().map(index => index.name)),
    );
    const store = new OracleStore({
      id: 'knowledge-composition',
      poolManager,
      storageIsolationKey: 'explicit-knowledge-identity',
    });
    expect((await store.getStore('knowledge'))!.getStorageIsolationKey()).toBe('explicit-knowledge-identity');
  });

  it('recovers an interrupted schema initialization and supports concurrent startup', async () => {
    const failing = new KnowledgeOracle({
      poolManager,
      skipDefaultIndexes: true,
      indexes: [{ name: 'ORACLE_KNOWLEDGE_BAD_TEST_IDX', table: TABLE_KNOWLEDGE_NODES, columns: ['missing_column'] }],
    });
    // These Knowledge tables belong exclusively to this integration fixture.
    await expect(failing.dangerouslyReset()).rejects.toThrow();
    const other = new KnowledgeOracle({ poolManager, skipDefaultIndexes: true });
    await Promise.all([knowledge.init(), other.init()]);
    expect(await knowledge.inspectSchema()).toEqual({ status: 'compatible', schemaVersion: 2 });
    const node = await knowledge.createNode({ name: 'Retained after startup', kind: 'test', scope: ['org:recovery'] });
    await Promise.all([knowledge.init(), other.init()]);
    expect(await other.getNode(node.id)).toMatchObject(node);
  });

  it('refuses incompatible schemas without mutating them until the explicit Knowledge-only reset', async () => {
    const db = new OracleDB({ poolManager });
    await db.none(`UPDATE ${db.table('mastra_knowledge_access_state')} SET "schemaVersion" = 1 WHERE id = 'global'`);
    const before = await db.manyOrNone(
      `SELECT id AS "id", name AS "name" FROM ${db.table(TABLE_KNOWLEDGE_NODES)} ORDER BY id`,
    );
    expect(await knowledge.inspectSchema()).toMatchObject({ status: 'incompatible-reset-required' });
    await expect(knowledge.init()).rejects.toThrow('Knowledge schema reset required');
    expect(
      await db.manyOrNone(`SELECT id AS "id", name AS "name" FROM ${db.table(TABLE_KNOWLEDGE_NODES)} ORDER BY id`),
    ).toEqual(before);
    await knowledge.dangerouslyReset();
    expect(await knowledge.inspectSchema()).toEqual({ status: 'compatible', schemaVersion: 2 });
  });

  it('publishes the initialization marker with the first visible access-state table', async () => {
    let publish!: () => void;
    let resume!: () => void;
    const visible = new Promise<void>(resolve => {
      publish = resolve;
    });
    const resumed = new Promise<void>(resolve => {
      resume = resolve;
    });
    const original = OracleDB.prototype.createTable;
    let paused = false;
    const create = vi
      .spyOn(OracleDB.prototype, 'createTable')
      .mockImplementation(async function (this: OracleDB, args) {
        await original.call(this, args);
        if (!paused && args.tableName === TABLE_KNOWLEDGE_ACCESS_STATE) {
          paused = true;
          publish();
          await resumed;
        }
      });
    const first = knowledge.dangerouslyReset();
    try {
      await visible;
      const concurrent = new KnowledgeOracle({ poolManager, skipDefaultIndexes: true });
      await expect(concurrent.init()).resolves.toBeUndefined();
    } finally {
      resume();
      await first;
      create.mockRestore();
    }
    expect(await knowledge.inspectSchema()).toEqual({ status: 'compatible', schemaVersion: 2 });
  });
});
