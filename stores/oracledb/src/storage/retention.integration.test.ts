import { randomUUID } from 'node:crypto';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { createSampleMessageV2 } from '../../../_test-utils/src/domains/memory/data';
import { createSampleScore } from '../../../_test-utils/src/domains/scores/data';
import { createSampleWorkflowSnapshot } from '../../../_test-utils/src/domains/workflows/data';
import { OracleVector } from '../vector';
import { OracleStore } from '.';

const DAY = 24 * 60 * 60 * 1000;

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle retention', () => {
  let store: OracleStore;

  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-retention-integration',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
    });
    await store.init();
  });

  beforeEach(async () => {
    await store.stores.memory!.dangerouslyClearAll();
    await store.stores.workflows!.dangerouslyClearAll();
    await store.stores.scores!.dangerouslyClearAll();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  afterAll(async () => {
    await store?.close();
  });

  async function saveThread(ageDays: number) {
    const id = randomUUID();
    const at = new Date(Date.now() - ageDays * DAY);
    return store.stores.memory!.saveThread({
      thread: { id, resourceId: `resource-${id}`, title: 'Retention test', createdAt: at, updatedAt: at },
    });
  }

  it('creates supporting indexes when retention is enabled with default indexes skipped', async () => {
    const names = [
      'MASTRA_THREADS_RETENTION_IDX_IDX',
      'MASTRA_MESSAGES_RETENTION_IDX_IDX',
      'MASTRA_RESOURCES_RETENTION_IDX_IDX',
      'MASTRA_WORKFLOW_RETENTION_IDX_IDX',
      'MASTRA_SCORES_RETENTION_IDX_IDX',
    ];
    for (const name of names) await store.db.executeDdl(`DROP INDEX "${name}"`, [-1418]);

    await store.stores.memory!.prune({
      threads: { maxAge: '30d' },
      messages: { maxAge: '30d' },
      resources: { maxAge: '30d' },
    });
    await store.stores.workflows!.prune({ workflowSnapshot: { maxAge: '30d' } });
    await store.stores.scores!.prune({ scorers: { maxAge: '30d' } });
    const indexes = await store.db.manyOrNone<{ name: string }>('SELECT index_name AS "name" FROM user_indexes');
    expect(indexes.map(index => index.name)).toEqual(expect.arrayContaining(names));
  });

  it('deletes threads older than the policy and preserves recent threads', async () => {
    const old = await saveThread(40);
    const recent = await saveThread(5);
    const memory = store.stores.memory!;

    expect(await memory.prune({ threads: { maxAge: '30d' } })).toEqual([
      { domain: 'memory', table: 'mastra_threads', deleted: 1, done: true },
    ]);
    expect(await memory.getThreadById({ threadId: old.id })).toBeNull();
    expect(await memory.getThreadById({ threadId: recent.id })).not.toBeNull();
  });

  it('bounds thread deletion and resumes the remaining batches', async () => {
    await saveThread(40);
    await saveThread(40);
    await saveThread(40);
    const memory = store.stores.memory!;
    const policy = { threads: { maxAge: '30d' as const, batchSize: 1 } };

    expect(await memory.prune(policy, { maxRows: 2 })).toEqual([
      { domain: 'memory', table: 'mastra_threads', deleted: 2, done: false },
    ]);
    expect((await memory.listThreads({ perPage: false })).threads).toHaveLength(1);
    expect(await memory.prune(policy)).toEqual([{ domain: 'memory', table: 'mastra_threads', deleted: 1, done: true }]);
  });

  it('preserves resources on cancellation and applies their age policy on the next call', async () => {
    const memory = store.stores.memory!;
    const old = { id: randomUUID(), createdAt: new Date(Date.now() - 40 * DAY), updatedAt: new Date() };
    const recent = { id: randomUUID(), createdAt: new Date(), updatedAt: new Date() };
    await memory.saveResource({ resource: old });
    await memory.saveResource({ resource: recent });
    const controller = new AbortController();
    controller.abort();
    const policy = { resources: { maxAge: '30d' as const } };

    expect(await memory.prune(policy, { signal: controller.signal })).toEqual([
      { domain: 'memory', table: 'mastra_resources', deleted: 0, done: false },
    ]);
    expect(await memory.getResourceById({ resourceId: old.id })).not.toBeNull();
    await memory.prune(policy);
    expect(await memory.getResourceById({ resourceId: old.id })).toBeNull();
    expect(await memory.getResourceById({ resourceId: recent.id })).not.toBeNull();
  });

  it('expires workflow snapshots by their last update and keeps recent runs', async () => {
    const workflows = store.stores.workflows!;
    const workflowName = 'retained-workflow';
    const old = createSampleWorkflowSnapshot('success');
    const recent = createSampleWorkflowSnapshot('success');
    await workflows.persistWorkflowSnapshot({
      workflowName,
      runId: old.runId,
      snapshot: old.snapshot,
      updatedAt: new Date(Date.now() - 40 * DAY),
    });
    await workflows.persistWorkflowSnapshot({ workflowName, runId: recent.runId, snapshot: recent.snapshot });

    expect(await workflows.prune({ workflowSnapshot: { maxAge: '30d' } })).toEqual([
      { domain: 'workflows', table: 'mastra_workflow_snapshot', deleted: 1, done: true },
    ]);
    expect(await workflows.loadWorkflowSnapshot({ workflowName, runId: old.runId })).toBeNull();
    expect(await workflows.loadWorkflowSnapshot({ workflowName, runId: recent.runId })).not.toBeNull();
  });

  it('expires old scores without deleting recent evaluations', async () => {
    const scores = store.stores.scores!;
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date(Date.now() - 40 * DAY));
    const old = await scores.saveScore(createSampleScore({ scorerId: 'retained-scorer' }));
    vi.useRealTimers();
    const recent = await scores.saveScore(createSampleScore({ scorerId: 'retained-scorer' }));

    expect(await scores.prune({ scorers: { maxAge: '30d' } })).toEqual([
      { domain: 'scores', table: 'mastra_scorers', deleted: 1, done: true },
    ]);
    expect(await scores.getScoreById({ id: old.score.id })).toBeNull();
    expect(await scores.getScoreById({ id: recent.score.id })).not.toBeNull();
  });

  it('removes embeddings for expired messages while preserving live and unrelated vectors', async () => {
    const memory = store.stores.memory!;
    const thread = await saveThread(1);
    const old = createSampleMessageV2({ threadId: thread.id, thread, createdAt: new Date(Date.now() - 40 * DAY) });
    const recent = createSampleMessageV2({ threadId: thread.id, thread });
    await memory.saveMessages({ messages: [old, recent] });

    const vector = new OracleVector({ id: 'oracle-retention-vectors', poolManager: store.getPoolManager() });
    const indexName = `memory_messages_retention_${randomUUID().replaceAll('-', '')}`;
    try {
      await vector.createIndex({ indexName, dimension: 3 });
      await vector.upsert({
        indexName,
        ids: ['old', 'recent', 'unrelated'],
        vectors: [
          [1, 0, 0],
          [0, 1, 0],
          [0, 0, 1],
        ],
        metadata: [
          { message_id: old.id, resource_id: thread.resourceId },
          { message_id: recent.id, resource_id: thread.resourceId },
          { resource_id: thread.resourceId },
        ],
      });

      await memory.prune({ messages: { maxAge: '30d', batchSize: 1 } });
      expect((await memory.listMessages({ threadId: thread.id })).messages.map(message => message.id)).toEqual([
        recent.id,
      ]);
      const remaining = await vector.query({ indexName, filter: { resource_id: thread.resourceId } });
      expect(remaining.map(result => result.id).sort()).toEqual(['recent', 'unrelated']);
    } finally {
      await vector.deleteIndex({ indexName });
      await vector.disconnect();
    }
  });
});
