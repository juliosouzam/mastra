import { randomUUID } from 'node:crypto';
import { Mastra } from '@mastra/core/mastra';
import { dispatchDueNotifications } from '@mastra/core/notifications';
import { TABLE_NOTIFICATIONS } from '@mastra/core/storage';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import type { Pool } from 'oracledb';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createSampleWorkflowSnapshot } from '../../../_test-utils/src/domains/workflows/data';
import { OraclePoolManager } from '../shared/connection';
import { OracleStore } from '.';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle read/write pools', () => {
  let manager: OraclePoolManager;
  let storage: OracleStore;
  const replicaQuery = vi.fn(async (sql: string) => ({ rows: /COUNT\(/i.test(sql) ? [{ count: 0 }] : [] }));
  const replicaClose = vi.fn();
  const replica = {
    getConnection: vi.fn(async () => ({ execute: replicaQuery, close: vi.fn() })),
    close: replicaClose,
  } as unknown as Pool;
  beforeAll(async () => {
    manager = new OraclePoolManager({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    storage = new OracleStore({
      id: 'oracle-read-routing',
      poolManager: manager,
      readPool: replica,
      skipDefaultIndexes: true,
    });
    await storage.init();
  });
  afterAll(async () => {
    await storage?.close();
    await manager?.close();
  });

  it('initializes only the writer and routes plain reads to the replica', async () => {
    expect(replicaQuery).not.toHaveBeenCalled();
    expect(await storage.getReadPool()).toBe(replica);
    const blobs = storage.stores.blobs!;
    const hash = randomUUID();
    await blobs.put({ hash, content: 'replica fixture', size: 15, mimeType: 'text/plain', createdAt: new Date() });
    expect(await blobs.get(hash)).toBeNull();
    expect(replicaQuery).toHaveBeenCalled();
    replicaQuery.mockClear();
    expect(await blobs.delete(hash)).toBe(true);
    expect(replicaQuery).not.toHaveBeenCalled();
  });

  it('keeps memory validation, mutation results and cloning on the writer', async () => {
    const memory = storage.stores.memory!;
    const threadId = randomUUID();
    const cloneId = randomUUID();
    const resourceId = randomUUID();
    const now = new Date();
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: 'before', metadata: { a: 1 }, createdAt: now, updatedAt: now },
    });
    try {
      expect(await memory.getThreadById({ threadId })).toBeNull();
      replicaQuery.mockClear();
      await memory.saveMessages({
        messages: [
          {
            id: randomUUID(),
            threadId,
            resourceId,
            role: 'user',
            type: 'text',
            content: { format: 2, parts: [{ type: 'text', text: 'hello' }] },
            createdAt: now,
          },
        ],
      });
      expect(await memory.updateThread({ id: threadId, title: 'after', metadata: { b: 2 } })).toMatchObject({
        title: 'after',
        metadata: { a: 1, b: 2 },
      });
      const cloned = await memory.cloneThread({ sourceThreadId: threadId, newThreadId: cloneId });
      expect(cloned.clonedMessages).toHaveLength(1);
      expect(replicaQuery).not.toHaveBeenCalled();
    } finally {
      await memory.deleteThread({ threadId });
      await memory.deleteThread({ threadId: cloneId });
    }
  });

  it('validates dataset mutations against committed writer rows', async () => {
    const datasets = storage.stores.datasets!;
    const dataset = await datasets.createDataset({ id: randomUUID(), name: 'replica dataset' });
    try {
      expect(await datasets.getDatasetById({ id: dataset.id })).toBeNull();
      replicaQuery.mockClear();
      const [item] = await datasets.batchInsertItems({ datasetId: dataset.id, items: [{ input: { q: 1 } }] });
      await datasets.updateItem({ datasetId: dataset.id, id: item!.id, input: { q: 2 } });
      await expect(
        datasets.updateDataset({
          id: dataset.id,
          inputSchema: { type: 'object', properties: { q: { type: 'string' } } },
        }),
      ).rejects.toThrow();
      expect(replicaQuery).not.toHaveBeenCalled();
    } finally {
      await datasets.deleteDataset({ id: dataset.id });
    }
  });

  it('reads and returns registry mutations on the writer', async () => {
    const agents = storage.stores.agents!;
    const id = randomUUID();
    await agents.create({
      agent: { id, name: 'replica agent', instructions: 'fixture', model: { provider: 'test', name: 'test-model' } },
    });
    try {
      expect(await agents.getById(id)).toBeNull();
      replicaQuery.mockClear();
      expect(await agents.update({ id, metadata: { changed: true } })).toMatchObject({ metadata: { changed: true } });
      expect(replicaQuery).not.toHaveBeenCalled();
    } finally {
      await agents.delete(id);
    }
  });

  it('preserves persisted workflow state when creating a run with a lagging replica', async () => {
    const schema = { type: 'object' as const, properties: {} };
    const workflow = createWorkflow({
      id: `read-routing-workflow-${randomUUID()}`,
      inputSchema: schema,
      outputSchema: schema,
    })
      .then(createStep({ id: 'fixture-step', inputSchema: schema, outputSchema: schema, execute: async () => ({}) }))
      .commit();
    new Mastra({ storage, workflows: { fixture: workflow }, logger: false });
    const workflows = storage.stores.workflows!;
    const { snapshot, runId } = createSampleWorkflowSnapshot('suspended');
    try {
      await workflows.persistWorkflowSnapshot({ workflowName: workflow.id, runId, snapshot });
      await workflow.createRun({ runId });
      expect(await workflows.loadWorkflowSnapshot({ workflowName: workflow.id, runId })).toEqual(snapshot);
    } finally {
      await workflows.deleteWorkflowRunById({ workflowName: workflow.id, runId });
    }
  });

  it('does not deliver a notification again when the replica retains its pending state', async () => {
    const connection = await (await manager.getPool()).getConnection();
    const snapshotPool = {
      getConnection: async () => ({ execute: connection.execute.bind(connection), close: async () => {} }),
    } as unknown as Pool;
    const lagged = new OracleStore({ id: 'oracle-notification-replica', poolManager: manager, readPool: snapshotPool });
    const notifications = lagged.stores.notifications!;
    const id = randomUUID();
    const threadId = randomUUID();
    const resourceId = randomUUID();
    const sent: string[] = [];
    const mastra = {
      getAgentById: async () => ({
        sendSignal: (signal: { id: string }) => {
          sent.push(signal.id);
          return {
            signal,
            accepted: Promise.resolve({ action: 'deliver', runId: 'notification-fixture' }),
            persisted: Promise.resolve(),
          };
        },
      }),
    } as unknown as Mastra;
    try {
      await notifications.createNotification({
        id,
        threadId,
        resourceId,
        agentId: randomUUID(),
        source: 'read-routing',
        kind: 'fixture',
        summary: 'Deliver once',
        deliverAt: new Date(Date.now() - 1000),
      });
      // Hold one database snapshot so the reader continues to see the original pending record.
      await connection.execute('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE');
      await connection.execute('SELECT 1 FROM dual');
      const now = new Date();
      const first = await dispatchDueNotifications({ mastra, storage: notifications, now, resourceId });
      const current = await notifications.getNotification({ id, threadId });
      const due = await notifications.listDueNotifications({ now, resourceId });
      const second = await dispatchDueNotifications({ mastra, storage: notifications, now, resourceId });
      expect({
        status: current?.status,
        deliveredSignalId: current?.deliveredSignalId,
        due: due.length,
        delivered: [first.delivered.length, second.delivered.length],
        sent: sent.length,
      }).toEqual({
        status: 'delivered',
        deliveredSignalId: first.signals[0]?.id,
        due: 0,
        delivered: [1, 0],
        sent: 1,
      });
    } finally {
      await connection.rollback();
      await connection.close();
      await lagged.close();
      await storage.db.none(`DELETE FROM ${storage.db.table(TABLE_NOTIFICATIONS)} WHERE "threadId" = :threadId`, {
        threadId,
      });
    }
  });

  it('counts committed running tasks when the replica still reports none', async () => {
    const writer = new OracleStore({ id: 'oracle-background-counter-writer', poolManager: manager });
    const tasks = storage.stores.backgroundTasks!;
    const previous = await writer.stores.backgroundTasks!.getRunningCount();
    const task = {
      id: randomUUID(),
      status: 'running' as const,
      toolName: 'read-routing',
      toolCallId: randomUUID(),
      args: {},
      agentId: randomUUID(),
      runId: randomUUID(),
      createdAt: new Date(),
      retryCount: 0,
      maxRetries: 0,
      timeoutMs: 60_000,
    };
    try {
      await tasks.createTask(task);
      expect({
        global: await tasks.getRunningCount(),
        agent: await tasks.getRunningCountByAgent(task.agentId),
      }).toEqual({ global: previous + 1, agent: 1 });
    } finally {
      await tasks.deleteTask(task.id);
      await writer.close();
    }
  });

  it('leaves borrowed writer and reader pools open', async () => {
    await storage.close();
    expect(replicaClose).not.toHaveBeenCalled();
    await expect(manager.withConnection(connection => connection.execute('SELECT 1 FROM dual'))).resolves.toBeDefined();
    const single = new OracleStore({ id: 'oracle-one-pool', poolManager: manager });
    expect(single.readDb).toBe(single.db);
    expect(await single.getReadPool()).toBe(await single.getPool());
  });
});
