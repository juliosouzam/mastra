import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';
import { createSampleTask } from '../../../../../_test-utils/src/domains/background-tasks/data';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle background task fidelity', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-background-task-fidelity',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
      poolMax: 1,
    });
    await store.init();
    await store.db.none("ALTER SESSION SET TIME_ZONE = '+09:00'");
  });
  afterAll(async () => {
    await store?.close();
  });

  it.each([false, 0, null, 'null', 'false', ''])(
    'preserves the JSON result %j and distinguishes clearing it',
    async result => {
      const tasks = (await store.getStore('backgroundTasks'))!;
      const task = createSampleTask({ result, suspendPayload: result });
      try {
        await tasks.createTask(task);
        expect(await tasks.getTask(task.id)).toMatchObject({ result, suspendPayload: result });
        expect(await tasks.updateTask(task.id, { result, suspendPayload: result, maxRetries: 7, timeoutMs: 10 })).toBe(
          true,
        );
        expect(await tasks.getTask(task.id)).toMatchObject({
          result,
          suspendPayload: result,
          maxRetries: 7,
          timeoutMs: 10,
        });
        await tasks.updateTask(task.id, { result: undefined, suspendPayload: undefined });
        expect((await tasks.getTask(task.id))!.result).toBeUndefined();
        expect((await tasks.getTask(task.id))!.suspendPayload).toBeUndefined();
      } finally {
        await tasks.deleteTask(task.id);
      }
    },
  );

  it('applies owner and lease conditions at the original timestamp instant', async () => {
    const tasks = (await store.getStore('backgroundTasks'))!;
    const leaseExpiresAt = new Date('2026-10-16T15:00:00.123Z');
    const task = createSampleTask({ status: 'running', ownerId: 'worker-1', leaseExpiresAt });
    try {
      await tasks.createTask(task);
      expect(
        await tasks.updateTask(
          task.id,
          { result: 'stale' },
          { expectedOwnerId: 'worker-1', expectedLeaseExpiresAt: new Date(leaseExpiresAt.getTime() + 1) },
        ),
      ).toBe(false);
      expect(
        await tasks.updateTask(
          task.id,
          { result: 'complete', status: 'completed', completedAt: new Date() },
          { expectedStatus: 'running', expectedOwnerId: 'worker-1', expectedLeaseExpiresAt: leaseExpiresAt },
        ),
      ).toBe(true);
      expect((await tasks.getTask(task.id))!.result).toBe('complete');
    } finally {
      await tasks.deleteTask(task.id);
    }
  });

  it('applies every deletion filter and keeps unfinished tasks during retention', async () => {
    const tasks = (await store.getStore('backgroundTasks'))!;
    const old = new Date(Date.now() - 40 * 24 * 60 * 60 * 1000);
    const threadId = randomUUID();
    const resourceId = randomUUID();
    const completed = createSampleTask({ status: 'completed', completedAt: old, createdAt: old, threadId, resourceId });
    const running = createSampleTask({ status: 'running', createdAt: old, threadId, resourceId });
    const unrelated = createSampleTask({ status: 'pending', resourceId: randomUUID() });
    try {
      for (const task of [completed, running, unrelated]) await tasks.createTask(task);
      await tasks.prune({ backgroundTasks: { maxAge: '30d', batchSize: 1 } });
      expect(await tasks.getTask(completed.id)).toBeNull();
      expect(await tasks.getTask(running.id)).not.toBeNull();
      expect(await tasks.listTasks({ resourceId, status: [] })).toEqual({ tasks: [], total: 0 });
      await tasks.deleteTasks({ resourceId, threadId, toolName: running.toolName });
      expect(await tasks.getTask(running.id)).toBeNull();
      expect(await tasks.getTask(unrelated.id)).not.toBeNull();
      await tasks.deleteTasks({});
      expect(await tasks.getTask(unrelated.id)).not.toBeNull();
    } finally {
      for (const task of [completed, running, unrelated]) await tasks.deleteTask(task.id);
    }
  });
});
