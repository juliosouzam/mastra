import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle thread state', () => {
  const config = {
    id: 'oracle-thread-state-integration',
    user: process.env.ORACLE_DATABASE_USER,
    password: process.env.ORACLE_DATABASE_PASSWORD,
    connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    skipDefaultIndexes: true,
  };
  let store: OracleStore;

  beforeAll(async () => {
    store = new OracleStore(config);
    await store.init();
  });

  afterAll(async () => {
    await store?.close();
  });

  it('persists a task list for another store instance to resume', async () => {
    const threadId = randomUUID();
    const tasks = [{ id: 'task-1', content: 'Search documents', status: 'pending', activeForm: 'Searching documents' }];
    const state = await store.getStore('threadState');
    expect(state).toBeDefined();
    await state!.setState({ threadId, type: 'task', value: tasks });

    const reopened = new OracleStore({ ...config, id: 'oracle-thread-state-reopened', disableInit: true });
    try {
      const persisted = await reopened.getStore('threadState');
      expect(await persisted!.getState({ threadId, type: 'task' })).toEqual(tasks);
    } finally {
      await state!.deleteState({ threadId, type: 'task' });
      await reopened.close();
    }
  });

  it('replaces values while isolating thread and type keys', async () => {
    const state = (await store.getStore('threadState'))!;
    const first = randomUUID();
    const second = randomUUID();
    try {
      expect(await state.getState({ threadId: first, type: 'task' })).toBeUndefined();
      await state.setState({ threadId: first, type: 'task', value: { previous: true, tasks: [1] } });
      await state.setState({
        threadId: first,
        type: 'goal',
        value: { objective: 'Complete research', status: 'active' },
      });
      await state.setState({ threadId: second, type: 'task', value: ['other thread'] });
      await state.setState({ threadId: first, type: 'task', value: { tasks: [2] } });
      expect(await state.getState({ threadId: first, type: 'task' })).toEqual({ tasks: [2] });
      expect(await state.getState({ threadId: first, type: 'goal' })).toEqual({
        objective: 'Complete research',
        status: 'active',
      });
      expect(await state.getState({ threadId: second, type: 'task' })).toEqual(['other thread']);
      await state.deleteState({ threadId: first, type: 'task' });
      await state.deleteState({ threadId: first, type: 'task' });
      expect(await state.getState({ threadId: first, type: 'task' })).toBeUndefined();
    } finally {
      await state.deleteState({ threadId: first, type: 'task' });
      await state.deleteState({ threadId: first, type: 'goal' });
      await state.deleteState({ threadId: second, type: 'task' });
    }
  });

  it.each([
    { name: 'null', value: null },
    { name: 'boolean', value: false },
    { name: 'number', value: 42 },
    { name: 'empty string', value: '' },
    { name: 'null string', value: 'null' },
    { name: 'number string', value: '42' },
    { name: 'Unicode string', value: 'Olá 🌎' },
    { name: 'large string', value: 'a'.repeat(80_000) },
  ])('round-trips a JSON $name', async ({ value }) => {
    const state = (await store.getStore('threadState'))!;
    const threadId = randomUUID();
    try {
      await state.setState({ threadId, type: 'custom', value });
      expect(await state.getState({ threadId, type: 'custom' })).toEqual(value);
    } finally {
      await state.deleteState({ threadId, type: 'custom' });
    }
  });

  it('accepts concurrent first writes across pools without creating duplicate slots', async () => {
    const state = (await store.getStore('threadState'))!;
    const other = new OracleStore({ ...config, id: 'oracle-thread-state-concurrent', disableInit: true });
    const otherState = (await other.getStore('threadState'))!;
    const threadId = randomUUID();
    try {
      // Warm both pools before racing the first insert, rather than serializing on connection setup.
      for (const provider of [store, other]) {
        const pool = await provider.getPool();
        const connections = await Promise.all(Array.from({ length: 4 }, () => pool.getConnection()));
        await Promise.all(connections.map(connection => connection.close()));
      }
      await Promise.all(
        Array.from({ length: 24 }, (_, value) =>
          (value % 2 ? state : otherState).setState({ threadId, type: 'task', value }),
        ),
      );
      const persisted = await state.getState<number>({ threadId, type: 'task' });
      expect(persisted).toBeGreaterThanOrEqual(0);
      expect(persisted).toBeLessThan(24);
      await state.deleteState({ threadId, type: 'task' });
      expect(await state.getState({ threadId, type: 'task' })).toBeUndefined();
    } finally {
      await state.deleteState({ threadId, type: 'task' });
      await other.close();
    }
  });

  it('prunes by last update in bounded batches and preserves refreshed state', async () => {
    const state = (await store.getStore('threadState'))!;
    const old = randomUUID();
    const active = randomUUID();
    const now = Date.now();
    try {
      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(now - 40 * 24 * 60 * 60 * 1000);
      await state.setState({ threadId: old, type: 'goal', value: { objective: 'old' } });
      await state.setState({ threadId: active, type: 'goal', value: { objective: 'active' } });
      vi.setSystemTime(now);
      await state.setState({ threadId: active, type: 'goal', value: { objective: 'refreshed' } });

      expect(await state.prune({ threadState: { maxAge: '30d', batchSize: 1 } }, { maxRows: 1 })).toEqual([
        { domain: 'threadState', table: 'mastra_thread_state', deleted: 1, done: false },
      ]);
      expect(await state.getState({ threadId: old, type: 'goal' })).toBeUndefined();
      expect(await state.getState({ threadId: active, type: 'goal' })).toEqual({ objective: 'refreshed' });
      expect(await state.prune({ threadState: { maxAge: '30d' } })).toEqual([
        { domain: 'threadState', table: 'mastra_thread_state', deleted: 0, done: true },
      ]);
    } finally {
      vi.useRealTimers();
      await state.deleteState({ threadId: old, type: 'goal' });
      await state.deleteState({ threadId: active, type: 'goal' });
    }
  });

  it('uses the same retention cutoff in a session with a different timezone', async () => {
    const zonedStore = new OracleStore({ ...config, id: 'oracle-retention-timezone', poolMax: 1, disableInit: true });
    const state = (await zonedStore.getStore('threadState'))!;
    const old = randomUUID();
    const recent = randomUUID();
    const now = Date.now();
    const cutoff = now - 30 * 24 * 60 * 60 * 1000;
    try {
      await zonedStore.db.none("ALTER SESSION SET TIME_ZONE = '+09:00'");
      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(cutoff - 60_000);
      await state.setState({ threadId: old, type: 'timezone', value: 'old' });
      vi.setSystemTime(cutoff + 60_000);
      await state.setState({ threadId: recent, type: 'timezone', value: 'recent' });
      vi.setSystemTime(now);
      await state.prune({ threadState: { maxAge: '30d' } });
      expect(await state.getState({ threadId: old, type: 'timezone' })).toBeUndefined();
      expect(await state.getState({ threadId: recent, type: 'timezone' })).toBe('recent');
    } finally {
      vi.useRealTimers();
      await state.deleteState({ threadId: old, type: 'timezone' });
      await state.deleteState({ threadId: recent, type: 'timezone' });
      await zonedStore.close();
    }
  });
});
