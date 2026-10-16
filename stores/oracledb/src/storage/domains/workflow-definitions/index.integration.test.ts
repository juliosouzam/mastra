import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle workflow definition fidelity', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-workflow-definitions',
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

  it('keeps timestamp instants independent of the database session timezone', async () => {
    const definitions = (await store.getStore('workflowDefinitions'))!;
    const id = randomUUID();
    const createdAt = new Date('2026-01-15T12:34:56.123Z');
    const updatedAt = new Date('2026-01-16T12:34:56.456Z');
    try {
      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(createdAt);
      const created = await definitions.upsert({
        id,
        inputSchema: { type: 'object' },
        outputSchema: { type: 'object' },
        graph: [],
      });
      expect(created.createdAt).toEqual(createdAt);
      expect(created.description).toBeUndefined();
      expect(created.updatedAt).toEqual(createdAt);
      vi.setSystemTime(updatedAt);
      const updated = await definitions.upsert({ id, description: 'updated' });
      expect(updated.createdAt).toEqual(createdAt);
      expect(updated.updatedAt).toEqual(updatedAt);
    } finally {
      vi.useRealTimers();
      await definitions.delete(id);
    }
  });

  it('preserves boolean schemas, empty descriptions, and schedules through partial updates', async () => {
    const definitions = (await store.getStore('workflowDefinitions'))!;
    const id = randomUUID();
    const schedule = [{ id: 'daily', cron: '0 9 * * *', timezone: 'America/Fortaleza', inputData: { query: 'Olá' } }];
    try {
      await definitions.upsert({ id, inputSchema: false, outputSchema: true, graph: [], description: '', schedule });
      expect(await definitions.get(id)).toMatchObject({
        inputSchema: false,
        outputSchema: true,
        description: '',
        schedule,
      });
      await definitions.upsert({ id, metadata: { revision: 2 } });
      expect(await definitions.get(id)).toMatchObject({ description: '', schedule, metadata: { revision: 2 } });
      await definitions.upsert({ id, schedule: null });
      expect((await definitions.get(id))!.schedule).toBeUndefined();
    } finally {
      await definitions.delete(id);
    }
  });
});
