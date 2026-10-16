import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle connection label fidelity', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-connection-labels',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
    });
    await store.init();
  });
  afterAll(async () => {
    await store?.close();
  });

  it('preserves scope and creation time when omitted by a label update', async () => {
    const connections = (await store.getStore('toolProviderConnections'))!;
    const key = { authorId: randomUUID(), providerId: 'composio', connectionId: randomUUID() };
    try {
      const first = await connections.upsertConnection({ ...key, toolkit: 'gmail', label: 'Work', scope: 'shared' });
      const updated = await connections.upsertConnection({ ...key, toolkit: 'calendar', label: '' });
      expect(updated).toMatchObject({ scope: 'shared', label: '', toolkit: 'calendar', createdAt: first.createdAt });
      const longLabel = 'Conta de trabalho 🌎 '.repeat(4000);
      await connections.upsertConnection({ ...key, toolkit: 'calendar', label: longLabel, scope: 'caller-supplied' });
      expect(await connections.getConnectionById(key)).toMatchObject({ label: longLabel, scope: 'caller-supplied' });
      await connections.upsertConnection({ ...key, toolkit: 'calendar', label: null });
      expect(await connections.getConnectionById(key)).toMatchObject({ label: null, scope: 'caller-supplied' });
    } finally {
      await connections.deleteConnection(key);
    }
  });

  it('keeps an explicit scope during concurrent first writes and label updates', async () => {
    const connections = (await store.getStore('toolProviderConnections'))!;
    const key = { authorId: randomUUID(), providerId: 'composio', connectionId: randomUUID() };
    const pool = await store.getPool();
    const borrowed = await Promise.all(Array.from({ length: 4 }, () => pool.getConnection()));
    await Promise.all(borrowed.map(connection => connection.close()));
    try {
      await Promise.all(
        Array.from({ length: 24 }, (_, index) =>
          connections.upsertConnection({
            ...key,
            toolkit: 'gmail',
            label: `Label ${index}`,
            ...(index === 3 ? { scope: 'shared' as const } : {}),
          }),
        ),
      );
      expect(await connections.listConnectionsByAuthor({ authorId: key.authorId })).toEqual([
        expect.objectContaining({ ...key, scope: 'shared' }),
      ]);
    } finally {
      await connections.deleteConnection(key);
    }
  });
});
