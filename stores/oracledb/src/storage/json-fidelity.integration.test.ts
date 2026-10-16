import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { OracleStore } from '.';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle native JSON fidelity', () => {
  let store: OracleStore;
  // Valid authored JSON, including a key that node-oracledb 6.10's OSON object decoder drops.
  const payload = {
    ...JSON.parse('{"__proto__":{"authored":true},"constructor":"value"}'),
    text: 'Olá 🧩'.repeat(10_000),
    nested: { enabled: false, empty: '', none: null },
  };
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-json-fidelity',
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

  it('preserves thread and working-memory metadata through reads and updates', async () => {
    const memory = store.stores.memory!;
    const id = randomUUID();
    const now = new Date();
    try {
      await memory.saveThread({ thread: { id, resourceId: id, metadata: payload, createdAt: now, updatedAt: now } });
      expect((await memory.getThreadById({ threadId: id }))?.metadata).toEqual(payload);
      await memory.updateThread({ id, title: 'updated' });
      expect((await memory.getThreadById({ threadId: id }))?.metadata).toEqual(payload);
      await memory.saveResource({
        resource: { id, workingMemory: 'memory', metadata: payload, createdAt: now, updatedAt: now },
      });
      expect((await memory.getResourceById({ resourceId: id }))?.metadata).toEqual(payload);
      await memory.updateResource({ resourceId: id, workingMemory: 'updated' });
      expect((await memory.getResourceById({ resourceId: id }))?.metadata).toEqual(payload);
    } finally {
      await memory.deleteThread({ threadId: id });
      await store.db.none('DELETE FROM MASTRA_RESOURCES WHERE id = :id', { id });
    }
  });

  it('preserves workflow definition metadata and authored schema extensions', async () => {
    const definitions = store.stores.workflowDefinitions!;
    const id = randomUUID();
    const inputSchema = { type: 'object', 'x-authored': payload };
    try {
      await definitions.upsert({ id, metadata: payload, inputSchema, outputSchema: true, graph: [] });
      expect(await definitions.get(id)).toMatchObject({ metadata: payload, inputSchema });
    } finally {
      await definitions.delete(id);
    }
  });

  it('preserves workspace metadata and versioned provider configuration', async () => {
    const workspaces = store.stores.workspaces!;
    const id = randomUUID();
    const filesystem = { provider: 'local', config: payload };
    try {
      await workspaces.create({ workspace: { id, name: 'JSON workspace', metadata: payload, filesystem } });
      expect((await workspaces.getById(id))?.metadata).toEqual(payload);
      expect((await workspaces.getLatestVersion(id))?.filesystem).toEqual(filesystem);
    } finally {
      await workspaces.delete(id);
    }
  });

  it('preserves channel configuration data', async () => {
    const channels = store.stores.channels!;
    const platform = randomUUID();
    try {
      await channels.saveConfig({ platform, data: payload, updatedAt: new Date() });
      expect((await channels.getConfig(platform))?.data).toEqual(payload);
    } finally {
      await channels.deleteConfig(platform);
    }
  });
});
