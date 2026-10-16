import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';
import { createSampleInstallation } from '../../../../../_test-utils/src/domains/channels/data';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle channel routing', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-channel-routing',
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

  it('prefers active installations and rejects conflicting webhook routing', async () => {
    const channels = (await store.getStore('channels'))!;
    const agentId = randomUUID();
    const active = createSampleInstallation({ agentId, status: 'active' });
    const pending = createSampleInstallation({ agentId, status: 'pending' });
    const error = createSampleInstallation({ agentId, status: 'error', error: 'failure details'.repeat(5000) });
    const duplicate = createSampleInstallation({ agentId, webhookId: active.webhookId });
    try {
      await channels.saveInstallation(active);
      await channels.saveInstallation(pending);
      await channels.saveInstallation(error);
      expect((await channels.getInstallationByAgent('slack', agentId))!.id).toBe(active.id);
      expect((await channels.getInstallation(error.id))!.error).toBe(error.error);
      await expect(channels.saveInstallation(duplicate)).rejects.toThrow();
      expect(await channels.getInstallation(duplicate.id)).toBeNull();
      expect((await channels.getInstallationByWebhookId(active.webhookId!))!.id).toBe(active.id);
      await channels.deleteInstallation(active.id);
      expect((await channels.getInstallationByAgent('slack', agentId))!.id).toBe(pending.id);
    } finally {
      for (const entry of [active, pending, error, duplicate]) await channels.deleteInstallation(entry.id);
    }
  });

  it('preserves large serialized data and tolerates concurrent first writes', async () => {
    const channels = (await store.getStore('channels'))!;
    const entry = createSampleInstallation({
      data: { serialized: 'a'.repeat(100_000), enabled: false },
      createdAt: new Date('2026-01-01T00:00:00.123Z'),
    });
    try {
      await Promise.all(Array.from({ length: 12 }, () => channels.saveInstallation(entry)));
      expect(await channels.getInstallation(entry.id)).toMatchObject({ data: entry.data, createdAt: entry.createdAt });
      await channels.saveInstallation({ ...entry, webhookId: undefined, configHash: undefined, error: '' });
      expect(await channels.getInstallationByWebhookId(entry.webhookId!)).toBeNull();
      expect(await channels.getInstallation(entry.id)).toMatchObject({
        webhookId: undefined,
        configHash: undefined,
        error: '',
      });
    } finally {
      await channels.deleteInstallation(entry.id);
    }
  });
});
