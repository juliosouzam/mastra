import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle notifications', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-notifications',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
    });
    await store.init();
  });
  beforeEach(async () => {
    await store.stores.notifications?.dangerouslyClearAll();
  });
  afterAll(async () => {
    await store?.stores.notifications?.dangerouslyClearAll();
    await store?.close();
  });

  it('persists notifications with full JSON data and isolates equal IDs by thread', async () => {
    const notifications = await store.getStore('notifications');
    expect(notifications).toBeDefined();
    const firstThread = randomUUID();
    const secondThread = randomUUID();
    const input = {
      id: randomUUID(),
      threadId: firstThread,
      agentId: 'agent-1',
      resourceId: 'resource-1',
      source: 'research',
      kind: 'complete',
      summary: 'Pesquisa concluída 🌎',
      payload: { findings: ['a', 'b'], enabled: false },
      metadata: { nested: { count: 1 } },
      attributes: { important: true, count: 0 },
      createdAt: new Date('2026-10-16T12:00:00.123Z'),
      deliverAt: new Date('2026-10-16T13:00:00.456Z'),
    };
    const created = await notifications!.createNotification(input);
    expect(created).toMatchObject({
      ...input,
      priority: 'medium',
      status: 'pending',
      coalescedCount: 1,
      deliveryAttempts: 0,
    });
    await notifications!.createNotification({ ...input, threadId: secondThread, summary: 'Other thread' });
    expect(await notifications!.getNotification({ threadId: firstThread, id: input.id })).toMatchObject(input);
    expect((await notifications!.listNotifications({ threadId: secondThread })).map(record => record.summary)).toEqual([
      'Other thread',
    ]);
  });

  it('coalesces concurrent notifications only within the full thread and tenant scope', async () => {
    const notifications = (await store.getStore('notifications'))!;
    const other = new OracleStore({
      id: 'oracle-notifications-other',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      disableInit: true,
    });
    const second = (await other.getStore('notifications'))!;
    const threadId = randomUUID();
    const common = {
      threadId,
      agentId: 'agent-a',
      resourceId: 'resource-a',
      source: 'research',
      kind: 'progress',
      dedupeKey: 'job-1',
      coalesceKey: 'progress',
      summary: 'Progress',
    };
    try {
      const results = await Promise.all(
        Array.from({ length: 16 }, (_, index) =>
          (index % 2 ? notifications : second).createNotification({
            ...common,
            summary: `Progress ${index}`,
            attributes: { [`worker${index}`]: true },
          }),
        ),
      );
      expect(new Set(results.map(record => record.id)).size).toBe(1);
      const list = await notifications.listNotifications({ threadId });
      expect(list).toHaveLength(1);
      expect(list[0]!.coalescedCount).toBe(16);
      expect(Object.keys(list[0]!.attributes!)).toHaveLength(16);
      const merged = await notifications.createNotification({
        ...common,
        dedupeKey: 'another',
        payload: false,
        priority: 'urgent',
        metadata: { attempt: 2 },
        summary: 'Final progress',
      });
      expect(merged).toMatchObject({
        id: list[0]!.id,
        coalescedCount: 17,
        priority: 'urgent',
        payload: false,
        metadata: { attempt: 2 },
      });
      await notifications.createNotification({ ...common, agentId: 'agent-b' });
      await notifications.createNotification({ ...common, resourceId: 'resource-b' });
      expect(await notifications.listNotifications({ threadId })).toHaveLength(3);
      await notifications.updateNotification({ threadId, id: merged.id, status: 'seen' });
      expect((await notifications.createNotification(common)).id).not.toBe(merged.id);
    } finally {
      await other.close();
    }
  });

  it('updates long ID lists once per record without crossing thread boundaries', async () => {
    const notifications = (await store.getStore('notifications'))!;
    const threadId = randomUUID();
    const otherThread = randomUUID();
    const input = { source: 'bulk', kind: 'test', summary: 'Bulk status' };
    await notifications.createNotification({ ...input, threadId, id: 'a' });
    await notifications.createNotification({ ...input, threadId, id: 'b' });
    await notifications.createNotification({ ...input, threadId: otherThread, id: 'a' });
    const ids = [...Array.from({ length: 70_000 }, (_, index) => `missing-${index}`), 'a', 'b', 'a'];
    const updated = await notifications.updateNotificationsStatus({ threadId, ids, status: 'seen' });
    expect(updated.map(record => record.id).sort()).toEqual(['a', 'b']);
    expect(updated.every(record => record.status === 'seen' && record.seenAt instanceof Date)).toBe(true);
    expect((await notifications.getNotification({ threadId: otherThread, id: 'a' }))!.status).toBe('pending');
    expect(await notifications.updateNotificationsStatus({ threadId, ids: [], status: 'archived' })).toEqual([]);
  }, 60_000);

  it('finds due records by the earlier delivery time and applies literal search and limits', async () => {
    const notifications = (await store.getStore('notifications'))!;
    const threadId = randomUUID();
    const agentId = randomUUID();
    const now = new Date();
    await notifications.createNotification({
      id: 'earlier-summary',
      threadId,
      agentId,
      source: 'release%source',
      kind: 'build',
      summary: 'Release',
      summaryAt: new Date(now.getTime() - 1000),
      deliverAt: new Date(now.getTime() + 10_000),
      priority: 'urgent',
    });
    await notifications.createNotification({
      id: 'later-delivery',
      threadId,
      agentId,
      source: 'release-source',
      kind: 'build',
      summary: 'Release',
      deliverAt: new Date(now.getTime() - 500),
    });
    await notifications.createNotification({
      id: 'unscheduled',
      threadId,
      agentId,
      source: 'research',
      kind: 'done',
      summary: 'Ready',
    });
    expect((await notifications.listDueNotifications({ now, agentId })).map(record => record.id)).toEqual([
      'earlier-summary',
      'later-delivery',
    ]);
    expect(await notifications.listDueNotifications({ now, agentId, limit: 0 })).toEqual([]);
    expect((await notifications.listNotifications({ threadId, search: '%' })).map(record => record.id)).toEqual([
      'earlier-summary',
    ]);
    expect(await notifications.listNotifications({ threadId, priority: [], limit: 10 })).toEqual([]);
    await notifications.updateNotification({
      threadId,
      id: 'earlier-summary',
      deliverAt: null,
      summaryAt: null,
      status: 'delivered',
    });
    expect((await notifications.listDueNotifications({ now, agentId })).map(record => record.id)).toEqual([
      'later-delivery',
    ]);
    expect((await notifications.getNotification({ threadId, id: 'earlier-summary' }))!.deliveredAt).toBeInstanceOf(
      Date,
    );
  });

  it('preserves large text and JSON null while retention expires only old records', async () => {
    const notifications = (await store.getStore('notifications'))!;
    const threadId = randomUUID();
    const common = {
      threadId,
      source: 'research',
      kind: 'result',
      summary: 'Long summary '.repeat(6000),
      payload: null,
    };
    const old = await notifications.createNotification({
      ...common,
      createdAt: new Date(Date.now() - 40 * 24 * 60 * 60 * 1000),
    });
    const recent = await notifications.createNotification(common);
    expect(recent.payload).toBeNull();
    await notifications.updateNotification({
      threadId,
      id: recent.id,
      payload: 'false',
      deliveryReason: '',
      lastDeliveryError: 'details '.repeat(6000),
    });
    expect(await notifications.getNotification({ threadId, id: recent.id })).toMatchObject({
      payload: 'false',
      deliveryReason: '',
      lastDeliveryError: 'details '.repeat(6000),
    });
    await notifications.prune({ notifications: { maxAge: '30d', batchSize: 1 } });
    expect(await notifications.getNotification({ threadId, id: old.id })).toBeNull();
    expect((await notifications.getNotification({ threadId, id: recent.id }))!.summary).toBe(common.summary);
  });
});
