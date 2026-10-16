import { randomUUID } from 'node:crypto';
import oracledb from 'oracledb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { OraclePoolManager } from '../../../../shared/connection';
import { ObservabilityStorageOracleVNext } from '.';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle observability commit cursors', () => {
  let manager: OraclePoolManager;
  let storage: ObservabilityStorageOracleVNext;
  beforeAll(async () => {
    manager = new OraclePoolManager({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    storage = new ObservabilityStorageOracleVNext({ poolManager: manager, skipDefaultIndexes: true });
    await storage.init();
    await storage.dangerouslyClearAll();
  });
  afterAll(async () => {
    await storage?.dangerouslyClearAll();
    await manager?.close();
  });

  it('captures a lower sequence value that commits after a higher sequence value', async () => {
    const connection = await (await manager.getPool()).getConnection();
    const firstId = randomUUID();
    const secondId = randomUUID();
    try {
      const head = await storage.listLogs({ mode: 'delta', limit: 1 });
      await connection.execute(
        `INSERT INTO MASTRA_OBS_LOG_EVENTS ("logId", "timestamp", "level", "message", "tags") VALUES (:id, :timestamp, 'info', 'late commit', JSON('[]'))`,
        { id: firstId, timestamp: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: new Date() } },
      );
      await storage.batchCreateLogs({
        logs: [{ logId: secondId, timestamp: new Date(), level: 'info', message: 'early commit' }],
      });
      const before = await storage.listLogs({ mode: 'delta', after: head.deltaCursor!, limit: 10 });
      expect(before.logs.map(row => row.logId)).toEqual([secondId]);
      await connection.commit();
      const after = await storage.listLogs({ mode: 'delta', after: before.deltaCursor!, limit: 10 });
      expect(after.logs.map(row => row.logId)).toEqual([firstId]);
      expect((await storage.listLogs({ mode: 'delta', after: after.deltaCursor!, limit: 10 })).logs).toEqual([]);
    } finally {
      await connection.rollback();
      await connection.close();
    }
  });

  it('prunes only expired signal rows in bounded batches', async () => {
    await storage.dangerouslyClearAll();
    const expired = Array.from({ length: 3 }, () => randomUUID());
    await storage.batchCreateLogs({
      logs: [
        ...expired.map(logId => ({
          logId,
          timestamp: new Date(Date.now() - 40 * 86_400_000),
          level: 'info' as const,
          message: 'expired',
        })),
        { logId: randomUUID(), timestamp: new Date(), level: 'info', message: 'recent' },
      ],
    });
    const first = await storage.prune({ logs: { maxAge: '30d', batchSize: 1 } }, { maxBatches: 1 });
    expect(first).toEqual([{ domain: 'observability', table: 'MASTRA_OBS_LOG_EVENTS', deleted: 1, done: false }]);
    const rest = await storage.prune({ logs: { maxAge: '30d', batchSize: 1 } });
    expect(rest).toEqual([{ domain: 'observability', table: 'MASTRA_OBS_LOG_EVENTS', deleted: 2, done: true }]);
    expect((await storage.listLogs({})).logs.map(row => row.message)).toEqual(['recent']);
  });
});
