import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { OraclePoolManager } from '../shared/connection';
import { OracleStore, OracleStoreVNext } from '.';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('OracleStoreVNext public integration', () => {
  const config = {
    user: process.env.ORACLE_DATABASE_USER,
    password: process.env.ORACLE_DATABASE_PASSWORD,
    connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
  };
  let primary: OraclePoolManager;
  let observability: OraclePoolManager;
  let store: OracleStoreVNext;
  beforeAll(async () => {
    primary = new OraclePoolManager(config);
    observability = new OraclePoolManager(config);
    store = new OracleStoreVNext({
      id: 'oracle-public-vnext',
      poolManager: primary,
      observability: { poolManager: observability },
    });
    await Promise.all([store.init(), store.init()]);
  });
  afterAll(async () => {
    await store?.close();
    await Promise.all([primary?.close(), observability?.close()]);
  });

  it('routes signal writes to the explicit observability pool and preserves legacy tables', async () => {
    const legacy = new OracleStore({ id: 'oracle-legacy-coexistence', poolManager: primary });
    await legacy.stores.observability!.init();
    const logId = randomUUID();
    const traceId = randomUUID();
    const log = { logId, traceId, timestamp: new Date(), level: 'info' as const, message: 'legacy' };
    await legacy.stores.observability!.batchCreateLogs({ logs: [log] });
    const writer = vi.spyOn(primary, 'withConnection');
    const signals = vi.spyOn(observability, 'withConnection');
    try {
      await (await store.getStore('observability'))!.batchCreateLogs({ logs: [{ ...log, message: 'advanced' }] });
      expect(signals).toHaveBeenCalled();
      expect(writer).not.toHaveBeenCalled();
      const advanced = await store.stores.observability!.listLogs({ filters: { traceId } });
      const original = await legacy.stores.observability!.listLogs({ filters: { traceId } });
      expect(advanced.logs.map(row => row.message)).toEqual(['advanced']);
      expect(original.logs.map(row => row.message)).toEqual(['legacy']);
      await store.close();
      await expect(
        primary.withConnection(connection => connection.execute('SELECT 1 FROM dual')),
      ).resolves.toBeDefined();
      await expect(
        observability.withConnection(connection => connection.execute('SELECT 1 FROM dual')),
      ).resolves.toBeDefined();
    } finally {
      writer.mockRestore();
      signals.mockRestore();
      await store.stores.observability!.batchDeleteTraces({ traceIds: [traceId] });
      await legacy.stores.observability!.batchDeleteTraces({ traceIds: [traceId] });
    }
  });
});
