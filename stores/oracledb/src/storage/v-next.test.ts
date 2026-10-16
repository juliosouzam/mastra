import { describe, expect, it, vi } from 'vitest';
import { OraclePoolManager } from '../shared/connection';
import { ObservabilityOracle } from './domains/observability';
import { ObservabilityStorageOracleVNext } from './domains/observability/v-next';
import { OracleStore, OracleStoreVNext } from '.';

describe('OracleStoreVNext lifecycle', () => {
  const config = () => ({ id: 'oracle-v-next-unit', pool: {} as any, observability: { pool: {} as any } });
  it('selects advanced observability explicitly while preserving the default store', () => {
    expect(new OracleStore({ id: 'oracle-legacy-unit', pool: {} as any }).stores.observability).toBeInstanceOf(
      ObservabilityOracle,
    );
    const storage = new OracleStoreVNext(config());
    expect(storage.stores.observability).toBeInstanceOf(ObservabilityStorageOracleVNext);
    expect(storage.name).toBe('OracleStoreVNext');
    expect(Object.keys(storage.stores)).toHaveLength(23);
    expect(() => new OracleStoreVNext({ id: 'missing-observability', pool: {} } as any)).toThrow(/observability/i);
  });
  it('keeps the legacy observability migration out of the primary connection', () => {
    const storage = new OracleStoreVNext(config());
    const migrations = (storage as unknown as { storageMigrations(): { id: string }[] }).storageMigrations();
    expect(migrations.some(migration => migration.id === 'R003_OBSERVABILITY_SCHEMA')).toBe(false);
    expect(migrations.some(migration => migration.id === 'R001_MEMORY_SCHEMA')).toBe(true);
  });
  it('coalesces initialization and retries after an observability failure', async () => {
    const storage = new OracleStoreVNext(config());
    const primary = vi.spyOn(OracleStore.prototype, 'init').mockResolvedValue();
    const failure = new Error('observability unavailable');
    const obs = vi.spyOn(storage.stores.observability!, 'init').mockRejectedValueOnce(failure).mockResolvedValue();
    try {
      await expect(storage.init()).rejects.toBe(failure);
      await Promise.all([storage.init(), storage.init()]);
      await storage.init();
      expect(primary).toHaveBeenCalledTimes(2);
      expect(obs).toHaveBeenCalledTimes(2);
    } finally {
      primary.mockRestore();
    }
  });
  it('leaves caller-owned managers open', async () => {
    const primary = new OraclePoolManager({ pool: {} as any });
    const obs = new OraclePoolManager({ pool: {} as any });
    const closePrimary = vi.spyOn(primary, 'close');
    const closeObs = vi.spyOn(obs, 'close');
    await new OracleStoreVNext({
      id: 'borrowed-oracle-managers',
      poolManager: primary,
      observability: { poolManager: obs },
    }).close();
    expect(closePrimary).not.toHaveBeenCalled();
    expect(closeObs).not.toHaveBeenCalled();
  });
});
