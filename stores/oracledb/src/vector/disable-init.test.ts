import type { Pool } from 'oracledb';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { OracleVector } from '.';

afterEach(() => {
  vi.unstubAllEnvs();
});

describe('OracleVector externally managed schemas', () => {
  it.each(['config', 'environment'] as const)(
    'does not connect for createIndex when initialization is disabled by %s',
    async mode => {
      const getConnection = vi.fn(() => {
        throw new Error('No connection should be acquired');
      });
      if (mode === 'environment') vi.stubEnv('MASTRA_DISABLE_STORAGE_INIT', 'true');
      const vector = new OracleVector({
        id: 'oracle-disabled-init',
        pool: { getConnection } as unknown as Pool,
        disableInit: mode === 'config',
      });

      await expect(vector.createIndex({ indexName: 'managed_index', dimension: 3 })).resolves.toBeUndefined();
      expect(getConnection).not.toHaveBeenCalled();
    },
  );
});
