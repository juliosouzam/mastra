import { afterAll, beforeAll, describe } from 'vitest';

import { createObservabilityVNextTests } from '../../../../../../_test-utils/src/domains/observability-vnext';
import { OraclePoolManager } from '../../../../shared/connection';
import { ObservabilityStorageOracleVNext } from '.';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle advanced observability', () => {
  let poolManager: OraclePoolManager;
  let storage: ObservabilityStorageOracleVNext;
  beforeAll(async () => {
    poolManager = new OraclePoolManager({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    storage = new ObservabilityStorageOracleVNext({ poolManager, discovery: { ttlSeconds: 0, lookbackSeconds: 0 } });
    await storage.init();
  });
  afterAll(async () => {
    await poolManager?.close();
  });
  createObservabilityVNextTests({
    getStorage: () => storage,
    capabilities: {
      label: 'Oracle',
      preferredStrategy: 'event-sourced',
      traceQuery: true,
      traceQueryDiscovery: true,
      threadQuery: true,
      traceQuerySpanWriteModel: 'event-sourced',
    },
  });
});
