import { afterAll, beforeAll, describe } from 'vitest';

import { createObservabilityVNextTests } from '../../../../../../_test-utils/src/domains/observability-vnext';
import { OraclePoolManager } from '../../../../shared/connection';
import { OracleObservabilityClient } from './client';
import { TABLE_SPAN_EVENTS } from './ddl';
import { SPAN_COLLAPSE_ORDER } from './sql';
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
    readTimeToFirstChunkMs: async (_storage, { traceId, spanId }) => {
      const row = await new OracleObservabilityClient({ poolManager }).one<{ timeToFirstChunkMs: number | null }>(
        `SELECT "timeToFirstChunkMs" FROM "${TABLE_SPAN_EVENTS}" WHERE "traceId" = :p1 AND "spanId" = :p2 ORDER BY ${SPAN_COLLAPSE_ORDER} FETCH FIRST 1 ROW ONLY`,
        [traceId, spanId],
      );
      return row.timeToFirstChunkMs;
    },
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
