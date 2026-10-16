import { afterAll, beforeAll, describe } from 'vitest';
import { createPreviewTests } from '../../../../../../_test-utils/src/domains/observability-vnext/previews';
import { createSpanQueryTests } from '../../../../../../_test-utils/src/domains/observability-vnext/span-query';
import { OraclePoolManager } from '../../../../shared/connection';
import { OracleObservabilityClient } from './client';
import { TABLE_SPAN_EVENTS } from './ddl';
import { spanRecordToRow } from './helpers';
import { writeSignals } from './sql';
import { ObservabilityStorageOracleVNext } from '.';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle span queries and previews', () => {
  let manager: OraclePoolManager;
  let storage: ObservabilityStorageOracleVNext;
  beforeAll(async () => {
    manager = new OraclePoolManager({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    storage = new ObservabilityStorageOracleVNext({ poolManager: manager });
    await storage.init();
  });
  afterAll(async () => {
    await storage?.dangerouslyClearAll();
    await manager?.close();
  });
  // Like Postgres, reads select the current completed snapshot instead of folding the earliest start event.
  createSpanQueryTests(() => storage);
  createPreviewTests(() => storage, {
    writeSpanWithoutPreviews: async span => {
      const row = { ...spanRecordToRow(span), inputPreview: null, outputPreview: null, errorPreview: null };
      await writeSignals(new OracleObservabilityClient({ poolManager: manager }), '', TABLE_SPAN_EVENTS, [row]);
    },
  });
});
