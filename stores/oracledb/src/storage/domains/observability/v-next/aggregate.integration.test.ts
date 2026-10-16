import { parseTraceAggregateRequest, planTraceAggregate } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { writeTraceAggregateFixture } from '../../../../../../_test-utils/src/domains/observability-vnext';
import {
  TRACE_AGGREGATE_CONFORMANCE_CASES,
  TRACE_AGGREGATE_FIXTURE_DATA,
  TRACE_AGGREGATE_TOKEN_CONFORMANCE_CASES,
  TRACE_AGGREGATE_TOKEN_FIXTURE_DATA,
  TRACE_AGGREGATE_TOKEN_EDGE_CONFORMANCE_CASES,
  TRACE_AGGREGATE_TOKEN_EDGE_FIXTURE_DATA,
  traceAggregateResponseMismatch,
} from '../../../../../../_test-utils/src/domains/observability-vnext/trace-aggregate';
import { OraclePoolManager } from '../../../../shared/connection';
import { ObservabilityStorageOracleVNext } from '.';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle trace aggregate contracts', () => {
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
  for (const [name, data, cases] of [
    ['aggregate', TRACE_AGGREGATE_FIXTURE_DATA, TRACE_AGGREGATE_CONFORMANCE_CASES],
    ['usage', TRACE_AGGREGATE_TOKEN_FIXTURE_DATA, TRACE_AGGREGATE_TOKEN_CONFORMANCE_CASES],
    ['usage edge cases', TRACE_AGGREGATE_TOKEN_EDGE_FIXTURE_DATA, TRACE_AGGREGATE_TOKEN_EDGE_CONFORMANCE_CASES],
  ] as const)
    describe(name, () => {
      beforeAll(async () => {
        await storage.dangerouslyClearAll();
        await writeTraceAggregateFixture(storage, data, 'event-sourced');
      });
      it.each(cases)('$name', async testCase => {
        const result = await storage.aggregateTraces(
          planTraceAggregate(parseTraceAggregateRequest(testCase.request), { scope: testCase.scope }),
        );
        expect(traceAggregateResponseMismatch(result, testCase)).toBeNull();
      });
    });
});
