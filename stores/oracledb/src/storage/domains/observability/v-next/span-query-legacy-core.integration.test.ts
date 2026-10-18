import { planSpanQuery } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { makeSpan } from '../../../../../../_test-utils/src/domains/observability-vnext/data';
import { OraclePoolManager } from '../../../../shared/connection';
import { buildSpanQueryResponse } from './preview-compat';
import { ObservabilityStorageOracleVNext } from '.';

vi.mock('@mastra/core/storage', async importOriginal => {
  const core = await importOriginal<typeof import('@mastra/core/storage')>();
  return {
    ...core,
    SPAN_QUERY_FIELD_REGISTRY: undefined,
    buildSpanQueryResponse: (...args: Parameters<typeof core.buildSpanQueryResponse>) => {
      expect(Array.isArray(args[3])).toBe(true);
      for (const row of args[1]) {
        expect(row).not.toHaveProperty('threadId');
        expect(row).not.toHaveProperty('parentEntityName');
        expect(row).not.toHaveProperty('version');
      }
      const response = core.buildSpanQueryResponse(...args);
      return {
        ...response,
        spans: response.spans.map(row => ({
          ...row,
          cost: core.resolveSpanQueryCost(
            args[3]!.filter(metric => core.spanQueryIdentityKey(metric) === core.spanQueryIdentityKey(row)),
          ),
        })),
      };
    },
  };
});

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle span queries with older Core', () => {
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
    await storage.dangerouslyClearAll();
  });
  afterAll(async () => {
    await storage?.dangerouslyClearAll();
    await manager?.close();
  });

  it('supplies the metrics argument required by the older response builder for an empty page', () => {
    const plan = planSpanQuery({ timeRange: { from: '2026-01-01T00:00:00Z', to: '2026-01-02T00:00:00Z' } });
    expect(buildSpanQueryResponse(plan, [], [])).toEqual({ spans: [], page: { next: null } });
  });

  it('preserves model/provider predicates, previews and costs without the new field registry', async () => {
    const startedAt = new Date();
    const request = {
      timeRange: { from: startedAt.toISOString(), to: new Date(startedAt.getTime() + 60_000).toISOString() },
    };
    expect(await storage.querySpans(planSpanQuery(request))).toEqual({ spans: [], page: { next: null } });
    await storage.batchCreateSpans({
      records: [
        makeSpan({
          traceId: 'legacy',
          spanId: 'legacy',
          startedAt,
          attributes: { model: 'model-a', provider: 'provider-a' },
          input: 'hello',
          threadId: 'legacy-thread',
        }),
      ],
    });
    expect((await storage.querySpans(planSpanQuery(request))).spans[0]).toMatchObject({ cost: { state: 'missing' } });
    await storage.batchCreateMetrics({
      metrics: [
        {
          metricId: 'priced',
          traceId: 'legacy',
          spanId: 'legacy',
          timestamp: new Date(startedAt.getTime() + 120_000),
          name: 'mastra_model_total_input_tokens',
          value: 10,
          labels: {},
          estimatedCost: 0.03,
          costUnit: 'usd',
          costMetadata: { allocation: 'query_total' },
        },
        {
          metricId: 'other',
          traceId: 'other',
          spanId: 'other',
          timestamp: startedAt,
          name: 'mastra_model_total_input_tokens',
          value: 10,
          labels: {},
          estimatedCost: 999,
          costUnit: 'usd',
          costMetadata: { allocation: 'query_total' },
        },
      ],
    });
    for (const field of ['model', 'provider'] as const) {
      // These trusted predicates were accepted by the older Core planner.
      const response = await storage.querySpans({
        ...planSpanQuery(request),
        where: { type: 'comparison', field, operator: 'eq', value: field + '-a' },
      });
      expect(response.spans).toHaveLength(1);
      expect(response.spans[0]).toMatchObject({
        spanId: 'legacy',
        model: 'model-a',
        provider: 'provider-a',
        inputPreview: 'hello',
        cost: { state: 'available', amount: 0.03, currency: 'usd' },
      });
    }
  });
});
