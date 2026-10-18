import { lightSpanRecordSchema, parseTraceQueryRequest, planTraceQuery } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';
import { makeSpan } from '../../../../../../_test-utils/src/domains/observability-vnext/data';
import type { OracleObservabilityClient } from './client';
import { rowToLightTraceRecord, rowToSpanRecord, spanRecordToRow } from './helpers';
import { queryTraces } from './trace-query';

vi.mock('@mastra/core/storage', async importOriginal => {
  const core = await importOriginal<typeof import('@mastra/core/storage')>();
  // Core 1.76 exposes input previews; its strict trace responses predate the other two fields.
  const trace = core.traceQueryTraceSchema.omit({ outputPreview: true, errorPreview: true });
  return {
    ...core,
    buildStoredPreview: undefined,
    traceQueryTraceSchema: trace,
    lightSpanRecordSchema: core.lightSpanRecordSchema.omit({ outputPreview: true, errorPreview: true }),
    spanRecordSchema: core.spanRecordSchema.omit({ timeToFirstChunkMs: true }),
    traceQueryResponseSchema: core.traceQueryPaginatedTraceResponseSchema.extend({ traces: trace.array() }),
  };
});

describe('Oracle observability with the Core 1.76 preview contract', () => {
  const now = new Date('2026-01-01T00:00:00.000Z');
  const span = makeSpan({
    traceId: 'compat-trace',
    spanId: 'compat-root',
    startedAt: now,
    endedAt: new Date(now.getTime() + 10),
    input: { messages: [{ role: 'user', content: 'compat question' }] },
  });
  const row = (inputPreview?: string) => ({
    ...spanRecordToRow({ ...span, inputPreview }),
    rootSpanId: span.spanId,
    metadata: null,
    metadataRaw: '{}',
    status: 'success',
    outputPreview: 'output',
    errorPreview: 'error',
  });

  it.each([undefined, '   '])('returns a valid older trace response with input preview %j', async inputPreview => {
    const client = {
      one: async () => ({ xactId: '42' }),
      any: async (sql: string) => (/COUNT\(\*\)/.test(sql) ? [{ count: '1' }] : [row(inputPreview)]),
      none: async () => {},
      setCallTimeout: () => {},
      tx: async (callback: (transaction: unknown) => unknown) => callback(client),
    } as unknown as OracleObservabilityClient;
    const response = await queryTraces(
      client,
      '',
      planTraceQuery(
        parseTraceQueryRequest({
          timeRange: { from: now.toISOString(), to: new Date(now.getTime() + 1000).toISOString() },
          pagination: { page: 0, perPage: 1 },
        }),
      ),
      5000,
    );
    if (!('traces' in response)) throw new Error('Expected traces');
    expect(response.traces[0]).toMatchObject({ traceId: span.traceId, inputPreview: 'compat question' });
    expect(response.traces[0]).not.toHaveProperty('outputPreview');
    expect(response.traces[0]).not.toHaveProperty('errorPreview');
  });

  it('keeps input previews and omits fields absent from the older light record schema', () => {
    const record = rowToLightTraceRecord(row());
    expect(record.inputPreview).toBe('compat question');
    expect(lightSpanRecordSchema.strict().parse(record)).toMatchObject({ traceId: span.traceId });
  });

  it('omits first-chunk timing from records when the installed Core schema predates it', () => {
    const record = rowToSpanRecord({ ...row(), timeToFirstChunkMs: 250 });
    expect(record).not.toHaveProperty('timeToFirstChunkMs');
    expect(record.traceId).toBe(span.traceId);
  });
});
