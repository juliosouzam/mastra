import { randomUUID } from 'node:crypto';
import { appendFile } from 'node:fs/promises';
import { parseTraceQueryRequest, planTraceQuery } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { makeSpan } from '../../../_test-utils/src/domains/observability-vnext/data';
import { ObservabilityStorageOracleVNext } from './domains/observability/v-next';
import { OracleStore } from '.';

// Reproducible framework workload on the disposable integration database; these measurements are not an application SLO.
describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle framework regression workload', () => {
  let store: OracleStore;
  let other: OracleStore;
  let observability: ObservabilityStorageOracleVNext;
  let databaseVersion: string | undefined;
  const runId = randomUUID();
  const threadId = randomUUID();
  const resourceId = randomUUID();
  const traceIds = Array.from({ length: 256 }, (_, index) => `${runId}-${index}`);
  beforeAll(async () => {
    const config = {
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    };
    store = new OracleStore({ id: 'oracle-framework-workload', ...config, messageBatchSize: 200 });
    other = new OracleStore({ id: 'oracle-framework-workload-replica', ...config });
    await store.init();
    observability = new ObservabilityStorageOracleVNext({ poolManager: store.getPoolManager() });
    await observability.init();
    await store.getPoolManager().withConnection(async connection => {
      databaseVersion = connection.oracleServerVersionString;
    });
  });
  afterAll(async () => {
    await observability?.batchDeleteTraces({ traceIds: [runId, ...traceIds] });
    await store?.stores.memory!.deleteThread({ threadId });
    await store?.db.none('DELETE FROM MASTRA_RESOURCES WHERE id = :id', { id: resourceId });
    await Promise.all([store?.close(), other?.close()]);
  });

  it('measures history, batch writes, concurrent memory, retention and observability', async () => {
    const rssBeforeBytes = process.memoryUsage().rss;
    const memory = store.stores.memory!;
    const now = new Date();
    const body = 'Message fixture. '.repeat(64);
    const messageCount = 1024;
    const historyQueries = 24;
    const updates = 40;
    const logCount = 512;
    const metricCount = 256;
    await memory.saveThread({ thread: { id: threadId, resourceId, createdAt: now, updatedAt: now } });
    const messages = Array.from({ length: messageCount }, (_, index) => ({
      id: `${runId}-message-${index}`,
      threadId,
      resourceId,
      role: 'user' as const,
      type: 'text',
      content: { format: 2 as const, parts: [{ type: 'text' as const, text: body }] },
      createdAt: new Date(now.getTime() - 40 * 86_400_000 + index),
    }));
    let started = performance.now();
    await memory.saveMessages({ messages });
    const batchWriteMs = performance.now() - started;
    const historyMs: number[] = [];
    for (let page = 0; page < historyQueries; page++) {
      started = performance.now();
      const result = await memory.listMessages({
        threadId,
        page,
        perPage: 20,
        includeTotal: false,
        orderBy: { field: 'createdAt', direction: 'ASC' },
      });
      historyMs.push(performance.now() - started);
      expect(result.messages).toHaveLength(20);
      expect(result.messages[0]!.id).toBe(messages[page * 20]!.id);
    }
    await memory.mergeResourceWorkingMemory({ resourceId, merge: () => '0' });
    started = performance.now();
    await Promise.all(
      Array.from({ length: updates }, (_, index) =>
        (index % 2 ? memory : other.stores.memory!).mergeResourceWorkingMemory({
          resourceId,
          merge: existing => String(Number(existing) + 1),
        }),
      ),
    );
    const concurrentMemoryMs = performance.now() - started;
    expect((await memory.getResourceById({ resourceId }))?.workingMemory).toBe(String(updates));
    started = performance.now();
    const pruned = await memory.prune({ messages: { maxAge: '30d', batchSize: 128 } });
    const retentionMs = performance.now() - started;
    expect(pruned.reduce((count, result) => count + result.deleted, 0)).toBeGreaterThanOrEqual(messageCount);
    expect((await memory.listMessages({ threadId, perPage: false })).messages).toHaveLength(0);

    started = performance.now();
    await observability.batchCreateSpans({
      records: traceIds.map((traceId, index) =>
        makeSpan({
          traceId,
          spanId: `${traceId}-root`,
          resourceId,
          startedAt: new Date(now.getTime() + index),
          endedAt: new Date(now.getTime() + index + 10),
        }),
      ),
    });
    await observability.batchCreateLogs({
      logs: Array.from({ length: logCount }, (_, index) => ({
        logId: `${runId}-log-${index}`,
        traceId: runId,
        timestamp: now,
        level: 'info' as const,
        message: body,
      })),
    });
    await observability.batchCreateMetrics({
      metrics: Array.from({ length: metricCount }, (_, index) => ({
        metricId: `${runId}-metric-${index}`,
        traceId: runId,
        timestamp: now,
        name: 'benchmark.tokens',
        value: 2,
      })),
    });
    const observabilityWriteMs = performance.now() - started;
    const traceQueryMs: number[] = [];
    const plan = planTraceQuery(
      parseTraceQueryRequest({
        timeRange: { from: now.toISOString(), to: new Date(now.getTime() + 60_000).toISOString() },
        where: { op: 'eq', left: { path: 'resourceId' }, right: { literal: resourceId } },
        pagination: { page: 0, perPage: 25 },
      }),
    );
    for (let index = 0; index < 12; index++) {
      started = performance.now();
      const result = await observability.queryTraces(plan);
      traceQueryMs.push(performance.now() - started);
      if (!('traces' in result)) throw new Error('Expected traces');
      expect(result.traces).toHaveLength(25);
      expect(result.pagination?.total).toBe(traceIds.length);
    }
    started = performance.now();
    expect(
      (
        await observability.getMetricAggregate({
          name: ['benchmark.tokens'],
          aggregation: 'sum',
          filters: { traceId: runId },
        })
      ).value,
    ).toBe(metricCount * 2);
    const metricAggregateMs = performance.now() - started;
    expect(
      (await observability.listLogs({ filters: { traceId: runId }, pagination: { page: 0, perPage: 50 } })).logs,
    ).toHaveLength(50);
    const p95 = (samples: number[]) => samples.toSorted((a, b) => a - b)[Math.ceil(samples.length * 0.95) - 1]!;
    const report = {
      kind: 'framework',
      databaseVersion,
      messageCount,
      messageTextBytes: Buffer.byteLength(body),
      messageBatchSize: 200,
      batchWriteMs,
      messagesPerSecond: messageCount / (batchWriteMs / 1000),
      historyQueries,
      historyP95Ms: p95(historyMs),
      concurrentUpdates: updates,
      concurrentMemoryMs,
      retentionRows: messageCount,
      retentionBatchSize: 128,
      retentionMs,
      spans: traceIds.length,
      logs: logCount,
      metrics: metricCount,
      observabilityWriteMs,
      traceQueryP95Ms: p95(traceQueryMs),
      metricAggregateMs,
      rssBeforeBytes,
      rssAfterBytes: process.memoryUsage().rss,
    };
    console.info('Oracle framework regression', JSON.stringify(report));
    if (process.env.ORACLE_BENCHMARK_REPORT)
      await appendFile(process.env.ORACLE_BENCHMARK_REPORT, `${JSON.stringify(report)}\n`);
    expect(p95(historyMs)).toBeLessThan(2000);
    expect(p95(traceQueryMs)).toBeLessThan(2000);
  }, 90_000);
});
