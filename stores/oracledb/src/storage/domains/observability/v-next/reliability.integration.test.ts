import { randomUUID } from 'node:crypto';
import { parseTraceQueryRequest, planTraceQuery, TraceQueryExecutionError } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { makeSpan } from '../../../../../../_test-utils/src/domains/observability-vnext/data';
import { OraclePoolManager } from '../../../../shared/connection';
import { OracleObservabilityClient } from './client';
import { runWithOracleTraceQueryTimeout } from './trace-query';
import { ObservabilityStorageOracleVNext } from '.';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle observability reliability', () => {
  let manager: OraclePoolManager;
  let storage: ObservabilityStorageOracleVNext;
  let client: OracleObservabilityClient;
  beforeAll(async () => {
    manager = new OraclePoolManager({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      poolMin: 1,
      poolMax: 1,
    });
    storage = new ObservabilityStorageOracleVNext({ poolManager: manager });
    client = new OracleObservabilityClient({ poolManager: manager });
    await storage.init();
    await storage.dangerouslyClearAll();
  });
  afterAll(async () => {
    await storage?.dangerouslyClearAll();
    await manager?.close();
  });

  it('cancels a slow Oracle statement and restores the pooled connection timeout', async () => {
    await expect(
      runWithOracleTraceQueryTimeout(client, 200, transaction => transaction.none('BEGIN DBMS_SESSION.SLEEP(2); END;')),
    ).rejects.toBeInstanceOf(TraceQueryExecutionError);
    await manager.withConnection(async connection => {
      expect(connection.callTimeout).toBe(0);
      await expect(connection.execute('SELECT 1 FROM dual')).resolves.toBeDefined();
    });
  });

  it('reads a consistent snapshot immediately after creating a table', async () => {
    const tableName = `ORA_OBS_SNAPSHOT_${randomUUID().replaceAll('-', '').slice(0, 12).toUpperCase()}`;
    await client.none(`CREATE TABLE "${tableName}" (id NUMBER)`);
    try {
      const result = await runWithOracleTraceQueryTimeout(
        client,
        5000,
        transaction => transaction.one<{ count: number }>(`SELECT COUNT(*) AS "count" FROM "${tableName}"`),
        { repeatableRead: true },
      );
      expect(result.count).toBe(0);
    } finally {
      await client.none(`DROP TABLE "${tableName}" PURGE`);
    }
  });

  it('keeps count and page reads on one snapshot while another pool commits', async () => {
    const other = new OraclePoolManager({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    const writer = new ObservabilityStorageOracleVNext({ poolManager: other });
    const traceId = randomUUID();
    const count = (reader: OracleObservabilityClient) =>
      reader.one<{ count: number }>('SELECT COUNT(*) AS "count" FROM MASTRA_OBS_LOG_EVENTS WHERE "traceId" = :p1', [
        traceId,
      ]);
    try {
      await storage.batchCreateLogs({
        logs: [{ logId: randomUUID(), traceId, timestamp: new Date(), level: 'info', message: 'before' }],
      });
      await runWithOracleTraceQueryTimeout(
        client,
        5000,
        async transaction => {
          expect((await count(transaction)).count).toBe(1);
          await writer.batchCreateLogs({
            logs: [{ logId: randomUUID(), traceId, timestamp: new Date(), level: 'info', message: 'after' }],
          });
          expect((await count(transaction)).count).toBe(1);
        },
        { repeatableRead: true },
      );
      expect((await count(client)).count).toBe(2);
    } finally {
      await other.close();
    }
  });

  it('keeps a completed root when its start event arrives late, while allowing a fresh resumed root', async () => {
    const traceId = randomUUID();
    const startedAt = new Date();
    const endedAt = new Date(startedAt.getTime() + 1000);
    const span = makeSpan({ traceId, spanId: randomUUID(), startedAt, endedAt });
    await storage.createSpan({ span });
    await storage.createSpan({ span: { ...span, endedAt: null } });
    expect((await storage.getSpan({ traceId, spanId: span.spanId }))?.span.endedAt).toEqual(endedAt);
    expect((await storage.getRootSpan({ traceId }))?.span.endedAt).toEqual(endedAt);
    const listed = await storage.listTraces({});
    expect(listed.spans.find(row => row.traceId === traceId)?.endedAt).toEqual(endedAt);
    const queried = await storage.queryTraces(
      planTraceQuery(
        parseTraceQueryRequest({
          timeRange: { from: startedAt.toISOString(), to: new Date(endedAt.getTime() + 1000).toISOString() },
        }),
      ),
    );
    if (!('traces' in queried)) throw new Error('Expected traces');
    expect(queried.traces.find(row => row.traceId === traceId)?.endedAt).toEqual(endedAt.toISOString());
    const resumed = makeSpan({ traceId, spanId: randomUUID(), startedAt: endedAt, endedAt: null });
    await storage.createSpan({ span: resumed });
    expect((await storage.getRootSpan({ traceId }))?.span.spanId).toBe(resumed.spanId);
    expect((await storage.listTraces({})).spans.find(row => row.traceId === traceId)?.endedAt).toBeNull();
  });
});
