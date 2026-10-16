import type {
  BatchCreateSpansArgs,
  BatchDeleteTracesArgs,
  CreateSpanArgs,
  GetSpansArgs,
  GetSpansResponse,
  GetSpanArgs,
  GetSpanResponse,
  GetTraceArgs,
  GetTraceResponse,
  GetTraceLightResponse,
  GetRootSpanArgs,
  GetRootSpanResponse,
} from '@mastra/core/storage';
import { clobBind } from '../../../../shared/connection';
import type { DbClient } from './client';
import { qualifiedTable, rawTable, TABLE_SPAN_EVENTS, ALL_SIGNAL_TABLES } from './ddl';
import { rowToLightSpanRecord, rowToSpanRecord, spanRecordToRow } from './helpers';
import { writeSignals, SPAN_COLLAPSE_ORDER, SPAN_LIGHT_SELECT_COLUMNS, SPAN_SELECT_COLUMNS } from './sql';

export async function createSpan(client: DbClient, schema: string, args: CreateSpanArgs): Promise<void> {
  await writeSignals(client, schema, TABLE_SPAN_EVENTS, [spanRecordToRow(args.span)], 'span');
}
export async function batchCreateSpans(client: DbClient, schema: string, args: BatchCreateSpansArgs): Promise<void> {
  await writeSignals(client, schema, TABLE_SPAN_EVENTS, args.records.map(spanRecordToRow), 'span');
}
function collapsedSelect(schema: string, projection: string, where: string): string {
  return `SELECT * FROM (SELECT ${projection}, ROW_NUMBER() OVER (PARTITION BY "spanId" ORDER BY ${SPAN_COLLAPSE_ORDER}) AS "spanRank" FROM ${qualifiedTable(schema, TABLE_SPAN_EVENTS)} WHERE ${where}) WHERE "spanRank" = 1 ORDER BY "startedAt" ASC`;
}
export async function getSpans(client: DbClient, schema: string, args: GetSpansArgs): Promise<GetSpansResponse> {
  if (!args.spanIds.length) return { traceId: args.traceId, spans: [] };
  const rows = await client.manyOrNone(
    collapsedSelect(
      schema,
      SPAN_SELECT_COLUMNS,
      `"traceId" = :p1 AND "spanId" IN (SELECT value FROM JSON_TABLE(:p2, '$[*]' COLUMNS(value VARCHAR2(512) PATH '$')))`,
    ),
    [args.traceId, clobBind(JSON.stringify(args.spanIds))],
  );
  return { traceId: args.traceId, spans: rows.map(rowToSpanRecord) };
}
export async function getSpan(client: DbClient, schema: string, args: GetSpanArgs): Promise<GetSpanResponse | null> {
  const row = await client.oneOrNone(
    `SELECT ${SPAN_SELECT_COLUMNS} FROM ${qualifiedTable(schema, TABLE_SPAN_EVENTS)} WHERE "traceId" = :p1 AND "spanId" = :p2 ORDER BY ${SPAN_COLLAPSE_ORDER} FETCH FIRST 1 ROW ONLY`,
    [args.traceId, args.spanId],
  );
  return row ? { span: rowToSpanRecord(row) } : null;
}
export async function getRootSpan(
  client: DbClient,
  schema: string,
  args: GetRootSpanArgs,
): Promise<GetRootSpanResponse | null> {
  const row = await client.oneOrNone(
    `SELECT ${SPAN_SELECT_COLUMNS} FROM ${qualifiedTable(schema, TABLE_SPAN_EVENTS)} r WHERE r."traceId" = :p1 AND r."parentSpanId" IS NULL AND (r."isPending" = 0 OR NOT EXISTS (SELECT 1 FROM ${qualifiedTable(schema, TABLE_SPAN_EVENTS)} completed WHERE completed."traceId" = r."traceId" AND completed."spanId" = r."spanId" AND completed."isPending" = 0)) ORDER BY "cursorId" DESC FETCH FIRST 1 ROW ONLY`,
    [args.traceId],
  );
  return row ? { span: rowToSpanRecord(row) } : null;
}
export async function getTrace(client: DbClient, schema: string, args: GetTraceArgs): Promise<GetTraceResponse | null> {
  const rows = await client.manyOrNone(collapsedSelect(schema, SPAN_SELECT_COLUMNS, '"traceId" = :p1'), [args.traceId]);
  return rows.length ? { traceId: args.traceId, spans: rows.map(rowToSpanRecord) } : null;
}
export async function getTraceLight(
  client: DbClient,
  schema: string,
  args: GetTraceArgs,
): Promise<GetTraceLightResponse | null> {
  const rows = await client.manyOrNone(collapsedSelect(schema, SPAN_LIGHT_SELECT_COLUMNS, '"traceId" = :p1'), [
    args.traceId,
  ]);
  return rows.length ? { traceId: args.traceId, spans: rows.map(rowToLightSpanRecord) } : null;
}
export async function batchDeleteTraces(client: DbClient, schema: string, args: BatchDeleteTracesArgs): Promise<void> {
  await client.tx(async tx => {
    for (let start = 0; start < args.traceIds.length; start += 500) {
      const ids = args.traceIds.slice(start, start + 500);
      const params: unknown[] = [...ids];
      const conditions = [`"traceId" IN (${ids.map((_, index) => `:p${index + 1}`).join(', ')})`];
      for (const field of ['organizationId', 'resourceId'] as const)
        if (args[field] !== undefined) {
          params.push(args[field]);
          conditions.push(`"${field}" = :p${params.length}`);
        }
      for (const table of ALL_SIGNAL_TABLES)
        await tx.none(`DELETE FROM ${rawTable(schema, table)} WHERE ${conditions.join(' AND ')}`, params);
    }
  });
}
