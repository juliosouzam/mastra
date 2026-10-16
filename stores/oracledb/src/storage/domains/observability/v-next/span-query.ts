import * as coreStorage from '@mastra/core/storage';
import type {
  SpanQueryCostMetric,
  SpanQueryIdentity,
  SpanQueryPayload,
  SpanQuerySelectedRow,
  TrustedSpanQueryPlan,
} from '@mastra/core/storage';
import type { DbClient } from './client';
import { qualifiedTable, TABLE_METRIC_EVENTS, TABLE_SPAN_EVENTS } from './ddl';
import { buildSpanQueryResponse, missingPreviewSql, readPreview } from './preview-compat';
import { compileSpanQueryPredicate, runWithOracleTraceQueryTimeout, durationMsSql } from './trace-query';

const identityFields = ['organizationId', 'resourceId', 'traceId', 'spanId'] as const;
const fields = [
  ...identityFields,
  'parentSpanId',
  'name',
  'spanType',
  'entityType',
  'entityId',
  'entityName',
  'entityVersionId',
  'parentEntityVersionId',
  'rootEntityVersionId',
  'runId',
  'sessionId',
  'userId',
];
const identities = identityFields.map(field => `"${field}"`).join(', ');
const identityOrder = identityFields.map(field => `"${field}" COLLATE BINARY ASC NULLS LAST`).join(', ');

function selectedKeys(rows: SpanQueryIdentity[], values: unknown[]): string {
  return rows
    .map(
      row =>
        `(${identityFields
          .map(field => {
            if (row[field] === null) return `"${field}" IS NULL`;
            values.push(row[field]);
            return `"${field}" = :p${values.length}`;
          })
          .join(' AND ')})`,
    )
    .join(' OR ');
}

function afterCursor(plan: TrustedSpanQueryPlan, values: unknown[]): string {
  if (!plan.cursor) return '1 = 1';
  const cursor = plan.cursor;
  values.push(new Date(cursor.sortValue));
  const timestamp = `:p${values.length}`;
  const field = `s."${plan.orderBy.field}"`;
  const terms = [`${field} ${plan.orderBy.direction === 'asc' ? '>' : '<'} ${timestamp}`];
  const equal = [`${field} = ${timestamp}`];
  for (const key of identityFields) {
    const ref = `s."${key}" COLLATE BINARY`;
    const value = cursor[key];
    if (value === null) {
      equal.push(`${ref} IS NULL`);
    } else {
      values.push(value);
      const parameter = `:p${values.length} COLLATE BINARY`;
      terms.push(`(${equal.join(' AND ')} AND (${ref} > ${parameter} OR ${ref} IS NULL))`);
      equal.push(`${ref} = ${parameter}`);
    }
  }
  return `(${terms.join(' OR ')})`;
}

export function compileOracleSpanQuery(schema: string, plan: TrustedSpanQueryPlan) {
  const values: unknown[] = [new Date(plan.timeRange.from), new Date(plan.timeRange.to)];
  const scope: string[] = [];
  if (plan.scope) {
    values.push(plan.scope.organizationId);
    scope.push(`"organizationId" = :p${values.length}`);
    if (plan.scope.resourceId !== undefined) {
      values.push(plan.scope.resourceId);
      scope.push(`"resourceId" = :p${values.length}`);
    }
  }
  const predicate = plan.where
    ? compileSpanQueryPredicate(plan.where, values.length + 1)
    : { sql: '1 = 1', values: [] };
  values.push(...predicate.values);
  const after = afterCursor(plan, values);
  values.push(plan.limit + 1);
  const table = qualifiedTable(schema, TABLE_SPAN_EVENTS);
  const sameIdentity = identityFields
    .map(field => `(e."${field}" = c."${field}" OR (e."${field}" IS NULL AND c."${field}" IS NULL))`)
    .join(' AND ');
  const text = `WITH candidate_ids AS (
    SELECT DISTINCT ${identities} FROM ${table} WHERE "startedAt" >= :p1 AND "startedAt" < :p2 AND "endedAt" >= :p1 AND "isPending" = 0 ${scope.length ? `AND ${scope.join(' AND ')}` : ''}
  ), current_spans AS (
    SELECT * FROM (SELECT ${fields.map(field => `"${field}"`).join(', ')}, "textSearch", "startedAt", "endedAt", TO_CHAR("cursorId") AS "version",
      CASE WHEN JSON_EXISTS("attributes", '$.model?(@.type() == "string")') THEN JSON_VALUE("attributes", '$.model' RETURNING VARCHAR2(32767) NULL ON ERROR) END AS "model",
      CASE WHEN JSON_EXISTS("attributes", '$.provider?(@.type() == "string")') THEN JSON_VALUE("attributes", '$.provider' RETURNING VARCHAR2(32767) NULL ON ERROR) END AS "provider",
      ${durationMsSql('"startedAt"', '"endedAt"')} AS "durationMs",
      CASE WHEN "error" IS NULL THEN 'success' ELSE 'error' END AS "status",
      CASE WHEN "error" IS NULL THEN 0 ELSE 1 END AS "hasError",
      ROW_NUMBER() OVER (PARTITION BY ${identities} ORDER BY "endedAt" DESC, "cursorId" DESC) AS "spanRank"
    FROM ${table} e WHERE "isPending" = 0 AND "endedAt" >= :p1 AND EXISTS (SELECT 1 FROM candidate_ids c WHERE ${sameIdentity}) ${scope.length ? `AND ${scope.join(' AND ')}` : ''}) WHERE "spanRank" = 1
  ), candidates AS (SELECT current_spans.*, CASE WHEN "hasError" = 1 THEN 'error' ELSE NULL END AS "error" FROM current_spans)
  SELECT ${[...identityFields, 'parentSpanId', 'name', 'spanType', 'entityType', 'entityId', 'entityName', 'model', 'provider', 'startedAt', 'endedAt', 'durationMs', 'status', 'version'].map(field => `s."${field}"`).join(', ')}
  FROM candidates s WHERE s."startedAt" >= :p1 AND s."startedAt" < :p2 AND (${predicate.sql}) AND ${after}
  ORDER BY s."${plan.orderBy.field}" ${plan.orderBy.direction === 'asc' ? 'ASC' : 'DESC'}, ${identityOrder}
  FETCH FIRST :p${values.length} ROWS ONLY`;
  return { text, values };
}

const PREVIEW_PAYLOADS = [
  ['inputPreview', 'input'],
  ['outputPreview', 'output'],
  ['errorPreview', 'error'],
] as const;

/**
 * Stored previews of a hydrated row, built from the payload where the stored preview is NULL.
 * Serialized JSON is decoded before `buildPreview(value)`. Native JSON may normalize object key
 * order, so a compact-JSON fallback preview can differ from what the writer would have stored from
 * the original value; that is acceptable for rows written before the preview columns existed.
 */
function toSpanQueryPayload(row: Record<string, unknown>): SpanQueryPayload {
  const [inputPreview, outputPreview, errorPreview] = PREVIEW_PAYLOADS.map(([preview, payload]) =>
    readPreview(row[preview], row[payload], true),
  );
  return {
    organizationId: (row.organizationId as string | null) ?? null,
    resourceId: (row.resourceId as string | null) ?? null,
    traceId: String(row.traceId),
    spanId: String(row.spanId),
    inputPreview: inputPreview!,
    outputPreview: outputPreview!,
    errorPreview: errorPreview!,
  };
}

type Selected = Omit<SpanQuerySelectedRow, 'startedAt' | 'endedAt'> & {
  startedAt: Date;
  endedAt: Date;
  version: string;
};

export async function querySpans(client: DbClient, schema: string, plan: TrustedSpanQueryPlan, timeoutMs: number) {
  const deadline = performance.now() + coreStorage.resolveTraceQueryTimeoutMs(timeoutMs);
  return runWithOracleTraceQueryTimeout(
    client,
    timeoutMs,
    async tx => {
      const query = compileOracleSpanQuery(schema, plan);
      const rows = await tx.any<Selected>(query.text, query.values);
      const selected = rows.map(({ version: _version, startedAt, endedAt, ...row }) => ({
        ...row,
        startedAt: startedAt.toISOString(),
        endedAt: endedAt.toISOString(),
      }));
      const page = rows.slice(0, plan.limit);
      if (!page.length) return coreStorage.buildSpanQueryResponse(plan, selected, [], []);
      const remaining = async () => {
        const ms = Math.floor(deadline - performance.now());
        if (ms <= 0) throw new coreStorage.TraceQueryExecutionError();
        tx.setCallTimeout(ms);
      };
      await remaining();
      const payloadValues: unknown[] = [];
      const payloadWhere = page
        .map(row => {
          const key = selectedKeys([row], payloadValues);
          payloadValues.push(row.version);
          return `(${key} AND "cursorId" = :p${payloadValues.length})`;
        })
        .join(' OR ');
      // Stored previews, plus the payload only where its preview is NULL (a row written before the
      // preview columns existed), so new rows never read their payloads here.
      const payloadRows = await tx.any<Record<string, unknown>>(
        `SELECT ${identities},
      ${PREVIEW_PAYLOADS.map(([preview, payload]) => `"${preview}", JSON_SERIALIZE(CASE WHEN ${missingPreviewSql(`"${preview}"`)} THEN "${payload}" END RETURNING CLOB) AS "${payload}"`).join(',\n      ')}
      FROM ${qualifiedTable(schema, TABLE_SPAN_EVENTS)} WHERE ${payloadWhere}`,
        payloadValues,
      );
      const payloads = payloadRows.map(toSpanQueryPayload);
      await remaining();
      const metricValues: unknown[] = [];
      const metricWhere = selectedKeys(page, metricValues);
      const metricRows = await tx.any<SpanQueryCostMetric & Record<string, unknown>>(
        `SELECT ${identities}, "name", "estimatedCost", "costUnit",
        JSON_VALUE("costMetadata", '$.allocation' RETURNING VARCHAR2(32767) NULL ON ERROR) AS "allocation",
        CASE WHEN JSON_EXISTS("costMetadata", '$.error') THEN 1 ELSE 0 END AS "costError"
        FROM (SELECT ${identities}, "name", "estimatedCost", "costUnit", "costMetadata",
          ROW_NUMBER() OVER (PARTITION BY ${identities}, "metricId" ORDER BY "cursorId" DESC) AS "metricRank"
          FROM ${qualifiedTable(schema, TABLE_METRIC_EVENTS)} WHERE (${metricWhere}) AND "name" LIKE 'mastra_model_%') m
        WHERE "metricRank" = 1 FETCH FIRST ${coreStorage.SPAN_QUERY_MAX_COST_METRICS + 1} ROWS ONLY`,
        metricValues,
      );
      const metrics = metricRows.map(row => ({ ...row, costError: Boolean(row.costError) }));
      if (metrics.length > coreStorage.SPAN_QUERY_MAX_COST_METRICS)
        throw new coreStorage.TraceQueryResourceLimitError();
      return buildSpanQueryResponse(plan, selected, payloads, metrics);
    },
    { repeatableRead: true },
  );
}
