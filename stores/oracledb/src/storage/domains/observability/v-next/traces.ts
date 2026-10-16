/**
 * Trace / branch list reads for the v-next Oracle observability domain.
 *
 * - `listTraces` / `getRootSpan` filter by `parentSpanId IS NULL`. The partial
 *   indexes declared in ddl.ts make this predicate selective enough to act as
 *   the root-span projection without a separate table.
 * - `listBranches` filters by `spanType IN (BRANCH_SPAN_TYPES)` (or the
 *   user-supplied spanType when it's a branch type). Branches include nested
 *   anchors as well as root spans of the listed types.
 */

import {
  BRANCH_SPAN_TYPES,
  listBranchesArgsSchema,
  listTracesArgsSchema,
  TraceStatus,
  toTraceSpans,
} from '@mastra/core/storage';
import type {
  GetRootSpanArgs,
  GetRootSpanResponse,
  ListBranchesArgs,
  ListBranchesResponse,
  ListTracesArgs,
  ListTracesLightResponse,
  ListTracesResponse,
  SpanRecord,
} from '@mastra/core/storage';

import { clobBind } from '../../../../shared/connection';
import type { DbClient } from './client';
import { qualifiedTable, TABLE_SPAN_EVENTS } from './ddl';
import { appendJsonContains } from './filters';
import { rowToLightTraceRecord, rowToSpanRecord } from './helpers';
import {
  assertDeltaPollingEnabled,
  decodeDeltaCursor,
  deltaPollingFeatureEnabled,
  encodeDeltaCursor,
  readSafeXactHorizon,
} from './polling';
import { needsPreview, readPreview } from './preview-compat';
import {
  SPAN_LIGHT_SELECT_COLUMN_NAMES,
  SPAN_EVENT_COLUMNS,
  buildNamedSelectColumns,
  buildSelectColumns,
} from './signal-schema';
import { SPAN_SELECT_COLUMNS } from './sql';

function asTimestamp(value: unknown): Date {
  return value instanceof Date ? value : new Date(value as string | number);
}

/**
 * Ordering that picks the row representing a trace's current root.
 *
 * A trace can hold more than one root row. A span is written once when it
 * starts and again when it ends, and a durable run that suspends ends its root
 * span and opens a fresh one on resume. After excluding late start events for completed spans, the newest row
 * describes the trace right now, and `cursorId` — a sequence assigned at
 * insert — is what "newest" means here. It also orders correctly where
 * timestamps cannot: a zero-duration span has `startedAt = endedAt`, and a
 * resumed root starts after the suspended root ended.
 */
const ROOT_COLLAPSE_ORDER = '"cursorId" DESC';

/**
 * Predicate that keeps only the winning row for each span — the ended row when
 * one exists, the newest row otherwise. Mirrors `SPAN_COLLAPSE_ORDER` as an
 * anti-join, for the same reason as {@link latestRootPredicate}.
 */
function latestSpanPredicate(spanTable: string): string {
  return `NOT EXISTS (
    SELECT 1 FROM ${spanTable} newer
    WHERE newer."traceId" = r."traceId"
      AND newer."spanId" = r."spanId"
      AND (newer."isPending" < r."isPending" OR (newer."isPending" = r."isPending" AND newer."cursorId" > r."cursorId"))
  )`;
}

/**
 * Predicate that keeps only a trace's current root row, as an anti-join rather
 * than grouping the full table. Written this way so the outer query still scans the
 * root indexes and streams rows in fetch order; grouping first would
 * force every root in the table to be read before the first page came back.
 */
function latestRootPredicate(spanTable: string): string {
  const candidate = (alias: string) => `(${alias}."isPending" = 0 OR NOT EXISTS (
    SELECT 1 FROM ${spanTable} completed
    WHERE completed."traceId" = ${alias}."traceId" AND completed."spanId" = ${alias}."spanId"
      AND completed."isPending" = 0
  ))`;
  return `${candidate('r')} AND NOT EXISTS (
    SELECT 1 FROM ${spanTable} newer
    WHERE newer."traceId" = r."traceId"
      AND newer."parentSpanId" IS NULL
      AND newer."cursorId" > r."cursorId"
      AND ${candidate('newer')}
  )`;
}

export async function getRootSpan(
  client: DbClient,
  schema: string,
  args: GetRootSpanArgs,
): Promise<GetRootSpanResponse | null> {
  const table = qualifiedTable(schema, TABLE_SPAN_EVENTS);
  const row = await client.oneOrNone<Record<string, any>>(
    `SELECT ${SPAN_SELECT_COLUMNS}
     FROM ${table} r
     WHERE r."traceId" = :p1 AND r."parentSpanId" IS NULL AND ${latestRootPredicate(table)}
     ORDER BY ${ROOT_COLLAPSE_ORDER}
     FETCH FIRST 1 ROW ONLY`,
    [args.traceId],
  );
  if (!row) return null;
  return { span: rowToSpanRecord(row) };
}

/**
 * Build the WHERE conditions and bind params for a listTraces query against
 * `MASTRA_OBS_SPAN_EVENTS r`. Always prepends the root-span predicate so the
 * partial root indexes (and the partial cursor index) get picked up by the
 * planner. Starts numbering from `nextParamIdx`.
 */
function buildListTracesFilters(
  filters: ListTracesArgs['filters'],
  spanTable: string,
  nextParamIdx: number,
): { conditions: string[]; params: unknown[]; nextParamIdx: number } {
  const conditions: string[] = [`r."parentSpanId" IS NULL`, latestRootPredicate(spanTable)];
  const params: unknown[] = [];
  let i = nextParamIdx;

  if (!filters) {
    return { conditions, params, nextParamIdx: i };
  }

  if (filters.startedAt?.start) {
    conditions.push(`r."startedAt" ${filters.startedAt.startExclusive ? '>' : '>='} :p${i++}`);
    params.push(asTimestamp(filters.startedAt.start));
  }
  if (filters.startedAt?.end) {
    conditions.push(`r."startedAt" ${filters.startedAt.endExclusive ? '<' : '<='} :p${i++}`);
    params.push(asTimestamp(filters.startedAt.end));
  }
  // A pending row stores a placeholder `endedAt`, so an endedAt range filter
  // has to exclude it: a span that has not ended has no end time to match.
  if (filters.endedAt?.start) {
    conditions.push(`(r."isPending" = 0 AND r."endedAt" ${filters.endedAt.startExclusive ? '>' : '>='} :p${i++})`);
    params.push(asTimestamp(filters.endedAt.start));
  }
  if (filters.endedAt?.end) {
    conditions.push(`(r."isPending" = 0 AND r."endedAt" ${filters.endedAt.endExclusive ? '<' : '<='} :p${i++})`);
    params.push(asTimestamp(filters.endedAt.end));
  }
  if (filters.spanType !== undefined) {
    conditions.push(`r."spanType" = :p${i++}`);
    params.push(filters.spanType);
  }
  if (filters.entityType !== undefined) {
    conditions.push(`r."entityType" = :p${i++}`);
    params.push(filters.entityType);
  }
  if (filters.entityId !== undefined) {
    conditions.push(`r."entityId" = :p${i++}`);
    params.push(filters.entityId);
  }
  if (filters.entityName !== undefined) {
    conditions.push(`r."entityName" = :p${i++}`);
    params.push(filters.entityName);
  }
  if (filters.userId !== undefined) {
    conditions.push(`r."userId" = :p${i++}`);
    params.push(filters.userId);
  }
  if (filters.organizationId !== undefined) {
    conditions.push(`r."organizationId" = :p${i++}`);
    params.push(filters.organizationId);
  }
  if (filters.resourceId !== undefined) {
    conditions.push(`r."resourceId" = :p${i++}`);
    params.push(filters.resourceId);
  }
  if (filters.runId !== undefined) {
    conditions.push(`r."runId" = :p${i++}`);
    params.push(filters.runId);
  }
  if (filters.sessionId !== undefined) {
    conditions.push(`r."sessionId" = :p${i++}`);
    params.push(filters.sessionId);
  }
  if (filters.threadId !== undefined) {
    conditions.push(`r."threadId" = :p${i++}`);
    params.push(filters.threadId);
  }
  if (filters.requestId !== undefined) {
    conditions.push(`r."requestId" = :p${i++}`);
    params.push(filters.requestId);
  }
  if (filters.environment !== undefined) {
    conditions.push(`r."environment" = :p${i++}`);
    params.push(filters.environment);
  }
  if (filters.source !== undefined) {
    conditions.push(`r."executionSource" = :p${i++}`);
    params.push(filters.source);
  }
  if (filters.serviceName !== undefined) {
    conditions.push(`r."serviceName" = :p${i++}`);
    params.push(filters.serviceName);
  }
  if (filters.metadata != null) {
    const acc = { conditions, params, next: i };
    appendJsonContains(acc, 'r."metadataSearch"', filters.metadata);
    i = acc.next;
  }
  if (filters.tags != null && filters.tags.length > 0) {
    for (const tag of filters.tags) {
      conditions.push(`JSON_EXISTS(r."tags", '$[*]?(@ == $tag)' PASSING :p${i++} AS "tag")`);
      params.push(tag);
    }
  }
  if (filters.status !== undefined) {
    switch (filters.status) {
      case TraceStatus.ERROR:
        conditions.push(`r."error" IS NOT NULL`);
        break;
      case TraceStatus.RUNNING:
        conditions.push(`r."isPending" = 1`);
        break;
      case TraceStatus.SUCCESS:
        conditions.push(`r."error" IS NULL AND r."isPending" = 0`);
        break;
    }
  }
  if (filters.hasChildError !== undefined) {
    const sub = `EXISTS (
      SELECT 1 FROM ${spanTable} c
      WHERE c."traceId" = r."traceId" AND c."spanId" <> r."spanId" AND c."error" IS NOT NULL
    )`;
    conditions.push(filters.hasChildError ? sub : `NOT ${sub}`);
  }

  return { conditions, params, nextParamIdx: i };
}

const SPAN_SELECT_COLUMNS_ALIASED = buildSelectColumns(SPAN_EVENT_COLUMNS, 'r.');

/** Columns a trace list projects and the mapper for its rows. */
type TraceListProjection<TSpan> = {
  columns: string;
  mapRows: (client: DbClient, span: string, rows: Record<string, any>[]) => TSpan[] | Promise<TSpan[]>;
};

const FULL_PROJECTION: TraceListProjection<ListTracesResponse['spans'][number]> = {
  columns: SPAN_SELECT_COLUMNS_ALIASED,
  mapRows: (_client, _span, rows) => toTraceSpans(rows.map(rowToSpanRecord)),
};

/**
 * Columns a light trace list renders: the timeline fields, memory context, metadata and the stored
 * previews. The `input` / `output` payloads stay off the read path; rows written before the
 * previews were stored get theirs from the payloads, read for just those rows.
 */
const TRACE_LIGHT_SELECT_COLUMNS = buildNamedSelectColumns(
  [
    ...SPAN_LIGHT_SELECT_COLUMN_NAMES,
    'threadId',
    'resourceId',
    'metadataRaw',
    'inputPreview',
    'outputPreview',
    'errorPreview',
  ],
  'r.',
);

const PREVIEW_PAYLOADS = [
  ['inputPreview', 'input'],
  ['outputPreview', 'output'],
  ['errorPreview', 'error'],
] as const;

/**
 * Builds the previews of rows written before previews were stored (NULL preview) from those rows'
 * payloads. Only the flagged rows' payloads are read, so this fades out as old rows age out.
 *
 * Serialized JSON is decoded for the preview. Native JSON may normalize key order, so a preview
 * built here can differ in key order from the original. This only affects older rows without previews.
 */
async function fillMissingPreviews(client: DbClient, span: string, rows: Record<string, any>[]): Promise<void> {
  const missing = rows.filter(row => PREVIEW_PAYLOADS.some(([preview]) => needsPreview(row[preview])));
  if (!missing.length) return;
  const payloads = await client.manyOrNone<Record<string, unknown>>(
    `SELECT e."cursorId" AS "cursorId",
      ${PREVIEW_PAYLOADS.map(([preview, payload]) => `${missing.some(row => needsPreview(row[preview])) ? `JSON_SERIALIZE(e."${payload}" RETURNING CLOB)` : 'NULL'} AS "${payload}"`).join(', ')}
     FROM ${span} e WHERE (e."traceId", e."cursorId") IN
       (SELECT ids.traceId, ids.cursorId FROM JSON_TABLE(:p1, '$[*]' COLUMNS(traceId VARCHAR2(512) PATH '$.traceId', cursorId NUMBER PATH '$.cursorId')) ids)`,
    [clobBind(JSON.stringify(missing.map(row => ({ traceId: row.traceId, cursorId: String(row.cursorId) }))))],
  );
  const byCursor = new Map(payloads.map(payload => [String(payload.cursorId), payload]));
  for (const row of missing) {
    const payload = byCursor.get(String(row.cursorId));
    for (const [preview, column] of PREVIEW_PAYLOADS) {
      if (needsPreview(row[preview])) row[preview] = readPreview(row[preview], payload?.[column], true);
    }
  }
}

const LIGHT_PROJECTION: TraceListProjection<ListTracesLightResponse['spans'][number]> = {
  columns: TRACE_LIGHT_SELECT_COLUMNS,
  mapRows: async (client, span, rows) => {
    await fillMissingPreviews(client, span, rows);
    return rows.map(rowToLightTraceRecord);
  },
};

export async function listTraces(client: DbClient, schema: string, args: ListTracesArgs): Promise<ListTracesResponse> {
  return listTraceRows(client, schema, args, FULL_PROJECTION);
}

/** Trace list projecting only the columns a list renders, with stored previews instead of payloads. */
export async function listTracesLight(
  client: DbClient,
  schema: string,
  args: ListTracesArgs,
): Promise<ListTracesLightResponse> {
  return listTraceRows(client, schema, args, LIGHT_PROJECTION);
}

async function listTraceRows<TSpan>(
  client: DbClient,
  schema: string,
  args: ListTracesArgs,
  projection: TraceListProjection<TSpan>,
) {
  const { mode, filters, pagination, orderBy, after, limit } = listTracesArgsSchema.parse(args);
  const span = qualifiedTable(schema, TABLE_SPAN_EVENTS);

  if (mode === 'delta') {
    assertDeltaPollingEnabled();
    return listTracesDelta(client, span, projection, filters, after, limit);
  }

  return listTracesPage(
    client,
    span,
    projection,
    filters,
    pagination.page,
    pagination.perPage,
    orderBy.field,
    orderBy.direction,
  );
}

async function listTracesPage<TSpan>(
  client: DbClient,
  span: string,
  projection: TraceListProjection<TSpan>,
  filters: ListTracesArgs['filters'],
  page: number,
  perPage: number,
  orderField: 'startedAt' | 'endedAt',
  orderDir: 'ASC' | 'DESC',
) {
  const { conditions, params, nextParamIdx } = buildListTracesFilters(filters, span, 1);
  let i = nextParamIdx;
  const whereClause = `WHERE ${conditions.join(' AND ')}`;
  const orderClause =
    orderField === 'endedAt'
      ? `ORDER BY CASE WHEN r."isPending" = 1 THEN NULL ELSE r."endedAt" END ${orderDir} NULLS ${orderDir === 'DESC' ? 'FIRST' : 'LAST'}, r."cursorId" ${orderDir}`
      : `ORDER BY r."${orderField}" ${orderDir}, r."cursorId" ${orderDir}`;

  const deltaCursor = deltaPollingFeatureEnabled()
    ? await readTracesStreamHeadCursor(client, span, filters)
    : undefined;

  const countRow = await client.oneOrNone<{ count: string }>(
    `SELECT COUNT(*) AS "count" FROM ${span} r ${whereClause}`,
    params,
  );
  const count = Number(countRow?.count ?? 0);

  let spans: TSpan[] = [];
  if (count > 0) {
    const rows = await client.manyOrNone<Record<string, any>>(
      `SELECT ${projection.columns}
       FROM ${span} r
       ${whereClause}
       ${orderClause}
       OFFSET :p${i + 1} ROWS FETCH NEXT :p${i} ROWS ONLY`,
      [...params, perPage, page * perPage],
    );
    spans = await projection.mapRows(client, span, rows);
  }

  return {
    pagination: { total: count, page, perPage, hasMore: (page + 1) * perPage < count },
    spans,
    ...(deltaCursor !== undefined ? { deltaCursor } : {}),
  };
}

async function listTracesDelta<TSpan>(
  client: DbClient,
  span: string,
  projection: TraceListProjection<TSpan>,
  filters: ListTracesArgs['filters'],
  after: string | undefined,
  limit: number,
) {
  if (after === undefined) {
    const deltaCursor = await readTracesStreamHeadCursor(client, span, filters);
    return { spans: [] as TSpan[], delta: { limit, hasMore: false }, deltaCursor };
  }

  const afterCursor = decodeDeltaCursor(after);
  const safeHorizon = await readSafeXactHorizon(client);
  const { conditions, params, nextParamIdx } = buildListTracesFilters(filters, span, 1);
  let i = nextParamIdx;
  const scn = i++;
  const cursor = i++;
  conditions.push(`(r."xactId" > :p${scn} OR (r."xactId" = :p${scn} AND r."cursorId" > :p${cursor}))`);
  params.push(afterCursor.xactId, afterCursor.cursorId);
  conditions.push(`r."xactId" < :p${i++}`);
  params.push(safeHorizon);

  const rows = await client.manyOrNone<Record<string, any>>(
    `SELECT ${projection.columns}
     FROM ${span} r
     WHERE ${conditions.join(' AND ')}
     ORDER BY r."xactId" ASC, r."cursorId" ASC
     FETCH FIRST :p${i++} ROWS ONLY`,
    [...params, limit + 1],
  );

  const hasMore = rows.length > limit;
  const visible = rows.slice(0, limit);
  const deltaCursor =
    visible.length > 0
      ? encodeDeltaCursor(visible[visible.length - 1]!.xactId, visible[visible.length - 1]!.cursorId)
      : encodeDeltaCursor(safeHorizon, 0);

  return {
    spans: await projection.mapRows(client, span, visible),
    delta: { limit, hasMore },
    deltaCursor,
  };
}

async function readTracesStreamHeadCursor(
  client: DbClient,
  span: string,
  filters: ListTracesArgs['filters'],
): Promise<string> {
  void span;
  void filters;
  return encodeDeltaCursor(await readSafeXactHorizon(client), 0);
}

// ---------------------------------------------------------------------------
// listBranches
// ---------------------------------------------------------------------------

const BRANCH_SPAN_TYPE_SET = new Set<string>(BRANCH_SPAN_TYPES);

/**
 * Build the spanType predicate for a branch query. If the caller supplied a
 * specific branch type, narrow to that one; otherwise match the full
 * BRANCH_SPAN_TYPES set. Pushes the bind params onto `params` and returns the
 * SQL fragment plus the next param index.
 */
function buildBranchSpanTypeClause(
  userSpanType: string | undefined,
  params: unknown[],
  startIdx: number,
): { clause: string; nextIdx: number } | null {
  if (userSpanType !== undefined) {
    if (!BRANCH_SPAN_TYPE_SET.has(userSpanType)) {
      // Caller asked for a non-branch spanType — intersection is empty.
      return null;
    }
    params.push(userSpanType);
    return { clause: `r."spanType" = :p${startIdx}`, nextIdx: startIdx + 1 };
  }
  const placeholders: string[] = [];
  for (const t of BRANCH_SPAN_TYPES) {
    placeholders.push(`:p${startIdx + placeholders.length}`);
    params.push(t);
  }
  return { clause: `r."spanType" IN (${placeholders.join(', ')})`, nextIdx: startIdx + placeholders.length };
}

/**
 * Apply branch filters (spanType + the shared context surface). Branches do
 * NOT have a `parentSpanId IS NULL` predicate — branches can be nested under
 * other spans. Returns the bound params and the next param index.
 */
function buildListBranchesFilters(
  filters: ListBranchesArgs['filters'],
  spanType: string | undefined,
  spanTable: string,
  nextParamIdx: number,
): { conditions: string[]; params: unknown[]; nextParamIdx: number } | null {
  const conditions: string[] = [latestSpanPredicate(spanTable)];
  const params: unknown[] = [];
  let i = nextParamIdx;

  const spanTypeClause = buildBranchSpanTypeClause(spanType, params, i);
  if (!spanTypeClause) return null;
  conditions.push(spanTypeClause.clause);
  i = spanTypeClause.nextIdx;

  if (!filters) return { conditions, params, nextParamIdx: i };

  if (filters.startedAt?.start) {
    conditions.push(`r."startedAt" ${filters.startedAt.startExclusive ? '>' : '>='} :p${i++}`);
    params.push(asTimestamp(filters.startedAt.start));
  }
  if (filters.startedAt?.end) {
    conditions.push(`r."startedAt" ${filters.startedAt.endExclusive ? '<' : '<='} :p${i++}`);
    params.push(asTimestamp(filters.startedAt.end));
  }
  // A pending row stores a placeholder `endedAt`, so an endedAt range filter
  // has to exclude it: a span that has not ended has no end time to match.
  if (filters.endedAt?.start) {
    conditions.push(`(r."isPending" = 0 AND r."endedAt" ${filters.endedAt.startExclusive ? '>' : '>='} :p${i++})`);
    params.push(asTimestamp(filters.endedAt.start));
  }
  if (filters.endedAt?.end) {
    conditions.push(`(r."isPending" = 0 AND r."endedAt" ${filters.endedAt.endExclusive ? '<' : '<='} :p${i++})`);
    params.push(asTimestamp(filters.endedAt.end));
  }
  if (filters.traceId !== undefined) {
    conditions.push(`r."traceId" = :p${i++}`);
    params.push(filters.traceId);
  }
  if (filters.entityType !== undefined) {
    conditions.push(`r."entityType" = :p${i++}`);
    params.push(filters.entityType);
  }
  if (filters.entityId !== undefined) {
    conditions.push(`r."entityId" = :p${i++}`);
    params.push(filters.entityId);
  }
  if (filters.entityName !== undefined) {
    conditions.push(`r."entityName" = :p${i++}`);
    params.push(filters.entityName);
  }
  if (filters.entityVersionId !== undefined) {
    conditions.push(`r."entityVersionId" = :p${i++}`);
    params.push(filters.entityVersionId);
  }
  if (filters.parentEntityType !== undefined) {
    conditions.push(`r."parentEntityType" = :p${i++}`);
    params.push(filters.parentEntityType);
  }
  if (filters.parentEntityId !== undefined) {
    conditions.push(`r."parentEntityId" = :p${i++}`);
    params.push(filters.parentEntityId);
  }
  if (filters.parentEntityName !== undefined) {
    conditions.push(`r."parentEntityName" = :p${i++}`);
    params.push(filters.parentEntityName);
  }
  if (filters.parentEntityVersionId !== undefined) {
    conditions.push(`r."parentEntityVersionId" = :p${i++}`);
    params.push(filters.parentEntityVersionId);
  }
  if (filters.rootEntityType !== undefined) {
    conditions.push(`r."rootEntityType" = :p${i++}`);
    params.push(filters.rootEntityType);
  }
  if (filters.rootEntityId !== undefined) {
    conditions.push(`r."rootEntityId" = :p${i++}`);
    params.push(filters.rootEntityId);
  }
  if (filters.rootEntityName !== undefined) {
    conditions.push(`r."rootEntityName" = :p${i++}`);
    params.push(filters.rootEntityName);
  }
  if (filters.rootEntityVersionId !== undefined) {
    conditions.push(`r."rootEntityVersionId" = :p${i++}`);
    params.push(filters.rootEntityVersionId);
  }
  if (filters.userId !== undefined) {
    conditions.push(`r."userId" = :p${i++}`);
    params.push(filters.userId);
  }
  if (filters.organizationId !== undefined) {
    conditions.push(`r."organizationId" = :p${i++}`);
    params.push(filters.organizationId);
  }
  if (filters.resourceId !== undefined) {
    conditions.push(`r."resourceId" = :p${i++}`);
    params.push(filters.resourceId);
  }
  if (filters.runId !== undefined) {
    conditions.push(`r."runId" = :p${i++}`);
    params.push(filters.runId);
  }
  if (filters.sessionId !== undefined) {
    conditions.push(`r."sessionId" = :p${i++}`);
    params.push(filters.sessionId);
  }
  if (filters.threadId !== undefined) {
    conditions.push(`r."threadId" = :p${i++}`);
    params.push(filters.threadId);
  }
  if (filters.requestId !== undefined) {
    conditions.push(`r."requestId" = :p${i++}`);
    params.push(filters.requestId);
  }
  if (filters.experimentId !== undefined) {
    conditions.push(`r."experimentId" = :p${i++}`);
    params.push(filters.experimentId);
  }
  if (filters.environment !== undefined) {
    conditions.push(`r."environment" = :p${i++}`);
    params.push(filters.environment);
  }
  if (filters.serviceName !== undefined) {
    conditions.push(`r."serviceName" = :p${i++}`);
    params.push(filters.serviceName);
  }
  if (filters.source !== undefined) {
    conditions.push(`r."executionSource" = :p${i++}`);
    params.push(filters.source);
  }
  if (filters.metadata != null) {
    const acc = { conditions, params, next: i };
    appendJsonContains(acc, 'r."metadataSearch"', filters.metadata);
    i = acc.next;
  }
  if (filters.tags != null && filters.tags.length > 0) {
    for (const tag of filters.tags) {
      conditions.push(`JSON_EXISTS(r."tags", '$[*]?(@ == $tag)' PASSING :p${i++} AS "tag")`);
      params.push(tag);
    }
  }
  if (filters.status !== undefined) {
    switch (filters.status) {
      case TraceStatus.ERROR:
        conditions.push(`r."error" IS NOT NULL`);
        break;
      case TraceStatus.RUNNING:
        conditions.push(`r."isPending" = 1`);
        break;
      case TraceStatus.SUCCESS:
        conditions.push(`r."error" IS NULL AND r."isPending" = 0`);
        break;
    }
  }

  return { conditions, params, nextParamIdx: i };
}

export async function listBranches(
  client: DbClient,
  schema: string,
  args: ListBranchesArgs,
): Promise<ListBranchesResponse> {
  const { mode, filters, pagination, orderBy, after, limit } = listBranchesArgsSchema.parse(args);
  const span = qualifiedTable(schema, TABLE_SPAN_EVENTS);

  if (mode === 'delta') {
    assertDeltaPollingEnabled();
    return listBranchesDelta(client, span, filters, after, limit);
  }

  return listBranchesPage(client, span, filters, pagination.page, pagination.perPage, orderBy.field, orderBy.direction);
}

async function listBranchesPage(
  client: DbClient,
  span: string,
  filters: ListBranchesArgs['filters'],
  page: number,
  perPage: number,
  orderField: 'startedAt' | 'endedAt',
  orderDir: 'ASC' | 'DESC',
): Promise<ListBranchesResponse> {
  const built = buildListBranchesFilters(filters, filters?.spanType, span, 1);
  if (!built) {
    // Caller asked for a non-branch spanType — nothing matches by definition.
    const deltaCursor = deltaPollingFeatureEnabled()
      ? await readBranchesStreamHeadCursor(client, span, filters)
      : undefined;
    return {
      pagination: { total: 0, page, perPage, hasMore: false },
      branches: [],
      ...(deltaCursor !== undefined ? { deltaCursor } : {}),
    };
  }

  const { conditions, params, nextParamIdx } = built;
  let i = nextParamIdx;
  const whereClause = `WHERE ${conditions.join(' AND ')}`;
  const orderClause =
    orderField === 'endedAt'
      ? `ORDER BY CASE WHEN r."isPending" = 1 THEN NULL ELSE r."endedAt" END ${orderDir} NULLS ${orderDir === 'DESC' ? 'FIRST' : 'LAST'}, r."cursorId" ${orderDir}`
      : `ORDER BY r."${orderField}" ${orderDir}, r."cursorId" ${orderDir}`;

  const deltaCursor = deltaPollingFeatureEnabled()
    ? await readBranchesStreamHeadCursor(client, span, filters)
    : undefined;

  const countRow = await client.oneOrNone<{ count: string }>(
    `SELECT COUNT(*) AS "count" FROM ${span} r ${whereClause}`,
    params,
  );
  const count = Number(countRow?.count ?? 0);

  let spans: SpanRecord[] = [];
  if (count > 0) {
    const rows = await client.manyOrNone<Record<string, any>>(
      `SELECT ${SPAN_SELECT_COLUMNS_ALIASED}
       FROM ${span} r
       ${whereClause}
       ${orderClause}
       OFFSET :p${i + 1} ROWS FETCH NEXT :p${i} ROWS ONLY`,
      [...params, perPage, page * perPage],
    );
    spans = rows.map(rowToSpanRecord);
  }

  return {
    pagination: { total: count, page, perPage, hasMore: (page + 1) * perPage < count },
    branches: toTraceSpans(spans),
    ...(deltaCursor !== undefined ? { deltaCursor } : {}),
  };
}

async function listBranchesDelta(
  client: DbClient,
  span: string,
  filters: ListBranchesArgs['filters'],
  after: string | undefined,
  limit: number,
): Promise<ListBranchesResponse> {
  if (after === undefined) {
    const deltaCursor = await readBranchesStreamHeadCursor(client, span, filters);
    return { branches: [], delta: { limit, hasMore: false }, deltaCursor };
  }

  const built = buildListBranchesFilters(filters, filters?.spanType, span, 1);
  if (!built) {
    return {
      branches: [],
      delta: { limit, hasMore: false },
      deltaCursor: await readBranchesStreamHeadCursor(client, span, filters),
    };
  }

  const { conditions, params, nextParamIdx } = built;
  let i = nextParamIdx;
  const afterCursor = decodeDeltaCursor(after);
  const safeHorizon = await readSafeXactHorizon(client);
  const scn = i++;
  const cursor = i++;
  conditions.push(`(r."xactId" > :p${scn} OR (r."xactId" = :p${scn} AND r."cursorId" > :p${cursor}))`);
  params.push(afterCursor.xactId, afterCursor.cursorId);
  conditions.push(`r."xactId" < :p${i++}`);
  params.push(safeHorizon);

  const rows = await client.manyOrNone<Record<string, any>>(
    `SELECT ${SPAN_SELECT_COLUMNS_ALIASED}
     FROM ${span} r
     WHERE ${conditions.join(' AND ')}
     ORDER BY r."xactId" ASC, r."cursorId" ASC
     FETCH FIRST :p${i++} ROWS ONLY`,
    [...params, limit + 1],
  );

  const hasMore = rows.length > limit;
  const visible = rows.slice(0, limit);
  const deltaCursor =
    visible.length > 0
      ? encodeDeltaCursor(visible[visible.length - 1]!.xactId, visible[visible.length - 1]!.cursorId)
      : encodeDeltaCursor(safeHorizon, 0);

  return {
    branches: toTraceSpans(visible.map(rowToSpanRecord)),
    delta: { limit, hasMore },
    deltaCursor,
  };
}

/**
 * Branch bootstrap cursor. It points at the safe transaction horizon, not at
 * the current max cursorId, so a late-committing branch cannot be skipped.
 */
async function readBranchesStreamHeadCursor(
  client: DbClient,
  span: string,
  filters: ListBranchesArgs['filters'],
): Promise<string> {
  void span;
  void filters;
  return encodeDeltaCursor(await readSafeXactHorizon(client), 0);
}
