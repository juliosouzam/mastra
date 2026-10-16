import * as coreStorage from '@mastra/core/storage';
import type {
  GetTraceQueryValuesResponse,
  QueryThreadsResult,
  TraceQueryCanonicalField,
  TraceQueryObservedFieldsResult,
  TraceQueryFeedbackField,
  TraceQueryField,
  TraceQueryPredicateField,
  TraceQueryTenantScope,
  TraceQueryResponse,
  TraceQueryScoreField,
  TraceQuerySpanField,
  TrustedThreadPredicate,
  TrustedThreadQueryPlan,
  TrustedTraceQueryObservedFieldsPlan,
  TrustedTraceQueryPlan,
  TrustedTraceQueryValuesPlan,
  TrustedTraceQueryPredicate,
  TrustedTraceQueryScalarPredicate,
} from '@mastra/core/storage';
import { clobBind, isOracleErrorCode } from '../../../../shared/connection';

import type { DbClient, TxClient } from './client';
import { qualifiedTable, TABLE_FEEDBACK_EVENTS, TABLE_SCORE_EVENTS, TABLE_SPAN_EVENTS } from './ddl';
import {
  assertDeltaPollingEnabled,
  decodeDeltaCursor,
  deltaPollingFeatureEnabled,
  encodeDeltaCursor,
  readSafeXactHorizon,
} from './polling';
import { needsPreview, previewFieldsForSchema, readPreview } from './preview-compat';
import { latestScorePredicate } from './scores';

type SqlFragment = { sql: string; values: unknown[] };
type FieldRegistry<TField extends string> = Record<TField, string>;
type RelatedCollection = 'spans' | 'scores' | 'feedback';
type TraceSelection = {
  timeRange: { from: string; to: string };
  where?: TrustedTraceQueryPredicate;
};

export const TRACE_STATUS_SQL = `CASE WHEN r."error" IS NOT NULL THEN 'error' ELSE 'success' END`;

export function durationMsSql(startedAt: string, endedAt: string): string {
  const interval = `(${endedAt} - ${startedAt})`;
  return `(EXTRACT(DAY FROM ${interval}) * 86400000 + EXTRACT(HOUR FROM ${interval}) * 3600000 + EXTRACT(MINUTE FROM ${interval}) * 60000 + EXTRACT(SECOND FROM ${interval}) * 1000)`;
}

const TRACE_FIELDS = {
  traceId: 'r."traceId"',
  threadId: 'r."threadId"',
  resourceId: 'r."resourceId"',
  runId: 'r."runId"',
  sessionId: 'r."sessionId"',
  userId: 'r."userId"',
  organizationId: 'r."organizationId"',
  startedAt: 'r."startedAt"',
  endedAt: 'r."endedAt"',
  durationMs: durationMsSql('r."startedAt"', 'r."endedAt"'),
  entityName: 'r."entityName"',
  entityType: 'r."entityType"',
  environment: 'r."environment"',
  status: TRACE_STATUS_SQL,
  tags: 'r."tags"',
} satisfies FieldRegistry<TraceQueryField>;

const SPAN_FIELDS = {
  name: 's."name"',
  spanType: 's."spanType"',
  model: 's."model"',
  provider: 's."provider"',
  startedAt: 's."startedAt"',
  endedAt: 's."endedAt"',
  durationMs: 's."durationMs"',
  status: 's."status"',
  error: 's."error"',
  entityType: 's."entityType"',
  entityId: 's."entityId"',
  entityName: 's."entityName"',
  entityVersionId: 's."entityVersionId"',
  parentEntityVersionId: 's."parentEntityVersionId"',
  rootEntityVersionId: 's."rootEntityVersionId"',
  runId: 's."runId"',
  sessionId: 's."sessionId"',
  userId: 's."userId"',
  organizationId: 's."organizationId"',
} satisfies FieldRegistry<TraceQuerySpanField>;

const SCORE_FIELDS = {
  scorerId: 's."scorerId"',
  scorerVersion: 's."scorerVersion"',
  scoreSource: 's."scoreSource"',
  score: 's."score"',
  timestamp: 's."timestamp"',
  spanId: 's."spanId"',
  entityVersionId: 's."entityVersionId"',
  parentEntityVersionId: 's."parentEntityVersionId"',
  rootEntityVersionId: 's."rootEntityVersionId"',
} satisfies FieldRegistry<TraceQueryScoreField>;

const FEEDBACK_FIELDS = {
  feedbackType: 's."feedbackType"',
  feedbackSource: 's."feedbackSource"',
  feedbackUserId: 's."feedbackUserId"',
  sourceId: 's."sourceId"',
  entityVersionId: 's."entityVersionId"',
  parentEntityVersionId: 's."parentEntityVersionId"',
  rootEntityVersionId: 's."rootEntityVersionId"',
  timestamp: 's."timestamp"',
  comment: 's."comment"',
} satisfies FieldRegistry<Exclude<TraceQueryFeedbackField, 'value'>>;

const TRACE_SELECT = `
  r."traceId" AS "traceId",
  r."spanId" AS "rootSpanId",
  r."name" AS "name",
  r."entityId" AS "entityId",
  r."parentSpanId" AS "parentSpanId",
  JSON_SERIALIZE(r."metadataRaw" RETURNING CLOB) AS "metadata",
  r."inputPreview" AS "inputPreview",
  r."outputPreview" AS "outputPreview",
  r."errorPreview" AS "errorPreview",
  r."cursorId" AS "cursorId",
  r."threadId" AS "threadId",
  r."resourceId" AS "resourceId",
  r."startedAt" AS "startedAt",
  r."endedAt" AS "endedAt",
  r."entityName" AS "entityName",
  r."entityType" AS "entityType",
  r."environment" AS "environment",
  ${TRACE_STATUS_SQL} AS "status"`;

function fieldSql<TField extends string>(
  registry: Partial<FieldRegistry<TField>>,
  field: TraceQueryCanonicalField,
): string {
  const sql = Object.hasOwn(registry, field) ? registry[field as TField] : undefined;
  if (sql === undefined) throw new Error(`Unsupported trusted trace-query field: ${field}`);
  return sql;
}

/**
 * Value of trace-root `metadata.<key>` (alias `r`) as filtering, discovery, and aggregate grouping
 * all see it: the normalized search value, falling back to the trimmed raw string.
 */
export function traceMetadataValueSql(key: string): string {
  const path = ('$.' + JSON.stringify(key)).replaceAll("'", "''");
  const raw = `TRIM(JSON_VALUE(r."metadataRaw", '${path}' RETURNING CLOB NULL ON ERROR))`;
  return `COALESCE(CASE WHEN JSON_EXISTS(r."metadataSearch", '${path}?(@.type() == "string")') THEN JSON_VALUE(r."metadataSearch", '${path}' RETURNING CLOB NULL ON ERROR) END,
    CASE WHEN JSON_EXISTS(r."metadataRaw", '${path}?(@.type() == "string")') AND DBMS_LOB.GETLENGTH(${raw}) > 0 THEN ${raw} END)`;
}

function isMetadataField(field: TraceQueryPredicateField): field is `metadata.${string}` {
  return field.startsWith('metadata.');
}

function placeholders(values: readonly unknown[], offset: number): string {
  return values.map((_, index) => `:p${offset + index}`).join(', ');
}

function compileScalarPredicate<TField extends string>(
  predicate: TrustedTraceQueryScalarPredicate,
  registry: Partial<FieldRegistry<TField>>,
  parameterOffset: number,
  allowMetadata = false,
): SqlFragment {
  if (predicate.type === 'boolean') {
    const values: unknown[] = [];
    const parts = predicate.args.map(arg => {
      const compiled = compileScalarPredicate(arg, registry, parameterOffset + values.length, allowMetadata);
      values.push(...compiled.values);
      return `(${compiled.sql})`;
    });
    return {
      sql: parts.length
        ? parts.join(predicate.operator === 'and' ? ' AND ' : ' OR ')
        : predicate.operator === 'and'
          ? '1 = 1'
          : '1 = 0',
      values,
    };
  }
  if (predicate.type === 'not') {
    const compiled = compileScalarPredicate(predicate.arg, registry, parameterOffset, allowMetadata);
    return { sql: `NOT (${compiled.sql})`, values: compiled.values };
  }
  const metadata = isMetadataField(predicate.field);
  if (metadata && !allowMetadata) throw new Error(`Unsupported trusted trace-query field: ${predicate.field}`);
  const field = isMetadataField(predicate.field)
    ? traceMetadataValueSql(predicate.field.slice('metadata.'.length))
    : fieldSql(registry, predicate.field);
  const clob = metadata || predicate.field === 'comment';
  const valueFor = (value: unknown) =>
    value == null
      ? null
      : clob
        ? clobBind(String(value))
        : ['startedAt', 'endedAt', 'timestamp'].includes(predicate.field)
          ? new Date(value as string)
          : value;
  if (predicate.type === 'presence')
    return { sql: `${field} IS ${predicate.operator === 'exists' ? 'NOT ' : ''}NULL`, values: [] };
  if (predicate.type === 'collection') {
    const nonempty = `JSON_EXISTS(${field}, '$[0]')`;
    if (predicate.operator === 'includes' || predicate.operator === 'notIncludes') {
      const found = `JSON_EXISTS(${field}, '$[*]?(@ == $tag)' PASSING :p${parameterOffset} AS "tag")`;
      return {
        sql: predicate.operator === 'includes' ? found : `${nonempty} AND NOT (${found})`,
        values: [predicate.value],
      };
    }
    return { sql: predicate.operator === 'empty' ? `NOT (${nonempty})` : nonempty, values: [] };
  }
  if (predicate.type === 'text') {
    const path = ('$.words.' + JSON.stringify(predicate.field)).replaceAll("'", "''");
    const words = `JSON_VALUE(s."textSearch", '${path}' RETURNING CLOB NULL ON ERROR)`;
    const found = `INSTR(${words}, :p${parameterOffset}) ${predicate.operator === 'matches' ? '>' : '='} 0`;
    return { sql: `CASE WHEN ${found} THEN 1 ELSE 0 END = 1`, values: [` ${predicate.value} `] };
  }
  const equal = (value: unknown, parameter: string) => {
    if (value === null) return `${field} IS NULL`;
    if (clob) return `DBMS_LOB.COMPARE(${field}, ${value === '' ? `NVL(${parameter}, EMPTY_CLOB())` : parameter}) = 0`;
    if (value === '') return '1 = 0';
    return `${field} = ${parameter}`;
  };
  if (predicate.type === 'membership') {
    if (!predicate.values.length) return { sql: predicate.operator === 'in' ? '1 = 0' : '1 = 1', values: [] };
    const values: unknown[] = [];
    const terms = predicate.values.map(value => {
      const parameter = `:p${parameterOffset + values.length}`;
      if (value !== null && (clob || value !== '')) values.push(valueFor(value));
      return `(${equal(value, parameter)})`;
    });
    const found = terms.join(' OR ');
    return {
      sql: predicate.operator === 'in' ? `${field} IS NOT NULL AND (${found})` : `${field} IS NULL OR NOT (${found})`,
      values,
    };
  }
  if (predicate.operator === 'eq' || predicate.operator === 'ne') {
    const equality = equal(predicate.value, `:p${parameterOffset}`);
    const values = predicate.value === null ? [] : [valueFor(predicate.value)];
    // An empty VARCHAR literal has no native representation; valid nullable context fields normalize it to NULL on write.
    if (!clob && predicate.value === '') return { sql: predicate.operator === 'eq' ? '1 = 0' : '1 = 1', values: [] };
    return {
      sql:
        predicate.operator === 'eq'
          ? equality
          : predicate.value === null
            ? `${field} IS NOT NULL`
            : `${field} IS NULL OR NOT (${equality})`,
      values,
    };
  }
  const operators = { lt: '<', lte: '<=', gt: '>', gte: '>=' } as const;
  return {
    sql: `${field} IS NOT NULL AND ${field} ${operators[predicate.operator]} :p${parameterOffset}`,
    values: [valueFor(predicate.value)],
  };
}

function compileFeedbackScalarPredicate(
  predicate: TrustedTraceQueryScalarPredicate,
  parameterOffset: number,
): SqlFragment {
  if (predicate.type === 'boolean') {
    const values: unknown[] = [];
    const parts = predicate.args.map(arg => {
      const compiled = compileFeedbackScalarPredicate(arg, parameterOffset + values.length);
      values.push(...compiled.values);
      return `(${compiled.sql})`;
    });
    return { sql: parts.join(predicate.operator === 'and' ? ' AND ' : ' OR '), values };
  }
  if (predicate.type === 'not') {
    const compiled = compileFeedbackScalarPredicate(predicate.arg, parameterOffset);
    return { sql: `NOT (${compiled.sql})`, values: compiled.values };
  }
  if (predicate.field !== 'value') {
    return compileScalarPredicate(predicate, FEEDBACK_FIELDS, parameterOffset);
  }
  if (predicate.type === 'collection') throw new Error(`Unsupported trusted trace-query field: ${predicate.field}`);
  if (predicate.type === 'presence') {
    const present = `(s."valueString" IS NOT NULL OR s."valueNumber" IS NOT NULL)`;
    return { sql: predicate.operator === 'exists' ? present : `NOT ${present}`, values: [] };
  }
  const sample = predicate.type === 'membership' ? predicate.values[0] : predicate.value;
  const field = typeof sample === 'number' ? 's."valueNumber"' : 's."valueString"';
  return compileScalarPredicate(predicate, { value: field }, parameterOffset);
}

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

function latestSpanPredicate(spanTable: string): string {
  return `NOT EXISTS (
    SELECT 1 FROM ${spanTable} newer
    WHERE newer."traceId" = s."traceId"
      AND newer."spanId" = s."spanId"
      AND (newer."isPending" < s."isPending" OR (newer."isPending" = s."isPending" AND newer."cursorId" > s."cursorId"))
  )`;
}

function latestFeedbackPredicate(feedbackTable: string): string {
  return `NOT EXISTS (
    SELECT 1 FROM ${feedbackTable} newer
    WHERE newer."feedbackId" = s."feedbackId"
      AND newer."cursorId" > s."cursorId"
  )`;
}

function collectRelationCollections(
  predicate: TrustedTraceQueryPredicate | undefined,
  collections = new Set<RelatedCollection>(),
): Set<RelatedCollection> {
  if (!predicate) return collections;
  if (predicate.type === 'relation') {
    collections.add(predicate.collection);
  } else if (predicate.type === 'boolean') {
    for (const arg of predicate.args) collectRelationCollections(arg, collections);
  } else if (predicate.type === 'not') {
    collectRelationCollections(predicate.arg, collections);
  }
  return collections;
}

function collectThreadRelationCollections(
  predicate: TrustedThreadPredicate | undefined,
  collections: Set<RelatedCollection>,
): Set<RelatedCollection> {
  if (!predicate) return collections;
  if (predicate.type === 'relation') {
    collectRelationCollections(predicate.predicate, collections);
  } else if (predicate.type === 'boolean') {
    for (const arg of predicate.args) collectThreadRelationCollections(arg, collections);
  } else {
    collectThreadRelationCollections(predicate.arg, collections);
  }
  return collections;
}

// Root columns with planner statistics. Computed fields (status, durationMs, metadata.*) and tags
// are evaluated after choosing the current root so they cannot select an older snapshot.
const ROOT_PUSHDOWN_FIELDS = new Set<string>([
  'traceId',
  'threadId',
  'resourceId',
  'runId',
  'sessionId',
  'userId',
  'organizationId',
  'startedAt',
  'endedAt',
  'entityName',
  'entityType',
  'environment',
]);

function isRootPushdownPredicate(predicate: TrustedTraceQueryPredicate): boolean {
  if (predicate.type === 'relation') return false;
  if (predicate.type === 'boolean') return predicate.args.every(isRootPushdownPredicate);
  if (predicate.type === 'not') return isRootPushdownPredicate(predicate.arg);
  return ROOT_PUSHDOWN_FIELDS.has(predicate.field);
}

/**
 * Splits `where` into top-level AND conjuncts that can filter `root_scope` directly and the rest,
 * which is applied in `candidates`. The latest-root check compares against the unfiltered span
 * table, so filtering a root earlier cannot change which root is current, but it stops long windows
 * from materializing every root (and running the latest-root check on it) before filtering.
 */
function splitRootPredicate(predicate: TrustedTraceQueryPredicate | undefined): {
  root: TrustedTraceQueryPredicate[];
  rest: TrustedTraceQueryPredicate[];
} {
  if (!predicate) return { root: [], rest: [] };
  const conjuncts = predicate.type === 'boolean' && predicate.operator === 'and' ? predicate.args : [predicate];
  const root: TrustedTraceQueryPredicate[] = [];
  const rest: TrustedTraceQueryPredicate[] = [];
  for (const conjunct of conjuncts) {
    (isRootPushdownPredicate(conjunct) ? root : rest).push(conjunct);
  }
  return { root, rest };
}

function compileConjunction(predicates: TrustedTraceQueryPredicate[], values: unknown[]): string {
  if (predicates.length === 0) return '1 = 1';
  return predicates
    .map(predicate => {
      const compiled = compilePredicate(predicate, values.length + 1);
      values.push(...compiled.values);
      return `(${compiled.sql})`;
    })
    .join(' AND ');
}

function compilePredicate(predicate: TrustedTraceQueryPredicate, parameterOffset: number): SqlFragment {
  if (predicate.type === 'relation') {
    const compiled =
      predicate.collection === 'feedback'
        ? compileFeedbackScalarPredicate(predicate.predicate, parameterOffset)
        : compileScalarPredicate(
            predicate.predicate,
            predicate.collection === 'spans' ? SPAN_FIELDS : SCORE_FIELDS,
            parameterOffset,
          );
    const table =
      predicate.collection === 'spans'
        ? 'current_spans'
        : predicate.collection === 'scores'
          ? 'current_scores'
          : 'current_feedback';
    const existence = `EXISTS (
      SELECT 1 FROM ${table} s
      WHERE s."traceId" IS NOT NULL
        AND s."traceId" = r."traceId"
        AND (${compiled.sql})
    )`;
    return {
      sql: predicate.quantifier === 'some' ? existence : `NOT ${existence}`,
      values: compiled.values,
    };
  }

  if (predicate.type === 'boolean') {
    const values: unknown[] = [];
    const parts = predicate.args.map(arg => {
      const compiled = compilePredicate(arg, parameterOffset + values.length);
      values.push(...compiled.values);
      return `(${compiled.sql})`;
    });
    return { sql: parts.join(predicate.operator === 'and' ? ' AND ' : ' OR '), values };
  }

  if (predicate.type === 'not') {
    const compiled = compilePredicate(predicate.arg, parameterOffset);
    return { sql: `NOT (${compiled.sql})`, values: compiled.values };
  }

  return compileScalarPredicate(predicate, TRACE_FIELDS, parameterOffset, true);
}

function compileThreadPredicate(predicate: TrustedThreadPredicate, parameterOffset: number): SqlFragment {
  if (predicate.type === 'relation') {
    const compiled = compilePredicate(predicate.predicate, parameterOffset);
    const existence = `EXISTS (
      SELECT 1 FROM eligible_roots r
      WHERE r."threadId" = t."threadId"
        AND (${compiled.sql})
    )`;
    return {
      sql: predicate.quantifier === 'some' ? existence : `NOT ${existence}`,
      values: compiled.values,
    };
  }

  if (predicate.type === 'boolean') {
    const values: unknown[] = [];
    const parts = predicate.args.map(arg => {
      const compiled = compileThreadPredicate(arg, parameterOffset + values.length);
      values.push(...compiled.values);
      return `(${compiled.sql})`;
    });
    return { sql: parts.join(predicate.operator === 'and' ? ' AND ' : ' OR '), values };
  }

  const compiled = compileThreadPredicate(predicate.arg, parameterOffset);
  return { sql: `NOT (${compiled.sql})`, values: compiled.values };
}

export interface CompiledOracleTraceQuery {
  text: string;
  values: unknown[];
}

function compileOracleTraceScope(
  schema: string,
  selection: TraceSelection,
  relationCollections: Set<RelatedCollection>,
  scope: TraceQueryTenantScope | undefined,
  deltaWindow?: { xactId: string; cursorId: string; safeHorizon: string },
  rootPredicates: TrustedTraceQueryPredicate[] = [],
): { ctes: string[]; values: unknown[] } {
  const spanTable = qualifiedTable(schema, TABLE_SPAN_EVENTS);
  const scoreTable = qualifiedTable(schema, TABLE_SCORE_EVENTS);
  const feedbackTable = qualifiedTable(schema, TABLE_FEEDBACK_EVENTS);
  const values: unknown[] = [new Date(selection.timeRange.from), new Date(selection.timeRange.to)];
  // The delta window binds fixed positions :p3..:p5, so it must be pushed before the
  // dynamically numbered tenant scope values.
  const deltaConditions: string[] = [];
  if (deltaWindow) {
    values.push(deltaWindow.xactId, deltaWindow.cursorId, deltaWindow.safeHorizon);
    // Restrict candidates before materializing roots and their related records.
    // latestRootPredicate must still see replacements outside this interval.
    deltaConditions.push(`(r."xactId" > :p3 OR (r."xactId" = :p3 AND r."cursorId" > :p4))`, `r."xactId" < :p5`);
  }
  // Tenant scope is ANDed into every scan (roots and related signals) so a related
  // row from another tenant sharing a traceId can never match.
  const scopeConditions: string[] = [];
  if (scope) {
    values.push(scope.organizationId);
    scopeConditions.push(`"organizationId" = :p${values.length}`);
    if (scope.resourceId !== undefined) {
      values.push(scope.resourceId);
      scopeConditions.push(`"resourceId" = :p${values.length}`);
    }
  }
  const scopeSql = (alias: string): string =>
    scopeConditions.map(condition => `\n      AND ${alias}.${condition}`).join('');
  const rootConditions = [
    `r."parentSpanId" IS NULL`,
    latestRootPredicate(spanTable),
    `r."isPending" = 0`,
    `r."endedAt" IS NOT NULL`,
    `r."startedAt" >= :p1`,
    `r."startedAt" < :p2`,
    ...deltaConditions,
    ...scopeConditions.map(condition => `r.${condition}`),
  ];
  if (rootPredicates.length > 0) rootConditions.push(compileConjunction(rootPredicates, values));
  const ctes = [
    `root_scope AS (
    SELECT *
    FROM ${spanTable} r
    WHERE ${rootConditions.join('\n      AND ')}
  )`,
  ];

  if (relationCollections.has('spans')) {
    ctes.push(`current_spans AS (
    SELECT
      s."traceId",
      s."textSearch",
      s."name",
      s."spanType",
      CASE WHEN JSON_EXISTS(s."attributes", '$.model?(@.type() == "string")') THEN JSON_VALUE(s."attributes", '$.model' RETURNING VARCHAR2(32767) NULL ON ERROR) END AS "model",
      CASE WHEN JSON_EXISTS(s."attributes", '$.provider?(@.type() == "string")') THEN JSON_VALUE(s."attributes", '$.provider' RETURNING VARCHAR2(32767) NULL ON ERROR) END AS "provider",
      s."startedAt",
      CASE WHEN s."isPending" = 1 THEN NULL ELSE s."endedAt" END AS "endedAt",
      CASE
        WHEN s."isPending" = 1 THEN NULL
        ELSE ${durationMsSql('s."startedAt"', 's."endedAt"')}
      END AS "durationMs",
      CASE WHEN s."error" IS NOT NULL THEN 'error' ELSE 'success' END AS "status",
      s."error",
      s."entityType",
      s."entityId",
      s."entityName",
      s."entityVersionId",
      s."parentEntityVersionId",
      s."rootEntityVersionId",
      s."runId",
      s."sessionId",
      s."userId",
      s."organizationId"
    FROM ${spanTable} s
    WHERE s."traceId" IS NOT NULL
      AND s."traceId" IN (SELECT "traceId" FROM root_scope)
      AND ${latestSpanPredicate(spanTable)}${scopeSql('s')}
  )`);
  }
  if (relationCollections.has('scores')) {
    ctes.push(`current_scores AS (
    SELECT
      s."traceId",
      s."spanId",
      s."timestamp",
      s."scorerId",
      s."scorerVersion",
      s."scoreSource",
      s."score",
      s."entityVersionId",
      s."parentEntityVersionId",
      s."rootEntityVersionId"
    FROM ${scoreTable} s
    WHERE s."traceId" IS NOT NULL
      AND s."traceId" IN (SELECT "traceId" FROM root_scope)
      AND ${latestScorePredicate(scoreTable)}${scopeSql('s')}
  )`);
  }
  if (relationCollections.has('feedback')) {
    ctes.push(`current_feedback AS (
    SELECT
      s."traceId",
      s."textSearch",
      s."feedbackType",
      s."feedbackSource",
      s."feedbackUserId",
      s."sourceId",
      s."valueString",
      s."valueNumber",
      s."comment",
      s."timestamp",
      s."entityVersionId",
      s."parentEntityVersionId",
      s."rootEntityVersionId"
    FROM ${feedbackTable} s
    WHERE s."traceId" IS NOT NULL
      AND s."traceId" IN (SELECT "traceId" FROM root_scope)
      AND ${latestFeedbackPredicate(feedbackTable)}${scopeSql('s')}
  )`);
  }

  return { ctes, values };
}

/**
 * Builds the CTE chain ending in `candidates`: the completed, current trace roots in the
 * selection's time range and tenant scope that match its `where` predicate. Trace queries and
 * trace aggregates both select from this CTE so they always see the same population.
 */
export function compileOracleTraceCandidates(
  schema: string,
  selection: TraceSelection & { scope?: TraceQueryTenantScope },
  columns: string,
  deltaWindow?: { xactId: string; cursorId: string; safeHorizon: string },
): { ctes: string[]; values: unknown[] } {
  const relationCollections = collectRelationCollections(selection.where);
  const { root, rest } = splitRootPredicate(selection.where);
  const { ctes, values } = compileOracleTraceScope(
    schema,
    selection,
    relationCollections,
    selection.scope,
    deltaWindow,
    root,
  );

  const predicateSql = compileConjunction(rest, values);
  ctes.push(`candidates AS (
    SELECT ${columns}
    FROM root_scope r
    WHERE ${predicateSql}
  )`);
  return { ctes, values };
}

export function compileOracleTraceQuery(
  schema: string,
  plan: TrustedTraceQueryPlan,
  mode: 'data' | 'count' = 'data',
  safeHorizon?: string,
): CompiledOracleTraceQuery {
  let deltaWindow: { xactId: string; cursorId: string; safeHorizon: string } | undefined;
  if (plan.paginationMode === 'delta') {
    const watermark = coreStorage.getTraceQueryDeltaWatermark(plan, 'oracle');
    if (watermark === undefined || safeHorizon === undefined)
      throw new Error('Delta query requires a cursor and safe horizon');
    deltaWindow = { ...decodeTraceDeltaWatermark(watermark), safeHorizon };
  }
  const { ctes, values } = compileOracleTraceCandidates(
    schema,
    plan,
    `${TRACE_SELECT}${plan.paginationMode === 'delta' ? ', r."xactId"' : ''}`,
    deltaWindow,
  );
  const candidates = `WITH ${ctes.join(',\n')}`;

  if (plan.paginationMode === 'page' && mode === 'count') {
    return {
      text: `${candidates}
SELECT COUNT(*) AS "count"
FROM candidates`,
      values,
    };
  }

  if (plan.paginationMode === 'delta') {
    values.push(plan.limit + 1);
    return {
      text: `${candidates}
SELECT * FROM candidates
ORDER BY "xactId" ASC, "cursorId" ASC
FETCH FIRST :p${values.length} ROWS ONLY`,
      values,
    };
  }

  if (plan.result === 'groups') {
    const pageCondition = plan.cursor ? `AND "threadId" > :p${values.length + 1}` : '';
    if (plan.cursor) values.push(plan.cursor.threadId);
    values.push(plan.limit + 1);
    return {
      text: `${candidates}
SELECT "threadId"
FROM candidates
WHERE "threadId" IS NOT NULL ${pageCondition}
GROUP BY "threadId"
ORDER BY "threadId" ASC
FETCH FIRST :p${values.length} ROWS ONLY`,
      values,
    };
  }

  const orderField = plan.orderBy.field === 'startedAt' ? '"startedAt"' : '"endedAt"';
  const direction = plan.orderBy.direction === 'asc' ? 'ASC' : 'DESC';
  if (plan.paginationMode === 'page') {
    values.push(plan.perPage, plan.page * plan.perPage);
    return {
      text: `${candidates}
SELECT *
FROM candidates
ORDER BY ${orderField} ${direction}, "traceId" ASC
OFFSET :p${values.length} ROWS FETCH NEXT :p${values.length - 1} ROWS ONLY`,
      values,
    };
  }

  let pageCondition = '';
  if (plan.cursor) {
    const comparison = plan.orderBy.direction === 'asc' ? '>' : '<';
    const sortParameter = `:p${values.length + 1}`;
    const idParameter = `:p${values.length + 2}`;
    pageCondition = `WHERE (${orderField} ${comparison} ${sortParameter} OR (${orderField} = ${sortParameter} AND "traceId" > ${idParameter}))`;
    values.push(new Date(plan.cursor.sortValue), plan.cursor.traceId);
  }
  values.push(plan.limit + 1);

  return {
    text: `${candidates}
SELECT *
FROM candidates
${pageCondition}
ORDER BY ${orderField} ${direction}, "traceId" ASC
FETCH FIRST :p${values.length} ROWS ONLY`,
    values,
  };
}

export function compileOracleThreadQuery(schema: string, plan: TrustedThreadQueryPlan): CompiledOracleTraceQuery {
  const relationCollections = collectRelationCollections(plan.traces.where);
  collectThreadRelationCollections(plan.where, relationCollections);
  const { ctes, values } = compileOracleTraceScope(schema, plan.traces, relationCollections, plan.scope);

  let eligibilitySql = '1 = 1';
  if (plan.traces.where) {
    const eligibility = compilePredicate(plan.traces.where, values.length + 1);
    eligibilitySql = eligibility.sql;
    values.push(...eligibility.values);
  }
  ctes.push(`eligible_roots AS (
    SELECT *
    FROM root_scope r
    WHERE ${eligibilitySql}
  )`);
  ctes.push(`thread_ids AS (
    SELECT "threadId" COLLATE BINARY AS "threadId"
    FROM eligible_roots
    WHERE "threadId" IS NOT NULL
    GROUP BY "threadId" COLLATE BINARY
  )`);

  let threadPredicateSql = '1 = 1';
  if (plan.where) {
    const predicate = compileThreadPredicate(plan.where, values.length + 1);
    threadPredicateSql = predicate.sql;
    values.push(...predicate.values);
  }
  ctes.push(`qualified_threads AS (
    SELECT t."threadId"
    FROM thread_ids t
    WHERE ${threadPredicateSql}
  )`);

  const pageCondition = plan.cursor ? `WHERE "threadId" > :p${values.length + 1}` : '';
  if (plan.cursor) values.push(plan.cursor.threadId);
  values.push(plan.limit + 1);

  return {
    text: `WITH ${ctes.join(',\n')}
SELECT "threadId"
FROM qualified_threads
${pageCondition}
ORDER BY "threadId" ASC
FETCH FIRST :p${values.length} ROWS ONLY`,
    values,
  };
}

function discoveryRegistry(scope: TrustedTraceQueryValuesPlan['predicateScope']): Partial<FieldRegistry<string>> {
  if (scope === 'trace') return TRACE_FIELDS;
  if (scope === 'spans') return SPAN_FIELDS;
  if (scope === 'scores') return SCORE_FIELDS;
  return FEEDBACK_FIELDS;
}

function discoverySource(scope: TrustedTraceQueryValuesPlan['predicateScope']): string {
  if (scope === 'trace') return 'root_scope r';
  if (scope === 'spans') return 'current_spans s';
  if (scope === 'scores') return 'current_scores s';
  return 'current_feedback s';
}

function discoveryCollections(scope: TrustedTraceQueryValuesPlan['predicateScope']): Set<RelatedCollection> {
  return scope === 'trace' ? new Set() : new Set([scope]);
}

export function compileOracleTraceQueryObservedFields(
  schema: string,
  plan: TrustedTraceQueryObservedFieldsPlan,
): CompiledOracleTraceQuery {
  const { ctes, values } = compileOracleTraceScope(schema, plan, new Set(), plan.scope);
  const search = plan.search ? `AND INSTR(LOWER(entry.path), LOWER(:p${values.length + 1})) > 0` : '';
  if (plan.search) values.push(plan.search);
  values.push(plan.limit + 1);
  return {
    text: `WITH ${ctes.join(',\n')}
    SELECT entry.path AS "path", COUNT(*) AS "occurrences" FROM root_scope r,
    JSON_TABLE(r."textSearch", '$.metadataKeys[*]' COLUMNS(path VARCHAR2(128) PATH '$')) entry
    WHERE 1 = 1 ${search} GROUP BY entry.path ORDER BY "occurrences" DESC, entry.path COLLATE BINARY ASC FETCH FIRST :p${values.length} ROWS ONLY`,
    values,
  };
}

export function compileOracleTraceQueryValues(
  schema: string,
  plan: TrustedTraceQueryValuesPlan,
): CompiledOracleTraceQuery {
  const { ctes, values } = compileOracleTraceScope(schema, plan, discoveryCollections(plan.predicateScope), plan.scope);
  let extracted: string;
  if (plan.predicateScope === 'trace' && plan.path === 'tags') {
    extracted = `SELECT value FROM (SELECT DISTINCT r."traceId", JSON_VALUE(tags.value, '$' RETURNING VARCHAR2(32767) NULL ON ERROR) AS value FROM root_scope r, JSON_TABLE(r."tags", '$[*]' COLUMNS(value JSON PATH '$')) tags) t`;
  } else if (plan.predicateScope === 'trace' && plan.path.startsWith('metadata.')) {
    const field = traceMetadataValueSql(plan.path.slice('metadata.'.length));
    // Values outside the scalar conversion bound also exceed Core's smaller suggestion byte bound.
    extracted = `SELECT JSON_VALUE(JSON_SCALAR(${field}), '$' RETURNING VARCHAR2(32767) NULL ON ERROR) AS value FROM root_scope r`;
  } else {
    const field = fieldSql(discoveryRegistry(plan.predicateScope), plan.path as TraceQueryCanonicalField);
    extracted = `SELECT ${field} AS value FROM ${discoverySource(plan.predicateScope)}`;
  }
  const search = plan.search ? `AND INSTR(LOWER(value), LOWER(:p${values.length + 1})) > 0` : '';
  if (plan.search) values.push(plan.search);
  values.push(plan.limit + 1);
  return {
    text: `WITH ${ctes.join(',\n')}, extracted AS (${extracted}) SELECT value AS "value", COUNT(*) AS "count"
    FROM extracted WHERE value IS NOT NULL AND LENGTHB(value) <= ${coreStorage.TRACE_QUERY_MAX_STRING_BYTES} ${search}
    GROUP BY value ORDER BY "count" DESC, value COLLATE BINARY ASC FETCH FIRST :p${values.length} ROWS ONLY`,
    values,
  };
}

function asIsoTimestamp(value: unknown): string {
  if (value === null || value === undefined) throw new Error('Trace query returned a null timestamp');
  return value instanceof Date ? value.toISOString() : new Date(value as string | number).toISOString();
}

export async function runWithOracleTraceQueryTimeout<T>(
  client: DbClient,
  timeoutMs: number,
  execute: (transaction: TxClient) => Promise<T>,
  options: { repeatableRead?: boolean } = {},
): Promise<T> {
  const resolvedTimeoutMs = coreStorage.resolveTraceQueryTimeoutMs(timeoutMs);
  const horizon = options.repeatableRead ? await readSafeXactHorizon(client) : undefined;
  try {
    return await client.tx(async transaction => {
      if (options.repeatableRead) {
        // Both modes provide a transaction snapshot. READ ONLY can raise ORA-01466
        // immediately after DDL on the supported releases; these callbacks only read.
        await transaction.none('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE');
        transaction.snapshotHorizon = horizon;
      }
      transaction.setCallTimeout(resolvedTimeoutMs);
      return execute(transaction);
    });
  } catch (error) {
    const code = error && typeof error === 'object' && 'code' in error ? String(error.code) : '';
    if (isOracleErrorCode(error, [-1013]) || ['DPI-1067', 'DPI-1080', 'NJS-040', 'NJS-123'].includes(code))
      throw new coreStorage.TraceQueryExecutionError();
    if (isOracleErrorCode(error, [-1652, -4030, -4031, -40180])) throw new coreStorage.TraceQueryResourceLimitError();
    throw error;
  }
}

export async function getTraceQueryObservedFields(
  client: DbClient,
  schema: string,
  plan: TrustedTraceQueryObservedFieldsPlan,
  timeoutMs: number,
): Promise<TraceQueryObservedFieldsResult> {
  if (plan.predicateScope !== 'trace') return { observedFields: [], observedFieldsTruncated: false };
  const query = compileOracleTraceQueryObservedFields(schema, plan);
  const rows = await runWithOracleTraceQueryTimeout(client, timeoutMs, transaction =>
    transaction.any<Record<string, unknown>>(query.text, query.values),
  );
  return {
    observedFields: rows
      .slice(0, plan.limit)
      .map(row => coreStorage.createTraceQueryObservedFieldDescriptor(String(row.path), Number(row.occurrences))),
    observedFieldsTruncated: rows.length > plan.limit,
  };
}

export async function getTraceQueryValues(
  client: DbClient,
  schema: string,
  plan: TrustedTraceQueryValuesPlan,
  timeoutMs: number,
): Promise<GetTraceQueryValuesResponse> {
  const query = compileOracleTraceQueryValues(schema, plan);
  const rows = await runWithOracleTraceQueryTimeout(client, timeoutMs, transaction =>
    transaction.any<Record<string, unknown>>(query.text, query.values),
  );
  return coreStorage.getTraceQueryValuesResponseSchema.parse({
    values: rows.slice(0, plan.limit).map(row => ({ value: String(row.value), count: Number(row.count) })),
    valuesTruncated: rows.length > plan.limit,
  });
}

function decodeTraceDeltaWatermark(watermark: string) {
  try {
    return decodeDeltaCursor(watermark);
  } catch {
    throw new coreStorage.TraceQueryCursorError('TRACE_QUERY_CURSOR_MALFORMED');
  }
}

function emptyDeltaWatermark(horizon: string, watermark?: string): string {
  // Preserve monotonicity if a query envelope contains a future native watermark.
  if (watermark !== undefined && BigInt(decodeTraceDeltaWatermark(watermark).xactId) >= BigInt(horizon))
    return watermark;
  return encodeDeltaCursor(horizon, 0);
}

async function setRemainingTimeout(transaction: TxClient, deadline: number): Promise<void> {
  const remainingTimeoutMs = Math.floor(deadline - performance.now());
  if (remainingTimeoutMs <= 0) throw new coreStorage.TraceQueryExecutionError();
  transaction.setCallTimeout(remainingTimeoutMs);
}

const PREVIEW_PAYLOADS = [
  ['inputPreview', 'input'],
  ['outputPreview', 'output'],
  ['errorPreview', 'error'],
] as const;

/**
 * Fill the previews of root rows written before the preview columns existed (stored preview NULL)
 * by reading just those rows' payloads, and only the payloads whose preview is missing. Rows are
 * keyed by `cursorId`, which identifies the exact root row the list picked.
 *
 * JSON is serialized to CLOB and decoded for `buildPreview(value)`. Native JSON may
 * normalize object key order, so a compact-JSON fallback preview can differ from the one the
 * writer would have stored from the original value. That is acceptable for these old rows.
 */
async function fillMissingTracePreviews(
  transaction: TxClient,
  schema: string,
  rows: Record<string, unknown>[],
  deadline: number,
): Promise<void> {
  const missing = rows.filter(row => PREVIEW_PAYLOADS.some(([preview]) => needsPreview(row[preview])));
  if (!missing.length) return;
  await setRemainingTimeout(transaction, deadline);
  const payloads = await transaction.any<Record<string, unknown>>(
    `SELECT e."cursorId" AS "cursorId", ${PREVIEW_PAYLOADS.map(([preview, payload]) => `${missing.some(row => needsPreview(row[preview])) ? `JSON_SERIALIZE(e."${payload}" RETURNING CLOB)` : 'NULL'} AS "${payload}"`).join(', ')}
    FROM ${qualifiedTable(schema, TABLE_SPAN_EVENTS)} e WHERE (e."traceId", e."cursorId") IN
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

function traceRowToResult(row: Record<string, unknown>) {
  return {
    traceId: String(row.traceId),
    rootSpanId: String(row.rootSpanId),
    name: row.name,
    entityId: row.entityId ?? null,
    parentSpanId: row.parentSpanId ?? null,
    createdAt: asIsoTimestamp(row.startedAt),
    metadata: typeof row.metadata === 'string' ? JSON.parse(row.metadata) : (row.metadata ?? null),
    ...previewFieldsForSchema(coreStorage.traceQueryTraceSchema, {
      inputPreview: readPreview(row.inputPreview),
      outputPreview: readPreview(row.outputPreview),
      errorPreview: readPreview(row.errorPreview),
    }),
    threadId: row.threadId == null ? null : String(row.threadId),
    resourceId: row.resourceId == null ? null : String(row.resourceId),
    startedAt: asIsoTimestamp(row.startedAt),
    endedAt: asIsoTimestamp(row.endedAt),
    entityName: row.entityName == null ? null : String(row.entityName),
    entityType: row.entityType == null ? null : String(row.entityType),
    environment: row.environment == null ? null : String(row.environment),
    status: row.status,
  };
}

export async function queryTraces(
  client: DbClient,
  schema: string,
  plan: TrustedTraceQueryPlan,
  timeoutMs: number,
): Promise<TraceQueryResponse> {
  if (plan.paginationMode === 'delta') {
    assertDeltaPollingEnabled();
    const watermark = coreStorage.getTraceQueryDeltaWatermark(plan, 'oracle');
    if (watermark !== undefined) decodeTraceDeltaWatermark(watermark);
    const resolvedTimeoutMs = coreStorage.resolveTraceQueryTimeoutMs(timeoutMs);
    const deadline = performance.now() + resolvedTimeoutMs;
    return runWithOracleTraceQueryTimeout(
      client,
      resolvedTimeoutMs,
      async transaction => {
        await setRemainingTimeout(transaction, deadline);
        const horizon = await readSafeXactHorizon(transaction);
        let rows: Record<string, unknown>[] = [];
        if (watermark !== undefined) {
          await setRemainingTimeout(transaction, deadline);
          const query = compileOracleTraceQuery(schema, plan, 'data', horizon);
          rows = await transaction.any<Record<string, unknown>>(query.text, query.values);
        }
        const visible = rows.slice(0, plan.limit);
        await fillMissingTracePreviews(transaction, schema, visible, deadline);
        const last = visible.at(-1);
        return coreStorage.traceQueryResponseSchema.parse({
          traces: visible.map(traceRowToResult),
          delta: { limit: plan.limit, hasMore: rows.length > plan.limit },
          deltaCursor: coreStorage.encodeTraceQueryDeltaCursor(
            plan,
            'oracle',
            last ? encodeDeltaCursor(last.xactId, last.cursorId) : emptyDeltaWatermark(horizon, watermark),
          ),
        });
      },
      { repeatableRead: true },
    );
  }
  if (plan.paginationMode === 'page') {
    const resolvedTimeoutMs = coreStorage.resolveTraceQueryTimeoutMs(timeoutMs);
    const deadline = performance.now() + resolvedTimeoutMs;
    const countQuery = compileOracleTraceQuery(schema, plan, 'count');
    const dataQuery = compileOracleTraceQuery(schema, plan);
    const { total, rows, deltaCursor } = await runWithOracleTraceQueryTimeout(
      client,
      resolvedTimeoutMs,
      async transaction => {
        // The list-polling feature predates the trace-query cursor encoder.
        let deltaCursor: string | undefined;
        if (deltaPollingFeatureEnabled() && typeof coreStorage.encodeTraceQueryDeltaCursor === 'function') {
          await setRemainingTimeout(transaction, deadline);
          const horizon = await readSafeXactHorizon(transaction);
          deltaCursor = coreStorage.encodeTraceQueryDeltaCursor(plan, 'oracle', encodeDeltaCursor(horizon, 0));
        }
        if (deltaCursor !== undefined) await setRemainingTimeout(transaction, deadline);
        const countRows = await transaction.any<{ count: string }>(countQuery.text, countQuery.values);
        const remainingTimeoutMs = Math.floor(deadline - performance.now());
        if (remainingTimeoutMs <= 0) throw new coreStorage.TraceQueryExecutionError();
        transaction.setCallTimeout(remainingTimeoutMs);
        const rows = await transaction.any<Record<string, unknown>>(dataQuery.text, dataQuery.values);
        await fillMissingTracePreviews(transaction, schema, rows, deadline);
        return { total: Number(countRows[0]?.count ?? 0), rows, deltaCursor };
      },
      { repeatableRead: true },
    );
    const traces = rows.map(traceRowToResult);
    return coreStorage.traceQueryResponseSchema.parse({
      traces,
      ...(deltaCursor === undefined ? {} : { deltaCursor }),
      pagination: {
        total,
        page: plan.page,
        perPage: plan.perPage,
        hasMore: (plan.page + 1) * plan.perPage < total,
      },
    });
  }

  const query = compileOracleTraceQuery(schema, plan);
  const deadline = performance.now() + coreStorage.resolveTraceQueryTimeoutMs(timeoutMs);
  const rows = await runWithOracleTraceQueryTimeout(client, timeoutMs, async transaction => {
    const rows = await transaction.any<Record<string, unknown>>(query.text, query.values);
    if (plan.result === 'traces')
      await fillMissingTracePreviews(transaction, schema, rows.slice(0, plan.limit), deadline);
    return rows;
  });
  const visibleRows = rows.slice(0, plan.limit);

  if (plan.result === 'groups') {
    const groups = visibleRows.map(row => ({ threadId: String(row.threadId) }));
    const last = groups.at(-1);
    return coreStorage.traceQueryResponseSchema.parse({
      groups,
      page: {
        next:
          rows.length > plan.limit && last
            ? coreStorage.encodeTraceQueryCursor(plan, { result: 'groups', threadId: last.threadId })
            : null,
      },
    });
  }

  const traces = visibleRows.map(traceRowToResult);
  const last = traces.at(-1);
  return coreStorage.traceQueryResponseSchema.parse({
    traces,
    page: {
      next:
        rows.length > plan.limit && last
          ? coreStorage.encodeTraceQueryCursor(plan, {
              result: 'traces',
              sortValue: last[plan.orderBy.field],
              traceId: last.traceId,
            })
          : null,
    },
  });
}

export async function queryThreads(
  client: DbClient,
  schema: string,
  plan: TrustedThreadQueryPlan,
  timeoutMs: number,
): Promise<QueryThreadsResult> {
  const query = compileOracleThreadQuery(schema, plan);
  const rows = await runWithOracleTraceQueryTimeout(client, timeoutMs, transaction =>
    transaction.any<Record<string, unknown>>(query.text, query.values),
  );
  const threads = rows.slice(0, plan.limit).map(row => ({ threadId: String(row.threadId) }));
  const last = threads.at(-1);
  return coreStorage.queryThreadsResultSchema.parse({
    threads,
    page: {
      next:
        rows.length > plan.limit && last
          ? coreStorage.encodeTraceQueryCursor(plan, { result: 'threads', threadId: last.threadId })
          : null,
    },
  });
}

/** Compile a span-row filter with exactly the same rules as trace span predicates. */
export function compileSpanQueryPredicate(predicate: TrustedTraceQueryScalarPredicate, offset: number): SqlFragment {
  return compileScalarPredicate(predicate, SPAN_FIELDS, offset);
}
