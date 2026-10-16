import * as coreStorage from '@mastra/core/storage';
import type {
  TraceAggregateCountDistinctField,
  TraceAggregateDimension,
  TraceAggregateResponse,
  TraceAggregateRow,
  TrustedTraceAggregateHavingPredicate,
  TrustedTraceAggregateMeasure,
  TrustedTraceAggregateMeasureName,
  TrustedTraceAggregatePlan,
} from '@mastra/core/storage';
import { clobBind } from '../../../../shared/connection';

import type { DbClient } from './client';
import { qualifiedTable, TABLE_METRIC_EVENTS } from './ddl';
import {
  compileOracleTraceCandidates,
  durationMsSql,
  runWithOracleTraceQueryTimeout,
  TRACE_STATUS_SQL,
  traceMetadataValueSql,
} from './trace-query';

export interface CompiledOracleTraceAggregate {
  text: string;
  values: unknown[];
}

/** Trace-root column expressions (alias `r`) for each groupable dimension; same values `where` sees. */
const DIMENSION_SQL: Record<string, string> = {
  entityType: 'r."entityType"',
  entityName: 'r."entityName"',
  environment: 'r."environment"',
  status: TRACE_STATUS_SQL,
  serviceName: 'r."serviceName"',
  executionSource: 'r."executionSource"',
  threadId: 'r."threadId"',
  resourceId: 'r."resourceId"',
  userId: 'r."userId"',
  sessionId: 'r."sessionId"',
  organizationId: 'r."organizationId"',
  experimentId: 'r."experimentId"',
};

const PERCENTILES: Record<string, number> = {
  'duration.p50': 0.5,
  'duration.p90': 0.9,
  'duration.p95': 0.95,
  'duration.p99': 0.99,
};

const COMPARISON_SQL = { eq: '=', ne: '<>', lt: '<', lte: '<=', gt: '>', gte: '>=' } as const;

/**
 * Compiles a trusted aggregate plan. The plan's caps are enforced by the planner and trusted here.
 *
 * Every candidate root is first reduced to a `facts` row (dimension values, duration, error flag,
 * countDistinct inputs, bucket) so grouping, measures, and joins only reference plain columns.
 * Without `interval`, groups are filtered, ordered, and limited in one grouped select. With
 * `interval`, a `ranked` CTE ranks groups on whole-window measures, then only the top `limit`
 * groups are expanded into per-bucket rows.
 */
export function compileOracleTraceAggregate(
  schema: string,
  plan: TrustedTraceAggregatePlan,
): CompiledOracleTraceAggregate {
  const { ctes, values } = compileOracleTraceCandidates(schema, plan, 'r.*');
  const param = (value: unknown): string => {
    values.push(value);
    return `:p${values.length}`;
  };
  const dimensionSql = (field: TraceAggregateDimension): string => {
    if (field.startsWith('metadata.')) return traceMetadataValueSql(field.slice('metadata.'.length));
    const sql = Object.hasOwn(DIMENSION_SQL, field) ? DIMENSION_SQL[field] : undefined;
    if (sql === undefined) throw new Error(`Unsupported trusted trace-aggregate field: ${field}`);
    return sql;
  };
  const distinctSql = (field: TraceAggregateCountDistinctField): string =>
    field === 'traceId' ? 'r."traceId"' : dimensionSql(field);

  const dimensionColumns = plan.dimensions.map((_, index) => `"d${index}"`);
  const distinctColumns = new Map<string, string>();
  const factColumns = plan.dimensions.map(
    (dimension, index) => `JSON_SCALAR(${dimensionSql(dimension)}) AS "d${index}"`,
  );
  for (const measure of plan.measures) {
    if (measure.type !== 'countDistinct' || distinctColumns.has(measure.field)) continue;
    const column = `"cd${distinctColumns.size}"`;
    distinctColumns.set(measure.field, column);
    factColumns.push(`JSON_SCALAR(${distinctSql(measure.field)}) AS ${column}`);
  }
  factColumns.push(
    `(${durationMsSql('r."startedAt"', 'r."endedAt"')}) AS "durationMs"`,
    `CASE WHEN r."error" IS NOT NULL THEN 1 ELSE 0 END AS "isError"`,
  );
  if (plan.interval !== undefined) {
    const intervalMs = coreStorage.TRACE_AGGREGATE_INTERVAL_MS[plan.interval];
    if (intervalMs === undefined) throw new Error(`Unsupported trusted trace-aggregate interval: ${plan.interval}`);
    // UTC-aligned bucket containing startedAt; may begin before timeRange.from.
    factColumns.push(
      `TO_TIMESTAMP_TZ('1970-01-01 00:00:00 +00:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM') + NUMTODSINTERVAL(FLOOR((CAST(SYS_EXTRACT_UTC(r."startedAt") AS DATE) - DATE '1970-01-01') * 86400000 / ${intervalMs}) * ${intervalMs / 1000}, 'SECOND') AS "bucket"`,
    );
  }
  const measureKinds = new Set(plan.measures.map(measure => measureRule(measure.name)?.kind));
  const hasCost = measureKinds.has('cost');
  let usageJoin = '';
  if (hasCost || measureKinds.has('tokens')) {
    ctes.push(...compileUsageCtes(schema, plan, param));
    usageJoin = '\n    LEFT JOIN usage u ON u."traceId" = r."traceId"';
    // A usage-bearing trace with no row of a name contributes 0; any other trace contributes NULL.
    USAGE_METRIC_NAMES.forEach((_, index) =>
      factColumns.push(`CASE WHEN u."traceId" IS NOT NULL THEN COALESCE(u."t${index}", 0) END AS "t${index}"`),
    );
    factColumns.push(
      `CASE WHEN u."traceId" IS NOT NULL THEN 1 ELSE 0 END AS "usageBearing"`,
      `CASE WHEN u."priced" = 1 THEN u."cost" END AS "traceCost"`,
      `CASE WHEN u."priced" = 1 AND u."pricingFailure" = 0 THEN 1 ELSE 0 END AS "covered"`,
      `u."unitMin"`,
      `u."unitMax"`,
    );
  }
  ctes.push(`facts AS (
    SELECT ${factColumns.join(',\n      ')}
    FROM candidates r${usageJoin}
  )`);

  // More than one distinct priced unit in the group; MIN/MAX are NULL when nothing is priced.
  const mixedUnitsSql = (alias: string) =>
    `MIN(${alias}"unitMin" COLLATE BINARY) <> MAX(${alias}"unitMax" COLLATE BINARY)`;

  // All measures are float8 so having literals compare without integer-cast errors.
  const measureSql = (name: TrustedTraceAggregateMeasureName, alias = ''): string => {
    const column = (name: string) => `${alias}"${name}"`;
    if (name === 'count') return 'COUNT(*)';
    const rule = measureRule(name);
    // SUM/AVG skip the NULL per-trace values of traces that are not usage-bearing (tokens) or
    // not priced (cost), and are NULL when no trace qualifies.
    if (rule?.kind === 'tokens' && rule.metricNames) {
      const value = rule.metricNames.map(metricName => column(`t${usageMetricIndex(metricName)}`)).join(' + ');
      return `(${rule.statistic === 'avg' ? 'AVG' : 'SUM'}(${value}))`;
    }
    if (rule?.kind === 'cost') {
      const aggregate = rule.statistic === 'avg' ? 'AVG' : 'SUM';
      return `(CASE WHEN ${mixedUnitsSql(alias)} THEN NULL ELSE ${aggregate}(${column('traceCost')}) END)`;
    }
    if (name === 'errorCount') return `(SUM(CASE WHEN ${column('isError')} = 1 THEN 1 ELSE 0 END))`;
    if (name === 'errorRate') return `(SUM(CASE WHEN ${column('isError')} = 1 THEN 1 ELSE 0 END)) / COUNT(*)`;
    if (name === 'duration.avg') return `AVG(${column('durationMs')})`;
    if (name === 'duration.min') return `MIN(${column('durationMs')})`;
    if (name === 'duration.max') return `MAX(${column('durationMs')})`;
    const percentile = PERCENTILES[name];
    if (percentile !== undefined) {
      return `percentile_cont(${percentile}) WITHIN GROUP (ORDER BY ${column('durationMs')})`;
    }
    const measure = plan.measures.find(
      (candidate): candidate is Extract<TrustedTraceAggregateMeasure, { type: 'countDistinct' }> =>
        candidate.type === 'countDistinct' && candidate.name === name,
    );
    const distinctColumn = measure && distinctColumns.get(measure.field);
    if (distinctColumn === undefined) throw new Error(`Unsupported trusted trace-aggregate measure: ${name}`);
    return `(COUNT(DISTINCT ${alias}${distinctColumn}))`;
  };

  const compileHaving = (predicate: TrustedTraceAggregateHavingPredicate): string => {
    if (predicate.type === 'boolean') {
      return predicate.args.map(arg => `(${compileHaving(arg)})`).join(predicate.operator === 'and' ? ' AND ' : ' OR ');
    }
    if (predicate.type === 'not') return `NOT (${compileHaving(predicate.arg)})`;
    const measure = measureSql(predicate.measure);
    if (predicate.type === 'membership') {
      if (predicate.values.length === 0) return predicate.operator === 'in' ? 'FALSE' : '1 = 1';
      const list = predicate.values.map(value => `${param(value)}`).join(', ');
      return `${measure} ${predicate.operator === 'in' ? 'IN' : 'NOT IN'} (${list})`;
    }
    const operator = COMPARISON_SQL[predicate.operator];
    if (operator === undefined) throw new Error(`Unsupported trusted trace-aggregate operator: ${predicate.operator}`);
    return `${measure} ${operator} ${param(predicate.value)}`;
  };

  // `count > 0` keeps an ungrouped aggregate over an empty population from yielding a zero row.
  const havingSql = ['COUNT(*) > 0', ...(plan.having ? [`(${compileHaving(plan.having)})`] : [])].join(' AND ');
  const direction = plan.orderBy.direction === 'asc' ? 'ASC' : 'DESC';
  let orderTarget: string;
  if (plan.orderBy.target === 'measure') {
    orderTarget = measureSql(plan.orderBy.measure);
  } else {
    const index = plan.dimensions.indexOf(plan.orderBy.dimension);
    if (index === -1) throw new Error(`Unsupported trusted trace-aggregate order dimension: ${plan.orderBy.dimension}`);
    orderTarget = `${dimensionColumns[index]}`;
  }
  const orderSql = [
    `${orderTarget} ${direction} NULLS LAST`,
    ...dimensionColumns.map(column => `${column} ASC NULLS LAST`),
  ].join(', ');
  const groupSql = dimensionColumns.length > 0 ? `GROUP BY ${dimensionColumns.join(', ')}` : '';
  const limitParameter = param(plan.limit + 1);
  const costColumns = (alias: string): string[] => {
    if (!hasCost) return [];
    const mixedUnit = param(coreStorage.TRACE_AGGREGATE_MIXED_COST_UNIT);
    return [
      `(SUM(CASE WHEN ${alias}"covered" = 1 THEN 1 ELSE 0 END)) / NULLIF(SUM(CASE WHEN ${alias}"usageBearing" = 1 THEN 1 ELSE 0 END), 0) AS "costCoverage"`,
      `CASE WHEN ${mixedUnitsSql(alias)} THEN ${mixedUnit} ELSE MIN(${alias}"unitMin" COLLATE BINARY) END AS "costUnit"`,
    ];
  };

  if (plan.interval === undefined) {
    const projected = [
      ...dimensionColumns,
      ...plan.measures.map((measure, index) => `${measureSql(measure.name)} AS "m${index}"`),
      ...costColumns(''),
    ];
    return {
      text: `WITH ${ctes.join(',\n')}
SELECT ${projected.join(', ')}
FROM facts
${groupSql}
HAVING ${havingSql}
ORDER BY ${orderSql}
FETCH FIRST ${limitParameter} ROWS ONLY`,
      values,
    };
  }

  const maxRankParameter = param(plan.limit);
  ctes.push(`ranked AS (
    SELECT ${[...dimensionColumns, `row_number() OVER (ORDER BY ${orderSql}) AS "rank"`].join(', ')}
    FROM facts
    ${groupSql}
    HAVING ${havingSql}
    ORDER BY ${orderSql}
    FETCH FIRST ${limitParameter} ROWS ONLY
  )`);
  const joinSql =
    dimensionColumns.length > 0
      ? dimensionColumns
          .map(column => `(f.${column} = g.${column} OR (f.${column} IS NULL AND g.${column} IS NULL))`)
          .join(' AND ')
      : '1 = 1';
  const projected = [
    ...dimensionColumns.map(column => `f.${column}`),
    'f."bucket"',
    ...plan.measures.map((measure, index) => `${measureSql(measure.name, 'f.')} AS "m${index}"`),
    ...costColumns('f.'),
    `CASE WHEN (SELECT COUNT(*) FROM ranked) > ${maxRankParameter} THEN 1 ELSE 0 END AS "truncated"`,
  ];
  return {
    text: `WITH ${ctes.join(',\n')}
SELECT ${projected.join(', ')}
FROM facts f
JOIN ranked g ON ${joinSql}
WHERE g."rank" <= ${maxRankParameter}
GROUP BY ${['g."rank"', ...dimensionColumns.map(column => `f.${column}`), 'f."bucket"'].join(', ')}
ORDER BY g."rank" ASC, f."bucket" ASC`,
    values,
  };
}

const USAGE_METRIC_NAMES: readonly string[] = coreStorage.TRACE_AGGREGATE_USAGE_METRIC_NAMES;

function measureRule(name: string): coreStorage.TraceAggregateMeasureRule | undefined {
  return coreStorage.isTraceAggregateCanonicalMeasure(name)
    ? coreStorage.TRACE_AGGREGATE_MEASURE_REGISTRY[name]
    : undefined;
}

function usageMetricIndex(name: string): number {
  const index = USAGE_METRIC_NAMES.indexOf(name);
  if (index === -1) throw new Error(`Unsupported trusted trace-aggregate token metric: ${name}`);
  return index;
}

/**
 * Builds `usage`: one row per candidate trace that has at least one token metric row, holding
 * per-name token sums (`t<i>`, indexed by `TRACE_AGGREGATE_USAGE_METRIC_NAMES`) and the trace's
 * pricing state. Metric rows are matched by traceId, usage metric name, tenant scope, and
 * `timestamp >= from` only: a trace that starts in the window can emit metrics after `to`, and
 * the candidates semi-join already bounds the scan.
 *
 * Rows are deduplicated on `metricId` alone. The identity index includes `timestamp`,
 * so copies with different timestamps can both be stored.
 */
function compileUsageCtes(schema: string, plan: TrustedTraceAggregatePlan, param: (value: unknown) => string) {
  const scope = plan.scope
    ? [
        `m."organizationId" = ${param(plan.scope.organizationId)}`,
        ...(plan.scope.resourceId === undefined ? [] : [`m."resourceId" = ${param(plan.scope.resourceId)}`]),
      ]
    : [];
  const list = (names: readonly string[]) =>
    `(SELECT value FROM JSON_TABLE(${param(clobBind(JSON.stringify(names)))}, '$[*]' COLUMNS(value VARCHAR2(128) PATH '$')))`;
  const conditions = [
    `m."traceId" IN (SELECT "traceId" FROM candidates)`,
    `m."name" IN ${list(USAGE_METRIC_NAMES)}`,
    `m."timestamp" >= ${param(new Date(plan.timeRange.from))}`,
    ...scope,
  ];
  const costRow = `"name" IN ${list(coreStorage.TRACE_AGGREGATE_COST_METRIC_NAMES)}`;
  const priced = `${costRow} AND "estimatedCost" IS NOT NULL AND "costUnit" IS NOT NULL AND "hasError" = 0`;
  const failed = `${costRow} AND ("hasError" = 1 OR ("estimatedCost" IS NOT NULL AND "costUnit" IS NULL))`;
  const tokens = USAGE_METRIC_NAMES.map(
    (name, index) => `SUM(CASE WHEN "name" = ${param(name)} THEN "value" END) AS "t${index}"`,
  );
  return [
    `usage_rows AS (
    SELECT * FROM (SELECT m."traceId", m."name", m."value", m."estimatedCost", m."costUnit",
      CASE WHEN JSON_EXISTS(m."costMetadata", '$.error?(@.type() != "null")') THEN 1 ELSE 0 END AS "hasError",
      ROW_NUMBER() OVER (PARTITION BY m."metricId" ORDER BY m."timestamp", m."cursorId") AS "metricRank"
    FROM ${qualifiedTable(schema, TABLE_METRIC_EVENTS)} m WHERE ${conditions.join(' AND ')}) WHERE "metricRank" = 1
  )`,
    `usage AS (
    SELECT "traceId", ${tokens.join(', ')}, SUM(CASE WHEN ${priced} THEN "estimatedCost" END) AS "cost",
      MAX(CASE WHEN ${priced} THEN 1 ELSE 0 END) AS "priced", MAX(CASE WHEN ${failed} THEN 1 ELSE 0 END) AS "pricingFailure",
      MIN(CASE WHEN ${priced} THEN "costUnit" COLLATE BINARY END) AS "unitMin", MAX(CASE WHEN ${priced} THEN "costUnit" COLLATE BINARY END) AS "unitMax"
    FROM usage_rows GROUP BY "traceId"
  )`,
  ];
}

function nullableNumber(value: unknown): number | null {
  return value === null || value === undefined ? null : Number(value);
}

function toIsoTimestamp(value: unknown): string {
  return value instanceof Date ? value.toISOString() : new Date(value as string).toISOString();
}

export async function aggregateTraces(
  client: DbClient,
  schema: string,
  plan: TrustedTraceAggregatePlan,
  timeoutMs: number,
): Promise<TraceAggregateResponse> {
  const compiled = compileOracleTraceAggregate(schema, plan);
  const result = await runWithOracleTraceQueryTimeout(client, timeoutMs, transaction =>
    transaction.any<Record<string, unknown>>(compiled.text, compiled.values),
  );

  let truncated: boolean;
  let resultRows: Record<string, unknown>[];
  if (plan.interval === undefined) {
    truncated = result.length > plan.limit;
    resultRows = result.slice(0, plan.limit);
  } else {
    truncated = Number(result[0]?.truncated) === 1;
    resultRows = result;
  }

  const rows = resultRows.map(row => {
    // Measures holds exactly the requested names, which the core record type cannot express.
    const shaped: TraceAggregateRow = {
      measures: Object.fromEntries(
        plan.measures.map((measure, index) => [measure.name, nullableNumber(row[`m${index}`])]),
      ) as TraceAggregateRow['measures'],
    };
    if ('costCoverage' in row) {
      shaped.cost = { coverage: nullableNumber(row.costCoverage), unit: (row.costUnit as string | null) ?? null };
    }
    if (plan.dimensions.length > 0) {
      shaped.dimensions = Object.fromEntries(
        plan.dimensions.map((dimension, index) => [dimension, (row[`d${index}`] as string | null) ?? null]),
      );
    }
    if (plan.interval !== undefined) shaped.bucket = toIsoTimestamp(row.bucket);
    return shaped;
  });
  return { rows, truncated };
}
