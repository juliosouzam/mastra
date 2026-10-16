import { createHash } from 'node:crypto';
/**
 * Discovery operations for the v-next Oracle observability domain.
 *
 * Backed by a single cache table (`mastra_observability_discovery`) keyed by a
 * cacheKey string. Reads use stale-while-revalidate semantics:
 *
 *   - if no cache row exists, compute synchronously and serve.
 *   - if cache row exists and is fresher than `discoveryTtlSeconds`, serve as-is.
 *   - if cache row is stale, kick off an async refresh and serve the cached
 *     value. The refresh upserts on a single row keyed by cacheKey, so
 *     concurrent readers race harmlessly with last-write-wins semantics.
 *
 * No in-memory caching: the table-backed cache works across multiple
 * frontends pointing at the same database and survives serverless restarts.
 */

import type { IMastraLogger } from '@mastra/core/logger';
import type {
  EntityType,
  GetEntityNamesArgs,
  GetEntityNamesResponse,
  GetEntityTypesArgs,
  GetEntityTypesResponse,
  GetEnvironmentsArgs,
  GetEnvironmentsResponse,
  GetMetricLabelKeysArgs,
  GetMetricLabelKeysResponse,
  GetMetricLabelValuesArgs,
  GetMetricLabelValuesResponse,
  GetMetricNamesArgs,
  GetMetricNamesResponse,
  GetServiceNamesArgs,
  GetServiceNamesResponse,
  GetTagsArgs,
  GetTagsResponse,
} from '@mastra/core/storage';

import { parseSqlIdentifier } from '@mastra/core/utils';
import { clobBind, isOracleErrorCode } from '../../../../shared/connection';

import type { DbClient } from './client';
import {
  qualifiedTable,
  TABLE_DISCOVERY,
  TABLE_LOG_EVENTS,
  TABLE_METRIC_EVENTS,
  TABLE_SCORE_EVENTS,
  TABLE_FEEDBACK_EVENTS,
  TABLE_SPAN_EVENTS,
  SIGNAL_TIME_COLUMN,
} from './ddl';

const DEFAULT_TTL_SECONDS = 5 * 60; // 5 minutes
const DEFAULT_LOOKBACK_SECONDS = 30 * 24 * 60 * 60; // 30 days

/** All signal tables that contain a column. Used by cross-signal discovery. */
const SIGNAL_TABLES_WITH_CONTEXT = [
  TABLE_SPAN_EVENTS,
  TABLE_METRIC_EVENTS,
  TABLE_LOG_EVENTS,
  TABLE_SCORE_EVENTS,
  TABLE_FEEDBACK_EVENTS,
] as const;

/** Tables for entity-type / entity-name / tag discovery (excludes scores/feedback). */
const ENTITY_DISCOVERY_TABLES = [TABLE_SPAN_EVENTS, TABLE_METRIC_EVENTS, TABLE_LOG_EVENTS] as const;

export interface DiscoveryConfig {
  /** TTL for cached values in seconds. Default 300 (5 minutes). */
  ttlSeconds?: number;
  /**
   * Only scan events newer than this many seconds when refreshing a discovery
   * value. Default 2592000 (30 days).
   *
   * The predicate uses each signal table's indexed event-time column to
   * bound the refresh scan. Oracle signal tables are not partitioned.
   *
   * Set to 0 to scan all history (the behavior before this option existed).
   * Note that a value last seen outside the window disappears from discovery
   * results until it is seen again.
   */
  lookbackSeconds?: number;
  /**
   * Logger used to report background refresh failures. Injected by the
   * domain class (`ObservabilityStorageOracleVNext`) so discovery
   * warnings land in the framework logger alongside the rest of the store.
   * Falls back to `console.warn` when absent so direct callers (tests,
   * scripts) still see refresh failures.
   * @internal
   */
  logger?: IMastraLogger;
}

/**
 * In-process dedupe for concurrent refreshes against the same cache key.
 *
 * Covers two stampede shapes:
 *  - cold start: N concurrent first-callers all want the values, but only
 *    one of them needs to actually run `refresh()` + `upsertCache()`. The
 *    others await the same promise.
 *  - stale refresh: N concurrent stale-readers serve the cached values
 *    immediately and share one background refresh.
 *
 * The map keys are `"schema:cacheKey"` so multiple schemas using the same
 * key don't share an entry. Entries clear themselves via `.finally` after
 * the refresh settles.
 */
const refreshesByClient = new WeakMap<DbClient, Map<string, Promise<string[]>>>();

function startOrJoinRefresh(
  inFlightRefreshes: Map<string, Promise<string[]>>,
  dedupeKey: string,
  cacheKey: string,
  refresh: () => Promise<string[]>,
  upsert: (values: string[]) => Promise<void>,
  logger: IMastraLogger | undefined,
): Promise<string[]> {
  const existing = inFlightRefreshes.get(dedupeKey);
  if (existing) return existing;

  const promise = (async () => {
    try {
      const values = await refresh();
      await upsert(values);
      return values;
    } catch (error) {
      // Surface refresh failures — silently swallowing them would mask real
      // DB/connectivity issues behind permanently stale data. Prefer the
      // framework logger so warnings land in the same stream as the rest
      // of the store; fall back to console.warn for direct callers (tests,
      // scripts) that don't inject one.
      const message = `[observability/v-next] background refresh failed for discovery cache key "${cacheKey}"`;
      if (logger) {
        logger.warn(message, { error });
      } else {
        console.warn(message + ':', error);
      }
      throw error;
    } finally {
      inFlightRefreshes.delete(dedupeKey);
    }
  })();

  inFlightRefreshes.set(dedupeKey, promise);
  return promise;
}

/**
 * Read the cache row for `cacheKey` and decide whether to refresh.
 *
 * Stale-while-revalidate semantics:
 *  - Fresh cache hit: return the stored values immediately.
 *  - Stale cache hit: return the cached values immediately AND kick off a
 *    background refresh (deduped) so the next reader sees fresh data.
 *    Concurrent stale readers don't await the shared refresh — that's the
 *    "while-revalidate" half of SWR.
 *  - Cold miss (no row at all): block on the refresh and return its values.
 *    Concurrent cold callers share one refresh promise via the dedupe map.
 */
async function readWithRefresh(
  client: DbClient,
  schema: string,
  cacheKey: string,
  refresh: () => Promise<string[]>,
  ttlSeconds: number,
  logger: IMastraLogger | undefined,
): Promise<string[]> {
  cacheKey = createHash('sha256').update(cacheKey).digest('hex');
  const table = qualifiedTable(schema, TABLE_DISCOVERY);
  // The driver returns `timestamptz` as a JS Date — type the field accordingly.
  const stored = await client.oneOrNone<{ values: string; refreshedAt: Date }>(
    `SELECT JSON_SERIALIZE("values" RETURNING CLOB) AS "values", "refreshedAt" FROM ${table} WHERE "cacheKey" = :p1`,
    [cacheKey],
  );

  const row = stored ? { ...stored, values: JSON.parse(stored.values) as string[] } : null;
  const dedupeKey = `${schema}:${cacheKey}`;
  let inFlightRefreshes = refreshesByClient.get(client);
  if (!inFlightRefreshes) {
    inFlightRefreshes = new Map();
    refreshesByClient.set(client, inFlightRefreshes);
  }
  const startRefresh = () =>
    startOrJoinRefresh(
      inFlightRefreshes!,
      dedupeKey,
      cacheKey,
      refresh,
      values => upsertCache(client, schema, cacheKey, values),
      logger,
    );

  // Force-refresh path: `ttlSeconds <= 0` is the contract used by
  // `refreshAllDiscoveryCaches()` (and the future `mastra observability
  // discovery refresh` CLI) to mean "block until the cache is rewritten".
  // Check this before cache freshness because consecutive forced refreshes can
  // occur in the same millisecond.
  if (ttlSeconds <= 0) {
    try {
      return await startRefresh();
    } catch {
      // Already logged inside startOrJoinRefresh. Fall back to whatever we
      // had cached so the caller still gets a defined value.
      return row?.values ?? [];
    }
  }

  const refreshedAtMs = row ? new Date(row.refreshedAt).getTime() : 0;
  const stale = !row || Date.now() - refreshedAtMs > ttlSeconds * 1000;

  if (!stale) return row!.values;

  if (!row) {
    // Cold path: no cached values to serve. Block on (or join) the refresh.
    try {
      return await startRefresh();
    } catch {
      // Already logged inside startOrJoinRefresh; return empty so the caller
      // gets a defined value instead of throwing.
      return [];
    }
  }

  // Stale path: serve the cached values immediately and revalidate in the
  // background — but only if this process wins the claim. `inFlightRefreshes`
  // dedupes within one process; the claim extends that to every process
  // pointing at the same database, so N frontends going stale together run
  // one refresh instead of N.
  const previousRefreshedAt = row.refreshedAt;
  void claimRefresh(client, schema, cacheKey, ttlSeconds)
    .then(claimedAt => {
      // Lost the claim: another process is already refreshing this key.
      if (!claimedAt) return;
      return startRefresh().then(
        // Success rewrites `refreshedAt` via upsertCache.
        () => {},
        // Failure (already logged by startOrJoinRefresh) hands the claim
        // back by restoring the timestamp we found, so the next reader
        // retries instead of waiting out a TTL on a stamp that never
        // corresponded to a completed refresh.
        () => releaseClaim(client, schema, cacheKey, previousRefreshedAt, claimedAt),
      );
    })
    .catch(() => {});
  return row.values;
}

/**
 * Try to become the process that refreshes `cacheKey`.
 *
 * The conditional UPDATE re-checks staleness in the database and stamps
 * `refreshedAt` in the same statement, so exactly one caller can transition a
 * given cache row out of the stale window — losers see zero rows updated and
 * skip their refresh. This is the cross-process counterpart to
 * `inFlightRefreshes`: N frontends going stale together run one refresh
 * instead of N.
 *
 * It reuses the cache table rather than an advisory lock because `DbClient`
 * is pool-backed, so a transaction-scoped lock would mean threading explicit
 * transaction handling through the discovery read path.
 *
 * The stamp is provisional in both directions: a completed refresh overwrites
 * it via `upsertCache`, and a failed one restores it via `releaseClaim`.
 *
 * Returns the exact provisional stamp written by the claim (or `null` when
 * the claim was lost) so `releaseClaim` can restore the row only while it
 * still holds this claim's stamp.
 */
async function claimRefresh(
  client: DbClient,
  schema: string,
  cacheKey: string,
  ttlSeconds: number,
): Promise<string | null> {
  const table = qualifiedTable(schema, TABLE_DISCOVERY);
  return client.tx(async tx => {
    const updated = await tx.query(
      `UPDATE ${table} SET "refreshedAt" = SYSTIMESTAMP WHERE "cacheKey" = :p1 AND "refreshedAt" <= SYSTIMESTAMP - NUMTODSINTERVAL(:p2, 'SECOND')`,
      [cacheKey, Math.floor(ttlSeconds)],
    );
    if (!updated.rowCount) return null;
    const row = await tx.one<{ stamp: string }>(
      `SELECT TO_CHAR("refreshedAt", 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM') AS "stamp" FROM ${table} WHERE "cacheKey" = :p1`,
      [cacheKey],
    );
    return row.stamp;
  });
}

/**
 * Hand back a claim whose refresh failed by restoring the timestamp the
 * claiming reader observed. Guarded on the row still holding this claim's
 * exact provisional stamp so a refresh that completed in the meantime
 * (another process, or a later claim) isn't dragged backwards into staleness.
 */
async function releaseClaim(
  client: DbClient,
  schema: string,
  cacheKey: string,
  previousRefreshedAt: Date,
  claimedAt: string,
): Promise<void> {
  const table = qualifiedTable(schema, TABLE_DISCOVERY);
  try {
    await client.query(
      `UPDATE ${table} SET "refreshedAt" = :p2
        WHERE "cacheKey" = :p1 AND "refreshedAt" = TO_TIMESTAMP_TZ(:p3, 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')`,
      [cacheKey, previousRefreshedAt, claimedAt],
    );
  } catch {
    // Best-effort: the claim expires on its own once the TTL elapses.
  }
}

async function upsertCache(client: DbClient, schema: string, cacheKey: string, values: string[]): Promise<void> {
  const table = qualifiedTable(schema, TABLE_DISCOVERY);
  await client.none(
    `BEGIN INSERT INTO ${table} ("cacheKey", "refreshedAt", "values") VALUES (:p1, SYSTIMESTAMP, JSON(:p2));
    EXCEPTION WHEN DUP_VAL_ON_INDEX THEN UPDATE ${table} SET "refreshedAt" = SYSTIMESTAMP, "values" = JSON(:p2) WHERE "cacheKey" = :p1; END;`,
    [cacheKey, clobBind(JSON.stringify(values))],
  );
}

// ---------------------------------------------------------------------------
// Per-discovery refresh queries
// ---------------------------------------------------------------------------

/**
 * Time predicate that bounds a discovery refresh to recent events.
 *
 * Each signal uses its own event-time column (`endedAt` for spans,
 * `timestamp` for the other signals). Build the bound per UNION branch
 * so each table can use its time index.
 *
 * The interval is interpolated rather than bound to a parameter on purpose:
 * the UNION branches in `distinctAcrossTables` and `getTags` all reference the
 * same positional placeholders, so adding a per-branch parameter would break
 * `:pN` numbering. Safety comes from coercing to a finite integer here — the
 * value originates from `DiscoveryConfig`, never from user input.
 */
function lookbackPredicate(table: string, lookbackSeconds: number): string {
  if (!Number.isFinite(lookbackSeconds) || lookbackSeconds <= 0) return '';
  const seconds = Math.floor(lookbackSeconds);
  const timeColumn = SIGNAL_TIME_COLUMN[table as keyof typeof SIGNAL_TIME_COLUMN];
  if (!timeColumn) return '';
  return `AND "${timeColumn}" >= SYSTIMESTAMP - NUMTODSINTERVAL(${seconds}, 'SECOND')`;
}

async function distinctAcrossTables(
  client: DbClient,
  schema: string,
  column: string,
  tables: readonly string[],
  lookbackSeconds: number,
  filterSql: string = '',
  filterParams: unknown[] = [],
): Promise<string[]> {
  // Defense-in-depth: every current caller passes a hardcoded column name,
  // but validating the identifier here makes the helper injection-safe by
  // construction in case a future caller takes it from user input.
  const safeColumn = parseSqlIdentifier(column, 'column name');
  // Each subquery references the same $N placeholders; pg parameters are
  // positional, so we pass `filterParams` exactly once.
  const unions = tables
    .map(
      t =>
        `SELECT DISTINCT "${safeColumn}" AS v FROM ${qualifiedTable(schema, t)} WHERE "${safeColumn}" IS NOT NULL ${filterSql} ${lookbackPredicate(t, lookbackSeconds)}`,
    )
    .join(' UNION ');
  const rows = await client.manyOrNone<{ v: string }>(`SELECT v FROM (${unions}) sub ORDER BY v`, filterParams);
  return rows.map(r => r.v);
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export async function getEntityTypes(
  client: DbClient,
  schema: string,
  _args: GetEntityTypesArgs,
  config: DiscoveryConfig,
): Promise<GetEntityTypesResponse> {
  const ttl = config.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  const lookback = config.lookbackSeconds ?? DEFAULT_LOOKBACK_SECONDS;
  const values = await readWithRefresh(
    client,
    schema,
    'entity_types',
    () => distinctAcrossTables(client, schema, 'entityType', ENTITY_DISCOVERY_TABLES, lookback),
    ttl,
    config.logger,
  );
  return { entityTypes: values as EntityType[] };
}

export async function getEntityNames(
  client: DbClient,
  schema: string,
  args: GetEntityNamesArgs,
  config: DiscoveryConfig,
): Promise<GetEntityNamesResponse> {
  const ttl = config.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  const lookback = config.lookbackSeconds ?? DEFAULT_LOOKBACK_SECONDS;
  const cacheKey = JSON.stringify(['entity_names', args.entityType ?? null]);
  const filterSql = args.entityType ? `AND "entityType" = :p1` : '';
  const filterParams = args.entityType ? [args.entityType] : [];
  const values = await readWithRefresh(
    client,
    schema,
    cacheKey,
    () =>
      distinctAcrossTables(client, schema, 'entityName', ENTITY_DISCOVERY_TABLES, lookback, filterSql, filterParams),
    ttl,
    config.logger,
  );
  return { names: values };
}

export async function getServiceNames(
  client: DbClient,
  schema: string,
  _args: GetServiceNamesArgs,
  config: DiscoveryConfig,
): Promise<GetServiceNamesResponse> {
  const ttl = config.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  const lookback = config.lookbackSeconds ?? DEFAULT_LOOKBACK_SECONDS;
  const values = await readWithRefresh(
    client,
    schema,
    'service_names',
    () => distinctAcrossTables(client, schema, 'serviceName', SIGNAL_TABLES_WITH_CONTEXT, lookback),
    ttl,
    config.logger,
  );
  return { serviceNames: values };
}

export async function getEnvironments(
  client: DbClient,
  schema: string,
  _args: GetEnvironmentsArgs,
  config: DiscoveryConfig,
): Promise<GetEnvironmentsResponse> {
  const ttl = config.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  const lookback = config.lookbackSeconds ?? DEFAULT_LOOKBACK_SECONDS;
  const values = await readWithRefresh(
    client,
    schema,
    'environments',
    () => distinctAcrossTables(client, schema, 'environment', SIGNAL_TABLES_WITH_CONTEXT, lookback),
    ttl,
    config.logger,
  );
  return { environments: values };
}

export async function getTags(
  client: DbClient,
  schema: string,
  args: GetTagsArgs,
  config: DiscoveryConfig,
): Promise<GetTagsResponse> {
  const ttl = config.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  const lookback = config.lookbackSeconds ?? DEFAULT_LOOKBACK_SECONDS;
  const cacheKey = JSON.stringify(['tags', args.entityType ?? null]);

  const refresh = async (): Promise<string[]> => {
    const filter = args.entityType ? `AND "entityType" = :p1` : '';
    const params = args.entityType ? [args.entityType] : [];
    const unions = ENTITY_DISCOVERY_TABLES.map(
      t =>
        `SELECT DISTINCT tags.v FROM ${qualifiedTable(schema, t)}, JSON_TABLE("tags", '$[*]' COLUMNS(v VARCHAR2(4000) PATH '$')) tags WHERE tags.v IS NOT NULL ${filter} ${lookbackPredicate(t, lookback)}`,
    ).join(' UNION ');
    const rows = await client.manyOrNone<{ v: string }>(
      `SELECT v FROM (${unions}) sub WHERE v IS NOT NULL ORDER BY v`,
      params,
    );
    return rows.map(r => r.v);
  };

  const values = await readWithRefresh(client, schema, cacheKey, refresh, ttl, config.logger);
  return { tags: values };
}

export async function getMetricNames(
  client: DbClient,
  schema: string,
  args: GetMetricNamesArgs,
  config: DiscoveryConfig,
): Promise<GetMetricNamesResponse> {
  const ttl = config.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  const lookback = config.lookbackSeconds ?? DEFAULT_LOOKBACK_SECONDS;
  const values = await readWithRefresh(
    client,
    schema,
    'metric_names',
    async () => {
      const rows = await client.manyOrNone<{ v: string }>(
        `SELECT DISTINCT "name" AS v FROM ${qualifiedTable(schema, TABLE_METRIC_EVENTS)}
         WHERE "name" IS NOT NULL ${lookbackPredicate(TABLE_METRIC_EVENTS, lookback)} ORDER BY "name"`,
      );
      return rows.map(r => r.v);
    },
    ttl,
    config.logger,
  );
  let filtered = values;
  if (args.prefix) filtered = filtered.filter(v => v.startsWith(args.prefix!));
  if (args.limit) filtered = filtered.slice(0, args.limit);
  return { names: filtered };
}

export async function getMetricLabelKeys(
  client: DbClient,
  schema: string,
  args: GetMetricLabelKeysArgs,
  config: DiscoveryConfig,
): Promise<GetMetricLabelKeysResponse> {
  const ttl = config.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  const lookback = config.lookbackSeconds ?? DEFAULT_LOOKBACK_SECONDS;
  const cacheKey = JSON.stringify(['metric_label_keys', args.metricName]);
  const values = await readWithRefresh(
    client,
    schema,
    cacheKey,
    async () => {
      const row = await client.one<{ guide: string | null }>(
        `SELECT JSON_DATAGUIDE("labels") AS "guide" FROM ${qualifiedTable(schema, TABLE_METRIC_EVENTS)} WHERE "name" = :p1 ${lookbackPredicate(TABLE_METRIC_EVENTS, lookback)}`,
        [args.metricName],
      );
      const guide = JSON.parse(row.guide ?? '[]') as Array<{ 'o:path': string; type: string }>;
      return [
        ...new Set(
          guide
            .filter(entry => entry.type === 'string' && entry['o:path'].startsWith('$.'))
            .map(entry => {
              const key = entry['o:path'].slice(2);
              return key.startsWith('"') ? (JSON.parse(key) as string) : key;
            }),
        ),
      ].sort();
    },
    ttl,
    config.logger,
  );
  return { keys: values };
}

export async function getMetricLabelValues(
  client: DbClient,
  schema: string,
  args: GetMetricLabelValuesArgs,
  config: DiscoveryConfig,
): Promise<GetMetricLabelValuesResponse> {
  const ttl = config.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  const lookback = config.lookbackSeconds ?? DEFAULT_LOOKBACK_SECONDS;
  const cacheKey = JSON.stringify(['metric_label_values', args.metricName, args.labelKey]);
  const values = await readWithRefresh(
    client,
    schema,
    cacheKey,
    async () => {
      const path = ('$.' + JSON.stringify(args.labelKey)).replaceAll("'", "''");
      const from = `FROM ${qualifiedTable(schema, TABLE_METRIC_EVENTS)} WHERE "name" = :p1 AND JSON_EXISTS("labels", '${path}') ${lookbackPredicate(TABLE_METRIC_EVENTS, lookback)}`;
      try {
        const rows = await client.manyOrNone<{ v: string }>(
          `SELECT DISTINCT JSON_VALUE("labels", '${path}' RETURNING VARCHAR2(4000) ERROR ON ERROR) AS v ${from} ORDER BY v`,
          [args.metricName],
        );
        return rows.map(row => row.v).filter(value => value != null && value !== '');
      } catch (error) {
        if (!isOracleErrorCode(error, [-40478, -40459])) throw error;
        const rows = await client.manyOrNone<{ v: string }>(
          `SELECT JSON_SERIALIZE(JSON_QUERY("labels", '${path}' RETURNING JSON) RETURNING CLOB) AS "v" ${from}`,
          [args.metricName],
        );
        return [
          ...new Set(
            rows.map(row => JSON.parse(row.v) as string).filter(value => typeof value === 'string' && value !== ''),
          ),
        ].sort();
      }
    },
    ttl,
    config.logger,
  );
  let filtered = values;
  if (args.prefix) filtered = filtered.filter(v => v.startsWith(args.prefix!));
  if (args.limit) filtered = filtered.slice(0, args.limit);
  return { values: filtered };
}

/**
 * Force-refresh the six unscoped discovery cache keys: `entity_types`,
 * `entity_names`, `service_names`, `environments`, `tags`, and
 * `metric_names`. Scoped keys (`entity_names:<entityType>`,
 * `tags:<entityType>`, `metric_label_keys:<metric>`,
 * `metric_label_values:<metric>:<key>`) are not touched here — they refresh
 * lazily on read against their own keys.
 *
 * Intended for the future `mastra observability discovery refresh` CLI
 * command.
 */
export async function refreshAllDiscoveryCaches(
  client: DbClient,
  schema: string,
  config: DiscoveryConfig,
): Promise<void> {
  await Promise.all([
    getEntityTypes(client, schema, {}, { ...config, ttlSeconds: 0 }),
    getEntityNames(client, schema, {}, { ...config, ttlSeconds: 0 }),
    getServiceNames(client, schema, {}, { ...config, ttlSeconds: 0 }),
    getEnvironments(client, schema, {}, { ...config, ttlSeconds: 0 }),
    getTags(client, schema, {}, { ...config, ttlSeconds: 0 }),
    getMetricNames(client, schema, {}, { ...config, ttlSeconds: 0 }),
  ]);
}
