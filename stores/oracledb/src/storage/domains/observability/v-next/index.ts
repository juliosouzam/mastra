import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import type {
  GetEntityTypesArgs,
  GetEntityTypesResponse,
  GetEntityNamesArgs,
  GetEntityNamesResponse,
  GetServiceNamesArgs,
  GetServiceNamesResponse,
  GetEnvironmentsArgs,
  GetEnvironmentsResponse,
  GetTagsArgs,
  GetTagsResponse,
  GetMetricNamesArgs,
  GetMetricNamesResponse,
  GetMetricLabelKeysArgs,
  GetMetricLabelKeysResponse,
  GetMetricLabelValuesArgs,
  GetMetricLabelValuesResponse,
  CreateSpanArgs,
  BatchCreateSpansArgs,
  GetSpanArgs,
  GetSpanResponse,
  GetSpansArgs,
  GetSpansResponse,
  GetRootSpanArgs,
  GetRootSpanResponse,
  GetTraceArgs,
  GetTraceResponse,
  GetTraceLightResponse,
  BatchDeleteTracesArgs,
  CreateScoreArgs,
  BatchCreateScoresArgs,
  DeleteScoresArgs,
  ListScoresArgs,
  ListScoresResponse,
  CreateFeedbackArgs,
  BatchCreateFeedbackArgs,
  DeleteFeedbackArgs,
  ListFeedbackArgs,
  ListFeedbackResponse,
  UpdateFeedbackReviewStatusArgs,
  FeedbackRecord,
  GetScoreAggregateArgs,
  GetScoreAggregateResponse,
  GetScoreBreakdownArgs,
  GetScoreBreakdownResponse,
  GetScoreTimeSeriesArgs,
  GetScoreTimeSeriesResponse,
  GetScorePercentilesArgs,
  GetScorePercentilesResponse,
  GetFeedbackAggregateArgs,
  GetFeedbackAggregateResponse,
  GetFeedbackBreakdownArgs,
  GetFeedbackBreakdownResponse,
  GetFeedbackTimeSeriesArgs,
  GetFeedbackTimeSeriesResponse,
  GetFeedbackPercentilesArgs,
  GetFeedbackPercentilesResponse,
  ScoreRecord,
  ListTracesArgs,
  ListTracesResponse,
  ListTracesLightResponse,
  ListBranchesArgs,
  ListBranchesResponse,
  TrustedTraceQueryPlan,
  TraceQueryResponse,
  TrustedThreadQueryPlan,
  QueryThreadsResult,
  TrustedTraceQueryObservedFieldsPlan,
  TraceQueryObservedFieldsResult,
  TrustedTraceQueryValuesPlan,
  GetTraceQueryValuesResponse,
  TrustedTraceAggregatePlan,
  TraceAggregateResponse,
  TrustedSpanQueryPlan,
  SpanQueryResponse,
  BatchCreateLogsArgs,
  ListLogsArgs,
  ListLogsResponse,
  ObservabilityStorageStrategy,
  PruneOptions,
  PruneResult,
  RetentionTablesDescriptor,
  TableRetentionPolicy,
  BatchCreateMetricsArgs,
  ListMetricsArgs,
  ListMetricsResponse,
  GetMetricAggregateArgs,
  GetMetricAggregateResponse,
  GetMetricBreakdownArgs,
  GetMetricBreakdownResponse,
  GetMetricTimeSeriesArgs,
  GetMetricTimeSeriesResponse,
  GetMetricPercentilesArgs,
  GetMetricPercentilesResponse,
} from '@mastra/core/storage';
import * as coreStorage from '@mastra/core/storage';
import {
  ObservabilityStorage,
  executeRetentionPrune,
  resolveRetentionTargets,
  retentionCutoffMs,
} from '@mastra/core/storage';
import { isOracleErrorCode } from '../../../../shared/connection';
import { normalizeIdentifier } from '../../../../vector/identifiers';

import { OracleDB, generateOracleIndexSQL } from '../../../db';
import type { OracleDomainConfig } from '../../../types';
import { OracleObservabilityClient } from './client';
import {
  allTableDDL,
  allIndexDDL,
  customIndexes,
  SIGNAL_COLUMNS,
  CURSOR_SEQUENCE,
  ALL_TABLE_NAMES,
  rawTable,
  TABLE_SPAN_EVENTS,
  TABLE_METRIC_EVENTS,
  TABLE_LOG_EVENTS,
  TABLE_SCORE_EVENTS,
  TABLE_FEEDBACK_EVENTS,
} from './ddl';
import type { DiscoveryConfig } from './discovery';
import * as discoveryOps from './discovery';
import * as feedbackOps from './feedback';
import * as logsOps from './logs';
import * as metricsOps from './metrics';
import { deltaPollingFeatureEnabled } from './polling';
import * as scoresOps from './scores';
import { buildColumnDefinitions } from './signal-schema';
import * as spanQueryOps from './span-query';
import * as traceAggregateOps from './trace-aggregate';
import * as traceQueryOps from './trace-query';
import * as tracesOps from './traces';
import * as tracingOps from './tracing';

export type VNextOracleObservabilityConfig = OracleDomainConfig & {
  traceQueryTimeoutMs?: number;
  discovery?: DiscoveryConfig;
};

export class ObservabilityStorageOracleVNext extends ObservabilityStorage {
  static readonly MANAGED_TABLES = ALL_TABLE_NAMES;
  static getExportDDL(
    options: Pick<VNextOracleObservabilityConfig, 'schemaName' | 'skipDefaultIndexes' | 'indexes'> = {},
  ): string[] {
    const schema = options.schemaName ? normalizeIdentifier(options.schemaName, 'schema name') : '';
    return [
      ...allTableDDL(schema),
      ...allIndexDDL(schema, options.skipDefaultIndexes),
      ...customIndexes(options.indexes).map(index => generateOracleIndexSQL(index, schema || undefined)),
    ];
  }
  static override readonly retentionTables: RetentionTablesDescriptor = {
    spans: { table: TABLE_SPAN_EVENTS, column: 'endedAt', indexed: true },
    metrics: { table: TABLE_METRIC_EVENTS, column: 'timestamp', indexed: true },
    logs: { table: TABLE_LOG_EVENTS, column: 'timestamp', indexed: true },
    scores: { table: TABLE_SCORE_EVENTS, column: 'timestamp', indexed: true },
    feedback: { table: TABLE_FEEDBACK_EVENTS, column: 'timestamp', indexed: true },
  };
  private readonly db: OracleDB;
  private readonly client: OracleObservabilityClient;
  private readonly schema: string;
  private readonly traceQueryTimeoutMs: number;
  private initializing?: Promise<void>;
  private readonly retentionIndexes = new Set<string>();
  constructor(private readonly config: VNextOracleObservabilityConfig) {
    super();
    this.schema = config.schemaName ? normalizeIdentifier(config.schemaName, 'schema name') : '';
    this.config = { ...config, schemaName: this.schema || undefined };
    this.db = new OracleDB(this.config);
    this.client = new OracleObservabilityClient(this.config);
    this.traceQueryTimeoutMs = coreStorage.resolveTraceQueryTimeoutMs(config.traceQueryTimeoutMs);
  }
  async init(): Promise<void> {
    if (this.initializing) return this.initializing;
    this.initializing = this.run('INIT', () => this.initialize());
    try {
      await this.initializing;
    } finally {
      this.initializing = undefined;
    }
  }
  private async initialize(): Promise<void> {
    try {
      await this.client.one('SELECT TO_CHAR(DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER) AS "scn" FROM dual');
    } catch (error) {
      if (!isOracleErrorCode(error, [-904, -1031])) throw error;
      throw new MastraError(
        {
          id: 'ORACLEDB_VNEXT_SCN_PRIVILEGE_REQUIRED',
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.USER,
          text: 'Grant EXECUTE ON SYS.DBMS_FLASHBACK to the observability user before initializing OracleStoreVNext.',
        },
        error,
      );
    }
    for (const ddl of allTableDDL(this.schema)) await this.db.executeDdl(ddl, [-955]);
    for (const tableName of [TABLE_SPAN_EVENTS, TABLE_FEEDBACK_EVENTS] as const)
      for (const column of SIGNAL_COLUMNS[tableName]) {
        if (
          !['textSearch', 'inputPreview', 'outputPreview', 'errorPreview', 'timeToFirstChunkMs'].includes(
            column.name,
          ) ||
          (await this.db.hasColumn(tableName, column.name))
        )
          continue;
        await this.db.executeDdl(
          `ALTER TABLE ${rawTable(this.schema, tableName)} ADD (${buildColumnDefinitions([column], rawTable(this.schema, CURSOR_SEQUENCE))})`,
          [-1430],
        );
      }
    for (const ddl of allIndexDDL(this.schema, this.config.skipDefaultIndexes)) await this.db.executeDdl(ddl, [-955]);
    for (const index of customIndexes(this.config.indexes)) await this.db.createIndex(index);
  }
  private async run<T>(operation: string, fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (error) {
      if (
        error instanceof MastraError ||
        error instanceof coreStorage.TraceQueryCursorError ||
        error instanceof coreStorage.TraceQueryExecutionError ||
        error instanceof coreStorage.TraceQueryResourceLimitError
      )
        throw error;
      throw new MastraError(
        {
          id: coreStorage.createStorageErrorId('ORACLEDB', `VNEXT_${operation}`, 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
        },
        error,
      );
    }
  }
  override get observabilityStrategy(): {
    preferred: ObservabilityStorageStrategy;
    supported: ObservabilityStorageStrategy[];
  } {
    return { preferred: 'event-sourced', supported: ['event-sourced'] };
  }
  override getFeatures() {
    const features = [
      'logs',
      'metrics',
      'feedback',
      'entity-type-discovery',
      'entity-name-discovery',
      'service-name-discovery',
      'environment-discovery',
      'tag-discovery',
      'metric-discovery',
      'trace-query',
      'trace-aggregate',
      'span-query',
      'span-query-context-fields',
      'trace-query-root-duration',
      'trace-query-discovery',
      'thread-query',
      'trace-query-tenant-scope',
      'trace-query-context-ids',
    ] as const;
    return deltaPollingFeatureEnabled() ? ([...features, 'delta-polling'] as const) : features;
  }
  override async createSpan(args: CreateSpanArgs): Promise<void> {
    return this.run('CREATE_SPAN', () => tracingOps.createSpan(this.client, this.schema, args));
  }
  override async batchCreateSpans(args: BatchCreateSpansArgs): Promise<void> {
    return this.run('BATCH_CREATE_SPANS', () => tracingOps.batchCreateSpans(this.client, this.schema, args));
  }
  override async getSpan(args: GetSpanArgs): Promise<GetSpanResponse | null> {
    return this.run('GET_SPAN', () => tracingOps.getSpan(this.client, this.schema, args));
  }
  override async getSpans(args: GetSpansArgs): Promise<GetSpansResponse> {
    return this.run('GET_SPANS', () => tracingOps.getSpans(this.client, this.schema, args));
  }
  override async getRootSpan(args: GetRootSpanArgs): Promise<GetRootSpanResponse | null> {
    return this.run('GET_ROOT_SPAN', () => tracingOps.getRootSpan(this.client, this.schema, args));
  }
  override async getTrace(args: GetTraceArgs): Promise<GetTraceResponse | null> {
    return this.run('GET_TRACE', () => tracingOps.getTrace(this.client, this.schema, args));
  }
  override async getTraceLight(args: GetTraceArgs): Promise<GetTraceLightResponse | null> {
    return this.run('GET_TRACE_LIGHT', () => tracingOps.getTraceLight(this.client, this.schema, args));
  }
  override async batchDeleteTraces(args: BatchDeleteTracesArgs): Promise<void> {
    return this.run('BATCH_DELETE_TRACES', () => tracingOps.batchDeleteTraces(this.client, this.schema, args));
  }
  override async batchCreateLogs(args: BatchCreateLogsArgs): Promise<void> {
    return this.run('BATCH_CREATE_LOGS', () => logsOps.batchCreateLogs(this.client, this.schema, args));
  }
  override async listLogs(args: ListLogsArgs): Promise<ListLogsResponse> {
    return this.run('LIST_LOGS', () => logsOps.listLogs(this.client, this.schema, args));
  }
  override async batchCreateMetrics(args: BatchCreateMetricsArgs): Promise<void> {
    return this.run('BATCH_CREATE_METRICS', () => metricsOps.batchCreateMetrics(this.client, this.schema, args));
  }
  override async listMetrics(args: ListMetricsArgs): Promise<ListMetricsResponse> {
    return this.run('LIST_METRICS', () => metricsOps.listMetrics(this.client, this.schema, args));
  }
  override async getMetricAggregate(args: GetMetricAggregateArgs): Promise<GetMetricAggregateResponse> {
    return this.run('GET_METRIC_AGGREGATE', () => metricsOps.getMetricAggregate(this.client, this.schema, args));
  }
  override async getMetricBreakdown(args: GetMetricBreakdownArgs): Promise<GetMetricBreakdownResponse> {
    return this.run('GET_METRIC_BREAKDOWN', () => metricsOps.getMetricBreakdown(this.client, this.schema, args));
  }
  override async getMetricTimeSeries(args: GetMetricTimeSeriesArgs): Promise<GetMetricTimeSeriesResponse> {
    return this.run('GET_METRIC_TIME_SERIES', () => metricsOps.getMetricTimeSeries(this.client, this.schema, args));
  }
  override async getMetricPercentiles(args: GetMetricPercentilesArgs): Promise<GetMetricPercentilesResponse> {
    return this.run('GET_METRIC_PERCENTILES', () => metricsOps.getMetricPercentiles(this.client, this.schema, args));
  }
  override async getEntityTypes(args: GetEntityTypesArgs): Promise<GetEntityTypesResponse> {
    return this.run('GET_ENTITY_TYPES', () =>
      discoveryOps.getEntityTypes(this.client, this.schema, args, { ...this.config.discovery, logger: this.logger }),
    );
  }
  override async getEntityNames(args: GetEntityNamesArgs): Promise<GetEntityNamesResponse> {
    return this.run('GET_ENTITY_NAMES', () =>
      discoveryOps.getEntityNames(this.client, this.schema, args, { ...this.config.discovery, logger: this.logger }),
    );
  }
  override async getServiceNames(args: GetServiceNamesArgs): Promise<GetServiceNamesResponse> {
    return this.run('GET_SERVICE_NAMES', () =>
      discoveryOps.getServiceNames(this.client, this.schema, args, { ...this.config.discovery, logger: this.logger }),
    );
  }
  override async getEnvironments(args: GetEnvironmentsArgs): Promise<GetEnvironmentsResponse> {
    return this.run('GET_ENVIRONMENTS', () =>
      discoveryOps.getEnvironments(this.client, this.schema, args, { ...this.config.discovery, logger: this.logger }),
    );
  }
  override async getTags(args: GetTagsArgs): Promise<GetTagsResponse> {
    return this.run('GET_TAGS', () =>
      discoveryOps.getTags(this.client, this.schema, args, { ...this.config.discovery, logger: this.logger }),
    );
  }
  override async getMetricNames(args: GetMetricNamesArgs): Promise<GetMetricNamesResponse> {
    return this.run('GET_METRIC_NAMES', () =>
      discoveryOps.getMetricNames(this.client, this.schema, args, { ...this.config.discovery, logger: this.logger }),
    );
  }
  override async getMetricLabelKeys(args: GetMetricLabelKeysArgs): Promise<GetMetricLabelKeysResponse> {
    return this.run('GET_METRIC_LABEL_KEYS', () =>
      discoveryOps.getMetricLabelKeys(this.client, this.schema, args, {
        ...this.config.discovery,
        logger: this.logger,
      }),
    );
  }
  override async getMetricLabelValues(args: GetMetricLabelValuesArgs): Promise<GetMetricLabelValuesResponse> {
    return this.run('GET_METRIC_LABEL_VALUES', () =>
      discoveryOps.getMetricLabelValues(this.client, this.schema, args, {
        ...this.config.discovery,
        logger: this.logger,
      }),
    );
  }
  override async createScore(args: CreateScoreArgs): Promise<void> {
    return this.run('CREATE_SCORE', () => scoresOps.createScore(this.client, this.schema, args));
  }
  override async batchCreateScores(args: BatchCreateScoresArgs): Promise<void> {
    return this.run('BATCH_CREATE_SCORES', () => scoresOps.batchCreateScores(this.client, this.schema, args));
  }
  override async deleteScores(args: DeleteScoresArgs): Promise<void> {
    return this.run('DELETE_SCORES', () => scoresOps.deleteScores(this.client, this.schema, args));
  }
  override async listScores(args: ListScoresArgs): Promise<ListScoresResponse> {
    return this.run('LIST_SCORES', () => scoresOps.listScores(this.client, this.schema, args));
  }
  override async createFeedback(args: CreateFeedbackArgs): Promise<void> {
    return this.run('CREATE_FEEDBACK', () => feedbackOps.createFeedback(this.client, this.schema, args));
  }
  override async batchCreateFeedback(args: BatchCreateFeedbackArgs): Promise<void> {
    return this.run('BATCH_CREATE_FEEDBACK', () => feedbackOps.batchCreateFeedback(this.client, this.schema, args));
  }
  override async deleteFeedback(args: DeleteFeedbackArgs): Promise<void> {
    return this.run('DELETE_FEEDBACK', () => feedbackOps.deleteFeedback(this.client, this.schema, args));
  }
  override async listFeedback(args: ListFeedbackArgs): Promise<ListFeedbackResponse> {
    return this.run('LIST_FEEDBACK', () => feedbackOps.listFeedback(this.client, this.schema, args));
  }
  override async updateFeedbackReviewStatus(args: UpdateFeedbackReviewStatusArgs): Promise<FeedbackRecord> {
    return this.run('UPDATE_FEEDBACK_REVIEW_STATUS', () =>
      feedbackOps.updateFeedbackReviewStatus(this.client, this.schema, args),
    );
  }
  override async getScoreAggregate(args: GetScoreAggregateArgs): Promise<GetScoreAggregateResponse> {
    return this.run('GET_SCORE_AGGREGATE', () => scoresOps.getScoreAggregate(this.client, this.schema, args));
  }
  override async getScoreBreakdown(args: GetScoreBreakdownArgs): Promise<GetScoreBreakdownResponse> {
    return this.run('GET_SCORE_BREAKDOWN', () => scoresOps.getScoreBreakdown(this.client, this.schema, args));
  }
  override async getScoreTimeSeries(args: GetScoreTimeSeriesArgs): Promise<GetScoreTimeSeriesResponse> {
    return this.run('GET_SCORE_TIME_SERIES', () => scoresOps.getScoreTimeSeries(this.client, this.schema, args));
  }
  override async getScorePercentiles(args: GetScorePercentilesArgs): Promise<GetScorePercentilesResponse> {
    return this.run('GET_SCORE_PERCENTILES', () => scoresOps.getScorePercentiles(this.client, this.schema, args));
  }
  override async getFeedbackAggregate(args: GetFeedbackAggregateArgs): Promise<GetFeedbackAggregateResponse> {
    return this.run('GET_FEEDBACK_AGGREGATE', () => feedbackOps.getFeedbackAggregate(this.client, this.schema, args));
  }
  override async getFeedbackBreakdown(args: GetFeedbackBreakdownArgs): Promise<GetFeedbackBreakdownResponse> {
    return this.run('GET_FEEDBACK_BREAKDOWN', () => feedbackOps.getFeedbackBreakdown(this.client, this.schema, args));
  }
  override async getFeedbackTimeSeries(args: GetFeedbackTimeSeriesArgs): Promise<GetFeedbackTimeSeriesResponse> {
    return this.run('GET_FEEDBACK_TIME_SERIES', () =>
      feedbackOps.getFeedbackTimeSeries(this.client, this.schema, args),
    );
  }
  override async getFeedbackPercentiles(args: GetFeedbackPercentilesArgs): Promise<GetFeedbackPercentilesResponse> {
    return this.run('GET_FEEDBACK_PERCENTILES', () =>
      feedbackOps.getFeedbackPercentiles(this.client, this.schema, args),
    );
  }
  override async getScoreById(scoreId: string): Promise<ScoreRecord | null> {
    return this.run('GET_SCORE_BY_ID', () => scoresOps.getScoreById(this.client, this.schema, scoreId));
  }
  override async listTraces(args: ListTracesArgs): Promise<ListTracesResponse> {
    return this.run('LIST_TRACES', () => tracesOps.listTraces(this.client, this.schema, args));
  }
  override async listTracesLight(args: ListTracesArgs): Promise<ListTracesLightResponse> {
    return this.run('LIST_TRACES_LIGHT', () => tracesOps.listTracesLight(this.client, this.schema, args));
  }
  override async listBranches(args: ListBranchesArgs): Promise<ListBranchesResponse> {
    return this.run('LIST_BRANCHES', () => tracesOps.listBranches(this.client, this.schema, args));
  }
  override async queryTraces(plan: TrustedTraceQueryPlan): Promise<TraceQueryResponse> {
    return this.run('QUERY_TRACES', () =>
      traceQueryOps.queryTraces(this.client, this.schema, plan, this.traceQueryTimeoutMs),
    );
  }
  override async queryThreads(plan: TrustedThreadQueryPlan): Promise<QueryThreadsResult> {
    return this.run('QUERY_THREADS', () =>
      traceQueryOps.queryThreads(this.client, this.schema, plan, this.traceQueryTimeoutMs),
    );
  }
  override async getTraceQueryObservedFields(
    plan: TrustedTraceQueryObservedFieldsPlan,
  ): Promise<TraceQueryObservedFieldsResult> {
    return this.run('GET_TRACE_QUERY_OBSERVED_FIELDS', () =>
      traceQueryOps.getTraceQueryObservedFields(this.client, this.schema, plan, this.traceQueryTimeoutMs),
    );
  }
  override async getTraceQueryValues(plan: TrustedTraceQueryValuesPlan): Promise<GetTraceQueryValuesResponse> {
    return this.run('GET_TRACE_QUERY_VALUES', () =>
      traceQueryOps.getTraceQueryValues(this.client, this.schema, plan, this.traceQueryTimeoutMs),
    );
  }
  override async aggregateTraces(plan: TrustedTraceAggregatePlan): Promise<TraceAggregateResponse> {
    return this.run('AGGREGATE_TRACES', () =>
      traceAggregateOps.aggregateTraces(this.client, this.schema, plan, this.traceQueryTimeoutMs),
    );
  }
  override async querySpans(plan: TrustedSpanQueryPlan): Promise<SpanQueryResponse> {
    return this.run('QUERY_SPANS', () =>
      spanQueryOps.querySpans(this.client, this.schema, plan, this.traceQueryTimeoutMs),
    );
  }
  override async prune(policies: Record<string, TableRetentionPolicy>, options?: PruneOptions): Promise<PruneResult[]> {
    const targets = resolveRetentionTargets({
      policies,
      descriptor: ObservabilityStorageOracleVNext.retentionTables,
      order: ['spans', 'metrics', 'logs', 'scores', 'feedback'],
    });
    return executeRetentionPrune({
      domain: 'observability',
      targets,
      options,
      cutoffFor: (target, now) => new Date(retentionCutoffMs(target.policy, now)),
      deleteBatch: async (target, cutoff, limit) => {
        if (!this.retentionIndexes.has(target.table)) {
          try {
            await this.db.executeDdl(
              `CREATE INDEX ${rawTable(this.schema, `${target.table}_TIME_IDX`)} ON ${rawTable(this.schema, target.table)} ("${target.column}")`,
              [-955],
            );
            this.retentionIndexes.add(target.table);
          } catch (error) {
            this.logger?.warn?.(`Failed to create retention index for ${target.table}:`, error);
          }
        }
        const table = rawTable(this.schema, target.table);
        const result = await this.client.query(
          `DELETE FROM ${table} WHERE ROWID IN (SELECT ROWID FROM ${table} WHERE "${target.column}" < :p1 ORDER BY "${target.column}" FETCH FIRST :p2 ROWS ONLY)`,
          [cutoff, limit],
        );
        return result.rowCount;
      },
    });
  }
  override async dangerouslyClearAll(): Promise<void> {
    await this.client.tx(async client => {
      for (const table of ALL_TABLE_NAMES) await client.none(`DELETE FROM ${rawTable(this.schema, table)}`);
    });
  }
}
