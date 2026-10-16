import type {
  BackgroundTask,
  BackgroundTaskStatus,
  TaskFilter,
  TaskListResult,
  UpdateBackgroundTask,
  UpdateBackgroundTaskOptions,
} from '@mastra/core/background-tasks';
import {
  BackgroundTasksStorage,
  TABLE_BACKGROUND_TASKS,
  TABLE_SCHEMAS,
  executeRetentionPrune,
  resolveRetentionTargets,
  retentionCutoffMs,
} from '@mastra/core/storage';
import type { PruneOptions, PruneResult, RetentionTablesDescriptor, TableRetentionPolicy } from '@mastra/core/storage';
import oracledb from 'oracledb';

import { asBindParameters, clobBind, jsonBindText } from '../../../shared/connection';
import { indexNameForTable } from '../../../vector/identifiers';
import { OracleDB, filterIndexesForTables, parseOracleJson } from '../../db';
import type { OracleCreateIndexOptions } from '../../db';
import { toDate } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

const COLUMNS = {
  id: 'id',
  toolCallId: 'tool_call_id',
  toolName: 'tool_name',
  agentId: 'agent_id',
  threadId: 'thread_id',
  resourceId: 'resource_id',
  runId: 'run_id',
  status: 'status',
  args: 'args',
  result: 'result',
  error: 'error',
  suspendPayload: 'suspend_payload',
  retryCount: 'retry_count',
  maxRetries: 'max_retries',
  timeoutMs: 'timeout_ms',
  createdAt: '"createdAt"',
  startedAt: '"startedAt"',
  suspendedAt: '"suspendedAt"',
  completedAt: '"completedAt"',
  ownerId: '"ownerId"',
  leaseExpiresAt: '"leaseExpiresAt"',
} as const satisfies Record<keyof BackgroundTask, string>;
const JSON_FIELDS = new Set<string>(['args', 'result', 'error', 'suspendPayload']);
const MUTABLE_FIELDS = [
  'status',
  'result',
  'error',
  'suspendPayload',
  'retryCount',
  'maxRetries',
  'timeoutMs',
  'startedAt',
  'suspendedAt',
  'completedAt',
  'ownerId',
  'leaseExpiresAt',
] as const satisfies readonly (keyof UpdateBackgroundTask)[];
const DATE_FIELDS = ['createdAt', 'startedAt', 'suspendedAt', 'completedAt'] as const;

function fieldBind(key: string, value: unknown): unknown {
  if (JSON_FIELDS.has(key))
    return value === undefined ? null : clobBind(value === null ? 'null' : jsonBindText(value)!);
  if (value instanceof Date) return { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: value };
  return value ?? null;
}

export class BackgroundTasksOracle extends BackgroundTasksStorage {
  static readonly MANAGED_TABLES = [TABLE_BACKGROUND_TASKS] as const;
  static override readonly retentionTables: RetentionTablesDescriptor = {
    backgroundTasks: { table: TABLE_BACKGROUND_TASKS, column: 'completedAt', indexed: true },
  };
  private readonly db: OracleDB;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({ ...config, jsonColumns: [...JSON_FIELDS] });
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, BackgroundTasksOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({ tableName: TABLE_BACKGROUND_TASKS, schema: TABLE_SCHEMAS[TABLE_BACKGROUND_TASKS] });
    if (!this.skipDefaultIndexes)
      for (const index of getDefaultBackgroundTaskIndexDefinitions()) {
        try {
          await this.db.createIndex(index);
        } catch (error) {
          this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
        }
      }
    for (const index of this.indexes) await this.db.createIndex(index);
  }

  async createTask(task: BackgroundTask): Promise<void> {
    const values: string[] = [];
    const columns: string[] = [];
    const binds: Record<string, unknown> = {};
    for (const [field, column] of Object.entries(COLUMNS)) {
      const key = field as keyof BackgroundTask;
      const name = `v${values.length}`;
      columns.push(column);
      values.push(JSON_FIELDS.has(field) ? `JSON(:${name})` : `:${name}`);
      binds[name] = fieldBind(field, task[key]);
    }
    await this.db.none(`INSERT INTO ${this.table()} (${columns.join(', ')}) VALUES (${values.join(', ')})`, binds);
  }

  async updateTask(
    taskId: string,
    update: UpdateBackgroundTask,
    options?: UpdateBackgroundTaskOptions,
  ): Promise<boolean> {
    const assignments: string[] = [];
    const binds: Record<string, unknown> = { taskId };
    for (const field of MUTABLE_FIELDS) {
      if (!(field in update)) continue;
      const name = `v${assignments.length}`;
      assignments.push(`${COLUMNS[field]} = ${JSON_FIELDS.has(field) ? `JSON(:${name})` : `:${name}`}`);
      binds[name] = fieldBind(field, update[field]);
    }
    if (!assignments.length) return false;
    const conditions = ['id = :taskId'];
    if (options?.expectedStatus !== undefined) {
      conditions.push('status = :expectedStatus');
      binds.expectedStatus = options.expectedStatus;
    }
    if (options?.expectedOwnerId !== undefined) {
      conditions.push(options.expectedOwnerId === null ? '"ownerId" IS NULL' : '"ownerId" = :expectedOwner');
      if (options.expectedOwnerId !== null) binds.expectedOwner = options.expectedOwnerId;
    }
    if (options?.expectedLeaseExpiresAt !== undefined) {
      conditions.push(
        options.expectedLeaseExpiresAt === null ? '"leaseExpiresAt" IS NULL' : '"leaseExpiresAt" = :expectedLease',
      );
      if (options.expectedLeaseExpiresAt !== null)
        binds.expectedLease = fieldBind('leaseExpiresAt', options.expectedLeaseExpiresAt);
    }
    return this.db.withConnection(async connection => {
      const result = await connection.execute(
        `UPDATE ${this.table()} SET ${assignments.join(', ')} WHERE ${conditions.join(' AND ')}`,
        asBindParameters(binds),
        { autoCommit: true },
      );
      return (result.rowsAffected ?? 0) > 0;
    });
  }

  async getTask(taskId: string): Promise<BackgroundTask | null> {
    const row = await this.db.oneOrNone(`${this.select()} WHERE id = :taskId`, { taskId });
    return row ? rowToTask(row) : null;
  }

  async listTasks(filter: TaskFilter): Promise<TaskListResult> {
    for (const value of [filter.page, filter.perPage])
      if (value !== undefined && (!Number.isSafeInteger(value) || value < 0))
        throw new Error('Task pagination values must be non-negative safe integers');
    const { where, binds } = this.where(filter);
    const count = await this.db.read.one<{ count: number }>(
      `SELECT COUNT(*) AS "count" FROM ${this.table()} ${where}`,
      binds,
    );
    const order = DATE_FIELDS.find(field => field === filter.orderBy) ?? 'createdAt';
    let sql = `${this.select()} ${where} ORDER BY "${order}" ${filter.orderDirection === 'desc' ? 'DESC' : 'ASC'}, id`;
    if (filter.perPage !== undefined) {
      sql += ' OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY';
      binds.offset = (filter.page ?? 0) * filter.perPage;
      binds.limit = filter.perPage;
    }
    return { tasks: (await this.db.read.manyOrNone(sql, binds)).map(rowToTask), total: Number(count.count) };
  }

  async deleteTask(taskId: string): Promise<void> {
    await this.db.none(`DELETE FROM ${this.table()} WHERE id = :taskId`, { taskId });
  }

  async deleteTasks(filter: TaskFilter): Promise<void> {
    const { where, binds } = this.where(filter);
    if (!where) return;
    await this.db.none(`DELETE FROM ${this.table()} ${where}`, binds);
  }

  async getRunningCount(): Promise<number> {
    // These counts gate dispatch and must include committed work from every worker.
    return Number(
      (await this.db.one<{ count: number }>(`SELECT COUNT(*) AS "count" FROM ${this.table()} WHERE status = 'running'`))
        .count,
    );
  }

  async getRunningCountByAgent(agentId: string): Promise<number> {
    return Number(
      (
        await this.db.one<{ count: number }>(
          `SELECT COUNT(*) AS "count" FROM ${this.table()} WHERE status = 'running' AND agent_id = :agentId`,
          { agentId },
        )
      ).count,
    );
  }

  async prune(policies: Record<string, TableRetentionPolicy>, options?: PruneOptions): Promise<PruneResult[]> {
    if (policies.backgroundTasks && !options?.signal?.aborted) {
      try {
        await this.db.createIndex(retentionIndex());
      } catch (error) {
        this.logger?.warn?.(`Failed to create retention index for ${TABLE_BACKGROUND_TASKS}:`, error);
      }
    }
    const targets = resolveRetentionTargets({
      policies,
      descriptor: BackgroundTasksOracle.retentionTables,
      order: ['backgroundTasks'],
    });
    return executeRetentionPrune({
      domain: 'backgroundTasks',
      targets,
      options,
      cutoffFor: (target, now) => new Date(retentionCutoffMs(target.policy, now)),
      deleteBatch: (target, cutoff, limit) =>
        this.db.pruneBatch({ tableName: target.table, column: target.column, cutoff, limit }),
    });
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.clearTable(TABLE_BACKGROUND_TASKS);
  }

  private table(): string {
    return this.db.table(TABLE_BACKGROUND_TASKS);
  }

  private select(): string {
    const columns = Object.entries(COLUMNS).map(
      ([key, column]) => `${JSON_FIELDS.has(key) ? `JSON_SERIALIZE(${column} RETURNING CLOB)` : column} AS "${key}"`,
    );
    // SQL NULL means unset; a JSON null is a legitimate result or suspend payload.
    columns.push(
      'CASE WHEN result IS NULL THEN 1 ELSE 0 END AS "resultIsNull"',
      'CASE WHEN suspend_payload IS NULL THEN 1 ELSE 0 END AS "suspendPayloadIsNull"',
    );
    return `SELECT ${columns.join(', ')} FROM ${this.table()}`;
  }

  private where(filter: TaskFilter): { where: string; binds: Record<string, unknown> } {
    const conditions: string[] = [];
    const binds: Record<string, unknown> = {};
    if (filter.status !== undefined) {
      const statuses = Array.isArray(filter.status) ? filter.status : [filter.status];
      const names = statuses.map((status, index) => {
        const name = `status${index}`;
        binds[name] = status;
        return `:${name}`;
      });
      conditions.push(names.length ? `status IN (${names.join(', ')})` : '1 = 0');
    }
    for (const field of ['agentId', 'threadId', 'resourceId', 'toolName', 'runId', 'toolCallId'] as const) {
      if (filter[field] === undefined) continue;
      conditions.push(`${COLUMNS[field]} = :${field}`);
      binds[field] = filter[field];
    }
    const dateColumn = DATE_FIELDS.find(field => field === filter.dateFilterBy) ?? 'createdAt';
    if (filter.fromDate !== undefined) {
      conditions.push(`"${dateColumn}" >= :fromDate`);
      binds.fromDate = fieldBind(dateColumn, filter.fromDate);
    }
    if (filter.toDate !== undefined) {
      conditions.push(`"${dateColumn}" < :toDate`);
      binds.toDate = fieldBind(dateColumn, filter.toDate);
    }
    return { where: conditions.length ? `WHERE ${conditions.join(' AND ')}` : '', binds };
  }
}

function rowToTask(row: Record<string, unknown>): BackgroundTask {
  return {
    id: String(row.id),
    status: String(row.status) as BackgroundTaskStatus,
    toolName: String(row.toolName),
    toolCallId: String(row.toolCallId),
    args: parseOracleJson<Record<string, unknown>>(row.args) ?? {},
    agentId: String(row.agentId),
    runId: String(row.runId),
    threadId: row.threadId == null ? undefined : String(row.threadId),
    resourceId: row.resourceId == null ? undefined : String(row.resourceId),
    result: Number(row.resultIsNull) === 1 ? undefined : parseOracleJson(row.result),
    error: parseOracleJson<BackgroundTask['error']>(row.error),
    suspendPayload: Number(row.suspendPayloadIsNull) === 1 ? undefined : parseOracleJson(row.suspendPayload),
    createdAt: toDate(row.createdAt),
    startedAt: row.startedAt == null ? undefined : toDate(row.startedAt),
    suspendedAt: row.suspendedAt == null ? undefined : toDate(row.suspendedAt),
    completedAt: row.completedAt == null ? undefined : toDate(row.completedAt),
    ownerId: row.ownerId == null ? undefined : String(row.ownerId),
    leaseExpiresAt: row.leaseExpiresAt == null ? undefined : toDate(row.leaseExpiresAt),
    retryCount: Number(row.retryCount),
    maxRetries: Number(row.maxRetries),
    timeoutMs: Number(row.timeoutMs),
  };
}

function retentionIndex(): OracleCreateIndexOptions {
  return {
    name: indexNameForTable('MASTRA_BACKGROUND_TASKS_RETENTION', 'IDX'),
    table: TABLE_BACKGROUND_TASKS,
    columns: ['completedAt'],
  };
}

export function getDefaultBackgroundTaskIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    retentionIndex(),
    {
      name: indexNameForTable('MASTRA_BACKGROUND_STATUS_CREATED', 'IDX'),
      table: TABLE_BACKGROUND_TASKS,
      columns: ['status', 'createdAt'],
    },
    {
      name: indexNameForTable('MASTRA_BACKGROUND_AGENT_STATUS', 'IDX'),
      table: TABLE_BACKGROUND_TASKS,
      columns: ['agent_id', 'status'],
    },
    {
      name: indexNameForTable('MASTRA_BACKGROUND_THREAD_CREATED', 'IDX'),
      table: TABLE_BACKGROUND_TASKS,
      columns: ['thread_id', 'createdAt'],
    },
    {
      name: indexNameForTable('MASTRA_BACKGROUND_TOOL_CALL', 'IDX'),
      table: TABLE_BACKGROUND_TASKS,
      columns: ['tool_call_id'],
    },
  ];
}
