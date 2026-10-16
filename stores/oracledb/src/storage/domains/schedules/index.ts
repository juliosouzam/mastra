import {
  normalizeScheduleTarget,
  SchedulesStorage,
  TABLE_SCHEDULES,
  TABLE_SCHEDULE_TRIGGERS,
  TABLE_SCHEMAS,
  executeRetentionPrune,
  resolveRetentionTargets,
  retentionCutoffMs,
} from '@mastra/core/storage';
import type {
  Schedule,
  ScheduleFilter,
  ScheduleTrigger,
  ScheduleTriggerListOptions,
  ScheduleStatus,
  ScheduleTarget,
  ScheduleUpdate,
  PruneOptions,
  PruneResult,
  RetentionTablesDescriptor,
  TableRetentionPolicy,
} from '@mastra/core/storage';

import { asBindParameters, clobBind, jsonBindText, nullableClobBind } from '../../../shared/connection';
import { indexNameForTable } from '../../../vector/identifiers';
import { OracleDB, filterIndexesForTables, parseOracleJson } from '../../db';
import type { OracleCreateIndexOptions } from '../../db';
import type { OracleDomainConfig } from '../../types';

const UPDATE_COLUMNS = {
  cron: 'cron',
  timezone: 'timezone',
  status: 'status',
  nextFireAt: 'next_fire_at',
  target: 'target',
  metadata: 'metadata',
  ownerType: 'owner_type',
  ownerId: 'owner_id',
} as const;

export class SchedulesOracle extends SchedulesStorage {
  static readonly MANAGED_TABLES = [TABLE_SCHEDULES, TABLE_SCHEDULE_TRIGGERS] as const;
  static override readonly retentionTables: RetentionTablesDescriptor = {
    triggers: { table: TABLE_SCHEDULE_TRIGGERS, column: 'actual_fire_at', indexed: true, anchorType: 'epoch-ms' },
  };
  private readonly db: OracleDB;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({ ...config, jsonColumns: ['target', 'metadata'] });
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, SchedulesOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    for (const tableName of SchedulesOracle.MANAGED_TABLES)
      await this.db.createTable({ tableName, schema: TABLE_SCHEMAS[tableName] });
    if (!this.skipDefaultIndexes)
      for (const index of getDefaultScheduleIndexDefinitions()) {
        try {
          await this.db.createIndex(index);
        } catch (error) {
          this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
        }
      }
    for (const index of this.indexes) await this.db.createIndex(index);
  }

  async createSchedule(schedule: Schedule): Promise<Schedule> {
    await this.db.none(
      `INSERT INTO ${this.db.table(TABLE_SCHEDULES)}
      (id, target, cron, timezone, status, next_fire_at, last_fire_at, last_run_id, created_at, updated_at, metadata, owner_type, owner_id)
      VALUES (:id, JSON(:target), :cron, :timezone, :status, :nextFireAt, :lastFireAt, :lastRunId, :createdAt, :updatedAt, JSON(:metadata), :ownerType, :ownerId)`,
      {
        id: schedule.id,
        target: clobBind(jsonBindText(schedule.target)!),
        cron: schedule.cron,
        timezone: schedule.timezone ?? null,
        status: schedule.status,
        nextFireAt: schedule.nextFireAt,
        lastFireAt: schedule.lastFireAt ?? null,
        lastRunId: schedule.lastRunId ?? null,
        createdAt: schedule.createdAt,
        updatedAt: schedule.updatedAt,
        metadata: nullableClobBind(jsonBindText(schedule.metadata)),
        ownerType: schedule.ownerType ?? null,
        ownerId: schedule.ownerId ?? null,
      },
    );
    return (await this.getScheduleWithClient(this.db, schedule.id))!;
  }

  async getSchedule(id: string): Promise<Schedule | null> {
    return this.getScheduleWithClient(this.db.read, id);
  }

  private async getScheduleWithClient(client: OracleDB, id: string): Promise<Schedule | null> {
    const row = await client.oneOrNone(`${this.selectSchedule()} WHERE id = :id`, { id });
    return row ? rowToSchedule(row) : null;
  }

  async listSchedules(filter?: ScheduleFilter): Promise<Schedule[]> {
    const conditions: string[] = [];
    const binds: Record<string, unknown> = {};
    if (filter?.status !== undefined) {
      conditions.push('status = :status');
      binds.status = filter.status;
    }
    if (filter?.workflowId !== undefined) {
      conditions.push("JSON_VALUE(target, '$.workflowId' RETURNING VARCHAR2(512) NULL ON ERROR) = :workflowId");
      binds.workflowId = filter.workflowId;
    }
    for (const [key, column] of [
      ['ownerType', 'owner_type'],
      ['ownerId', 'owner_id'],
    ] as const) {
      if (filter?.[key] === undefined) continue;
      conditions.push(filter[key] === null ? `${column} IS NULL` : `${column} = :${key}`);
      if (filter[key] !== null) binds[key] = filter[key];
    }
    return (
      await this.db.read.manyOrNone(
        `${this.selectSchedule()}${conditions.length ? ` WHERE ${conditions.join(' AND ')}` : ''} ORDER BY created_at ASC, id`,
        binds,
      )
    ).map(rowToSchedule);
  }

  async listDueSchedules(now: number, limit = 100): Promise<Schedule[]> {
    validateLimit(limit);
    return (
      await this.db.manyOrNone(
        `${this.selectSchedule()} WHERE status = 'active' AND next_fire_at <= :now ORDER BY next_fire_at ASC, id FETCH FIRST :limit ROWS ONLY`,
        { now, limit: Math.floor(limit) },
      )
    ).map(rowToSchedule);
  }

  async updateSchedule(id: string, patch: ScheduleUpdate): Promise<Schedule> {
    const sets: string[] = [];
    const binds: Record<string, unknown> = { id };
    for (const [field, column] of Object.entries(UPDATE_COLUMNS)) {
      const key = field as keyof ScheduleUpdate;
      if (!(key in patch)) continue;
      const value = patch[key];
      if (value === undefined && (key === 'cron' || key === 'status' || key === 'nextFireAt' || key === 'target'))
        continue;
      const json = key === 'target' || key === 'metadata';
      const name = `v${sets.length}`;
      sets.push(`${column} = ${json ? `JSON(:${name})` : `:${name}`}`);
      binds[name] = json ? nullableClobBind(jsonBindText(value)) : (value ?? null);
    }
    if (sets.length) {
      sets.push('updated_at = :now');
      binds.now = Date.now();
      await this.db.none(`UPDATE ${this.db.table(TABLE_SCHEDULES)} SET ${sets.join(', ')} WHERE id = :id`, binds);
    }
    const updated = await this.getScheduleWithClient(this.db, id);
    if (!updated) throw new Error(`Schedule ${id} not found`);
    return updated;
  }

  async updateScheduleNextFire(
    id: string,
    expectedNextFireAt: number,
    newNextFireAt: number,
    lastFireAt: number,
    lastRunId: string,
    newStatus?: ScheduleStatus,
  ): Promise<boolean> {
    return this.db.withConnection(async connection => {
      const result = await connection.execute(
        `UPDATE ${this.db.table(TABLE_SCHEDULES)} SET next_fire_at = :newNextFireAt,
        last_fire_at = :lastFireAt, last_run_id = :lastRunId, updated_at = :now, status = NVL(:newStatus, status)
        WHERE id = :id AND next_fire_at = :expectedNextFireAt AND status = 'active'`,
        asBindParameters({
          id,
          expectedNextFireAt,
          newNextFireAt,
          lastFireAt,
          lastRunId,
          now: Date.now(),
          newStatus: newStatus ?? null,
        }),
        { autoCommit: true },
      );
      return (result.rowsAffected ?? 0) > 0;
    });
  }

  async deleteSchedule(id: string): Promise<void> {
    await this.db.tx(async client => {
      await client.none(`DELETE FROM ${this.db.table(TABLE_SCHEDULE_TRIGGERS)} WHERE schedule_id = :id`, { id });
      await client.none(`DELETE FROM ${this.db.table(TABLE_SCHEDULES)} WHERE id = :id`, { id });
    });
  }

  async recordTrigger(trigger: ScheduleTrigger): Promise<void> {
    await this.db.none(
      `INSERT INTO ${this.db.table(TABLE_SCHEDULE_TRIGGERS)}
      (id, schedule_id, run_id, scheduled_fire_at, actual_fire_at, outcome, error, trigger_kind, parent_trigger_id, metadata)
      VALUES (:id, :scheduleId, :runId, :scheduledFireAt, :actualFireAt, :outcome,
        CASE WHEN :emptyError = 1 THEN EMPTY_CLOB() ELSE :errorText END, :triggerKind, :parentTriggerId, JSON(:metadata))`,
      {
        id: trigger.id ?? crypto.randomUUID(),
        scheduleId: trigger.scheduleId,
        runId: trigger.runId,
        scheduledFireAt: trigger.scheduledFireAt,
        actualFireAt: trigger.actualFireAt,
        outcome: trigger.outcome,
        errorText: trigger.error === undefined ? null : clobBind(trigger.error),
        emptyError: trigger.error === '' ? 1 : 0,
        triggerKind: trigger.triggerKind ?? 'schedule-fire',
        parentTriggerId: trigger.parentTriggerId ?? null,
        metadata: nullableClobBind(jsonBindText(trigger.metadata)),
      },
    );
  }

  async listTriggers(scheduleId: string, opts?: ScheduleTriggerListOptions): Promise<ScheduleTrigger[]> {
    const conditions = ['schedule_id = :scheduleId'];
    const binds: Record<string, unknown> = { scheduleId };
    if (opts?.fromActualFireAt !== undefined) {
      conditions.push('actual_fire_at >= :fromTime');
      binds.fromTime = opts.fromActualFireAt;
    }
    if (opts?.toActualFireAt !== undefined) {
      conditions.push('actual_fire_at < :toTime');
      binds.toTime = opts.toActualFireAt;
    }
    let limit = '';
    if (opts?.limit !== undefined) {
      validateLimit(opts.limit);
      limit = ' FETCH FIRST :limit ROWS ONLY';
      binds.limit = Math.floor(opts.limit);
    }
    const rows = await this.db.read.manyOrNone(
      `SELECT id AS "id", schedule_id AS "scheduleId", run_id AS "runId", scheduled_fire_at AS "scheduledFireAt",
      actual_fire_at AS "actualFireAt", outcome AS "outcome", error AS "errorText", CASE WHEN error IS NULL THEN 1 ELSE 0 END AS "errorIsNull",
      trigger_kind AS "triggerKind", parent_trigger_id AS "parentTriggerId", JSON_SERIALIZE(metadata RETURNING CLOB) AS "metadata"
      FROM ${this.db.table(TABLE_SCHEDULE_TRIGGERS)} WHERE ${conditions.join(' AND ')} ORDER BY actual_fire_at DESC, id${limit}`,
      binds,
    );
    return rows.map(row => ({
      id: String(row.id),
      scheduleId: String(row.scheduleId),
      runId: row.runId == null ? null : String(row.runId),
      scheduledFireAt: Number(row.scheduledFireAt),
      actualFireAt: Number(row.actualFireAt),
      outcome: String(row.outcome) as ScheduleTrigger['outcome'],
      error: Number(row.errorIsNull) === 1 ? undefined : String(row.errorText ?? ''),
      triggerKind: (row.triggerKind ?? 'schedule-fire') as ScheduleTrigger['triggerKind'],
      parentTriggerId: row.parentTriggerId == null ? undefined : String(row.parentTriggerId),
      metadata: parseOracleJson<Record<string, unknown>>(row.metadata),
    }));
  }

  async prune(policies: Record<string, TableRetentionPolicy>, options?: PruneOptions): Promise<PruneResult[]> {
    if (policies.triggers && !options?.signal?.aborted) {
      try {
        await this.db.createIndex(retentionIndex());
      } catch (error) {
        this.logger?.warn?.(`Failed to create retention index for ${TABLE_SCHEDULE_TRIGGERS}:`, error);
      }
    }
    const targets = resolveRetentionTargets({
      policies,
      descriptor: SchedulesOracle.retentionTables,
      order: ['triggers'],
    });
    return executeRetentionPrune({
      domain: 'schedules',
      targets,
      options,
      cutoffFor: (target, now) => retentionCutoffMs(target.policy, now),
      deleteBatch: (target, cutoff, limit) =>
        this.db.pruneBatch({ tableName: target.table, column: target.column, cutoff, limit }),
    });
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.tx(async client => {
      await client.none(`DELETE FROM ${this.db.table(TABLE_SCHEDULE_TRIGGERS)}`);
      await client.none(`DELETE FROM ${this.db.table(TABLE_SCHEDULES)}`);
    });
  }

  private selectSchedule(): string {
    return `SELECT id AS "id", JSON_SERIALIZE(target RETURNING CLOB) AS "target", cron AS "cron", timezone AS "timezone", status AS "status", next_fire_at AS "nextFireAt",
      last_fire_at AS "lastFireAt", last_run_id AS "lastRunId", created_at AS "createdAt", updated_at AS "updatedAt", JSON_SERIALIZE(metadata RETURNING CLOB) AS "metadata",
      owner_type AS "ownerType", owner_id AS "ownerId" FROM ${this.db.table(TABLE_SCHEDULES)}`;
  }
}

function rowToSchedule(row: Record<string, unknown>): Schedule {
  const target = parseOracleJson<ScheduleTarget>(row.target);
  if (!target) throw new Error(`Schedule ${String(row.id)} has an invalid target`);
  return {
    id: String(row.id),
    target: normalizeScheduleTarget(target),
    cron: String(row.cron),
    timezone: row.timezone == null ? undefined : String(row.timezone),
    status: String(row.status) as ScheduleStatus,
    nextFireAt: Number(row.nextFireAt),
    lastFireAt: row.lastFireAt == null ? undefined : Number(row.lastFireAt),
    lastRunId: row.lastRunId == null ? undefined : String(row.lastRunId),
    createdAt: Number(row.createdAt),
    updatedAt: Number(row.updatedAt),
    metadata: parseOracleJson<Record<string, unknown>>(row.metadata),
    ownerType: row.ownerType == null ? undefined : String(row.ownerType),
    ownerId: row.ownerId == null ? undefined : String(row.ownerId),
  };
}

function validateLimit(limit: number): void {
  if (!Number.isFinite(limit) || limit < 0 || !Number.isSafeInteger(Math.floor(limit)))
    throw new Error('Schedule query limit must be a non-negative finite number');
}

function retentionIndex(): OracleCreateIndexOptions {
  return {
    name: indexNameForTable('MASTRA_SCHEDULE_TRIGGERS_RETENTION', 'IDX'),
    table: TABLE_SCHEDULE_TRIGGERS,
    columns: ['actual_fire_at'],
  };
}

export function getDefaultScheduleIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    retentionIndex(),
    {
      name: indexNameForTable('MASTRA_SCHEDULES_STATUS_FIRE', 'IDX'),
      table: TABLE_SCHEDULES,
      columns: ['status', 'next_fire_at'],
    },
    {
      name: indexNameForTable('MASTRA_SCHEDULE_TRIGGERS_FIRE', 'IDX'),
      table: TABLE_SCHEDULE_TRIGGERS,
      columns: ['schedule_id', 'actual_fire_at DESC'],
    },
  ];
}
