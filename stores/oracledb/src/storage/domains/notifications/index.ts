import {
  NotificationsStorage,
  TABLE_NOTIFICATIONS,
  TABLE_SCHEMAS,
  executeRetentionPrune,
  resolveRetentionTargets,
  retentionCutoffMs,
} from '@mastra/core/storage';
import type {
  CreateNotificationInput,
  ListNotificationsInput,
  ListDueNotificationsInput,
  NotificationRecord,
  NotificationPriority,
  NotificationStatus,
  UpdateNotificationInput,
  UpdateNotificationsStatusInput,
  PruneOptions,
  PruneResult,
  RetentionTablesDescriptor,
  TableRetentionPolicy,
} from '@mastra/core/storage';
import oracledb from 'oracledb';

import { clobBind, jsonBindText } from '../../../shared/connection';
import { indexNameForTable } from '../../../vector/identifiers';
import { OracleDB, filterIndexesForTables, parseOracleJson } from '../../db';
import type { OracleCreateIndexOptions, OracleTxClient } from '../../db';
import { toDate } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

const SCHEMA = TABLE_SCHEMAS[TABLE_NOTIFICATIONS];
const LONG_TEXT = new Set(['summary', 'deliveryReason', 'lastDeliveryError']);
const TEXT_ALIASES: Record<string, string> = {
  summary: 'notificationSummary',
  deliveryReason: 'notificationReason',
  lastDeliveryError: 'notificationError',
};
const OPTIONAL_DATES = [
  'deliveredAt',
  'seenAt',
  'dismissedAt',
  'archivedAt',
  'discardedAt',
  'deliverAt',
  'summaryAt',
  'lastDeliveryAttemptAt',
] as const;
const OPTIONAL_STRINGS = [
  'resourceId',
  'agentId',
  'sourceId',
  'dedupeKey',
  'coalesceKey',
  'deliveredSignalId',
  'summarySignalId',
] as const;

function columnSql(field: string): string {
  if (!Object.hasOwn(SCHEMA, field)) throw new Error(`Unknown notification field: ${field}`);
  return field === field.toLowerCase() ? field : `"${field}"`;
}

function prepareFields(data: Record<string, unknown>): {
  columns: string[];
  values: string[];
  binds: Record<string, unknown>;
} {
  const columns: string[] = [];
  const values: string[] = [];
  const binds: Record<string, unknown> = {};
  for (const [field, value] of Object.entries(data)) {
    const name = `v${columns.length}`;
    const type = SCHEMA[field]?.type;
    columns.push(columnSql(field));
    if (type === 'jsonb') {
      values.push(`JSON(:${name})`);
      binds[name] = value === undefined ? null : clobBind(value === null ? 'null' : jsonBindText(value)!);
    } else if (type === 'timestamp') {
      values.push(`:${name}`);
      binds[name] = value == null ? null : { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: value };
    } else if (LONG_TEXT.has(field)) {
      values.push(value === '' ? `NVL(:${name}, EMPTY_CLOB())` : `:${name}`);
      binds[name] = value == null ? null : clobBind(String(value));
    } else {
      values.push(`:${name}`);
      binds[name] = value ?? null;
    }
  }
  return { columns, values, binds };
}

function statusTimestamp(status: NotificationStatus, now: Date): Record<string, Date> {
  if (status === 'delivered') return { deliveredAt: now };
  if (status === 'seen') return { seenAt: now };
  if (status === 'dismissed') return { dismissedAt: now };
  if (status === 'archived') return { archivedAt: now };
  if (status === 'discarded') return { discardedAt: now };
  return {};
}

export class NotificationsOracle extends NotificationsStorage {
  static readonly MANAGED_TABLES = [TABLE_NOTIFICATIONS] as const;
  static override readonly retentionTables: RetentionTablesDescriptor = {
    notifications: { table: TABLE_NOTIFICATIONS, column: 'createdAt', indexed: true },
  };
  private readonly db: OracleDB;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({ ...config, jsonColumns: ['payload', 'attributes', 'metadata'] });
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, NotificationsOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_NOTIFICATIONS,
      schema: SCHEMA,
      compositePrimaryKey: ['threadId', 'id'],
    });
    if (!this.skipDefaultIndexes)
      for (const index of getDefaultNotificationIndexDefinitions()) {
        try {
          await this.db.createIndex(index);
        } catch (error) {
          this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
        }
      }
    for (const index of this.indexes) await this.db.createIndex(index);
  }

  async createNotification(input: CreateNotificationInput): Promise<NotificationRecord> {
    const now = input.createdAt ?? new Date();
    const record: NotificationRecord = {
      id: input.id ?? crypto.randomUUID(),
      threadId: input.threadId,
      source: input.source,
      kind: input.kind,
      priority: input.priority ?? 'medium',
      status: 'pending',
      summary: input.summary,
      payload: input.payload,
      resourceId: input.resourceId,
      agentId: input.agentId,
      sourceId: input.sourceId,
      dedupeKey: input.dedupeKey,
      coalesceKey: input.coalesceKey,
      coalescedCount: 1,
      attributes: input.attributes,
      createdAt: now,
      updatedAt: now,
      deliverAt: input.deliverAt,
      summaryAt: input.summaryAt,
      deliveryReason: input.deliveryReason,
      deliveryAttempts: 0,
      metadata: input.metadata,
    };
    return this.db.tx(async client => {
      if (input.dedupeKey || input.coalesceKey) {
        // Follow Oracle observational memory's short table-lock pattern. It also covers the
        // first write, when there is no row to lock, without requiring DBMS_LOCK privileges.
        await client.none(`LOCK TABLE ${this.table()} IN SHARE ROW EXCLUSIVE MODE`);
        const existing = await this.findCoalescable(client, input);
        if (existing) {
          await this.updateRow(client, existing.threadId, existing.id, {
            summary: input.summary,
            payload: input.payload ?? existing.payload,
            priority: input.priority ?? existing.priority,
            attributes: input.attributes ? { ...existing.attributes, ...input.attributes } : existing.attributes,
            metadata: input.metadata ? { ...existing.metadata, ...input.metadata } : existing.metadata,
            updatedAt: new Date(),
            deliverAt: input.deliverAt ?? existing.deliverAt,
            summaryAt: input.summaryAt ?? existing.summaryAt,
            deliveryReason: input.deliveryReason ?? existing.deliveryReason,
            coalescedCount: (existing.coalescedCount ?? 1) + 1,
          });
          return this.getRequired(client, existing.threadId, existing.id);
        }
      }
      const fields = prepareFields(record);
      await client.none(
        `INSERT INTO ${this.table()} (${fields.columns.join(', ')}) VALUES (${fields.values.join(', ')})`,
        fields.binds,
      );
      return this.getRequired(client, record.threadId, record.id);
    });
  }

  async getNotification({ threadId, id }: { threadId: string; id: string }): Promise<NotificationRecord | null> {
    // The dispatcher rechecks delivery state here before sending a signal.
    const row = await this.db.oneOrNone(`${this.select()} WHERE "threadId" = :threadId AND id = :id`, {
      threadId,
      id,
    });
    return row ? rowToNotification(row) : null;
  }

  async updateNotification(input: UpdateNotificationInput): Promise<NotificationRecord> {
    const { threadId, id, ...patch } = input;
    const now = new Date();
    const data = Object.fromEntries(Object.entries(patch).filter(([, value]) => value !== undefined));
    return this.db.tx(async client => {
      await this.updateRow(client, threadId, id, {
        ...data,
        ...(input.status ? statusTimestamp(input.status, now) : {}),
        updatedAt: now,
      });
      return this.getRequired(client, threadId, id);
    });
  }

  override async updateNotificationsStatus(input: UpdateNotificationsStatusInput): Promise<NotificationRecord[]> {
    const ids = [...new Set(input.ids)].sort();
    if (!ids.length) return [];
    const now = new Date();
    const fields = prepareFields({ status: input.status, ...statusTimestamp(input.status, now), updatedAt: now });
    return this.db.tx(async client => {
      const result: NotificationRecord[] = [];
      for (let start = 0; start < ids.length; start += 500) {
        const keys: Record<string, unknown> = { threadId: input.threadId };
        const names = ids.slice(start, start + 500).map((id, index) => {
          const name = `id${index}`;
          keys[name] = id;
          return `:${name}`;
        });
        const where = `"threadId" = :threadId AND id IN (${names.join(', ')})`;
        await client.none(
          `UPDATE ${this.table()} SET ${fields.columns.map((column, index) => `${column} = ${fields.values[index]}`).join(', ')} WHERE ${where}`,
          { ...fields.binds, ...keys },
        );
        result.push(...(await client.manyOrNone(`${this.select()} WHERE ${where}`, keys)).map(rowToNotification));
      }
      return result;
    });
  }

  async listNotifications(input: ListNotificationsInput): Promise<NotificationRecord[]> {
    const conditions = ['"threadId" = :threadId'];
    const binds: Record<string, unknown> = { threadId: input.threadId };
    for (const field of ['status', 'priority'] as const) {
      const value = input[field];
      if (value === undefined) continue;
      const items = Array.isArray(value) ? value : [value];
      const names = items.map((item, index) => {
        const name = `${field}${index}`;
        binds[name] = item;
        return `:${name}`;
      });
      conditions.push(names.length ? `${field} IN (${names.join(', ')})` : '1 = 0');
    }
    for (const field of ['source', 'resourceId', 'agentId'] as const) {
      if (!input[field]) continue;
      conditions.push(`${columnSql(field)} = :${field}`);
      binds[field] = input[field];
    }
    if (input.search) {
      conditions.push(
        '(INSTR(LOWER(summary), LOWER(:search)) > 0 OR INSTR(LOWER(kind), LOWER(:search)) > 0 OR INSTR(LOWER(source), LOWER(:search)) > 0)',
      );
      binds.search = input.search;
    }
    const limit = limitClause(input.limit, binds);
    return (
      await this.db.read.manyOrNone(
        `${this.select()} WHERE ${conditions.join(' AND ')} ORDER BY "updatedAt" DESC, id${limit}`,
        binds,
      )
    ).map(rowToNotification);
  }

  async listDueNotifications(input: ListDueNotificationsInput): Promise<NotificationRecord[]> {
    const conditions = ["status = 'pending'", '("deliverAt" <= :now OR "summaryAt" <= :now)'];
    const binds: Record<string, unknown> = { now: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: input.now } };
    for (const field of ['agentId', 'resourceId'] as const) {
      if (!input[field]) continue;
      conditions.push(`${columnSql(field)} = :${field}`);
      binds[field] = input[field];
    }
    const limit = limitClause(input.limit, binds);
    return (
      await this.db.manyOrNone(
        `${this.select()} WHERE ${conditions.join(' AND ')}
      ORDER BY CASE WHEN "deliverAt" IS NULL THEN "summaryAt" WHEN "summaryAt" IS NULL THEN "deliverAt"
        WHEN "deliverAt" <= "summaryAt" THEN "deliverAt" ELSE "summaryAt" END ASC, "updatedAt" ASC, id${limit}`,
        binds,
      )
    ).map(rowToNotification);
  }

  async prune(policies: Record<string, TableRetentionPolicy>, options?: PruneOptions): Promise<PruneResult[]> {
    if (policies.notifications && !options?.signal?.aborted) {
      try {
        await this.db.createIndex(retentionIndex());
      } catch (error) {
        this.logger?.warn?.(`Failed to create retention index for ${TABLE_NOTIFICATIONS}:`, error);
      }
    }
    const targets = resolveRetentionTargets({
      policies,
      descriptor: NotificationsOracle.retentionTables,
      order: ['notifications'],
    });
    return executeRetentionPrune({
      domain: 'notifications',
      targets,
      options,
      cutoffFor: (target, now) => new Date(retentionCutoffMs(target.policy, now)),
      deleteBatch: (target, cutoff, limit) =>
        this.db.pruneBatch({ tableName: target.table, column: target.column, cutoff, limit }),
    });
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.clearTable(TABLE_NOTIFICATIONS);
  }

  private table(): string {
    return this.db.table(TABLE_NOTIFICATIONS);
  }

  private async findCoalescable(
    client: OracleTxClient,
    input: CreateNotificationInput,
  ): Promise<NotificationRecord | undefined> {
    const row = await client.oneOrNone(
      `${this.select()}
      WHERE "threadId" = :threadId AND source = :source AND kind = :kind AND status = 'pending'
      AND ("agentId" = :agentId OR ("agentId" IS NULL AND :agentId IS NULL))
      AND ("resourceId" = :resourceId OR ("resourceId" IS NULL AND :resourceId IS NULL))
      AND ((:dedupeKey IS NOT NULL AND "dedupeKey" = :dedupeKey) OR (:coalesceKey IS NOT NULL AND "coalesceKey" = :coalesceKey))
      ORDER BY "updatedAt" DESC, id FETCH FIRST 1 ROW ONLY`,
      {
        threadId: input.threadId,
        source: input.source,
        kind: input.kind,
        agentId: input.agentId ?? null,
        resourceId: input.resourceId ?? null,
        dedupeKey: input.dedupeKey ?? null,
        coalesceKey: input.coalesceKey ?? null,
      },
    );
    return row ? rowToNotification(row) : undefined;
  }

  private select(): string {
    const fields = Object.entries(SCHEMA).map(
      ([field, definition]) =>
        `${definition.type === 'jsonb' ? `JSON_SERIALIZE(${columnSql(field)} RETURNING CLOB)` : columnSql(field)} AS "${TEXT_ALIASES[field] ?? field}"`,
    );
    for (const field of ['payload', 'deliveryReason', 'lastDeliveryError'])
      fields.push(`CASE WHEN ${columnSql(field)} IS NULL THEN 1 ELSE 0 END AS "${field}IsNull"`);
    return `SELECT ${fields.join(', ')} FROM ${this.table()}`;
  }

  private async getRequired(client: OracleTxClient, threadId: string, id: string): Promise<NotificationRecord> {
    const row = await client.oneOrNone(`${this.select()} WHERE "threadId" = :threadId AND id = :id`, { threadId, id });
    if (!row) throw new Error(`Notification ${id} was not found for thread ${threadId}`);
    return rowToNotification(row);
  }

  private async updateRow(
    client: OracleTxClient,
    threadId: string,
    id: string,
    data: Record<string, unknown>,
  ): Promise<void> {
    const fields = prepareFields(data);
    await client.none(
      `UPDATE ${this.table()} SET ${fields.columns.map((column, index) => `${column} = ${fields.values[index]}`).join(', ')} WHERE "threadId" = :threadId AND id = :id`,
      { ...fields.binds, threadId, id },
    );
  }
}

function rowToNotification(row: Record<string, unknown>): NotificationRecord {
  const record: NotificationRecord = {
    id: String(row.id),
    threadId: String(row.threadId),
    source: String(row.source),
    kind: String(row.kind),
    priority: String(row.priority) as NotificationPriority,
    status: String(row.status) as NotificationStatus,
    summary: String(row.notificationSummary ?? ''),
    payload: Number(row.payloadIsNull) === 1 ? undefined : parseOracleJson(row.payload),
    coalescedCount: Number(row.coalescedCount ?? 1),
    attributes: parseOracleJson<NotificationRecord['attributes']>(row.attributes),
    metadata: parseOracleJson<NotificationRecord['metadata']>(row.metadata),
    createdAt: toDate(row.createdAt),
    updatedAt: toDate(row.updatedAt),
    deliveryAttempts: Number(row.deliveryAttempts ?? 0),
    deliveryReason: Number(row.deliveryReasonIsNull) === 1 ? undefined : String(row.notificationReason ?? ''),
    lastDeliveryError: Number(row.lastDeliveryErrorIsNull) === 1 ? undefined : String(row.notificationError ?? ''),
  };
  for (const field of OPTIONAL_DATES) record[field] = row[field] == null ? undefined : toDate(row[field]);
  for (const field of OPTIONAL_STRINGS) record[field] = row[field] == null ? undefined : String(row[field]);
  return record;
}

function limitClause(limit: number | undefined, binds: Record<string, unknown>): string {
  if (limit === undefined) return '';
  if (!Number.isSafeInteger(limit) || limit < 0)
    throw new Error('Notification limit must be a non-negative safe integer');
  binds.limit = limit;
  return ' FETCH FIRST :limit ROWS ONLY';
}

function retentionIndex(): OracleCreateIndexOptions {
  return {
    name: indexNameForTable('MASTRA_NOTIFICATIONS_RETENTION', 'IDX'),
    table: TABLE_NOTIFICATIONS,
    columns: ['createdAt'],
  };
}

export function getDefaultNotificationIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    retentionIndex(),
    {
      name: indexNameForTable('MASTRA_NOTIFICATIONS_THREAD_STATUS', 'IDX'),
      table: TABLE_NOTIFICATIONS,
      columns: ['threadId', 'status', 'updatedAt'],
    },
    {
      name: indexNameForTable('MASTRA_NOTIFICATIONS_COALESCE', 'IDX'),
      table: TABLE_NOTIFICATIONS,
      columns: ['threadId', 'source', 'kind', 'status', 'agentId', 'resourceId', 'dedupeKey', 'coalesceKey'],
    },
    {
      name: indexNameForTable('MASTRA_NOTIFICATIONS_DUE', 'IDX'),
      table: TABLE_NOTIFICATIONS,
      columns: ['status', 'deliverAt', 'summaryAt'],
    },
  ];
}
