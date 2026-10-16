import {
  ExperimentsStorage,
  TABLE_EXPERIMENTS,
  TABLE_EXPERIMENT_RESULTS,
  TABLE_DATASETS,
  TABLE_DATASET_ITEMS,
  TABLE_SCHEMAS,
  normalizePerPage,
  calculatePagination,
  resolveListOrderBy,
  runRetentionBatches,
  retentionCutoffMs,
} from '@mastra/core/storage';
import type {
  Experiment,
  ExperimentResult,
  ExperimentReviewCounts,
  ExperimentTenancyFilters,
  CreateExperimentInput,
  UpdateExperimentInput,
  AddExperimentResultInput,
  UpsertExperimentResultInput,
  UpdateExperimentResultInput,
  ListExperimentsInput,
  ListExperimentsOutput,
  ListExperimentResultsInput,
  ListExperimentResultsOutput,
  StorageColumn,
  StoragePagination,
  PruneOptions,
  PruneResult,
  RetentionTablesDescriptor,
  TableRetentionPolicy,
} from '@mastra/core/storage';
import oracledb from 'oracledb';

import { asBindParameters, clobBind, isOracleErrorCode, rollbackQuietly } from '../../../shared/connection';
import { indexNameForTable } from '../../../vector/identifiers';
import { OracleDB, filterIndexesForTables } from '../../db';
import type { OracleCreateIndexOptions, OracleTxClient } from '../../db';
import { toDate } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

// Caller-driven experiments can reference external dataset/item identities. A versioned item's
// id is not unique by itself, so the neutral schema's item-id FK cannot be installed in Oracle.
export const ORACLE_EXPERIMENTS_SCHEMA: Record<string, StorageColumn> = {
  ...TABLE_SCHEMAS[TABLE_EXPERIMENTS],
  datasetId: { ...TABLE_SCHEMAS[TABLE_EXPERIMENTS].datasetId!, references: undefined },
};
export const ORACLE_EXPERIMENT_RESULTS_SCHEMA: Record<string, StorageColumn> = {
  ...TABLE_SCHEMAS[TABLE_EXPERIMENT_RESULTS],
  itemId: { ...TABLE_SCHEMAS[TABLE_EXPERIMENT_RESULTS].itemId!, references: undefined },
};
type ExperimentTable = typeof TABLE_EXPERIMENTS | typeof TABLE_EXPERIMENT_RESULTS;
const UPDATE_FIELDS = [
  'name',
  'description',
  'metadata',
  'status',
  'totalItems',
  'succeededCount',
  'failedCount',
  'skippedCount',
  'startedAt',
  'completedAt',
] as const;

function column(field: string): string {
  return field === 'comment' || field !== field.toLowerCase() ? `"${field}"` : field;
}
function schema(table: ExperimentTable) {
  return table === TABLE_EXPERIMENTS ? ORACLE_EXPERIMENTS_SCHEMA : ORACLE_EXPERIMENT_RESULTS_SCHEMA;
}
function jsonField<T = unknown>(row: Record<string, unknown>, field: string): T | null {
  const text = row[`${field}Json`];
  return text == null ? null : (JSON.parse(String(text)) as T);
}
function scope(conditions: string[], binds: Record<string, unknown>, filters?: ExperimentTenancyFilters): void {
  for (const field of ['organizationId', 'projectId'] as const)
    if (filters?.[field] !== undefined) {
      conditions.push(`${column(field)} = :${field}`);
      binds[field] = filters[field];
    }
}
function pageInfo(total: number, input: StoragePagination) {
  if (!Number.isSafeInteger(input.page) || input.page < 0) throw new Error('page must be a non-negative safe integer');
  const perPage = normalizePerPage(input.perPage, 100);
  const { offset, perPage: perPageForResponse } = calculatePagination(input.page, input.perPage, perPage);
  return {
    offset,
    limit: input.perPage === false ? total : perPage,
    pagination: {
      total,
      page: input.page,
      perPage: perPageForResponse,
      hasMore: input.perPage === false ? false : offset + perPage < total,
    },
  };
}
function prepare(table: ExperimentTable, data: Record<string, unknown>) {
  const columns: string[] = [];
  const values: string[] = [];
  const binds: Record<string, unknown> = {};
  for (const [field, value] of Object.entries(data)) {
    if (!Object.hasOwn(schema(table), field)) throw new Error(`Unknown experiment field: ${field}`);
    const definition = schema(table)[field]!;
    const name = `v${columns.length}`;
    columns.push(column(field));
    if (definition.type === 'jsonb') {
      values.push(`JSON(:${name})`);
      binds[name] = value == null && field !== 'input' ? null : clobBind(JSON.stringify(value ?? null));
    } else if (definition.type === 'timestamp') {
      values.push(`:${name}`);
      binds[name] = value == null ? null : { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: value };
    } else if (['name', 'description', 'comment'].includes(field)) {
      values.push(value === '' ? `NVL(:${name}, EMPTY_CLOB())` : `:${name}`);
      binds[name] = value == null ? null : clobBind(String(value));
    } else {
      values.push(`:${name}`);
      binds[name] = value ?? null;
    }
  }
  return { columns, values, binds };
}

export class ExperimentsOracle extends ExperimentsStorage {
  static readonly MANAGED_TABLES = [TABLE_EXPERIMENTS, TABLE_EXPERIMENT_RESULTS] as const;
  static override readonly retentionTables: RetentionTablesDescriptor = {
    experiments: { table: TABLE_EXPERIMENTS, column: 'completedAt', indexed: true },
  };
  private readonly db: OracleDB;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB(config);
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, ExperimentsOracle.MANAGED_TABLES);
  }
  async init(): Promise<void> {
    for (const tableName of ExperimentsOracle.MANAGED_TABLES)
      await this.db.createTable({ tableName, schema: schema(tableName) });
    for (const index of getDefaultExperimentIndexDefinitions()) {
      if (this.skipDefaultIndexes && !index.unique) continue;
      try {
        await this.db.createIndex(index);
      } catch (error) {
        if (index.unique) throw error;
        this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
      }
    }
    for (const index of this.indexes) await this.db.createIndex(index);
  }
  async createExperiment(input: CreateExperimentInput): Promise<Experiment> {
    const id = input.id ?? crypto.randomUUID();
    const now = new Date();
    return this.db.tx(async client => {
      await this.insert(client, TABLE_EXPERIMENTS, {
        ...input,
        id,
        status: 'pending',
        succeededCount: 0,
        failedCount: 0,
        skippedCount: 0,
        startedAt: null,
        completedAt: null,
        createdAt: now,
        updatedAt: now,
      });
      return mapExperiment(await client.one(`${this.select(TABLE_EXPERIMENTS)} WHERE id = :id`, { id }));
    });
  }
  async updateExperiment(input: UpdateExperimentInput): Promise<Experiment> {
    return this.db.tx(async client => {
      const existing = await client.oneOrNone(
        `SELECT id AS "id" FROM ${this.db.table(TABLE_EXPERIMENTS)} WHERE id = :id FOR UPDATE`,
        { id: input.id },
      );
      if (!existing) throw new Error(`Experiment ${input.id} not found`);
      const data: Record<string, unknown> = { updatedAt: new Date() };
      for (const field of UPDATE_FIELDS) if (input[field] !== undefined) data[field] = input[field];
      await this.update(client, TABLE_EXPERIMENTS, data, 'id = :id', { id: input.id });
      return mapExperiment(await client.one(`${this.select(TABLE_EXPERIMENTS)} WHERE id = :id`, { id: input.id }));
    });
  }
  async getExperimentById({
    id,
    filters,
  }: {
    id: string;
    filters?: ExperimentTenancyFilters;
  }): Promise<Experiment | null> {
    const conditions = ['id = :id'];
    const binds: Record<string, unknown> = { id };
    scope(conditions, binds, filters);
    const row = await this.db.read.oneOrNone(
      `${this.select(TABLE_EXPERIMENTS)} WHERE ${conditions.join(' AND ')}`,
      binds,
    );
    return row ? mapExperiment(row) : null;
  }
  async listExperiments(args: ListExperimentsInput): Promise<ListExperimentsOutput> {
    const order = resolveListOrderBy(args.orderBy, ['createdAt', 'status'], { field: 'createdAt', direction: 'DESC' });
    const conditions: string[] = [];
    const binds: Record<string, unknown> = {};
    for (const field of [
      'datasetId',
      'targetType',
      'targetId',
      'agentVersion',
      'status',
      'experimentSetId',
      'comparisonId',
      'variantId',
      'trialIndex',
    ] as const) {
      if (args[field] === undefined) continue;
      conditions.push(`${column(field)} = :${field}`);
      binds[field] = args[field];
    }
    scope(conditions, binds, args.filters);
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const total = Number(
      (
        await this.db.read.one<{ count: number }>(
          `SELECT COUNT(*) AS "count" FROM ${this.db.table(TABLE_EXPERIMENTS)} ${where}`,
          binds,
        )
      ).count,
    );
    const page = pageInfo(total, args.pagination);
    const rows = page.limit
      ? await this.db.read.manyOrNone(
          `${this.select(TABLE_EXPERIMENTS)} ${where} ORDER BY ${column(order.field)} ${order.direction}, id OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
          { ...binds, offset: page.offset, limit: page.limit },
        )
      : [];
    return { experiments: rows.map(mapExperiment), pagination: page.pagination };
  }
  async deleteExperiment({ id, filters }: { id: string; filters?: ExperimentTenancyFilters }): Promise<void> {
    const conditions = ['id = :id'];
    const binds: Record<string, unknown> = { id };
    scope(conditions, binds, filters);
    await this.db.tx(async client => {
      const parent = await client.oneOrNone(
        `SELECT id AS "id" FROM ${this.db.table(TABLE_EXPERIMENTS)} WHERE ${conditions.join(' AND ')} FOR UPDATE`,
        binds,
      );
      if (!parent) return;
      await client.none(`DELETE FROM ${this.db.table(TABLE_EXPERIMENT_RESULTS)} WHERE "experimentId" = :id`, { id });
      await client.none(`DELETE FROM ${this.db.table(TABLE_EXPERIMENTS)} WHERE ${conditions.join(' AND ')}`, binds);
    });
  }

  async addExperimentResult(input: AddExperimentResultInput): Promise<ExperimentResult> {
    return this.db.tx(async client => {
      const purge = await this.lockResultOwner(client, input.experimentId, input.itemId);
      const id = input.id ?? crypto.randomUUID();
      await this.insert(client, TABLE_EXPERIMENT_RESULTS, { ...resultRecord(input, purge), id, createdAt: new Date() });
      return mapResult(await client.one(`${this.select(TABLE_EXPERIMENT_RESULTS)} WHERE id = :id`, { id }));
    });
  }

  async upsertExperimentResult(input: UpsertExperimentResultInput): Promise<ExperimentResult> {
    return this.db.tx(async client => {
      const purge = await this.lockResultOwner(client, input.experimentId, input.itemId);
      const attempt = input.attempt ?? 0;
      const existing = await client.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.db.table(TABLE_EXPERIMENT_RESULTS)}
        WHERE "experimentId" = :experimentId AND "itemId" = :itemId AND NVL(attempt, 0) = :attempt FOR UPDATE`,
        { experimentId: input.experimentId, itemId: input.itemId, attempt },
      );
      const id = existing?.id ?? crypto.randomUUID();
      const data = resultRecord(input, purge);
      if (existing) {
        // Replacement preserves the row identity, creation instant, and reviewer-authored comment.
        await this.update(client, TABLE_EXPERIMENT_RESULTS, data, 'id = :id', { id });
      } else {
        await this.insert(client, TABLE_EXPERIMENT_RESULTS, { ...data, id, createdAt: new Date() });
      }
      return mapResult(await client.one(`${this.select(TABLE_EXPERIMENT_RESULTS)} WHERE id = :id`, { id }));
    });
  }

  async updateExperimentResult(input: UpdateExperimentResultInput): Promise<ExperimentResult> {
    return this.db.tx(async client => {
      const conditions = ['id = :id'];
      const binds: Record<string, unknown> = { id: input.id };
      if (input.experimentId !== undefined) {
        conditions.push('"experimentId" = :experimentId');
        binds.experimentId = input.experimentId;
      }
      const owner = await client.oneOrNone<{ experimentId: string; itemId: string }>(
        `SELECT "experimentId", "itemId" FROM ${this.db.table(TABLE_EXPERIMENT_RESULTS)} WHERE ${conditions.join(' AND ')}`,
        binds,
      );
      if (!owner) throw new Error(`Experiment result ${input.id} not found`);
      const purge = await this.lockResultOwner(client, owner.experimentId, owner.itemId);
      const data: Record<string, unknown> = {};
      if (input.status !== undefined) data.status = input.status;
      if (purge) {
        data.tags = null;
        data.comment = null;
      } else {
        if (input.tags !== undefined) data.tags = input.tags;
        if (input.comment !== undefined) data.comment = input.comment;
      }
      await this.update(client, TABLE_EXPERIMENT_RESULTS, data, conditions.join(' AND '), binds);
      const updated = await client.oneOrNone(
        `${this.select(TABLE_EXPERIMENT_RESULTS)} WHERE ${conditions.join(' AND ')}`,
        binds,
      );
      if (!updated) throw new Error(`Experiment result ${input.id} not found`);
      return mapResult(updated);
    });
  }

  private async lockResultOwner(
    client: OracleTxClient,
    experimentId: string,
    itemId: string,
  ): Promise<Record<string, unknown> | null> {
    const hint = await client.oneOrNone<{ datasetId: string | null }>(
      `SELECT "datasetId" FROM ${this.db.table(TABLE_EXPERIMENTS)} WHERE id = :id`,
      { id: experimentId },
    );
    if (!hint) throw new Error(`Experiment ${experimentId} not found`);
    let datasetLocked = false;
    if (hint.datasetId) {
      try {
        datasetLocked = !!(await client.oneOrNone(
          `SELECT id AS "id" FROM ${this.db.table(TABLE_DATASETS)} WHERE id = :id FOR UPDATE`,
          { id: hint.datasetId },
        ));
      } catch (error) {
        // Standalone caller-driven experiment stores need no local dataset domain.
        if (!isOracleErrorCode(error, [-942])) throw error;
      }
    }
    const owner = await client.oneOrNone<{ datasetId: string | null }>(
      `SELECT "datasetId" FROM ${this.db.table(TABLE_EXPERIMENTS)} WHERE id = :id FOR UPDATE`,
      { id: experimentId },
    );
    if (!owner) throw new Error(`Experiment ${experimentId} not found`);
    if (owner.datasetId !== hint.datasetId)
      throw new Error('Experiment changed while writing its result; retry the submission');
    if (!datasetLocked || !owner.datasetId) return null;
    const row = await client.oneOrNone<{ metadataJson: string }>(
      `SELECT JSON_SERIALIZE(metadata RETURNING CLOB) AS "metadataJson"
      FROM ${this.db.table(TABLE_DATASET_ITEMS)} WHERE id = :itemId AND "datasetId" = :datasetId
      AND JSON_EXISTS(metadata, '$?(@.__purged.type() == "boolean" && @.__purged == true)') FETCH FIRST 1 ROW ONLY`,
      { itemId, datasetId: owner.datasetId },
    );
    return row ? (JSON.parse(row.metadataJson) as Record<string, unknown>) : null;
  }

  async prune(policies: Record<string, TableRetentionPolicy>, options?: PruneOptions): Promise<PruneResult[]> {
    const policy = policies.experiments;
    if (!policy) return [];
    if (options?.signal?.aborted)
      return [
        { domain: 'experiments', table: TABLE_EXPERIMENT_RESULTS, deleted: 0, done: false },
        { domain: 'experiments', table: TABLE_EXPERIMENTS, deleted: 0, done: false },
      ];
    try {
      await this.db.createIndex(retentionIndex());
    } catch (error) {
      this.logger?.warn?.(`Failed to create retention index for ${TABLE_EXPERIMENTS}:`, error);
    }
    const cutoff = new Date(retentionCutoffMs(policy));
    let childDeleted = 0;
    const parent = await runRetentionBatches({
      batchSize: policy.batchSize ?? 1000,
      options,
      deleteBatch: limit =>
        this.db.withConnection(async connection => {
          let children = 0;
          try {
            const table = this.db.table(TABLE_EXPERIMENTS);
            const selected = await connection.execute<{ id: string }>(
              `SELECT id AS "id" FROM ${table}
            WHERE ROWID IN (SELECT ROWID FROM ${table} WHERE "completedAt" < :cutoff ORDER BY "completedAt", id FETCH FIRST :limit ROWS ONLY)
              AND "completedAt" < :cutoff ORDER BY "completedAt", id FOR UPDATE`,
              asBindParameters({ cutoff, limit }),
              { outFormat: oracledb.OUT_FORMAT_OBJECT },
            );
            const ids = (selected.rows ?? []).map(row => row.id);
            let parents = 0;
            for (let start = 0; start < ids.length; start += 500) {
              const binds: Record<string, string> = {};
              const names = ids.slice(start, start + 500).map((id, index) => {
                const name = `id${index}`;
                binds[name] = id;
                return `:${name}`;
              });
              const removedChildren = await connection.execute(
                `DELETE FROM ${this.db.table(TABLE_EXPERIMENT_RESULTS)} WHERE "experimentId" IN (${names.join(', ')})`,
                binds,
              );
              const removedParents = await connection.execute(
                `DELETE FROM ${table} WHERE id IN (${names.join(', ')})`,
                binds,
              );
              children += removedChildren.rowsAffected ?? 0;
              parents += removedParents.rowsAffected ?? 0;
            }
            await connection.commit();
            childDeleted += children;
            return parents;
          } catch (error) {
            await rollbackQuietly(connection);
            throw error;
          }
        }),
    });
    return [
      { domain: 'experiments', table: TABLE_EXPERIMENT_RESULTS, deleted: childDeleted, done: parent.done },
      { domain: 'experiments', table: TABLE_EXPERIMENTS, deleted: parent.deleted, done: parent.done },
    ];
  }

  async getExperimentResultById({
    id,
    filters,
  }: {
    id: string;
    filters?: ExperimentTenancyFilters;
  }): Promise<ExperimentResult | null> {
    const conditions = ['id = :id'];
    const binds: Record<string, unknown> = { id };
    scope(conditions, binds, filters);
    const row = await this.db.read.oneOrNone(
      `${this.select(TABLE_EXPERIMENT_RESULTS)} WHERE ${conditions.join(' AND ')}`,
      binds,
    );
    return row ? mapResult(row) : null;
  }
  async listExperimentResults(args: ListExperimentResultsInput): Promise<ListExperimentResultsOutput> {
    const order = resolveListOrderBy(args.orderBy, ['startedAt', 'createdAt'], {
      field: 'startedAt',
      direction: 'ASC',
    });
    const conditions = ['"experimentId" = :experimentId'];
    const binds: Record<string, unknown> = { experimentId: args.experimentId };
    for (const field of ['traceId', 'status'] as const)
      if (args[field]) {
        conditions.push(`${column(field)} = :${field}`);
        binds[field] = args[field];
      }
    for (const [index, tag] of (args.tags ?? []).entries()) {
      if (tag === '') conditions.push(`JSON_EXISTS(tags, '$[*]?(@ == "")')`);
      else {
        const name = `tag${index}`;
        binds[name] = tag;
        conditions.push(`JSON_EXISTS(tags, '$[*]?(@ == $tag)' PASSING :${name} AS "tag")`);
      }
    }
    scope(conditions, binds, args.filters);
    const where = `WHERE ${conditions.join(' AND ')}`;
    const total = Number(
      (
        await this.db.read.one<{ count: number }>(
          `SELECT COUNT(*) AS "count" FROM ${this.db.table(TABLE_EXPERIMENT_RESULTS)} ${where}`,
          binds,
        )
      ).count,
    );
    const page = pageInfo(total, args.pagination);
    const rows = page.limit
      ? await this.db.read.manyOrNone(
          `${this.select(TABLE_EXPERIMENT_RESULTS)} ${where} ORDER BY ${column(order.field)} ${order.direction}, id OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
          { ...binds, offset: page.offset, limit: page.limit },
        )
      : [];
    return { results: rows.map(mapResult), pagination: page.pagination };
  }
  async deleteExperimentResults({
    experimentId,
    filters,
  }: {
    experimentId: string;
    filters?: ExperimentTenancyFilters;
  }): Promise<void> {
    const conditions = ['"experimentId" = :experimentId'];
    const binds: Record<string, unknown> = { experimentId };
    scope(conditions, binds, filters);
    await this.db.none(
      `DELETE FROM ${this.db.table(TABLE_EXPERIMENT_RESULTS)} WHERE ${conditions.join(' AND ')}`,
      binds,
    );
  }
  async getReviewSummary(): Promise<ExperimentReviewCounts[]> {
    const rows = await this.db.read.manyOrNone(`SELECT "experimentId", COUNT(*) AS "total",
      SUM(CASE WHEN status = 'needs-review' THEN 1 ELSE 0 END) AS "needsReview", SUM(CASE WHEN status = 'reviewed' THEN 1 ELSE 0 END) AS "reviewed",
      SUM(CASE WHEN status = 'complete' THEN 1 ELSE 0 END) AS "complete" FROM ${this.db.table(TABLE_EXPERIMENT_RESULTS)} GROUP BY "experimentId"`);
    return rows.map(row => ({
      experimentId: String(row.experimentId),
      total: Number(row.total),
      needsReview: Number(row.needsReview),
      reviewed: Number(row.reviewed),
      complete: Number(row.complete),
    }));
  }
  async dangerouslyClearAll(): Promise<void> {
    await this.db.tx(async client => {
      await client.none(`DELETE FROM ${this.db.table(TABLE_EXPERIMENT_RESULTS)}`);
      await client.none(`DELETE FROM ${this.db.table(TABLE_EXPERIMENTS)}`);
    });
  }
  private select(table: ExperimentTable): string {
    const fields = Object.entries(schema(table)).map(([field, definition]) =>
      definition.type === 'jsonb'
        ? `JSON_SERIALIZE(${column(field)} RETURNING CLOB) AS "${field}Json"`
        : `${column(field)} AS "${field === 'name' ? 'experimentName' : field === 'comment' ? 'experimentComment' : field}"`,
    );
    for (const field of table === TABLE_EXPERIMENTS ? ['name', 'description'] : ['comment'])
      fields.push(`CASE WHEN ${column(field)} IS NULL THEN 1 ELSE 0 END AS "${field}IsNull"`);
    return `SELECT ${fields.join(', ')} FROM ${this.db.table(table)}`;
  }
  private async insert(
    client: Pick<OracleTxClient, 'none'>,
    table: ExperimentTable,
    record: Record<string, unknown>,
  ): Promise<void> {
    const fields = prepare(table, record);
    await client.none(
      `INSERT INTO ${this.db.table(table)} (${fields.columns.join(', ')}) VALUES (${fields.values.join(', ')})`,
      fields.binds,
    );
  }
  private async update(
    client: Pick<OracleTxClient, 'none'>,
    table: ExperimentTable,
    data: Record<string, unknown>,
    where: string,
    keys: Record<string, unknown>,
  ): Promise<void> {
    const fields = prepare(table, data);
    if (!fields.columns.length) return;
    await client.none(
      `UPDATE ${this.db.table(table)} SET ${fields.columns.map((field, index) => `${field} = ${fields.values[index]}`).join(', ')} WHERE ${where}`,
      { ...fields.binds, ...keys },
    );
  }
}

function resultRecord(
  input: AddExperimentResultInput | UpsertExperimentResultInput,
  purge: Record<string, unknown> | null,
): Record<string, unknown> {
  return {
    experimentId: input.experimentId,
    itemId: input.itemId,
    itemDatasetVersion: input.itemDatasetVersion ?? null,
    organizationId: input.organizationId ?? null,
    projectId: input.projectId ?? null,
    input: purge ? null : input.input,
    output: purge ? null : (input.output ?? null),
    groundTruth: purge ? null : (input.groundTruth ?? null),
    metadata: purge ?? input.metadata ?? null,
    error: purge ? null : (input.error ?? null),
    startedAt: input.startedAt,
    completedAt: input.completedAt,
    retryCount: input.retryCount,
    attempt: input.attempt ?? 0,
    traceId: input.traceId ?? null,
    status: input.status ?? null,
    tags: purge ? null : (input.tags ?? null),
    toolMockReport: purge ? null : (input.toolMockReport ?? null),
  };
}

function mapExperiment(row: Record<string, unknown>): Experiment {
  return {
    id: String(row.id),
    name: Number(row.nameIsNull) === 1 ? undefined : String(row.experimentName ?? ''),
    description: Number(row.descriptionIsNull) === 1 ? undefined : String(row.description ?? ''),
    metadata: jsonField<Record<string, unknown>>(row, 'metadata') ?? undefined,
    provenance: jsonField(row, 'provenance'),
    runnerAttestation: jsonField(row, 'runnerAttestation'),
    experimentSetId: row.experimentSetId == null ? null : String(row.experimentSetId),
    comparisonId: row.comparisonId == null ? null : String(row.comparisonId),
    variantId: row.variantId == null ? null : String(row.variantId),
    trialIndex: row.trialIndex == null ? null : Number(row.trialIndex),
    datasetId: row.datasetId == null ? null : String(row.datasetId),
    datasetVersion: row.datasetVersion == null ? null : Number(row.datasetVersion),
    targetType: row.targetType as Experiment['targetType'],
    targetId: row.targetId == null ? null : String(row.targetId),
    scorerIds: jsonField(row, 'scorerIds'),
    status: row.status as Experiment['status'],
    totalItems: Number(row.totalItems),
    succeededCount: Number(row.succeededCount),
    failedCount: Number(row.failedCount),
    skippedCount: Number(row.skippedCount ?? 0),
    agentVersion: row.agentVersion == null ? null : String(row.agentVersion),
    organizationId: row.organizationId == null ? null : String(row.organizationId),
    projectId: row.projectId == null ? null : String(row.projectId),
    startedAt: row.startedAt == null ? null : toDate(row.startedAt),
    completedAt: row.completedAt == null ? null : toDate(row.completedAt),
    createdAt: toDate(row.createdAt),
    updatedAt: toDate(row.updatedAt),
  };
}
function mapResult(row: Record<string, unknown>): ExperimentResult {
  return {
    id: String(row.id),
    experimentId: String(row.experimentId),
    itemId: String(row.itemId),
    itemDatasetVersion: row.itemDatasetVersion == null ? null : Number(row.itemDatasetVersion),
    input: jsonField(row, 'input'),
    output: jsonField(row, 'output'),
    groundTruth: jsonField(row, 'groundTruth'),
    metadata: jsonField(row, 'metadata'),
    error: jsonField(row, 'error'),
    startedAt: toDate(row.startedAt),
    completedAt: toDate(row.completedAt),
    retryCount: Number(row.retryCount),
    attempt: Number(row.attempt ?? 0),
    traceId: row.traceId == null ? null : String(row.traceId),
    status: row.status as ExperimentResult['status'],
    tags: jsonField(row, 'tags'),
    comment: Number(row.commentIsNull) === 1 ? null : String(row.experimentComment ?? ''),
    toolMockReport: jsonField(row, 'toolMockReport'),
    organizationId: row.organizationId == null ? null : String(row.organizationId),
    projectId: row.projectId == null ? null : String(row.projectId),
    createdAt: toDate(row.createdAt),
  };
}
function retentionIndex(): OracleCreateIndexOptions {
  return {
    name: indexNameForTable('MASTRA_EXPERIMENTS_RETENTION', 'IDX'),
    table: TABLE_EXPERIMENTS,
    columns: ['completedAt'],
  };
}
export function getDefaultExperimentIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    retentionIndex(),
    { name: indexNameForTable('MASTRA_EXPERIMENT_DATASET', 'IDX'), table: TABLE_EXPERIMENTS, columns: ['datasetId'] },
    {
      name: indexNameForTable('MASTRA_EXPERIMENT_GROUPING', 'IDX'),
      table: TABLE_EXPERIMENTS,
      columns: ['experimentSetId', 'comparisonId', 'variantId', 'trialIndex'],
    },
    {
      name: indexNameForTable('MASTRA_EXPERIMENT_RESULTS_IDENTITY', 'IDX'),
      table: TABLE_EXPERIMENT_RESULTS,
      columns: ['experimentId', 'itemId', 'NVL(attempt, 0)'],
      unique: true,
    },
    {
      name: indexNameForTable('MASTRA_EXPERIMENT_SCOPE', 'IDX'),
      table: TABLE_EXPERIMENTS,
      columns: ['organizationId', 'projectId'],
    },
    {
      name: indexNameForTable('MASTRA_EXPERIMENT_RESULTS_SCOPE', 'IDX'),
      table: TABLE_EXPERIMENT_RESULTS,
      columns: ['organizationId', 'projectId'],
    },
  ];
}
