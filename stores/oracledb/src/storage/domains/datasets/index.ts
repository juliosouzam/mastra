import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import {
  DatasetsStorage,
  TABLE_DATASETS,
  TABLE_DATASET_ITEMS,
  TABLE_DATASET_VERSIONS,
  TABLE_EXPERIMENTS,
  TABLE_EXPERIMENT_RESULTS,
  TABLE_SCHEMAS,
  normalizePerPage,
  calculatePagination,
  resolveListOrderBy,
} from '@mastra/core/storage';
import type {
  DatasetRecord,
  DatasetItem,
  DatasetItemRow,
  DatasetVersion,
  CreateDatasetInput,
  UpdateDatasetInput,
  AddDatasetItemInput,
  UpdateDatasetItemInput,
  DeleteDatasetItemInput,
  PurgeDatasetItemInput,
  ListDatasetsInput,
  ListDatasetsOutput,
  ListDatasetItemsInput,
  ListDatasetItemsOutput,
  ListDatasetVersionsInput,
  ListDatasetVersionsOutput,
  BatchInsertItemsInput,
  BatchDeleteItemsInput,
  DatasetTenancyFilters,
  StoragePagination,
  TABLE_NAMES,
} from '@mastra/core/storage';
import oracledb from 'oracledb';

import { clobBind, isOracleErrorCode } from '../../../shared/connection';
import { indexNameForTable } from '../../../vector/identifiers';
import { OracleDB, filterIndexesForTables } from '../../db';
import type { OracleCreateIndexOptions, OracleTxClient } from '../../db';
import { toDate } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

type DatasetTable = typeof TABLE_DATASETS | typeof TABLE_DATASET_ITEMS | typeof TABLE_DATASET_VERSIONS;
type DatasetReadClient = Pick<OracleTxClient, 'one' | 'oneOrNone' | 'manyOrNone'>;
const PAYLOAD_FIELDS = [
  'input',
  'groundTruth',
  'expectedTrajectory',
  'toolMocks',
  'unmockedToolPolicy',
  'scorerIds',
  'requestContext',
  'metadata',
  'source',
] as const;
const CONFIG_FIELDS = [
  'name',
  'description',
  'metadata',
  'inputSchema',
  'groundTruthSchema',
  'requestContextSchema',
  'tags',
  'targetType',
  'targetIds',
  'scorerIds',
] as const;
const AUTHORED_JSON = new Set(['input', 'groundTruth', 'expectedTrajectory']);

function column(field: string): string {
  return field === field.toLowerCase() ? field : `"${field}"`;
}

function prepare(
  table: DatasetTable,
  data: Record<string, unknown>,
): { columns: string[]; values: string[]; binds: Record<string, unknown> } {
  const columns: string[] = [];
  const values: string[] = [];
  const binds: Record<string, unknown> = {};
  for (const [field, value] of Object.entries(data)) {
    if (!Object.hasOwn(TABLE_SCHEMAS[table], field)) throw new Error(`Unknown dataset storage field: ${field}`);
    const definition = TABLE_SCHEMAS[table][field];
    if (!definition) throw new Error(`Unknown dataset storage field: ${field}`);
    const name = `v${columns.length}`;
    columns.push(column(field));
    if (definition.type === 'jsonb') {
      values.push(`JSON(:${name})`);
      const absent =
        value === undefined || (value === null && !(table === TABLE_DATASET_ITEMS && AUTHORED_JSON.has(field)));
      binds[name] = absent ? null : clobBind(JSON.stringify(value));
    } else if (definition.type === 'timestamp') {
      values.push(`:${name}`);
      binds[name] = value == null ? null : { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: value };
    } else if (field === 'description') {
      values.push(value === '' ? `NVL(:${name}, EMPTY_CLOB())` : `:${name}`);
      binds[name] = value == null ? null : clobBind(String(value));
    } else {
      values.push(`:${name}`);
      binds[name] = definition.type === 'boolean' ? (value ? 1 : 0) : (value ?? null);
    }
  }
  return { columns, values, binds };
}

function jsonField<T = unknown>(row: Record<string, unknown>, field: string): T | undefined {
  const text = row[`${field}Json`];
  return text == null ? undefined : (JSON.parse(String(text)) as T);
}

function tenancy(conditions: string[], binds: Record<string, unknown>, filters?: DatasetTenancyFilters): void {
  for (const field of ['organizationId', 'projectId'] as const) {
    if (filters?.[field] === undefined) continue;
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

export class DatasetsOracle extends DatasetsStorage {
  static readonly MANAGED_TABLES = [TABLE_DATASETS, TABLE_DATASET_ITEMS, TABLE_DATASET_VERSIONS] as const;
  private readonly db: OracleDB;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB(config);
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, DatasetsOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    for (const tableName of DatasetsOracle.MANAGED_TABLES)
      await this.db.createTable({
        tableName,
        schema: TABLE_SCHEMAS[tableName],
        ...(tableName === TABLE_DATASET_ITEMS ? { compositePrimaryKey: ['id', 'datasetVersion'] } : {}),
      });
    if (!this.skipDefaultIndexes)
      for (const index of getDefaultDatasetIndexDefinitions()) {
        try {
          await this.db.createIndex(index);
        } catch (error) {
          this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
        }
      }
    for (const index of this.indexes) await this.db.createIndex(index);
  }

  async createDataset(input: CreateDatasetInput): Promise<DatasetRecord> {
    const id = input.id ?? crypto.randomUUID();
    if (input.id !== undefined) this.validateCallerDefinedDatasetId(input.id);
    const now = new Date();
    try {
      return await this.db.tx(async client => {
        await this.insert(client, TABLE_DATASETS, { ...input, id, version: 0, createdAt: now, updatedAt: now });
        return mapDataset(await client.one(`${this.select(TABLE_DATASETS)} WHERE id = :id`, { id }));
      });
    } catch (error) {
      if (input.id !== undefined && isOracleErrorCode(error, [-1])) {
        const existing = await this.getDatasetWithClient(this.db, { id });
        if (existing) return this.resolveExistingDataset(existing, { ...input, id });
      }
      throw error;
    }
  }

  async getDatasetById(args: { id: string; filters?: DatasetTenancyFilters }): Promise<DatasetRecord | null> {
    return this.getDatasetWithClient(this.db.read, args);
  }

  protected override getDatasetForMutation(args: {
    id: string;
    filters?: DatasetTenancyFilters;
  }): Promise<DatasetRecord | null> {
    return this.getDatasetWithClient(this.db, args);
  }

  protected override listItemsForMutation(args: ListDatasetItemsInput): Promise<ListDatasetItemsOutput> {
    return this.listItemsWithClient(this.db, args);
  }

  private async getDatasetWithClient(
    client: OracleDB,
    { id, filters }: { id: string; filters?: DatasetTenancyFilters },
  ): Promise<DatasetRecord | null> {
    const conditions = ['id = :id'];
    const binds: Record<string, unknown> = { id };
    tenancy(conditions, binds, filters);
    const row = await client.oneOrNone(`${this.select(TABLE_DATASETS)} WHERE ${conditions.join(' AND ')}`, binds);
    return row ? mapDataset(row) : null;
  }

  protected async _doUpdateDataset(args: UpdateDatasetInput): Promise<DatasetRecord> {
    return this.db.tx(async client => {
      await this.lockDataset(client, args.id, args.filters);
      const data: Record<string, unknown> = { updatedAt: new Date() };
      for (const field of CONFIG_FIELDS) if (args[field] !== undefined) data[field] = args[field];
      await this.update(client, TABLE_DATASETS, data, 'id = :id', { id: args.id });
      return mapDataset(await client.one(`${this.select(TABLE_DATASETS)} WHERE id = :id`, { id: args.id }));
    });
  }

  async deleteDataset({ id, filters }: { id: string; filters?: DatasetTenancyFilters }): Promise<void> {
    const experimentsExist = await this.experimentTablesExist();
    await this.db.tx(async client => {
      const conditions = ['id = :id'];
      const binds: Record<string, unknown> = { id };
      tenancy(conditions, binds, filters);
      const existing = await client.oneOrNone(
        `SELECT id AS "id" FROM ${this.db.table(TABLE_DATASETS)} WHERE ${conditions.join(' AND ')} FOR UPDATE`,
        binds,
      );
      if (!existing) return;
      if (experimentsExist) {
        await client.manyOrNone(
          `SELECT id AS "id" FROM ${this.db.table(TABLE_EXPERIMENTS)} WHERE "datasetId" = :id ORDER BY id FOR UPDATE`,
          { id },
        );
        await client.none(
          `DELETE FROM ${this.db.table(TABLE_EXPERIMENT_RESULTS)} WHERE "experimentId" IN (SELECT id FROM ${this.db.table(TABLE_EXPERIMENTS)} WHERE "datasetId" = :id)`,
          { id },
        );
        await client.none(
          `UPDATE ${this.db.table(TABLE_EXPERIMENTS)} SET "datasetId" = NULL, "datasetVersion" = NULL WHERE "datasetId" = :id`,
          { id },
        );
      }
      await client.none(`DELETE FROM ${this.db.table(TABLE_DATASET_ITEMS)} WHERE "datasetId" = :id`, { id });
      await client.none(`DELETE FROM ${this.db.table(TABLE_DATASET_VERSIONS)} WHERE "datasetId" = :id`, { id });
      await client.none(`DELETE FROM ${this.db.table(TABLE_DATASETS)} WHERE id = :id`, { id });
    });
  }

  async dangerouslyClearAll(): Promise<void> {
    const experimentsExist = await this.experimentTablesExist();
    await this.db.tx(async client => {
      if (experimentsExist) {
        await client.none(
          `DELETE FROM ${this.db.table(TABLE_EXPERIMENT_RESULTS)} WHERE "experimentId" IN (SELECT id FROM ${this.db.table(TABLE_EXPERIMENTS)} WHERE "datasetId" IS NOT NULL)`,
        );
        await client.none(
          `UPDATE ${this.db.table(TABLE_EXPERIMENTS)} SET "datasetId" = NULL, "datasetVersion" = NULL WHERE "datasetId" IS NOT NULL`,
        );
      }
      await client.none(`DELETE FROM ${this.db.table(TABLE_DATASET_ITEMS)}`);
      await client.none(`DELETE FROM ${this.db.table(TABLE_DATASET_VERSIONS)}`);
      await client.none(`DELETE FROM ${this.db.table(TABLE_DATASETS)}`);
    });
  }

  async listDatasets(args: ListDatasetsInput): Promise<ListDatasetsOutput> {
    const order = resolveListOrderBy(args.orderBy, ['createdAt', 'updatedAt', 'name'], {
      field: 'createdAt',
      direction: 'DESC',
    });
    const conditions: string[] = [];
    const binds: Record<string, unknown> = {};
    tenancy(conditions, binds, args.filters);
    if (args.filters?.datasetIds !== undefined) {
      const matches: string[] = [];
      for (let start = 0; start < args.filters.datasetIds.length; start += 500) {
        const names = args.filters.datasetIds.slice(start, start + 500).map((id, index) => {
          const key = `datasetId${start + index}`;
          binds[key] = id;
          return `:${key}`;
        });
        matches.push(`id IN (${names.join(', ')})`);
      }
      conditions.push(matches.length ? `(${matches.join(' OR ')})` : '1 = 0');
    }
    for (const field of ['candidateKey', 'candidateId', 'targetType'] as const)
      if (args.filters?.[field] !== undefined) {
        conditions.push(`${column(field)} = :${field}`);
        binds[field] = args.filters[field];
      }
    if (args.filters?.name) {
      conditions.push('INSTR(LOWER(name), LOWER(:nameFilter)) > 0');
      binds.nameFilter = args.filters.name;
    }
    if (args.filters?.targetIds?.length) {
      const matches = args.filters.targetIds.map((id, index) => {
        if (id === '') return `JSON_EXISTS("targetIds", '$[*]?(@ == "")')`;
        const key = `target${index}`;
        binds[key] = id;
        return `JSON_EXISTS("targetIds", '$[*]?(@ == $candidate)' PASSING :${key} AS "candidate")`;
      });
      conditions.push(`(${matches.join(' OR ')})`);
    }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const total = Number(
      (
        await this.db.read.one<{ count: number }>(
          `SELECT COUNT(*) AS "count" FROM ${this.db.table(TABLE_DATASETS)} ${where}`,
          binds,
        )
      ).count,
    );
    const page = pageInfo(total, args.pagination);
    const rows = page.limit
      ? await this.db.read.manyOrNone(
          `${this.select(TABLE_DATASETS)} ${where} ORDER BY ${column(order.field)} ${order.direction}, id OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
          { ...binds, offset: page.offset, limit: page.limit },
        )
      : [];
    return { datasets: rows.map(mapDataset), pagination: page.pagination };
  }

  protected async _doAddItem(args: AddDatasetItemInput): Promise<DatasetItem> {
    const { datasetId, filters, ...item } = args;
    return (await this._doBatchInsertItems({ datasetId, filters, items: [item] }))[0]!;
  }

  protected async _doBatchInsertItems(input: BatchInsertItemsInput): Promise<DatasetItem[]> {
    return this.db.tx(async client => {
      const parent = await this.lockDataset(client, input.datasetId, input.filters);
      const externalIds = [...new Set(input.items.flatMap(item => (item.externalId ? [item.externalId] : [])))];
      const history: DatasetItemRow[] = [];
      for (let start = 0; start < externalIds.length; start += 500) {
        const binds: Record<string, unknown> = { datasetId: input.datasetId };
        const names = externalIds.slice(start, start + 500).map((value, index) => {
          const name = `external${index}`;
          binds[name] = value;
          return `:${name}`;
        });
        history.push(
          ...(
            await client.manyOrNone(
              `${this.select(TABLE_DATASET_ITEMS)} WHERE "datasetId" = :datasetId AND "externalId" IN (${names.join(', ')}) ORDER BY "datasetVersion" ASC`,
              binds,
            )
          ).map(mapItem),
        );
      }
      const plan = this.planDatasetItemBatch(input.items, history, () => crypto.randomUUID());
      const result = new Map([...plan.existingCurrentItems].map(([id, row]) => [id, this.datasetItemFromRow(row)]));
      if (!plan.inserts.length) return plan.resolvedIds.map(id => result.get(id)!);
      const now = new Date();
      const version = await this.advanceVersion(client, parent);
      for (const { id, item } of plan.inserts) {
        const row: DatasetItemRow = {
          ...item,
          id,
          datasetId: input.datasetId,
          datasetVersion: version,
          externalId: item.externalId ?? null,
          organizationId: parent.organizationId ?? null,
          projectId: parent.projectId ?? null,
          validTo: null,
          isDeleted: false,
          createdAt: now,
          updatedAt: now,
        };
        await this.insert(client, TABLE_DATASET_ITEMS, { ...row });
        result.set(id, this.datasetItemFromRow(row));
      }
      await this.insertVersion(client, input.datasetId, version, now);
      return plan.resolvedIds.map(id => result.get(id)!);
    });
  }

  protected async _doUpdateItem(args: UpdateDatasetItemInput): Promise<DatasetItem> {
    return this.db.tx(async client => {
      const parent = await this.lockDataset(client, args.datasetId, args.filters);
      const current = await this.readItem(client, args.id);
      if (!current) throw new Error(`Dataset item not found: ${args.id}`);
      if (current.datasetId !== args.datasetId)
        throw new Error(`Dataset item ${args.id} does not belong to dataset ${args.datasetId}`);
      if (current.metadata?.__purged === true)
        throw new MastraError({
          id: 'DATASET_ITEM_PURGED',
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.USER,
          text: `Purged dataset item cannot be updated: ${args.id}`,
        });
      const version = await this.advanceVersion(client, parent);
      const now = new Date();
      const next: DatasetItemRow = {
        ...current,
        datasetVersion: version,
        organizationId: parent.organizationId ?? null,
        projectId: parent.projectId ?? null,
        updatedAt: now,
      };
      for (const field of PAYLOAD_FIELDS) {
        if (args[field] === undefined) continue;
        // A null scorer override clears it, while authored null JSON remains a value.
        Object.assign(next, { [field]: field === 'scorerIds' ? (args[field] ?? undefined) : args[field] });
      }
      await client.none(
        `UPDATE ${this.db.table(TABLE_DATASET_ITEMS)} SET "validTo" = :version WHERE id = :id AND "datasetId" = :datasetId AND "validTo" IS NULL AND "isDeleted" = 0`,
        { version, id: args.id, datasetId: args.datasetId },
      );
      await this.insert(client, TABLE_DATASET_ITEMS, { ...next });
      await this.insertVersion(client, args.datasetId, version, now);
      return this.datasetItemFromRow(next);
    });
  }

  protected async _doDeleteItem(args: DeleteDatasetItemInput): Promise<void> {
    await this.db.tx(async client => {
      const parent = await this.lockDataset(client, args.datasetId, args.filters);
      const current = await this.readItem(client, args.id);
      if (!current) return;
      if (current.datasetId !== args.datasetId)
        throw new Error(`Dataset item ${args.id} does not belong to dataset ${args.datasetId}`);
      await this.deleteCurrentItems(client, parent, [current]);
    });
  }

  protected async _doBatchDeleteItems(input: BatchDeleteItemsInput): Promise<void> {
    await this.db.tx(async client => {
      const parent = await this.lockDataset(client, input.datasetId, input.filters);
      const ids = [...new Set(input.itemIds)];
      const items: DatasetItemRow[] = [];
      for (let start = 0; start < ids.length; start += 500) {
        const binds: Record<string, unknown> = { datasetId: input.datasetId };
        const names = ids.slice(start, start + 500).map((id, index) => {
          const key = `item${index}`;
          binds[key] = id;
          return `:${key}`;
        });
        items.push(
          ...(
            await client.manyOrNone(
              `${this.select(TABLE_DATASET_ITEMS)} WHERE "datasetId" = :datasetId AND id IN (${names.join(', ')}) AND "validTo" IS NULL AND "isDeleted" = 0`,
              binds,
            )
          ).map(mapItem),
        );
      }
      if (items.length) await this.deleteCurrentItems(client, parent, items);
    });
  }

  private async deleteCurrentItems(
    client: OracleTxClient,
    parent: DatasetRecord,
    items: DatasetItemRow[],
  ): Promise<void> {
    const version = await this.advanceVersion(client, parent);
    const now = new Date();
    for (const item of items) {
      await client.none(
        `UPDATE ${this.db.table(TABLE_DATASET_ITEMS)} SET "validTo" = :version WHERE id = :id AND "datasetId" = :datasetId AND "validTo" IS NULL AND "isDeleted" = 0`,
        { version, id: item.id, datasetId: parent.id },
      );
      await this.insert(client, TABLE_DATASET_ITEMS, {
        ...item,
        datasetVersion: version,
        validTo: null,
        isDeleted: true,
        organizationId: parent.organizationId ?? null,
        projectId: parent.projectId ?? null,
        updatedAt: now,
      });
    }
    await this.insertVersion(client, parent.id, version, now);
  }

  protected async _doPurgeItem(args: PurgeDatasetItemInput): Promise<void> {
    const experimentsExist = await this.experimentTablesExist();
    await this.db.tx(async client => {
      await this.lockDataset(client, args.datasetId, args.filters);
      const existing = await client.oneOrNone(
        `SELECT id AS "id" FROM ${this.db.table(TABLE_DATASET_ITEMS)} WHERE id = :id AND "datasetId" = :datasetId FETCH FIRST 1 ROW ONLY`,
        { id: args.id, datasetId: args.datasetId },
      );
      if (!existing) return;
      const metadata = clobBind(JSON.stringify({ __purged: true, purgedAt: new Date().toISOString() }));
      await client.none(
        `UPDATE ${this.db.table(TABLE_DATASET_ITEMS)} SET input = JSON('null'), "groundTruth" = NULL,
        "expectedTrajectory" = NULL, "toolMocks" = NULL, "unmockedToolPolicy" = NULL, "scorerIds" = NULL, "requestContext" = NULL,
        metadata = JSON(:metadata), source = NULL WHERE id = :id AND "datasetId" = :datasetId`,
        { metadata, id: args.id, datasetId: args.datasetId },
      );
      if (experimentsExist)
        await client.none(
          `UPDATE ${this.db.table(TABLE_EXPERIMENT_RESULTS)} SET input = JSON('null'), output = NULL,
        "groundTruth" = NULL, error = NULL, "toolMockReport" = NULL, tags = NULL, "comment" = NULL, metadata = JSON(:metadata)
        WHERE "itemId" = :id AND "experimentId" IN (SELECT id FROM ${this.db.table(TABLE_EXPERIMENTS)} WHERE "datasetId" = :datasetId)`,
          { metadata, id: args.id, datasetId: args.datasetId },
        );
    });
  }

  async getItemById({ id, datasetVersion }: { id: string; datasetVersion?: number }): Promise<DatasetItem | null> {
    // Core updateItem uses this lookup for its purge guard; keep it authoritative.
    const row = await this.readItem(this.db, id, datasetVersion);
    return row ? this.datasetItemFromRow(row) : null;
  }

  private async readItem(client: DatasetReadClient, id: string, version?: number): Promise<DatasetItemRow | null> {
    const binds: Record<string, unknown> = { id };
    const validity =
      version === undefined
        ? '"validTo" IS NULL'
        : '"datasetVersion" <= :version AND ("validTo" IS NULL OR "validTo" > :version)';
    if (version !== undefined) binds.version = version;
    const row = await client.oneOrNone(
      `${this.select(TABLE_DATASET_ITEMS)} WHERE id = :id AND ${validity} AND "isDeleted" = 0 ORDER BY "datasetVersion" DESC FETCH FIRST 1 ROW ONLY`,
      binds,
    );
    return row ? mapItem(row) : null;
  }

  async getItemsByVersion({ datasetId, version }: { datasetId: string; version: number }): Promise<DatasetItem[]> {
    const rows = await this.db.read.manyOrNone(
      `${this.select(TABLE_DATASET_ITEMS)} WHERE "datasetId" = :datasetId AND "datasetVersion" <= :version
      AND ("validTo" IS NULL OR "validTo" > :version) AND "isDeleted" = 0 ORDER BY "createdAt" DESC, id`,
      { datasetId, version },
    );
    return rows.map(row => this.datasetItemFromRow(mapItem(row)));
  }

  async getItemHistory(itemId: string): Promise<DatasetItemRow[]> {
    return (
      await this.db.read.manyOrNone(
        `${this.select(TABLE_DATASET_ITEMS)} WHERE id = :id ORDER BY "datasetVersion" DESC`,
        {
          id: itemId,
        },
      )
    ).map(mapItem);
  }

  async listItems(args: ListDatasetItemsInput): Promise<ListDatasetItemsOutput> {
    return this.listItemsWithClient(this.db.read, args);
  }

  private async listItemsWithClient(client: OracleDB, args: ListDatasetItemsInput): Promise<ListDatasetItemsOutput> {
    const order = resolveListOrderBy(args.orderBy, ['createdAt', 'updatedAt'], {
      field: 'createdAt',
      direction: 'DESC',
    });
    const conditions = ['"datasetId" = :datasetId', '"isDeleted" = 0'];
    const binds: Record<string, unknown> = { datasetId: args.datasetId };
    if (args.version === undefined) conditions.push('"validTo" IS NULL');
    else {
      conditions.push('"datasetVersion" <= :version', '("validTo" IS NULL OR "validTo" > :version)');
      binds.version = args.version;
    }
    tenancy(conditions, binds, args.filters);
    if (args.search) {
      conditions.push(
        '(INSTR(LOWER(JSON_SERIALIZE(input RETURNING CLOB)), LOWER(:search)) > 0 OR INSTR(LOWER(JSON_SERIALIZE("groundTruth" RETURNING CLOB)), LOWER(:search)) > 0)',
      );
      binds.search = args.search;
    }
    const where = `WHERE ${conditions.join(' AND ')}`;
    const total = Number(
      (
        await client.one<{ count: number }>(
          `SELECT COUNT(*) AS "count" FROM ${this.db.table(TABLE_DATASET_ITEMS)} ${where}`,
          binds,
        )
      ).count,
    );
    const page = pageInfo(total, args.pagination);
    const rows = page.limit
      ? await client.manyOrNone(
          `${this.select(TABLE_DATASET_ITEMS)} ${where} ORDER BY ${column(order.field)} ${order.direction}, id OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
          { ...binds, offset: page.offset, limit: page.limit },
        )
      : [];
    return { items: rows.map(row => this.datasetItemFromRow(mapItem(row))), pagination: page.pagination };
  }

  async createDatasetVersion(datasetId: string, version: number): Promise<DatasetVersion> {
    return this.insertVersion(this.db, datasetId, version, new Date());
  }

  async listDatasetVersions(input: ListDatasetVersionsInput): Promise<ListDatasetVersionsOutput> {
    const binds = { datasetId: input.datasetId };
    const where = 'WHERE "datasetId" = :datasetId';
    const total = Number(
      (
        await this.db.read.one<{ count: number }>(
          `SELECT COUNT(*) AS "count" FROM ${this.db.table(TABLE_DATASET_VERSIONS)} ${where}`,
          binds,
        )
      ).count,
    );
    const page = pageInfo(total, input.pagination);
    const rows = page.limit
      ? await this.db.read.manyOrNone(
          `${this.select(TABLE_DATASET_VERSIONS)} ${where} ORDER BY version DESC, id OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
          { ...binds, offset: page.offset, limit: page.limit },
        )
      : [];
    return { versions: rows.map(mapVersion), pagination: page.pagination };
  }

  private async advanceVersion(client: OracleTxClient, parent: DatasetRecord): Promise<number> {
    const version = parent.version + 1;
    await client.none(`UPDATE ${this.db.table(TABLE_DATASETS)} SET version = :version WHERE id = :id`, {
      version,
      id: parent.id,
    });
    return version;
  }

  private async insertVersion(
    client: Pick<OracleTxClient, 'none'>,
    datasetId: string,
    version: number,
    createdAt: Date,
  ): Promise<DatasetVersion> {
    const record = { id: crypto.randomUUID(), datasetId, version, createdAt };
    await this.insert(client, TABLE_DATASET_VERSIONS, record);
    return record;
  }

  private async insert(
    client: Pick<OracleTxClient, 'none'>,
    table: DatasetTable,
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
    table: DatasetTable,
    data: Record<string, unknown>,
    where: string,
    keys: Record<string, unknown>,
  ): Promise<void> {
    const fields = prepare(table, data);
    await client.none(
      `UPDATE ${this.db.table(table)} SET ${fields.columns.map((field, index) => `${field} = ${fields.values[index]}`).join(', ')} WHERE ${where}`,
      { ...fields.binds, ...keys },
    );
  }

  private select(table: DatasetTable): string {
    const fields = Object.entries(TABLE_SCHEMAS[table]).map(([field, definition]) =>
      definition.type === 'jsonb'
        ? `JSON_SERIALIZE(${column(field)} RETURNING CLOB) AS "${field}Json"`
        : `${column(field)} AS "${field}"`,
    );
    if (table === TABLE_DATASETS) fields.push('CASE WHEN description IS NULL THEN 1 ELSE 0 END AS "descriptionIsNull"');
    return `SELECT ${fields.join(', ')} FROM ${this.db.table(table)}`;
  }

  private async lockDataset(
    client: DatasetReadClient,
    id: string,
    filters?: DatasetTenancyFilters,
  ): Promise<DatasetRecord> {
    const conditions = ['id = :id'];
    const binds: Record<string, unknown> = { id };
    tenancy(conditions, binds, filters);
    const row = await client.oneOrNone(
      `${this.select(TABLE_DATASETS)} WHERE ${conditions.join(' AND ')} FOR UPDATE`,
      binds,
    );
    if (!row) throw new Error(`Dataset not found: ${id}`);
    return mapDataset(row);
  }

  private async experimentTablesExist(): Promise<boolean> {
    return (
      (await this.db.hasColumn(TABLE_EXPERIMENTS, 'id')) && (await this.db.hasColumn(TABLE_EXPERIMENT_RESULTS, 'id'))
    );
  }
}

function mapDataset(row: Record<string, unknown>): DatasetRecord {
  return {
    id: String(row.id),
    name: String(row.name),
    description: Number(row.descriptionIsNull) === 1 ? undefined : String(row.description ?? ''),
    metadata: jsonField(row, 'metadata'),
    inputSchema: jsonField(row, 'inputSchema'),
    groundTruthSchema: jsonField(row, 'groundTruthSchema'),
    requestContextSchema: jsonField(row, 'requestContextSchema'),
    tags: jsonField(row, 'tags'),
    targetType: row.targetType == null ? undefined : (row.targetType as DatasetRecord['targetType']),
    targetIds: jsonField(row, 'targetIds'),
    scorerIds: jsonField(row, 'scorerIds'),
    organizationId: row.organizationId == null ? null : String(row.organizationId),
    projectId: row.projectId == null ? null : String(row.projectId),
    candidateKey: row.candidateKey == null ? null : String(row.candidateKey),
    candidateId: row.candidateId == null ? null : String(row.candidateId),
    version: Number(row.version),
    createdAt: toDate(row.createdAt),
    updatedAt: toDate(row.updatedAt),
  };
}

function mapItem(row: Record<string, unknown>): DatasetItemRow {
  const metadata = jsonField<DatasetItemRow['metadata']>(row, 'metadata');
  const empty = metadata?.__purged === true ? null : undefined;
  const optionalJson = <T>(field: string): T | null | undefined =>
    row[`${field}Json`] == null ? empty : jsonField<T>(row, field);
  return {
    id: String(row.id),
    datasetId: String(row.datasetId),
    datasetVersion: Number(row.datasetVersion),
    externalId: row.externalId == null ? null : String(row.externalId),
    organizationId: row.organizationId == null ? null : String(row.organizationId),
    projectId: row.projectId == null ? null : String(row.projectId),
    validTo: row.validTo == null ? null : Number(row.validTo),
    isDeleted: Number(row.isDeleted) === 1,
    input: row.inputJson == null ? null : jsonField(row, 'input'),
    groundTruth: optionalJson('groundTruth'),
    expectedTrajectory: optionalJson('expectedTrajectory'),
    toolMocks: optionalJson<NonNullable<DatasetItemRow['toolMocks']>>('toolMocks'),
    unmockedToolPolicy: (row.unmockedToolPolicy ?? empty) as DatasetItemRow['unmockedToolPolicy'],
    scorerIds: optionalJson<string[]>('scorerIds'),
    requestContext: optionalJson<Record<string, unknown>>('requestContext'),
    metadata,
    source: optionalJson<NonNullable<DatasetItemRow['source']>>('source'),
    createdAt: toDate(row.createdAt),
    updatedAt: toDate(row.updatedAt),
  };
}

function mapVersion(row: Record<string, unknown>): DatasetVersion {
  return {
    id: String(row.id),
    datasetId: String(row.datasetId),
    version: Number(row.version),
    createdAt: toDate(row.createdAt),
  };
}

export function getDefaultDatasetIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    {
      name: indexNameForTable('MASTRA_DATASET_ITEMS_EXTERNAL_VERSION', 'IDX'),
      table: TABLE_DATASET_ITEMS,
      columns: ['datasetId', 'externalId', 'datasetVersion'],
    },
    {
      name: indexNameForTable('MASTRA_DATASET_ITEMS_CURRENT', 'IDX'),
      table: TABLE_DATASET_ITEMS,
      columns: ['datasetId', 'validTo', 'isDeleted'],
    },
    {
      name: indexNameForTable('MASTRA_DATASET_VERSIONS_VERSION', 'IDX'),
      table: TABLE_DATASET_VERSIONS,
      columns: ['datasetId', 'version'],
    },
  ];
}
