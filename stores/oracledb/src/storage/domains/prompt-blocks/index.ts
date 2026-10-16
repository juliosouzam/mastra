import { ErrorCategory, MastraError } from '@mastra/core/error';
import {
  calculatePagination,
  PromptBlocksStorage,
  normalizePerPage,
  TABLE_PROMPT_BLOCKS,
  TABLE_PROMPT_BLOCK_VERSIONS,
  TABLE_SCHEMAS,
} from '@mastra/core/storage';
import type {
  StorageCreatePromptBlockInput,
  StorageListPromptBlocksInput,
  StorageListPromptBlocksOutput,
  StoragePromptBlockType,
  StorageUpdatePromptBlockInput,
} from '@mastra/core/storage';
import type {
  CreatePromptBlockVersionInput,
  ListPromptBlockVersionsInput,
  ListPromptBlockVersionsOutput,
  PromptBlockVersion,
} from '@mastra/core/storage/domains/prompt-blocks';

import oracledb from 'oracledb';

import { clobBind, jsonBind, nullableJsonBind } from '../../../shared/connection';
import { indexNameForTable, qualifyName } from '../../../vector/identifiers';
import { OracleDB, createOracleIndex, filterIndexesForTables, parseOracleJson } from '../../db';
import type { OracleCreateIndexOptions, OracleTxClient } from '../../db';
import {
  createOracleStorageError,
  buildOracleJsonContains,
  parseOptionalJsonObject,
  parseOptionalStringArray,
  toDate,
} from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

// Prompt blocks use the same registry/version pattern as agents: parent rows handle discovery while version rows snapshot template content and conditional rules.
const STORE_NAME = 'ORACLEDB';
const SNAPSHOT_FIELDS = ['name', 'description', 'content', 'rules', 'requestContextSchema'] as const;

const BLOCK_ACTIVE_VERSION_ID = '"activeVersionId"';
const BLOCK_AUTHOR_ID = '"authorId"';
const BLOCK_CREATED_AT = '"createdAt"';
const BLOCK_UPDATED_AT = '"updatedAt"';
const VERSION_PROMPT_BLOCK_ID = '"blockId"';
const VERSION_VERSION_NUMBER = '"versionNumber"';
const VERSION_CHANGED_FIELDS = '"changedFields"';
const VERSION_CHANGE_MESSAGE = '"changeMessage"';
const VERSION_CREATED_AT = '"createdAt"';

type PromptBlockRow = {
  id: string;
  status: StoragePromptBlockType['status'];
  activeVersionId?: string | null;
  authorId?: string | null;
  metadata?: unknown;
  createdAt: Date | string;
  updatedAt: Date | string;
};

type PromptBlockVersionRow = {
  id: string;
  blockId: string;
  versionNumber: number | string;
  name: string;
  description?: string | null;
  content: string | null;
  descriptionIsNull: number;
  changeMessageIsNull: number;
  rules?: unknown;
  requestContextSchema?: unknown;
  changedFields?: unknown;
  changeMessage?: string | null;
  createdAt: Date | string;
};

type VersionWriteClient = Pick<OracleTxClient, 'none'>;

export class PromptBlocksOracle extends PromptBlocksStorage {
  // Prompt blocks follow the registry pattern: parent lifecycle row plus versioned prompt content snapshots.
  static readonly MANAGED_TABLES = [TABLE_PROMPT_BLOCKS, TABLE_PROMPT_BLOCK_VERSIONS] as const;

  private readonly db: OracleDB;
  private readonly schemaName?: string;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({ ...config, jsonColumns: ['metadata', 'rules', 'requestContextSchema', 'changedFields'] });
    this.schemaName = config.schemaName;
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, PromptBlocksOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_PROMPT_BLOCKS,
      schema: TABLE_SCHEMAS[TABLE_PROMPT_BLOCKS],
    });
    await this.db.createTable({
      tableName: TABLE_PROMPT_BLOCK_VERSIONS,
      schema: TABLE_SCHEMAS[TABLE_PROMPT_BLOCK_VERSIONS],
    });
    await this.createIndexes();
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.tx(async client => {
      await client.none(`DELETE FROM ${this.table(TABLE_PROMPT_BLOCK_VERSIONS)}`);
      await client.none(`DELETE FROM ${this.table(TABLE_PROMPT_BLOCKS)}`);
    });
  }

  getDefaultIndexDefinitions(): OracleCreateIndexOptions[] {
    return getDefaultPromptBlockIndexDefinitions(this.indexName.bind(this));
  }

  async getById(id: string): Promise<StoragePromptBlockType | null> {
    return this.getByIdWithClient(this.db.read, id);
  }

  private async getByIdWithClient(client: OracleDB, id: string): Promise<StoragePromptBlockType | null> {
    try {
      const row = await client.oneOrNone<PromptBlockRow>(
        `${this.blockSelect()} FROM ${this.table(TABLE_PROMPT_BLOCKS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parsePromptBlockRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_PROMPT_BLOCK_BY_ID', 'FAILED', { blockId: id }, error);
    }
  }

  async create(input: { promptBlock: StorageCreatePromptBlockInput }): Promise<StoragePromptBlockType> {
    const { promptBlock } = input;

    try {
      const now = new Date();

      await this.db.tx(async client => {
        const existing = await client.oneOrNone<{ id: string }>(
          `SELECT id AS "id" FROM ${this.table(TABLE_PROMPT_BLOCKS)} WHERE id = :id`,
          { id: promptBlock.id },
        );
        if (existing) {
          throw new Error(`Prompt block with id ${promptBlock.id} already exists`);
        }

        await client.none(
          `INSERT INTO ${this.table(TABLE_PROMPT_BLOCKS)} (
            id, status, ${BLOCK_ACTIVE_VERSION_ID}, ${BLOCK_AUTHOR_ID}, metadata,
            ${BLOCK_CREATED_AT}, ${BLOCK_UPDATED_AT}
          ) VALUES (
            :id, :status, :activeVersionId, :authorId, :metadata,
            :createdAt, :updatedAt
          )`,
          {
            id: promptBlock.id,
            status: 'draft',
            activeVersionId: null,
            authorId: promptBlock.authorId ?? null,
            metadata: nullableJsonBind(promptBlock.metadata),
            createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: now },
            updatedAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: now },
          },
        );

        // Keep prompt content and rules in immutable version snapshots.
        const { id: _id, authorId: _authorId, metadata: _metadata, ...snapshotConfig } = promptBlock;
        await this.insertVersion(client, {
          id: crypto.randomUUID(),
          blockId: promptBlock.id,
          versionNumber: 1,
          ...snapshotConfig,
          changedFields: [...SNAPSHOT_FIELDS],
          changeMessage: 'Initial version',
        });
      });

      return {
        id: promptBlock.id,
        status: 'draft',
        activeVersionId: undefined,
        authorId: promptBlock.authorId,
        metadata: promptBlock.metadata,
        createdAt: now,
        updatedAt: now,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('CREATE_PROMPT_BLOCK', 'FAILED', { blockId: promptBlock.id }, error);
    }
  }

  async update(input: StorageUpdatePromptBlockInput): Promise<StoragePromptBlockType> {
    const { id, ...updates } = input;

    try {
      const existingBlock = await this.getByIdWithClient(this.db, id);
      if (!existingBlock) {
        throw new Error(`Prompt block with id ${id} not found`);
      }

      const { authorId, activeVersionId, metadata, status } = updates;
      const setClauses: string[] = [];
      const binds: Record<string, unknown> = { id };

      if (authorId !== undefined) {
        setClauses.push(`${BLOCK_AUTHOR_ID} = :authorId`);
        binds.authorId = authorId;
      }

      if (activeVersionId !== undefined) {
        setClauses.push(`${BLOCK_ACTIVE_VERSION_ID} = :activeVersionId`);
        binds.activeVersionId = activeVersionId;
      }

      if (status !== undefined) {
        setClauses.push(`status = :status`);
        binds.status = status;
      }

      if (metadata !== undefined) {
        const mergedMetadata = { ...(existingBlock.metadata ?? {}), ...metadata };
        // Discovery metadata belongs to the parent; prompt content belongs to version rows.
        setClauses.push(`metadata = :metadata`);
        binds.metadata = jsonBind(mergedMetadata);
      }

      setClauses.push(`${BLOCK_UPDATED_AT} = :updatedAt`);
      binds.updatedAt = { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: new Date() };

      await this.db.none(
        `UPDATE ${this.table(TABLE_PROMPT_BLOCKS)} SET ${setClauses.join(', ')} WHERE id = :id`,
        binds,
      );

      const updatedBlock = await this.getByIdWithClient(this.db, id);
      if (!updatedBlock) {
        throw new Error(`Prompt block with id ${id} not found after update`);
      }
      return updatedBlock;
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('UPDATE_PROMPT_BLOCK', 'FAILED', { blockId: id }, error);
    }
  }

  async delete(id: string): Promise<void> {
    try {
      await this.db.tx(async client => {
        await client.none(
          `DELETE FROM ${this.table(TABLE_PROMPT_BLOCK_VERSIONS)} WHERE ${VERSION_PROMPT_BLOCK_ID} = :id`,
          {
            id,
          },
        );
        await client.none(`DELETE FROM ${this.table(TABLE_PROMPT_BLOCKS)} WHERE id = :id`, { id });
      });
    } catch (error) {
      throw this.storageError('DELETE_PROMPT_BLOCK', 'FAILED', { blockId: id }, error);
    }
  }

  async list(args?: StorageListPromptBlocksInput): Promise<StorageListPromptBlocksOutput> {
    const { page = 0, perPage: perPageInput, orderBy, authorId, metadata, status = 'published' } = args || {};

    try {
      this.validatePagination(page, perPageInput, 100);
    } catch (error) {
      throw this.storageError('LIST_PROMPT_BLOCKS', 'INVALID_INPUT', { page }, error, ErrorCategory.USER);
    }

    const perPage = normalizePerPage(perPageInput, 100);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const { field, direction } = this.parseOrderBy(orderBy);
      const { whereClause, binds } = this.blockWhereClause({ status, authorId, metadata });

      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_PROMPT_BLOCKS)} ${whereClause}`,
        binds,
      );
      const total = Number(countRow?.count ?? 0);

      if (total === 0) {
        return {
          promptBlocks: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      if (perPage === 0) {
        return {
          promptBlocks: [],
          total,
          page,
          perPage: perPageForResponse,
          hasMore: total > 0,
        };
      }

      const limit = perPageInput === false ? total : perPage;
      // `id ASC` breaks ties between rows sharing the same order-by value so
      // pages stay stable instead of duplicating or dropping rows across pages.
      const rows = await this.db.read.manyOrNone<PromptBlockRow>(
        `${this.blockSelect()} FROM ${this.table(TABLE_PROMPT_BLOCKS)} ${whereClause} ORDER BY ${this.blockOrderColumn(
          field,
        )} ${direction}, id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { ...binds, offset, limit },
      );

      return {
        promptBlocks: rows.map(row => this.parsePromptBlockRow(row)),
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('LIST_PROMPT_BLOCKS', 'FAILED', {}, error);
    }
  }

  async createVersion(input: CreatePromptBlockVersionInput): Promise<PromptBlockVersion> {
    try {
      const existingById = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(TABLE_PROMPT_BLOCK_VERSIONS)} WHERE id = :id`,
        { id: input.id },
      );
      if (existingById) {
        throw new Error(`Version with id ${input.id} already exists`);
      }

      const existingByNumber = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(
          TABLE_PROMPT_BLOCK_VERSIONS,
        )} WHERE ${VERSION_PROMPT_BLOCK_ID} = :blockId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { blockId: input.blockId, versionNumber: input.versionNumber },
      );
      if (existingByNumber) {
        throw new Error(`Version number ${input.versionNumber} already exists for Prompt block ${input.blockId}`);
      }

      const createdAt = new Date();
      await this.insertVersion(this.db, input, createdAt);

      return {
        ...input,
        createdAt,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError(
        'CREATE_PROMPT_BLOCK_VERSION',
        'FAILED',
        { versionId: input.id, blockId: input.blockId },
        error,
      );
    }
  }

  async getVersion(id: string): Promise<PromptBlockVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<PromptBlockVersionRow>(
        `${this.versionSelect()} FROM ${this.table(TABLE_PROMPT_BLOCK_VERSIONS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_PROMPT_BLOCK_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async getVersionByNumber(blockId: string, versionNumber: number): Promise<PromptBlockVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<PromptBlockVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_PROMPT_BLOCK_VERSIONS,
        )} WHERE ${VERSION_PROMPT_BLOCK_ID} = :blockId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { blockId, versionNumber },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_PROMPT_BLOCK_VERSION_BY_NUMBER', 'FAILED', { blockId, versionNumber }, error);
    }
  }

  async getLatestVersion(blockId: string): Promise<PromptBlockVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<PromptBlockVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_PROMPT_BLOCK_VERSIONS,
        )} WHERE ${VERSION_PROMPT_BLOCK_ID} = :blockId ORDER BY ${VERSION_VERSION_NUMBER} DESC FETCH FIRST 1 ROWS ONLY`,
        { blockId },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_LATEST_PROMPT_BLOCK_VERSION', 'FAILED', { blockId }, error);
    }
  }

  async listVersions(input: ListPromptBlockVersionsInput): Promise<ListPromptBlockVersionsOutput> {
    const { blockId, page = 0, perPage: perPageInput, orderBy } = input;

    try {
      this.validatePagination(page, perPageInput, 20);
    } catch (error) {
      throw this.storageError(
        'LIST_PROMPT_BLOCK_VERSIONS',
        'INVALID_INPUT',
        { page, blockId },
        error,
        ErrorCategory.USER,
      );
    }

    const perPage = normalizePerPage(perPageInput, 20);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const { field, direction } = this.parseVersionOrderBy(orderBy);
      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_PROMPT_BLOCK_VERSIONS)} WHERE ${VERSION_PROMPT_BLOCK_ID} = :blockId`,
        { blockId },
      );
      const total = Number(countRow?.count ?? 0);

      if (total === 0) {
        return {
          versions: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      if (perPage === 0) {
        return {
          versions: [],
          total,
          page,
          perPage: perPageForResponse,
          hasMore: total > 0,
        };
      }

      const limit = perPageInput === false ? total : perPage;
      // `id ASC` breaks ties between versions sharing the same order-by value so
      // pages stay stable instead of duplicating or dropping rows across pages.
      const rows = await this.db.read.manyOrNone<PromptBlockVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_PROMPT_BLOCK_VERSIONS,
        )} WHERE ${VERSION_PROMPT_BLOCK_ID} = :blockId ORDER BY ${this.versionOrderColumn(
          field,
        )} ${direction}, id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { blockId, offset, limit },
      );

      return {
        versions: rows.map(row => this.parseVersionRow(row)),
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('LIST_PROMPT_BLOCK_VERSIONS', 'FAILED', { blockId }, error);
    }
  }

  async deleteVersion(id: string): Promise<void> {
    try {
      await this.db.none(`DELETE FROM ${this.table(TABLE_PROMPT_BLOCK_VERSIONS)} WHERE id = :id`, { id });
    } catch (error) {
      throw this.storageError('DELETE_PROMPT_BLOCK_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async deleteVersionsByParentId(entityId: string): Promise<void> {
    try {
      await this.db.none(
        `DELETE FROM ${this.table(TABLE_PROMPT_BLOCK_VERSIONS)} WHERE ${VERSION_PROMPT_BLOCK_ID} = :entityId`,
        {
          entityId,
        },
      );
    } catch (error) {
      throw this.storageError(
        'DELETE_PROMPT_BLOCK_VERSIONS_BY_PROMPT_BLOCK_ID',
        'FAILED',
        { blockId: entityId },
        error,
      );
    }
  }

  async countVersions(blockId: string): Promise<number> {
    try {
      const row = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_PROMPT_BLOCK_VERSIONS)} WHERE ${VERSION_PROMPT_BLOCK_ID} = :blockId`,
        { blockId },
      );
      return Number(row?.count ?? 0);
    } catch (error) {
      throw this.storageError('COUNT_PROMPT_BLOCK_VERSIONS', 'FAILED', { blockId }, error);
    }
  }

  private async createIndexes(): Promise<void> {
    await this.db.withConnection(async connection => {
      for (const index of this.getDefaultIndexDefinitions()) {
        // Version-number uniqueness is an invariant, not an optional performance index.
        if (this.skipDefaultIndexes && !index.unique) continue;
        try {
          await createOracleIndex(connection, index, this.schemaName);
        } catch (error) {
          if (index.unique) throw error;
          this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
        }
      }

      for (const index of this.indexes) {
        await createOracleIndex(connection, index, this.schemaName);
      }
    });
  }

  private async insertVersion(
    client: VersionWriteClient,
    input: CreatePromptBlockVersionInput,
    createdAt = new Date(),
  ): Promise<void> {
    // CLOBs preserve full prompt text; rules and context schemas use native JSON.
    await client.none(
      `INSERT INTO ${this.table(TABLE_PROMPT_BLOCK_VERSIONS)} (
        id, ${VERSION_PROMPT_BLOCK_ID}, ${VERSION_VERSION_NUMBER},
        name, description, content, rules, "requestContextSchema",
        ${VERSION_CHANGED_FIELDS}, ${VERSION_CHANGE_MESSAGE},
        ${VERSION_CREATED_AT}
      ) VALUES (
        :id, :blockId, :versionNumber,
        :name, CASE WHEN :emptyDescription = 1 THEN EMPTY_CLOB() ELSE :description END, NVL(:content, EMPTY_CLOB()), :rules, :requestContextSchema,
        :changedFields, CASE WHEN :emptyChangeMessage = 1 THEN EMPTY_CLOB() ELSE :changeMessage END,
        :createdAt
      )`,
      {
        id: input.id,
        blockId: input.blockId,
        versionNumber: input.versionNumber,
        name: input.name,
        description: input.description === undefined ? null : clobBind(input.description),
        emptyDescription: input.description === '' ? 1 : 0,
        content: clobBind(input.content),
        rules: nullableJsonBind(input.rules),
        requestContextSchema: nullableJsonBind(input.requestContextSchema),
        changedFields: nullableJsonBind(input.changedFields),
        changeMessage: input.changeMessage === undefined ? null : clobBind(input.changeMessage),
        emptyChangeMessage: input.changeMessage === '' ? 1 : 0,
        createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: createdAt },
      },
    );
  }

  private blockWhereClause(args: {
    status: StoragePromptBlockType['status'];
    authorId?: string;
    metadata?: Record<string, unknown>;
  }): { whereClause: string; binds: Record<string, unknown> } {
    const conditions = ['status = :status'];
    const binds: Record<string, unknown> = { status: args.status };

    if (args.authorId !== undefined) {
      conditions.push(`${BLOCK_AUTHOR_ID} = :authorId`);
      binds.authorId = args.authorId;
    }

    if (args.metadata && Object.keys(args.metadata).length > 0) {
      const filter = buildOracleJsonContains('metadata', args.metadata);
      conditions.push(filter.sql);
      Object.assign(binds, filter.binds);
    }

    return { whereClause: `WHERE ${conditions.join(' AND ')}`, binds };
  }

  private blockSelect(): string {
    return [
      `SELECT id AS "id"`,
      `status AS "status"`,
      `${BLOCK_ACTIVE_VERSION_ID} AS "activeVersionId"`,
      `${BLOCK_AUTHOR_ID} AS "authorId"`,
      `JSON_SERIALIZE(metadata RETURNING CLOB) AS "metadata"`,
      `${BLOCK_CREATED_AT} AS "createdAt"`,
      `${BLOCK_UPDATED_AT} AS "updatedAt"`,
    ].join(', ');
  }

  private versionSelect(): string {
    return [
      `SELECT id AS "id"`,
      `${VERSION_PROMPT_BLOCK_ID} AS "blockId"`,
      `${VERSION_VERSION_NUMBER} AS "versionNumber"`,
      `name AS "name"`,
      `description AS "description"`,
      `content AS "content"`,
      `JSON_SERIALIZE(rules RETURNING CLOB) AS "rules"`,
      `"requestContextSchema"`,
      `CASE WHEN description IS NULL THEN 1 ELSE 0 END AS "descriptionIsNull"`,
      `CASE WHEN ${VERSION_CHANGE_MESSAGE} IS NULL THEN 1 ELSE 0 END AS "changeMessageIsNull"`,
      `JSON_SERIALIZE(${VERSION_CHANGED_FIELDS} RETURNING CLOB) AS "changedFields"`,
      `${VERSION_CHANGE_MESSAGE} AS "changeMessage"`,
      `${VERSION_CREATED_AT} AS "createdAt"`,
    ].join(', ');
  }

  private parsePromptBlockRow(row: PromptBlockRow): StoragePromptBlockType {
    return {
      id: String(row.id),
      status: row.status,
      activeVersionId: optionalString(row.activeVersionId),
      authorId: optionalString(row.authorId),
      metadata: parseOptionalJsonObject(row.metadata),
      createdAt: toDate(row.createdAt),
      updatedAt: toDate(row.updatedAt),
    };
  }

  private parseVersionRow(row: PromptBlockVersionRow): PromptBlockVersion {
    // Return the stored prompt snapshot without versioning metadata conversions leaking into its fields.
    return {
      id: String(row.id),
      blockId: String(row.blockId),
      versionNumber: Number(row.versionNumber),
      name: String(row.name),
      description: Number(row.descriptionIsNull) === 1 ? undefined : String(row.description ?? ''),
      content: row.content ?? '',
      rules: parseOracleJson<PromptBlockVersion['rules']>(row.rules),
      requestContextSchema: parseOracleJson<PromptBlockVersion['requestContextSchema']>(row.requestContextSchema),
      changedFields: parseOptionalStringArray(row.changedFields),
      changeMessage: Number(row.changeMessageIsNull) === 1 ? undefined : String(row.changeMessage ?? ''),
      createdAt: toDate(row.createdAt),
    };
  }

  private blockOrderColumn(field: string): string {
    return field === 'updatedAt' ? BLOCK_UPDATED_AT : BLOCK_CREATED_AT;
  }

  private versionOrderColumn(field: string): string {
    return field === 'createdAt' ? VERSION_CREATED_AT : VERSION_VERSION_NUMBER;
  }

  private validatePagination(page: number, perPageInput: number | false | undefined, defaultPerPage: number): void {
    if (page < 0) {
      throw new Error('page must be >= 0');
    }

    const perPage = normalizePerPage(perPageInput, defaultPerPage);
    const maxOffset = Number.MAX_SAFE_INTEGER / 2;
    if (perPageInput !== false && page * perPage > maxOffset) {
      throw new Error('page value too large');
    }
  }

  private table(tableName: string): string {
    return qualifyName(tableName, this.schemaName);
  }

  private indexName(indexName: string): string {
    return indexNameForTable(indexName, 'IDX');
  }

  private storageError(
    operation: string,
    reason: string,
    details: Record<string, string | number | boolean | undefined>,
    cause: unknown,
    category: ErrorCategory = ErrorCategory.THIRD_PARTY,
  ): MastraError {
    return createOracleStorageError({ storeName: STORE_NAME, operation, reason, details, cause, category });
  }
}

export function getDefaultPromptBlockIndexDefinitions(indexName: (name: string) => string): OracleCreateIndexOptions[] {
  return [
    {
      name: indexName('MASTRA_PROMPT_BLOCK_VERSIONS_BLOCK_VERSION'),
      table: TABLE_PROMPT_BLOCK_VERSIONS,
      columns: ['blockId', 'versionNumber'],
      unique: true,
    },
  ];
}

function optionalString(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  return String(value);
}
