import { ErrorCategory, MastraError } from '@mastra/core/error';
import {
  calculatePagination,
  MCPServersStorage,
  normalizePerPage,
  TABLE_MCP_SERVERS,
  TABLE_MCP_SERVER_VERSIONS,
  TABLE_SCHEMAS,
} from '@mastra/core/storage';
import type {
  StorageCreateMCPServerInput,
  StorageListMCPServersInput,
  StorageListMCPServersOutput,
  StorageMCPServerType,
  StorageUpdateMCPServerInput,
} from '@mastra/core/storage';
import type {
  CreateMCPServerVersionInput,
  ListMCPServerVersionsInput,
  ListMCPServerVersionsOutput,
  MCPServerVersion,
} from '@mastra/core/storage/domains/mcp-servers';

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

// MCP servers use the same registry/version pattern as agents: parent rows handle discovery while version rows snapshot server capabilities and configuration.
const STORE_NAME = 'ORACLEDB';
const SNAPSHOT_FIELDS = [
  'name',
  'version',
  'description',
  'instructions',
  'repository',
  'releaseDate',
  'isLatest',
  'packageCanonical',
  'tools',
  'agents',
  'workflows',
] as const;

const SERVER_ACTIVE_VERSION_ID = '"activeVersionId"';
const SERVER_AUTHOR_ID = '"authorId"';
const SERVER_CREATED_AT = '"createdAt"';
const SERVER_UPDATED_AT = '"updatedAt"';
const VERSION_MCP_SERVER_ID = '"mcpServerId"';
const VERSION_VERSION_NUMBER = '"versionNumber"';
const VERSION_CHANGED_FIELDS = '"changedFields"';
const VERSION_CHANGE_MESSAGE = '"changeMessage"';
const VERSION_CREATED_AT = '"createdAt"';

type MCPServerRow = {
  id: string;
  status: StorageMCPServerType['status'];
  activeVersionId?: string | null;
  authorId?: string | null;
  metadata?: unknown;
  createdAt: Date | string;
  updatedAt: Date | string;
};

type MCPServerVersionRow = {
  id: string;
  mcpServerId: string;
  versionNumber: number | string;
  name: string;
  version: string;
  description?: string | null;
  instructions?: string | null;
  descriptionIsNull: number;
  instructionsIsNull: number;
  changeMessageIsNull: number;
  repository?: unknown;
  releaseDate?: string | null;
  isLatest?: number | null;
  packageCanonical?: string | null;
  tools?: unknown;
  agents?: unknown;
  workflows?: unknown;
  changedFields?: unknown;
  changeMessage?: string | null;
  createdAt: Date | string;
};

type VersionWriteClient = Pick<OracleTxClient, 'none'>;

export class MCPServersOracle extends MCPServersStorage {
  // MCP servers follow the registry pattern: parent lifecycle row plus versioned server configuration snapshots.
  static readonly MANAGED_TABLES = [TABLE_MCP_SERVERS, TABLE_MCP_SERVER_VERSIONS] as const;

  private readonly db: OracleDB;
  private readonly schemaName?: string;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({
      ...config,
      jsonColumns: ['metadata', 'repository', 'tools', 'agents', 'workflows', 'changedFields'],
    });
    this.schemaName = config.schemaName;
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, MCPServersOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_MCP_SERVERS,
      schema: TABLE_SCHEMAS[TABLE_MCP_SERVERS],
    });
    await this.db.createTable({
      tableName: TABLE_MCP_SERVER_VERSIONS,
      schema: TABLE_SCHEMAS[TABLE_MCP_SERVER_VERSIONS],
    });
    await this.createIndexes();
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.tx(async client => {
      await client.none(`DELETE FROM ${this.table(TABLE_MCP_SERVER_VERSIONS)}`);
      await client.none(`DELETE FROM ${this.table(TABLE_MCP_SERVERS)}`);
    });
  }

  getDefaultIndexDefinitions(): OracleCreateIndexOptions[] {
    return getDefaultMCPServerIndexDefinitions(this.indexName.bind(this));
  }

  async getById(id: string): Promise<StorageMCPServerType | null> {
    return this.getByIdWithClient(this.db.read, id);
  }

  private async getByIdWithClient(client: OracleDB, id: string): Promise<StorageMCPServerType | null> {
    try {
      const row = await client.oneOrNone<MCPServerRow>(
        `${this.serverSelect()} FROM ${this.table(TABLE_MCP_SERVERS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseMCPServerRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_MCP_SERVER_BY_ID', 'FAILED', { mcpServerId: id }, error);
    }
  }

  async create(input: { mcpServer: StorageCreateMCPServerInput }): Promise<StorageMCPServerType> {
    const { mcpServer } = input;

    try {
      const now = new Date();

      await this.db.tx(async client => {
        const existing = await client.oneOrNone<{ id: string }>(
          `SELECT id AS "id" FROM ${this.table(TABLE_MCP_SERVERS)} WHERE id = :id`,
          { id: mcpServer.id },
        );
        if (existing) {
          throw new Error(`MCP server with id ${mcpServer.id} already exists`);
        }

        await client.none(
          `INSERT INTO ${this.table(TABLE_MCP_SERVERS)} (
            id, status, ${SERVER_ACTIVE_VERSION_ID}, ${SERVER_AUTHOR_ID}, metadata,
            ${SERVER_CREATED_AT}, ${SERVER_UPDATED_AT}
          ) VALUES (
            :id, :status, :activeVersionId, :authorId, :metadata,
            :createdAt, :updatedAt
          )`,
          {
            id: mcpServer.id,
            status: 'draft',
            activeVersionId: null,
            authorId: mcpServer.authorId ?? null,
            metadata: nullableJsonBind(mcpServer.metadata),
            createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: now },
            updatedAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: now },
          },
        );

        // Keep server capability bindings and configuration in immutable version snapshots.
        const { id: _id, authorId: _authorId, metadata: _metadata, ...snapshotConfig } = mcpServer;
        await this.insertVersion(client, {
          id: crypto.randomUUID(),
          mcpServerId: mcpServer.id,
          versionNumber: 1,
          ...snapshotConfig,
          changedFields: [...SNAPSHOT_FIELDS],
          changeMessage: 'Initial version',
        });
      });

      return {
        id: mcpServer.id,
        status: 'draft',
        activeVersionId: undefined,
        authorId: mcpServer.authorId,
        metadata: mcpServer.metadata,
        createdAt: now,
        updatedAt: now,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('CREATE_MCP_SERVER', 'FAILED', { mcpServerId: mcpServer.id }, error);
    }
  }

  async update(input: StorageUpdateMCPServerInput): Promise<StorageMCPServerType> {
    const { id, ...updates } = input;

    try {
      const existingServer = await this.getByIdWithClient(this.db, id);
      if (!existingServer) {
        throw new Error(`MCP server with id ${id} not found`);
      }

      const { authorId, activeVersionId, metadata, status } = updates;
      const setClauses: string[] = [];
      const binds: Record<string, unknown> = { id };

      if (authorId !== undefined) {
        setClauses.push(`${SERVER_AUTHOR_ID} = :authorId`);
        binds.authorId = authorId;
      }

      if (activeVersionId !== undefined) {
        setClauses.push(`${SERVER_ACTIVE_VERSION_ID} = :activeVersionId`);
        binds.activeVersionId = activeVersionId;
      }

      if (status !== undefined) {
        setClauses.push(`status = :status`);
        binds.status = status;
      }

      if (metadata !== undefined) {
        const mergedMetadata = { ...(existingServer.metadata ?? {}), ...metadata };
        // Discovery metadata belongs to the parent; server configuration belongs to version rows.
        setClauses.push(`metadata = :metadata`);
        binds.metadata = jsonBind(mergedMetadata);
      }

      setClauses.push(`${SERVER_UPDATED_AT} = :updatedAt`);
      binds.updatedAt = { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: new Date() };

      await this.db.none(`UPDATE ${this.table(TABLE_MCP_SERVERS)} SET ${setClauses.join(', ')} WHERE id = :id`, binds);

      const updatedServer = await this.getByIdWithClient(this.db, id);
      if (!updatedServer) {
        throw new Error(`MCP server with id ${id} not found after update`);
      }
      return updatedServer;
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('UPDATE_MCP_SERVER', 'FAILED', { mcpServerId: id }, error);
    }
  }

  async delete(id: string): Promise<void> {
    try {
      await this.db.tx(async client => {
        await client.none(`DELETE FROM ${this.table(TABLE_MCP_SERVER_VERSIONS)} WHERE ${VERSION_MCP_SERVER_ID} = :id`, {
          id,
        });
        await client.none(`DELETE FROM ${this.table(TABLE_MCP_SERVERS)} WHERE id = :id`, { id });
      });
    } catch (error) {
      throw this.storageError('DELETE_MCP_SERVER', 'FAILED', { mcpServerId: id }, error);
    }
  }

  async list(args?: StorageListMCPServersInput): Promise<StorageListMCPServersOutput> {
    const { page = 0, perPage: perPageInput, orderBy, authorId, metadata, status = 'published' } = args || {};

    try {
      this.validatePagination(page, perPageInput, 100);
    } catch (error) {
      throw this.storageError('LIST_MCP_SERVERS', 'INVALID_INPUT', { page }, error, ErrorCategory.USER);
    }

    const perPage = normalizePerPage(perPageInput, 100);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const { field, direction } = this.parseOrderBy(orderBy);
      const { whereClause, binds } = this.serverWhereClause({ status, authorId, metadata });

      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_MCP_SERVERS)} ${whereClause}`,
        binds,
      );
      const total = Number(countRow?.count ?? 0);

      if (total === 0) {
        return {
          mcpServers: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      if (perPage === 0) {
        return {
          mcpServers: [],
          total,
          page,
          perPage: perPageForResponse,
          hasMore: total > 0,
        };
      }

      const limit = perPageInput === false ? total : perPage;
      // `id ASC` breaks ties between rows sharing the same order-by value so
      // pages stay stable instead of duplicating or dropping rows across pages.
      const rows = await this.db.read.manyOrNone<MCPServerRow>(
        `${this.serverSelect()} FROM ${this.table(TABLE_MCP_SERVERS)} ${whereClause} ORDER BY ${this.serverOrderColumn(
          field,
        )} ${direction}, id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { ...binds, offset, limit },
      );

      return {
        mcpServers: rows.map(row => this.parseMCPServerRow(row)),
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('LIST_MCP_SERVERS', 'FAILED', {}, error);
    }
  }

  async createVersion(input: CreateMCPServerVersionInput): Promise<MCPServerVersion> {
    try {
      const existingById = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(TABLE_MCP_SERVER_VERSIONS)} WHERE id = :id`,
        { id: input.id },
      );
      if (existingById) {
        throw new Error(`Version with id ${input.id} already exists`);
      }

      const existingByNumber = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(
          TABLE_MCP_SERVER_VERSIONS,
        )} WHERE ${VERSION_MCP_SERVER_ID} = :mcpServerId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { mcpServerId: input.mcpServerId, versionNumber: input.versionNumber },
      );
      if (existingByNumber) {
        throw new Error(`Version number ${input.versionNumber} already exists for MCP server ${input.mcpServerId}`);
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
        'CREATE_MCP_SERVER_VERSION',
        'FAILED',
        { versionId: input.id, mcpServerId: input.mcpServerId },
        error,
      );
    }
  }

  async getVersion(id: string): Promise<MCPServerVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<MCPServerVersionRow>(
        `${this.versionSelect()} FROM ${this.table(TABLE_MCP_SERVER_VERSIONS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_MCP_SERVER_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async getVersionByNumber(mcpServerId: string, versionNumber: number): Promise<MCPServerVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<MCPServerVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_MCP_SERVER_VERSIONS,
        )} WHERE ${VERSION_MCP_SERVER_ID} = :mcpServerId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { mcpServerId, versionNumber },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_MCP_SERVER_VERSION_BY_NUMBER', 'FAILED', { mcpServerId, versionNumber }, error);
    }
  }

  async getLatestVersion(mcpServerId: string): Promise<MCPServerVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<MCPServerVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_MCP_SERVER_VERSIONS,
        )} WHERE ${VERSION_MCP_SERVER_ID} = :mcpServerId ORDER BY ${VERSION_VERSION_NUMBER} DESC FETCH FIRST 1 ROWS ONLY`,
        { mcpServerId },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_LATEST_MCP_SERVER_VERSION', 'FAILED', { mcpServerId }, error);
    }
  }

  async listVersions(input: ListMCPServerVersionsInput): Promise<ListMCPServerVersionsOutput> {
    const { mcpServerId, page = 0, perPage: perPageInput, orderBy } = input;

    try {
      this.validatePagination(page, perPageInput, 20);
    } catch (error) {
      throw this.storageError(
        'LIST_MCP_SERVER_VERSIONS',
        'INVALID_INPUT',
        { page, mcpServerId },
        error,
        ErrorCategory.USER,
      );
    }

    const perPage = normalizePerPage(perPageInput, 20);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const { field, direction } = this.parseVersionOrderBy(orderBy);
      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_MCP_SERVER_VERSIONS)} WHERE ${VERSION_MCP_SERVER_ID} = :mcpServerId`,
        { mcpServerId },
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
      const rows = await this.db.read.manyOrNone<MCPServerVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_MCP_SERVER_VERSIONS,
        )} WHERE ${VERSION_MCP_SERVER_ID} = :mcpServerId ORDER BY ${this.versionOrderColumn(
          field,
        )} ${direction}, id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { mcpServerId, offset, limit },
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
      throw this.storageError('LIST_MCP_SERVER_VERSIONS', 'FAILED', { mcpServerId }, error);
    }
  }

  async deleteVersion(id: string): Promise<void> {
    try {
      await this.db.none(`DELETE FROM ${this.table(TABLE_MCP_SERVER_VERSIONS)} WHERE id = :id`, { id });
    } catch (error) {
      throw this.storageError('DELETE_MCP_SERVER_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async deleteVersionsByParentId(entityId: string): Promise<void> {
    try {
      await this.db.none(
        `DELETE FROM ${this.table(TABLE_MCP_SERVER_VERSIONS)} WHERE ${VERSION_MCP_SERVER_ID} = :entityId`,
        {
          entityId,
        },
      );
    } catch (error) {
      throw this.storageError(
        'DELETE_MCP_SERVER_VERSIONS_BY_MCP_SERVER_ID',
        'FAILED',
        { mcpServerId: entityId },
        error,
      );
    }
  }

  async countVersions(mcpServerId: string): Promise<number> {
    try {
      const row = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_MCP_SERVER_VERSIONS)} WHERE ${VERSION_MCP_SERVER_ID} = :mcpServerId`,
        { mcpServerId },
      );
      return Number(row?.count ?? 0);
    } catch (error) {
      throw this.storageError('COUNT_MCP_SERVER_VERSIONS', 'FAILED', { mcpServerId }, error);
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
    input: CreateMCPServerVersionInput,
    createdAt = new Date(),
  ): Promise<void> {
    await client.none(
      `INSERT INTO ${this.table(TABLE_MCP_SERVER_VERSIONS)} (
        id, ${VERSION_MCP_SERVER_ID}, ${VERSION_VERSION_NUMBER},
        name, version, description, instructions, repository, "releaseDate", "isLatest", "packageCanonical", tools, agents, workflows,
        ${VERSION_CHANGED_FIELDS}, ${VERSION_CHANGE_MESSAGE}, ${VERSION_CREATED_AT}
      ) VALUES (
        :id, :mcpServerId, :versionNumber, :name, :serverVersion,
        CASE WHEN :emptyDescription = 1 THEN EMPTY_CLOB() ELSE :description END,
        CASE WHEN :emptyInstructions = 1 THEN EMPTY_CLOB() ELSE :instructions END,
        :repository, :releaseDate, :isLatest, :packageCanonical, :tools, :agents, :workflows,
        :changedFields, CASE WHEN :emptyChangeMessage = 1 THEN EMPTY_CLOB() ELSE :changeMessage END, :createdAt
      )`,
      {
        id: input.id,
        mcpServerId: input.mcpServerId,
        versionNumber: input.versionNumber,
        name: input.name,
        serverVersion: input.version,
        description: input.description === undefined ? null : clobBind(input.description),
        emptyDescription: input.description === '' ? 1 : 0,
        instructions: input.instructions === undefined ? null : clobBind(input.instructions),
        emptyInstructions: input.instructions === '' ? 1 : 0,
        repository: nullableJsonBind(input.repository),
        releaseDate: input.releaseDate ?? null,
        isLatest: input.isLatest === undefined ? null : input.isLatest ? 1 : 0,
        packageCanonical: input.packageCanonical ?? null,
        tools: nullableJsonBind(input.tools),
        agents: nullableJsonBind(input.agents),
        workflows: nullableJsonBind(input.workflows),
        changedFields: nullableJsonBind(input.changedFields),
        changeMessage: input.changeMessage === undefined ? null : clobBind(input.changeMessage),
        emptyChangeMessage: input.changeMessage === '' ? 1 : 0,
        createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: createdAt },
      },
    );
  }

  private serverWhereClause(args: {
    status: StorageMCPServerType['status'];
    authorId?: string;
    metadata?: Record<string, unknown>;
  }): { whereClause: string; binds: Record<string, unknown> } {
    const conditions = ['status = :status'];
    const binds: Record<string, unknown> = { status: args.status };

    if (args.authorId !== undefined) {
      conditions.push(`${SERVER_AUTHOR_ID} = :authorId`);
      binds.authorId = args.authorId;
    }

    if (args.metadata && Object.keys(args.metadata).length > 0) {
      const filter = buildOracleJsonContains('metadata', args.metadata);
      conditions.push(filter.sql);
      Object.assign(binds, filter.binds);
    }

    return { whereClause: `WHERE ${conditions.join(' AND ')}`, binds };
  }

  private serverSelect(): string {
    return [
      `SELECT id AS "id"`,
      `status AS "status"`,
      `${SERVER_ACTIVE_VERSION_ID} AS "activeVersionId"`,
      `${SERVER_AUTHOR_ID} AS "authorId"`,
      `JSON_SERIALIZE(metadata RETURNING CLOB) AS "metadata"`,
      `${SERVER_CREATED_AT} AS "createdAt"`,
      `${SERVER_UPDATED_AT} AS "updatedAt"`,
    ].join(', ');
  }

  private versionSelect(): string {
    return [
      `SELECT id AS "id"`,
      `${VERSION_MCP_SERVER_ID} AS "mcpServerId"`,
      `${VERSION_VERSION_NUMBER} AS "versionNumber"`,
      `name AS "name"`,
      `version AS "version"`,
      `description AS "description"`,
      `instructions AS "instructions"`,
      `JSON_SERIALIZE(repository RETURNING CLOB) AS "repository"`,
      `"releaseDate"`,
      `"isLatest"`,
      `"packageCanonical"`,
      `JSON_SERIALIZE(tools RETURNING CLOB) AS "tools"`,
      `JSON_SERIALIZE(agents RETURNING CLOB) AS "agents"`,
      `JSON_SERIALIZE(workflows RETURNING CLOB) AS "workflows"`,
      `CASE WHEN description IS NULL THEN 1 ELSE 0 END AS "descriptionIsNull"`,
      `CASE WHEN instructions IS NULL THEN 1 ELSE 0 END AS "instructionsIsNull"`,
      `CASE WHEN ${VERSION_CHANGE_MESSAGE} IS NULL THEN 1 ELSE 0 END AS "changeMessageIsNull"`,
      `JSON_SERIALIZE(${VERSION_CHANGED_FIELDS} RETURNING CLOB) AS "changedFields"`,
      `${VERSION_CHANGE_MESSAGE} AS "changeMessage"`,
      `${VERSION_CREATED_AT} AS "createdAt"`,
    ].join(', ');
  }

  private parseMCPServerRow(row: MCPServerRow): StorageMCPServerType {
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

  private parseVersionRow(row: MCPServerVersionRow): MCPServerVersion {
    return {
      id: String(row.id),
      mcpServerId: String(row.mcpServerId),
      versionNumber: Number(row.versionNumber),
      name: String(row.name),
      version: String(row.version),
      description: Number(row.descriptionIsNull) === 1 ? undefined : String(row.description ?? ''),
      instructions: Number(row.instructionsIsNull) === 1 ? undefined : String(row.instructions ?? ''),
      repository: parseOracleJson<MCPServerVersion['repository']>(row.repository),
      releaseDate: optionalString(row.releaseDate),
      isLatest: row.isLatest == null ? undefined : Number(row.isLatest) === 1,
      packageCanonical: optionalString(row.packageCanonical),
      tools: parseOracleJson<MCPServerVersion['tools']>(row.tools),
      agents: parseOracleJson<MCPServerVersion['agents']>(row.agents),
      workflows: parseOracleJson<MCPServerVersion['workflows']>(row.workflows),
      changedFields: parseOptionalStringArray(row.changedFields),
      changeMessage: Number(row.changeMessageIsNull) === 1 ? undefined : String(row.changeMessage ?? ''),
      createdAt: toDate(row.createdAt),
    };
  }

  private serverOrderColumn(field: string): string {
    return field === 'updatedAt' ? SERVER_UPDATED_AT : SERVER_CREATED_AT;
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

export function getDefaultMCPServerIndexDefinitions(indexName: (name: string) => string): OracleCreateIndexOptions[] {
  return [
    {
      name: indexName('MASTRA_MCP_SERVER_VERSIONS_SERVER_VERSION'),
      table: TABLE_MCP_SERVER_VERSIONS,
      columns: ['mcpServerId', 'versionNumber'],
      unique: true,
    },
  ];
}

function optionalString(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  return String(value);
}
