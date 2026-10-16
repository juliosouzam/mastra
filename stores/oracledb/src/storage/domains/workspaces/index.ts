import { isDeepStrictEqual } from 'node:util';
import { ErrorCategory, MastraError } from '@mastra/core/error';
import {
  calculatePagination,
  WorkspacesStorage,
  normalizePerPage,
  TABLE_WORKSPACES,
  TABLE_WORKSPACE_VERSIONS,
  TABLE_SCHEMAS,
} from '@mastra/core/storage';
import type {
  StorageCreateWorkspaceInput,
  StorageListWorkspacesInput,
  StorageListWorkspacesOutput,
  StorageWorkspaceType,
  StorageUpdateWorkspaceInput,
} from '@mastra/core/storage';
import type {
  CreateWorkspaceVersionInput,
  ListWorkspaceVersionsInput,
  ListWorkspaceVersionsOutput,
  WorkspaceVersion,
} from '@mastra/core/storage/domains/workspaces';

import oracledb from 'oracledb';

import { clobBind, jsonBind, nullableJsonBind, safeJsonValue } from '../../../shared/connection';
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

// Workspaces use the same registry/version pattern as agents: parent rows handle discovery while version rows snapshot workspace filesystem and execution configuration.
const STORE_NAME = 'ORACLEDB';
const SNAPSHOT_FIELDS = [
  'name',
  'description',
  'filesystem',
  'sandbox',
  'mounts',
  'search',
  'skills',
  'tools',
  'autoSync',
  'operationTimeout',
] as const;

const WORKSPACE_ACTIVE_VERSION_ID = '"activeVersionId"';
const WORKSPACE_AUTHOR_ID = '"authorId"';
const WORKSPACE_CREATED_AT = '"createdAt"';
const WORKSPACE_UPDATED_AT = '"updatedAt"';
const VERSION_WORKSPACE_ID = '"workspaceId"';
const VERSION_VERSION_NUMBER = '"versionNumber"';
const VERSION_CHANGED_FIELDS = '"changedFields"';
const VERSION_CHANGE_MESSAGE = '"changeMessage"';
const VERSION_CREATED_AT = '"createdAt"';

type WorkspaceRow = {
  id: string;
  status: StorageWorkspaceType['status'];
  activeVersionId?: string | null;
  authorId?: string | null;
  metadata?: unknown;
  createdAt: Date | string;
  updatedAt: Date | string;
};

type WorkspaceVersionRow = {
  id: string;
  workspaceId: string;
  versionNumber: number | string;
  name: string;
  description?: string | null;
  descriptionIsNull: number;
  changeMessageIsNull: number;
  filesystem?: unknown;
  sandbox?: unknown;
  mounts?: unknown;
  search?: unknown;
  skills?: unknown;
  tools?: unknown;
  autoSync?: number | null;
  operationTimeout?: number | null;
  changedFields?: unknown;
  changeMessage?: string | null;
  createdAt: Date | string;
};

type VersionWriteClient = Pick<OracleTxClient, 'none'>;

export class WorkspacesOracle extends WorkspacesStorage {
  // Workspaces follow the registry pattern: parent lifecycle row plus versioned workspace configuration snapshots.
  static readonly MANAGED_TABLES = [TABLE_WORKSPACES, TABLE_WORKSPACE_VERSIONS] as const;

  private readonly db: OracleDB;
  private readonly schemaName?: string;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({
      ...config,
      jsonColumns: ['metadata', 'filesystem', 'sandbox', 'mounts', 'search', 'skills', 'tools', 'changedFields'],
    });
    this.schemaName = config.schemaName;
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, WorkspacesOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_WORKSPACES,
      schema: TABLE_SCHEMAS[TABLE_WORKSPACES],
    });
    await this.db.createTable({
      tableName: TABLE_WORKSPACE_VERSIONS,
      schema: TABLE_SCHEMAS[TABLE_WORKSPACE_VERSIONS],
    });
    await this.createIndexes();
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.tx(async client => {
      await client.none(`DELETE FROM ${this.table(TABLE_WORKSPACE_VERSIONS)}`);
      await client.none(`DELETE FROM ${this.table(TABLE_WORKSPACES)}`);
    });
  }

  getDefaultIndexDefinitions(): OracleCreateIndexOptions[] {
    return getDefaultWorkspaceIndexDefinitions(this.indexName.bind(this));
  }

  async getById(id: string): Promise<StorageWorkspaceType | null> {
    try {
      const row = await this.db.read.oneOrNone<WorkspaceRow>(
        `${this.workspaceSelect()} FROM ${this.table(TABLE_WORKSPACES)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseWorkspaceRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_WORKSPACE_BY_ID', 'FAILED', { workspaceId: id }, error);
    }
  }

  async create(input: { workspace: StorageCreateWorkspaceInput }): Promise<StorageWorkspaceType> {
    const { workspace } = input;

    try {
      const now = new Date();

      await this.db.tx(async client => {
        const existing = await client.oneOrNone<{ id: string }>(
          `SELECT id AS "id" FROM ${this.table(TABLE_WORKSPACES)} WHERE id = :id`,
          { id: workspace.id },
        );
        if (existing) {
          throw new Error(`Workspace with id ${workspace.id} already exists`);
        }

        await client.none(
          `INSERT INTO ${this.table(TABLE_WORKSPACES)} (
            id, status, ${WORKSPACE_ACTIVE_VERSION_ID}, ${WORKSPACE_AUTHOR_ID}, metadata,
            ${WORKSPACE_CREATED_AT}, ${WORKSPACE_UPDATED_AT}
          ) VALUES (
            :id, :status, :activeVersionId, :authorId, :metadata,
            :createdAt, :updatedAt
          )`,
          {
            id: workspace.id,
            status: 'draft',
            activeVersionId: null,
            authorId: workspace.authorId ?? null,
            metadata: nullableJsonBind(workspace.metadata),
            createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: now },
            updatedAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: now },
          },
        );

        // Keep filesystem and execution configuration in immutable version snapshots.
        const { id: _id, authorId: _authorId, metadata: _metadata, ...snapshotConfig } = workspace;
        await this.insertVersion(client, {
          id: crypto.randomUUID(),
          workspaceId: workspace.id,
          versionNumber: 1,
          ...snapshotConfig,
          changedFields: [...SNAPSHOT_FIELDS],
          changeMessage: 'Initial version',
        });
      });

      return {
        id: workspace.id,
        status: 'draft',
        activeVersionId: undefined,
        authorId: workspace.authorId,
        metadata: workspace.metadata,
        createdAt: now,
        updatedAt: now,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('CREATE_WORKSPACE', 'FAILED', { workspaceId: workspace.id }, error);
    }
  }

  async update(input: StorageUpdateWorkspaceInput): Promise<StorageWorkspaceType> {
    const { id, authorId, activeVersionId, metadata, status, ...rawConfigFields } = input;
    try {
      return await this.db.tx(async client => {
        // Serialize automatic version allocation on the parent, as with other Oracle state updates.
        const row = await client.oneOrNone<WorkspaceRow>(
          `${this.workspaceSelect()} FROM ${this.table(TABLE_WORKSPACES)} WHERE id = :id FOR UPDATE`,
          { id },
        );
        if (!row) throw new Error(`Workspace with id ${id} not found`);
        const existing = this.parseWorkspaceRow(row);
        const configFields = Object.fromEntries(
          Object.entries(rawConfigFields).filter(([, value]) => value !== undefined),
        );
        if (SNAPSHOT_FIELDS.some(field => field in configFields)) {
          const latestRow = await client.oneOrNone<WorkspaceVersionRow>(
            `${this.versionSelect()} FROM ${this.table(TABLE_WORKSPACE_VERSIONS)} WHERE ${VERSION_WORKSPACE_ID} = :id ORDER BY ${VERSION_VERSION_NUMBER} DESC FETCH FIRST 1 ROW ONLY`,
            { id },
          );
          if (!latestRow) throw new Error(`No versions found for workspace ${id}`);
          const latest = this.parseVersionRow(latestRow);
          const {
            id: _id,
            workspaceId: _workspaceId,
            versionNumber: _number,
            changedFields: _changedFields,
            changeMessage: _message,
            createdAt: _at,
            ...latestConfig
          } = latest;
          const normalized = (value: unknown) => (value === undefined ? undefined : safeJsonValue(value));
          const changedFields = SNAPSHOT_FIELDS.filter(
            field =>
              field in configFields &&
              !isDeepStrictEqual(normalized(configFields[field]), normalized(latestConfig[field])),
          );
          if (changedFields.length) {
            await this.insertVersion(client, {
              id: crypto.randomUUID(),
              workspaceId: id,
              versionNumber: latest.versionNumber + 1,
              ...latestConfig,
              ...configFields,
              changedFields,
              changeMessage: `Updated ${changedFields.join(', ')}`,
            });
          }
        }
        const setClauses = [`${WORKSPACE_UPDATED_AT} = :updatedAt`];
        const binds: Record<string, unknown> = {
          id,
          updatedAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: new Date() },
        };
        if (authorId !== undefined) {
          setClauses.push(`${WORKSPACE_AUTHOR_ID} = :authorId`);
          binds.authorId = authorId;
        }
        if (activeVersionId !== undefined) {
          setClauses.push(`${WORKSPACE_ACTIVE_VERSION_ID} = :activeVersionId`);
          binds.activeVersionId = activeVersionId;
          if (status === undefined) {
            setClauses.push('status = :status');
            binds.status = 'published';
          }
        }
        if (status !== undefined) {
          setClauses.push('status = :status');
          binds.status = status;
        }
        if (metadata !== undefined) {
          setClauses.push('metadata = :metadata');
          binds.metadata = jsonBind({
            ...(existing.metadata ?? {}),
            ...(safeJsonValue(metadata) as Record<string, unknown>),
          });
        }
        await client.none(`UPDATE ${this.table(TABLE_WORKSPACES)} SET ${setClauses.join(', ')} WHERE id = :id`, binds);
        return this.parseWorkspaceRow(
          await client.one<WorkspaceRow>(
            `${this.workspaceSelect()} FROM ${this.table(TABLE_WORKSPACES)} WHERE id = :id`,
            { id },
          ),
        );
      });
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('UPDATE_WORKSPACE', 'FAILED', { workspaceId: id }, error);
    }
  }

  async delete(id: string): Promise<void> {
    try {
      await this.db.tx(async client => {
        await client.none(`DELETE FROM ${this.table(TABLE_WORKSPACE_VERSIONS)} WHERE ${VERSION_WORKSPACE_ID} = :id`, {
          id,
        });
        await client.none(`DELETE FROM ${this.table(TABLE_WORKSPACES)} WHERE id = :id`, { id });
      });
    } catch (error) {
      throw this.storageError('DELETE_WORKSPACE', 'FAILED', { workspaceId: id }, error);
    }
  }

  async list(args?: StorageListWorkspacesInput): Promise<StorageListWorkspacesOutput> {
    const { page = 0, perPage: perPageInput, orderBy, authorId, metadata } = args || {};

    try {
      this.validatePagination(page, perPageInput, 100);
    } catch (error) {
      throw this.storageError('LIST_WORKSPACES', 'INVALID_INPUT', { page }, error, ErrorCategory.USER);
    }

    const perPage = normalizePerPage(perPageInput, 100);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const { field, direction } = this.parseOrderBy(orderBy);
      const { whereClause, binds } = this.workspaceWhereClause({ authorId, metadata });

      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_WORKSPACES)} ${whereClause}`,
        binds,
      );
      const total = Number(countRow?.count ?? 0);

      if (total === 0) {
        return {
          workspaces: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      if (perPage === 0) {
        return {
          workspaces: [],
          total,
          page,
          perPage: perPageForResponse,
          hasMore: total > 0,
        };
      }

      const limit = perPageInput === false ? total : perPage;
      // `id ASC` breaks ties between rows sharing the same order-by value so
      // pages stay stable instead of duplicating or dropping rows across pages.
      const rows = await this.db.read.manyOrNone<WorkspaceRow>(
        `${this.workspaceSelect()} FROM ${this.table(TABLE_WORKSPACES)} ${whereClause} ORDER BY ${this.workspaceOrderColumn(
          field,
        )} ${direction}, id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { ...binds, offset, limit },
      );

      return {
        workspaces: rows.map(row => this.parseWorkspaceRow(row)),
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('LIST_WORKSPACES', 'FAILED', {}, error);
    }
  }

  async createVersion(input: CreateWorkspaceVersionInput): Promise<WorkspaceVersion> {
    try {
      const existingById = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(TABLE_WORKSPACE_VERSIONS)} WHERE id = :id`,
        { id: input.id },
      );
      if (existingById) {
        throw new Error(`Version with id ${input.id} already exists`);
      }

      const existingByNumber = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(
          TABLE_WORKSPACE_VERSIONS,
        )} WHERE ${VERSION_WORKSPACE_ID} = :workspaceId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { workspaceId: input.workspaceId, versionNumber: input.versionNumber },
      );
      if (existingByNumber) {
        throw new Error(`Version number ${input.versionNumber} already exists for Workspace ${input.workspaceId}`);
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
        'CREATE_WORKSPACE_VERSION',
        'FAILED',
        { versionId: input.id, workspaceId: input.workspaceId },
        error,
      );
    }
  }

  async getVersion(id: string): Promise<WorkspaceVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<WorkspaceVersionRow>(
        `${this.versionSelect()} FROM ${this.table(TABLE_WORKSPACE_VERSIONS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_WORKSPACE_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async getVersionByNumber(workspaceId: string, versionNumber: number): Promise<WorkspaceVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<WorkspaceVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_WORKSPACE_VERSIONS,
        )} WHERE ${VERSION_WORKSPACE_ID} = :workspaceId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { workspaceId, versionNumber },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_WORKSPACE_VERSION_BY_NUMBER', 'FAILED', { workspaceId, versionNumber }, error);
    }
  }

  async getLatestVersion(workspaceId: string): Promise<WorkspaceVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<WorkspaceVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_WORKSPACE_VERSIONS,
        )} WHERE ${VERSION_WORKSPACE_ID} = :workspaceId ORDER BY ${VERSION_VERSION_NUMBER} DESC FETCH FIRST 1 ROWS ONLY`,
        { workspaceId },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_LATEST_WORKSPACE_VERSION', 'FAILED', { workspaceId }, error);
    }
  }

  async listVersions(input: ListWorkspaceVersionsInput): Promise<ListWorkspaceVersionsOutput> {
    const { workspaceId, page = 0, perPage: perPageInput, orderBy } = input;

    try {
      this.validatePagination(page, perPageInput, 20);
    } catch (error) {
      throw this.storageError(
        'LIST_WORKSPACE_VERSIONS',
        'INVALID_INPUT',
        { page, workspaceId },
        error,
        ErrorCategory.USER,
      );
    }

    const perPage = normalizePerPage(perPageInput, 20);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const { field, direction } = this.parseVersionOrderBy(orderBy);
      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_WORKSPACE_VERSIONS)} WHERE ${VERSION_WORKSPACE_ID} = :workspaceId`,
        { workspaceId },
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
      const rows = await this.db.read.manyOrNone<WorkspaceVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_WORKSPACE_VERSIONS,
        )} WHERE ${VERSION_WORKSPACE_ID} = :workspaceId ORDER BY ${this.versionOrderColumn(
          field,
        )} ${direction}, id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { workspaceId, offset, limit },
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
      throw this.storageError('LIST_WORKSPACE_VERSIONS', 'FAILED', { workspaceId }, error);
    }
  }

  async deleteVersion(id: string): Promise<void> {
    try {
      await this.db.none(`DELETE FROM ${this.table(TABLE_WORKSPACE_VERSIONS)} WHERE id = :id`, { id });
    } catch (error) {
      throw this.storageError('DELETE_WORKSPACE_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async deleteVersionsByParentId(entityId: string): Promise<void> {
    try {
      await this.db.none(
        `DELETE FROM ${this.table(TABLE_WORKSPACE_VERSIONS)} WHERE ${VERSION_WORKSPACE_ID} = :entityId`,
        {
          entityId,
        },
      );
    } catch (error) {
      throw this.storageError('DELETE_WORKSPACE_VERSIONS_BY_WORKSPACE_ID', 'FAILED', { workspaceId: entityId }, error);
    }
  }

  async countVersions(workspaceId: string): Promise<number> {
    try {
      const row = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_WORKSPACE_VERSIONS)} WHERE ${VERSION_WORKSPACE_ID} = :workspaceId`,
        { workspaceId },
      );
      return Number(row?.count ?? 0);
    } catch (error) {
      throw this.storageError('COUNT_WORKSPACE_VERSIONS', 'FAILED', { workspaceId }, error);
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
    input: CreateWorkspaceVersionInput,
    createdAt = new Date(),
  ): Promise<void> {
    await client.none(
      `INSERT INTO ${this.table(TABLE_WORKSPACE_VERSIONS)} (
        id, ${VERSION_WORKSPACE_ID}, ${VERSION_VERSION_NUMBER},
        name, description, filesystem, sandbox, mounts, search, skills, tools, "autoSync", "operationTimeout",
        ${VERSION_CHANGED_FIELDS}, ${VERSION_CHANGE_MESSAGE}, ${VERSION_CREATED_AT}
      ) VALUES (
        :id, :workspaceId, :versionNumber, :name,
        CASE WHEN :emptyDescription = 1 THEN EMPTY_CLOB() ELSE :description END,
        :filesystem, :sandbox, :mounts, :search, :skills, :tools, :autoSync, :operationTimeout,
        :changedFields, CASE WHEN :emptyChangeMessage = 1 THEN EMPTY_CLOB() ELSE :changeMessage END, :createdAt
      )`,
      {
        id: input.id,
        workspaceId: input.workspaceId,
        versionNumber: input.versionNumber,
        name: input.name,
        description: input.description === undefined ? null : clobBind(input.description),
        emptyDescription: input.description === '' ? 1 : 0,
        filesystem: nullableJsonBind(input.filesystem),
        sandbox: nullableJsonBind(input.sandbox),
        mounts: nullableJsonBind(input.mounts),
        search: nullableJsonBind(input.search),
        skills: nullableJsonBind(input.skills),
        tools: nullableJsonBind(input.tools),
        autoSync: input.autoSync === undefined ? null : input.autoSync ? 1 : 0,
        operationTimeout: input.operationTimeout ?? null,
        changedFields: nullableJsonBind(input.changedFields),
        changeMessage: input.changeMessage === undefined ? null : clobBind(input.changeMessage),
        emptyChangeMessage: input.changeMessage === '' ? 1 : 0,
        createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: createdAt },
      },
    );
  }

  private workspaceWhereClause(args: { authorId?: string; metadata?: Record<string, unknown> }): {
    whereClause: string;
    binds: Record<string, unknown>;
  } {
    const conditions: string[] = [];
    const binds: Record<string, unknown> = {};

    if (args.authorId !== undefined) {
      conditions.push(`${WORKSPACE_AUTHOR_ID} = :authorId`);
      binds.authorId = args.authorId;
    }

    if (args.metadata && Object.keys(args.metadata).length > 0) {
      const filter = buildOracleJsonContains('metadata', args.metadata);
      conditions.push(filter.sql);
      Object.assign(binds, filter.binds);
    }

    return { whereClause: conditions.length ? `WHERE ${conditions.join(' AND ')}` : '', binds };
  }

  private workspaceSelect(): string {
    return [
      `SELECT id AS "id"`,
      `status AS "status"`,
      `${WORKSPACE_ACTIVE_VERSION_ID} AS "activeVersionId"`,
      `${WORKSPACE_AUTHOR_ID} AS "authorId"`,
      `JSON_SERIALIZE(metadata RETURNING CLOB) AS "metadata"`,
      `${WORKSPACE_CREATED_AT} AS "createdAt"`,
      `${WORKSPACE_UPDATED_AT} AS "updatedAt"`,
    ].join(', ');
  }

  private versionSelect(): string {
    return [
      `SELECT id AS "id"`,
      `${VERSION_WORKSPACE_ID} AS "workspaceId"`,
      `${VERSION_VERSION_NUMBER} AS "versionNumber"`,
      `name AS "name"`,
      `description AS "description"`,
      `JSON_SERIALIZE(filesystem RETURNING CLOB) AS "filesystem"`,
      `JSON_SERIALIZE(sandbox RETURNING CLOB) AS "sandbox"`,
      `JSON_SERIALIZE(mounts RETURNING CLOB) AS "mounts"`,
      `JSON_SERIALIZE(search RETURNING CLOB) AS "search"`,
      `JSON_SERIALIZE(skills RETURNING CLOB) AS "skills"`,
      `JSON_SERIALIZE(tools RETURNING CLOB) AS "tools"`,
      `"autoSync"`,
      `"operationTimeout"`,
      `CASE WHEN description IS NULL THEN 1 ELSE 0 END AS "descriptionIsNull"`,
      `CASE WHEN ${VERSION_CHANGE_MESSAGE} IS NULL THEN 1 ELSE 0 END AS "changeMessageIsNull"`,
      `JSON_SERIALIZE(${VERSION_CHANGED_FIELDS} RETURNING CLOB) AS "changedFields"`,
      `${VERSION_CHANGE_MESSAGE} AS "changeMessage"`,
      `${VERSION_CREATED_AT} AS "createdAt"`,
    ].join(', ');
  }

  private parseWorkspaceRow(row: WorkspaceRow): StorageWorkspaceType {
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

  private parseVersionRow(row: WorkspaceVersionRow): WorkspaceVersion {
    return {
      id: String(row.id),
      workspaceId: String(row.workspaceId),
      versionNumber: Number(row.versionNumber),
      name: String(row.name),
      description: Number(row.descriptionIsNull) === 1 ? undefined : String(row.description ?? ''),
      filesystem: parseOracleJson<WorkspaceVersion['filesystem']>(row.filesystem),
      sandbox: parseOracleJson<WorkspaceVersion['sandbox']>(row.sandbox),
      mounts: parseOracleJson<WorkspaceVersion['mounts']>(row.mounts),
      search: parseOracleJson<WorkspaceVersion['search']>(row.search),
      skills: parseOracleJson<WorkspaceVersion['skills']>(row.skills),
      tools: parseOracleJson<WorkspaceVersion['tools']>(row.tools),
      autoSync: row.autoSync == null ? undefined : Number(row.autoSync) === 1,
      operationTimeout: row.operationTimeout == null ? undefined : Number(row.operationTimeout),
      changedFields: parseOptionalStringArray(row.changedFields),
      changeMessage: Number(row.changeMessageIsNull) === 1 ? undefined : String(row.changeMessage ?? ''),
      createdAt: toDate(row.createdAt),
    };
  }

  private workspaceOrderColumn(field: string): string {
    return field === 'updatedAt' ? WORKSPACE_UPDATED_AT : WORKSPACE_CREATED_AT;
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

export function getDefaultWorkspaceIndexDefinitions(indexName: (name: string) => string): OracleCreateIndexOptions[] {
  return [
    {
      name: indexName('MASTRA_WORKSPACE_VERSIONS_WORKSPACE_VERSION'),
      table: TABLE_WORKSPACE_VERSIONS,
      columns: ['workspaceId', 'versionNumber'],
      unique: true,
    },
  ];
}

function optionalString(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  return String(value);
}
