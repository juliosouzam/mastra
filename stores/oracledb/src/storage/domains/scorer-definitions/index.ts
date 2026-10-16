import { ErrorCategory, MastraError } from '@mastra/core/error';
import {
  calculatePagination,
  normalizePerPage,
  ScorerDefinitionsStorage,
  TABLE_SCHEMAS,
  TABLE_SCORER_DEFINITION_VERSIONS,
  TABLE_SCORER_DEFINITIONS,
} from '@mastra/core/storage';
import type {
  StorageCreateScorerDefinitionInput,
  StorageListScorerDefinitionsInput,
  StorageListScorerDefinitionsOutput,
  StorageScorerDefinitionType,
  StorageUpdateScorerDefinitionInput,
} from '@mastra/core/storage';
import type {
  CreateScorerDefinitionVersionInput,
  ListScorerDefinitionVersionsInput,
  ListScorerDefinitionVersionsOutput,
  ScorerDefinitionVersion,
} from '@mastra/core/storage/domains/scorer-definitions';

import { jsonBind, nullableClobBind, nullableJsonBind } from '../../../shared/connection';
import { indexNameForTable, qualifyName } from '../../../vector/identifiers';
import { OracleDB, createOracleIndex, filterIndexesForTables } from '../../db';
import type { OracleCreateIndexOptions, OracleTxClient } from '../../db';
import {
  buildOracleJsonContains,
  createOracleStorageError,
  parseOptionalJson,
  parseOptionalJsonObject,
  parseOptionalStringArray,
  toDate,
} from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

// Scorer definitions are versioned scoring configurations. Parent rows cover
// lifecycle/listing while version rows keep model, instructions, and ranges.
const STORE_NAME = 'ORACLEDB';
const SNAPSHOT_FIELDS = [
  'name',
  'description',
  'type',
  'model',
  'instructions',
  'scoreRange',
  'presetConfig',
  'defaultSampling',
] as const;

const SCORER_ACTIVE_VERSION_ID = '"activeVersionId"';
const SCORER_AUTHOR_ID = '"authorId"';
const SCORER_ORGANIZATION_ID = '"organizationId"';
const SCORER_PROJECT_ID = '"projectId"';
const SCORER_CREATED_AT = '"createdAt"';
const SCORER_UPDATED_AT = '"updatedAt"';

const VERSION_SCORER_DEFINITION_ID = '"scorerDefinitionId"';
const VERSION_VERSION_NUMBER = '"versionNumber"';
const VERSION_SCORE_RANGE = '"scoreRange"';
const VERSION_PRESET_CONFIG = '"presetConfig"';
const VERSION_DEFAULT_SAMPLING = '"defaultSampling"';
const VERSION_CHANGED_FIELDS = '"changedFields"';
const VERSION_CHANGE_MESSAGE = '"changeMessage"';
const VERSION_CREATED_AT = '"createdAt"';

type ScorerDefinitionRow = {
  id: string;
  status: StorageScorerDefinitionType['status'];
  activeVersionId?: string | null;
  authorId?: string | null;
  organizationId?: string | null;
  projectId?: string | null;
  metadata?: unknown;
  metadataJson?: string | null;
  createdAt: Date | string;
  updatedAt: Date | string;
};

type ScorerDefinitionVersionRow = {
  id: string;
  scorerDefinitionId: string;
  versionNumber: number | string;
  name: string;
  description?: string | null;
  type: ScorerDefinitionVersion['type'];
  model?: unknown;
  instructions?: string | null;
  scoreRange?: unknown;
  presetConfig?: unknown;
  defaultSampling?: unknown;
  changedFields?: unknown;
  changeMessage?: string | null;
  createdAt: Date | string;
};

type VersionWriteClient = Pick<OracleTxClient, 'none'>;

export class ScorerDefinitionsOracle extends ScorerDefinitionsStorage {
  // Scorer definitions use a small parent row plus versioned scoring configuration.
  static readonly MANAGED_TABLES = [TABLE_SCORER_DEFINITIONS, TABLE_SCORER_DEFINITION_VERSIONS] as const;

  private readonly db: OracleDB;
  private readonly schemaName?: string;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({
      ...config,
      jsonColumns: ['model', 'scoreRange', 'presetConfig', 'defaultSampling', 'changedFields'],
    });
    this.schemaName = config.schemaName;
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, ScorerDefinitionsOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_SCORER_DEFINITIONS,
      schema: TABLE_SCHEMAS[TABLE_SCORER_DEFINITIONS],
    });
    await this.db.createTable({
      tableName: TABLE_SCORER_DEFINITION_VERSIONS,
      schema: TABLE_SCHEMAS[TABLE_SCORER_DEFINITION_VERSIONS],
    });
    await this.db.alterTable({
      tableName: TABLE_SCORER_DEFINITIONS,
      schema: TABLE_SCHEMAS[TABLE_SCORER_DEFINITIONS],
      ifNotExists: ['organizationId', 'projectId'],
    });
    await this.createIndexes();
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.tx(async client => {
      await client.none(`DELETE FROM ${this.table(TABLE_SCORER_DEFINITION_VERSIONS)}`);
      await client.none(`DELETE FROM ${this.table(TABLE_SCORER_DEFINITIONS)}`);
    });
  }

  getDefaultIndexDefinitions(): OracleCreateIndexOptions[] {
    return getDefaultScorerDefinitionIndexDefinitions(this.indexName.bind(this));
  }

  async getById(id: string): Promise<StorageScorerDefinitionType | null> {
    return this.getByIdWithClient(this.db.read, id);
  }

  private async getByIdWithClient(client: OracleDB, id: string): Promise<StorageScorerDefinitionType | null> {
    try {
      const row = await client.oneOrNone<ScorerDefinitionRow>(
        `${this.scorerSelect()} FROM ${this.table(TABLE_SCORER_DEFINITIONS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseScorerDefinitionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_SCORER_DEFINITION_BY_ID', 'FAILED', { scorerDefinitionId: id }, error);
    }
  }

  async create(input: { scorerDefinition: StorageCreateScorerDefinitionInput }): Promise<StorageScorerDefinitionType> {
    const { scorerDefinition } = input;

    try {
      const now = new Date();

      await this.db.tx(async client => {
        const existing = await client.oneOrNone<{ id: string }>(
          `SELECT id AS "id" FROM ${this.table(TABLE_SCORER_DEFINITIONS)} WHERE id = :id`,
          { id: scorerDefinition.id },
        );
        if (existing) {
          throw new Error(`Scorer definition with id ${scorerDefinition.id} already exists`);
        }

        await client.none(
          `INSERT INTO ${this.table(TABLE_SCORER_DEFINITIONS)} (
            id, status, ${SCORER_ACTIVE_VERSION_ID}, ${SCORER_AUTHOR_ID},
            ${SCORER_ORGANIZATION_ID}, ${SCORER_PROJECT_ID}, metadata,
            ${SCORER_CREATED_AT}, ${SCORER_UPDATED_AT}
          ) VALUES (
            :id, :status, :activeVersionId, :authorId, :organizationId, :projectId, :metadata,
            :createdAt, :updatedAt
          )`,
          {
            id: scorerDefinition.id,
            status: 'draft',
            activeVersionId: null,
            authorId: scorerDefinition.authorId ?? null,
            organizationId: scorerDefinition.organizationId ?? null,
            projectId: scorerDefinition.projectId ?? null,
            metadata: nullableJsonBind(scorerDefinition.metadata),
            createdAt: now,
            updatedAt: now,
          },
        );

        // Version rows keep scorer model, rubric, and sampling config reproducible across eval runs.
        const {
          id: _id,
          authorId: _authorId,
          organizationId: _organizationId,
          projectId: _projectId,
          metadata: _metadata,
          ...snapshotConfig
        } = scorerDefinition;
        await this.insertVersion(client, {
          id: crypto.randomUUID(),
          scorerDefinitionId: scorerDefinition.id,
          versionNumber: 1,
          ...snapshotConfig,
          changedFields: [...SNAPSHOT_FIELDS],
          changeMessage: 'Initial version',
        });
      });

      return {
        id: scorerDefinition.id,
        status: 'draft',
        activeVersionId: undefined,
        authorId: scorerDefinition.authorId,
        organizationId: scorerDefinition.organizationId,
        projectId: scorerDefinition.projectId,
        metadata: scorerDefinition.metadata,
        createdAt: now,
        updatedAt: now,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('CREATE_SCORER_DEFINITION', 'FAILED', { scorerDefinitionId: scorerDefinition.id }, error);
    }
  }

  async update(input: StorageUpdateScorerDefinitionInput): Promise<StorageScorerDefinitionType> {
    const { id, ...updates } = input;

    try {
      const existingScorer = await this.getByIdWithClient(this.db, id);
      if (!existingScorer) {
        throw new Error(`Scorer definition with id ${id} not found`);
      }

      const { authorId, activeVersionId, metadata, status } = updates;
      const setClauses: string[] = [];
      const binds: Record<string, unknown> = { id };

      if (authorId !== undefined) {
        setClauses.push(`${SCORER_AUTHOR_ID} = :authorId`);
        binds.authorId = authorId;
      }

      if (activeVersionId !== undefined) {
        setClauses.push(`${SCORER_ACTIVE_VERSION_ID} = :activeVersionId`);
        binds.activeVersionId = activeVersionId;
      }

      if (status !== undefined) {
        setClauses.push(`status = :status`);
        binds.status = status;
      }

      if (metadata !== undefined) {
        const existingMetadata =
          existingScorer.metadata &&
          typeof existingScorer.metadata === 'object' &&
          !Array.isArray(existingScorer.metadata)
            ? existingScorer.metadata
            : {};
        // Discovery metadata is mutable on the parent row; scorer model/rubric
        // changes belong in explicit version rows.
        setClauses.push(`metadata = :metadata`);
        binds.metadata = jsonBind({ ...existingMetadata, ...metadata });
      }

      setClauses.push(`${SCORER_UPDATED_AT} = :updatedAt`);
      binds.updatedAt = new Date();

      await this.db.none(
        `UPDATE ${this.table(TABLE_SCORER_DEFINITIONS)} SET ${setClauses.join(', ')} WHERE id = :id`,
        binds,
      );

      const updatedScorer = await this.getByIdWithClient(this.db, id);
      if (!updatedScorer) {
        throw new Error(`Scorer definition with id ${id} not found after update`);
      }
      return updatedScorer;
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('UPDATE_SCORER_DEFINITION', 'FAILED', { scorerDefinitionId: id }, error);
    }
  }

  async delete(id: string): Promise<void> {
    try {
      await this.db.tx(async client => {
        await client.none(
          `DELETE FROM ${this.table(TABLE_SCORER_DEFINITION_VERSIONS)} WHERE ${VERSION_SCORER_DEFINITION_ID} = :id`,
          { id },
        );
        await client.none(`DELETE FROM ${this.table(TABLE_SCORER_DEFINITIONS)} WHERE id = :id`, { id });
      });
    } catch (error) {
      throw this.storageError('DELETE_SCORER_DEFINITION', 'FAILED', { scorerDefinitionId: id }, error);
    }
  }

  async list(args?: StorageListScorerDefinitionsInput): Promise<StorageListScorerDefinitionsOutput> {
    const {
      page = 0,
      perPage: perPageInput,
      orderBy,
      authorId,
      organizationId,
      projectId,
      metadata,
      status,
    } = args || {};

    try {
      this.validatePagination(page, perPageInput, 100);
    } catch (error) {
      throw this.storageError('LIST_SCORER_DEFINITIONS', 'INVALID_INPUT', { page }, error, ErrorCategory.USER);
    }

    const perPage = normalizePerPage(perPageInput, 100);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const { field, direction } = this.parseOrderBy(orderBy);
      const { whereClause, binds } = this.scorerWhereClause({ status, authorId, organizationId, projectId, metadata });

      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_SCORER_DEFINITIONS)} ${whereClause}`,
        binds,
      );
      const total = Number(countRow?.count ?? 0);

      if (total === 0) {
        return {
          scorerDefinitions: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      if (perPage === 0) {
        return {
          scorerDefinitions: [],
          total,
          page,
          perPage: perPageForResponse,
          hasMore: total > 0,
        };
      }

      const limit = perPageInput === false ? total : perPage;
      // `id ASC` breaks ties between rows sharing the same order-by value so
      // pages stay stable instead of duplicating or dropping rows across pages.
      const rows = await this.db.read.manyOrNone<ScorerDefinitionRow>(
        `${this.scorerSelect()} FROM ${this.table(
          TABLE_SCORER_DEFINITIONS,
        )} ${whereClause} ORDER BY ${this.scorerOrderColumn(
          field,
        )} ${direction}, id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { ...binds, offset, limit },
      );

      const scorerDefinitions = rows.flatMap(row => {
        try {
          return [this.parseScorerDefinitionRow(row)];
        } catch (error) {
          this.logger?.warn?.('[Oracle] Failed to map scorer definition row, skipping', { id: row?.id, error });
          return [];
        }
      });

      return {
        scorerDefinitions,
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('LIST_SCORER_DEFINITIONS', 'FAILED', {}, error);
    }
  }

  async createVersion(input: CreateScorerDefinitionVersionInput): Promise<ScorerDefinitionVersion> {
    try {
      const existingById = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(TABLE_SCORER_DEFINITION_VERSIONS)} WHERE id = :id`,
        { id: input.id },
      );
      if (existingById) {
        throw new Error(`Version with id ${input.id} already exists`);
      }

      const existingByNumber = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(
          TABLE_SCORER_DEFINITION_VERSIONS,
        )} WHERE ${VERSION_SCORER_DEFINITION_ID} = :scorerDefinitionId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { scorerDefinitionId: input.scorerDefinitionId, versionNumber: input.versionNumber },
      );
      if (existingByNumber) {
        throw new Error(
          `Version number ${input.versionNumber} already exists for scorer definition ${input.scorerDefinitionId}`,
        );
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
        'CREATE_SCORER_DEFINITION_VERSION',
        'FAILED',
        { versionId: input.id, scorerDefinitionId: input.scorerDefinitionId },
        error,
      );
    }
  }

  async getVersion(id: string): Promise<ScorerDefinitionVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<ScorerDefinitionVersionRow>(
        `${this.versionSelect()} FROM ${this.table(TABLE_SCORER_DEFINITION_VERSIONS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_SCORER_DEFINITION_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async getVersionByNumber(scorerDefinitionId: string, versionNumber: number): Promise<ScorerDefinitionVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<ScorerDefinitionVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_SCORER_DEFINITION_VERSIONS,
        )} WHERE ${VERSION_SCORER_DEFINITION_ID} = :scorerDefinitionId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { scorerDefinitionId, versionNumber },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError(
        'GET_SCORER_DEFINITION_VERSION_BY_NUMBER',
        'FAILED',
        { scorerDefinitionId, versionNumber },
        error,
      );
    }
  }

  async getLatestVersion(scorerDefinitionId: string): Promise<ScorerDefinitionVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<ScorerDefinitionVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_SCORER_DEFINITION_VERSIONS,
        )} WHERE ${VERSION_SCORER_DEFINITION_ID} = :scorerDefinitionId ORDER BY ${VERSION_VERSION_NUMBER} DESC FETCH FIRST 1 ROWS ONLY`,
        { scorerDefinitionId },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_LATEST_SCORER_DEFINITION_VERSION', 'FAILED', { scorerDefinitionId }, error);
    }
  }

  async listVersions(input: ListScorerDefinitionVersionsInput): Promise<ListScorerDefinitionVersionsOutput> {
    const { scorerDefinitionId, page = 0, perPage: perPageInput, orderBy } = input;

    try {
      this.validatePagination(page, perPageInput, 20);
    } catch (error) {
      throw this.storageError(
        'LIST_SCORER_DEFINITION_VERSIONS',
        'INVALID_INPUT',
        { page, scorerDefinitionId },
        error,
        ErrorCategory.USER,
      );
    }

    const perPage = normalizePerPage(perPageInput, 20);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const { field, direction } = this.parseVersionOrderBy(orderBy);
      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(
          TABLE_SCORER_DEFINITION_VERSIONS,
        )} WHERE ${VERSION_SCORER_DEFINITION_ID} = :scorerDefinitionId`,
        { scorerDefinitionId },
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
      const rows = await this.db.read.manyOrNone<ScorerDefinitionVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_SCORER_DEFINITION_VERSIONS,
        )} WHERE ${VERSION_SCORER_DEFINITION_ID} = :scorerDefinitionId ORDER BY ${this.versionOrderColumn(
          field,
        )} ${direction}, id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { scorerDefinitionId, offset, limit },
      );

      const versions = rows.flatMap(row => {
        try {
          return [this.parseVersionRow(row)];
        } catch (error) {
          this.logger?.warn?.('[Oracle] Failed to map scorer definition version row, skipping', {
            id: row?.id,
            error,
          });
          return [];
        }
      });

      return {
        versions,
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('LIST_SCORER_DEFINITION_VERSIONS', 'FAILED', { scorerDefinitionId }, error);
    }
  }

  async deleteVersion(id: string): Promise<void> {
    try {
      await this.db.none(`DELETE FROM ${this.table(TABLE_SCORER_DEFINITION_VERSIONS)} WHERE id = :id`, { id });
    } catch (error) {
      throw this.storageError('DELETE_SCORER_DEFINITION_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async deleteVersionsByParentId(entityId: string): Promise<void> {
    try {
      await this.db.none(
        `DELETE FROM ${this.table(TABLE_SCORER_DEFINITION_VERSIONS)} WHERE ${VERSION_SCORER_DEFINITION_ID} = :entityId`,
        { entityId },
      );
    } catch (error) {
      throw this.storageError(
        'DELETE_SCORER_DEFINITION_VERSIONS_BY_SCORER_DEFINITION_ID',
        'FAILED',
        { scorerDefinitionId: entityId },
        error,
      );
    }
  }

  async countVersions(scorerDefinitionId: string): Promise<number> {
    try {
      const row = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(
          TABLE_SCORER_DEFINITION_VERSIONS,
        )} WHERE ${VERSION_SCORER_DEFINITION_ID} = :scorerDefinitionId`,
        { scorerDefinitionId },
      );
      return Number(row?.count ?? 0);
    } catch (error) {
      throw this.storageError('COUNT_SCORER_DEFINITION_VERSIONS', 'FAILED', { scorerDefinitionId }, error);
    }
  }

  private async createIndexes(): Promise<void> {
    await this.db.withConnection(async connection => {
      for (const index of this.getDefaultIndexDefinitions()) {
        if (this.skipDefaultIndexes && !index.unique) continue;
        try {
          await createOracleIndex(connection, index, this.schemaName);
        } catch (error) {
          if (index.unique) throw error;
          this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
        }
      }

      for (const index of this.indexes) {
        try {
          await createOracleIndex(connection, index, this.schemaName);
        } catch (error) {
          this.logger?.warn?.(`Failed to create Oracle custom index ${index.name}:`, error);
        }
      }
    });
  }

  private async insertVersion(
    client: VersionWriteClient,
    input: CreateScorerDefinitionVersionInput,
    createdAt = new Date(),
  ): Promise<void> {
    // Version rows keep evaluation behavior reproducible: model, instructions,
    // score range, presets, and sampling are all stored as one snapshot.
    await client.none(
      `INSERT INTO ${this.table(TABLE_SCORER_DEFINITION_VERSIONS)} (
        id, ${VERSION_SCORER_DEFINITION_ID}, ${VERSION_VERSION_NUMBER},
        name, description, type, model, instructions, ${VERSION_SCORE_RANGE}, ${VERSION_PRESET_CONFIG},
        ${VERSION_DEFAULT_SAMPLING}, ${VERSION_CHANGED_FIELDS}, ${VERSION_CHANGE_MESSAGE},
        ${VERSION_CREATED_AT}
      ) VALUES (
        :id, :scorerDefinitionId, :versionNumber,
        :name, :description, :type, :model, :instructions, :scoreRange, :presetConfig,
        :defaultSampling, :changedFields, :changeMessage,
        :createdAt
      )`,
      {
        id: input.id,
        scorerDefinitionId: input.scorerDefinitionId,
        versionNumber: input.versionNumber,
        name: input.name,
        description: nullableClobBind(input.description),
        type: input.type,
        model: nullableJsonBind(input.model),
        instructions: nullableClobBind(input.instructions),
        scoreRange: nullableJsonBind(input.scoreRange),
        presetConfig: nullableJsonBind(input.presetConfig),
        defaultSampling: nullableJsonBind(input.defaultSampling),
        changedFields: nullableJsonBind(input.changedFields),
        changeMessage: nullableClobBind(input.changeMessage),
        createdAt,
      },
    );
  }

  private scorerWhereClause(args: {
    status?: StorageScorerDefinitionType['status'];
    authorId?: string;
    organizationId?: string;
    projectId?: string;
    metadata?: Record<string, unknown>;
  }): { whereClause: string; binds: Record<string, unknown> } {
    const conditions: string[] = [];
    const binds: Record<string, unknown> = {};

    if (args.status) {
      conditions.push(`status = :status`);
      binds.status = args.status;
    }

    if (args.authorId !== undefined) {
      conditions.push(`${SCORER_AUTHOR_ID} = :authorId`);
      binds.authorId = args.authorId;
    }

    if (args.organizationId !== undefined) {
      conditions.push(`${SCORER_ORGANIZATION_ID} = :organizationId`);
      binds.organizationId = args.organizationId;
    }
    if (args.projectId !== undefined) {
      conditions.push(`${SCORER_PROJECT_ID} = :projectId`);
      binds.projectId = args.projectId;
    }

    if (args.metadata && Object.keys(args.metadata).length > 0) {
      const metadata = buildOracleJsonContains('metadata', args.metadata);
      conditions.push(metadata.sql);
      Object.assign(binds, metadata.binds);
    }

    return { whereClause: conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '', binds };
  }

  private scorerSelect(): string {
    return [
      `SELECT id AS "id"`,
      `status AS "status"`,
      `${SCORER_ACTIVE_VERSION_ID} AS "activeVersionId"`,
      `${SCORER_AUTHOR_ID} AS "authorId"`,
      `${SCORER_ORGANIZATION_ID} AS "organizationId"`,
      `${SCORER_PROJECT_ID} AS "projectId"`,
      `JSON_SERIALIZE(metadata RETURNING CLOB) AS "metadataJson"`,
      `${SCORER_CREATED_AT} AS "createdAt"`,
      `${SCORER_UPDATED_AT} AS "updatedAt"`,
    ].join(', ');
  }

  private versionSelect(): string {
    return [
      `SELECT id AS "id"`,
      `${VERSION_SCORER_DEFINITION_ID} AS "scorerDefinitionId"`,
      `${VERSION_VERSION_NUMBER} AS "versionNumber"`,
      `name AS "name"`,
      `description AS "description"`,
      `type AS "type"`,
      `JSON_SERIALIZE(model RETURNING CLOB) AS "model"`,
      `instructions AS "instructions"`,
      `JSON_SERIALIZE(${VERSION_SCORE_RANGE} RETURNING CLOB) AS "scoreRange"`,
      `JSON_SERIALIZE(${VERSION_PRESET_CONFIG} RETURNING CLOB) AS "presetConfig"`,
      `JSON_SERIALIZE(${VERSION_DEFAULT_SAMPLING} RETURNING CLOB) AS "defaultSampling"`,
      `JSON_SERIALIZE(${VERSION_CHANGED_FIELDS} RETURNING CLOB) AS "changedFields"`,
      `${VERSION_CHANGE_MESSAGE} AS "changeMessage"`,
      `${VERSION_CREATED_AT} AS "createdAt"`,
    ].join(', ');
  }

  private parseScorerDefinitionRow(row: ScorerDefinitionRow): StorageScorerDefinitionType {
    return {
      id: String(row.id),
      status: row.status,
      activeVersionId: optionalString(row.activeVersionId),
      authorId: optionalString(row.authorId),
      organizationId: optionalString(row.organizationId),
      projectId: optionalString(row.projectId),
      metadata: parseOptionalJsonObject(row.metadataJson ?? row.metadata),
      createdAt: toDate(row.createdAt),
      updatedAt: toDate(row.updatedAt),
    };
  }

  private parseVersionRow(row: ScorerDefinitionVersionRow): ScorerDefinitionVersion {
    // Normalize JSON config columns and CLOB text before returning the public
    // ScorerDefinitionVersion expected by core.
    return {
      id: String(row.id),
      scorerDefinitionId: String(row.scorerDefinitionId),
      versionNumber: Number(row.versionNumber),
      name: String(row.name),
      description: optionalString(row.description),
      type: row.type,
      model: parseOptionalJson<ScorerDefinitionVersion['model']>(row.model),
      instructions: optionalString(row.instructions),
      scoreRange: parseOptionalJson<ScorerDefinitionVersion['scoreRange']>(row.scoreRange),
      presetConfig: parseOptionalJson<ScorerDefinitionVersion['presetConfig']>(row.presetConfig),
      defaultSampling: parseOptionalJson<ScorerDefinitionVersion['defaultSampling']>(row.defaultSampling),
      changedFields: parseOptionalStringArray(row.changedFields),
      changeMessage: optionalString(row.changeMessage),
      createdAt: toDate(row.createdAt),
    };
  }

  private scorerOrderColumn(field: string): string {
    return field === 'updatedAt' ? SCORER_UPDATED_AT : SCORER_CREATED_AT;
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

export function getDefaultScorerDefinitionIndexDefinitions(
  indexName: (name: string) => string,
): OracleCreateIndexOptions[] {
  return [
    {
      name: indexName('MASTRA_SCORER_DEFINITION_VERSIONS_DEF_VERSION'),
      table: TABLE_SCORER_DEFINITION_VERSIONS,
      columns: ['scorerDefinitionId', 'versionNumber'],
      unique: true,
    },
  ];
}

function optionalString(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  return String(value);
}
