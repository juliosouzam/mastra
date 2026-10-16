import { ErrorCategory, MastraError } from '@mastra/core/error';
import {
  AgentsStorage,
  calculatePagination,
  normalizePerPage,
  TABLE_AGENTS,
  TABLE_AGENT_VERSIONS,
  TABLE_FAVORITES,
  TABLE_SCHEMAS,
} from '@mastra/core/storage';
import type {
  AgentInstructionBlock,
  StorageAgentType,
  StorageCreateAgentInput,
  StorageListAgentsInput,
  StorageListAgentsOutput,
  StorageUpdateAgentInput,
} from '@mastra/core/storage';
import type {
  AgentVersion,
  CreateVersionInput,
  ListVersionsInput,
  ListVersionsOutput,
} from '@mastra/core/storage/domains/agents';

import { clobBind, jsonBind, nullableClobBind, nullableJsonBind, safeJsonValue } from '../../../shared/connection';
import { indexNameForTable, qualifyName } from '../../../vector/identifiers';
import { OracleDB, createOracleIndex, filterIndexesForTables, parseOracleJson } from '../../db';
import type { OracleCreateIndexOptions, OracleTxClient } from '../../db';
import {
  buildOracleJsonContains,
  createOracleStorageError,
  parseOptionalJson,
  parseOptionalJsonObject,
  toDate,
} from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

// Agents are versioned registry records: the parent row supports listing and lifecycle state while version rows preserve editable snapshots.
const STORE_NAME = 'ORACLEDB';
const AGENT_ACTIVE_VERSION_ID = '"activeVersionId"';
const AGENT_AUTHOR_ID = '"authorId"';
const AGENT_FAVORITE_COUNT = '"favoriteCount"';
const AGENT_CREATED_AT = '"createdAt"';
const AGENT_UPDATED_AT = '"updatedAt"';

const VERSION_AGENT_ID = '"agentId"';
const VERSION_VERSION_NUMBER = '"versionNumber"';
const VERSION_DEFAULT_OPTIONS = '"defaultOptions"';
const VERSION_INTEGRATION_TOOLS = '"integrationTools"';
const VERSION_TOOL_PROVIDERS = '"toolProviders"';
const VERSION_SKILLS_FORMAT = '"skillsFormat"';
const VERSION_INPUT_PROCESSORS = '"inputProcessors"';
const VERSION_OUTPUT_PROCESSORS = '"outputProcessors"';
const VERSION_MCP_CLIENTS = '"mcpClients"';
const VERSION_REQUEST_CONTEXT_SCHEMA = '"requestContextSchema"';
const VERSION_CHANGED_FIELDS = '"changedFields"';
const VERSION_CHANGE_MESSAGE = '"changeMessage"';
const VERSION_CREATED_AT = '"createdAt"';

type AgentRow = {
  id: string;
  status: StorageAgentType['status'];
  activeVersionId?: string | null;
  authorId?: string | null;
  visibility?: StorageAgentType['visibility'] | null;
  metadata?: unknown;
  metadataJson?: string | null;
  favoriteCount?: number | string | null;
  createdAt: Date | string;
  updatedAt: Date | string;
};

type AgentVersionRow = {
  id: string;
  agentId: string;
  versionNumber: number | string;
  name: string;
  description?: string | null;
  instructions?: string | null;
  model: unknown;
  tools?: unknown;
  defaultOptions?: unknown;
  workflows?: unknown;
  agents?: unknown;
  integrationTools?: unknown;
  toolProviders?: unknown;
  inputProcessors?: unknown;
  outputProcessors?: unknown;
  memory?: unknown;
  scorers?: unknown;
  mcpClients?: unknown;
  requestContextSchema?: unknown;
  workspace?: unknown;
  skills?: unknown;
  skillsFormat?: AgentVersion['skillsFormat'] | null;
  durable?: unknown;
  browser?: unknown;
  changedFields?: unknown;
  changeMessage?: string | null;
  createdAt: Date | string;
};

type VersionWriteClient = Pick<OracleTxClient, 'none'>;

const ORACLE_AGENT_VERSIONS_SCHEMA = TABLE_SCHEMAS[TABLE_AGENT_VERSIONS];

export { ORACLE_AGENT_VERSIONS_SCHEMA };

export class AgentsOracle extends AgentsStorage {
  // Agents use a parent row for lifecycle/status and a version row for each editable snapshot.
  static readonly MANAGED_TABLES = [TABLE_AGENTS, TABLE_AGENT_VERSIONS] as const;

  private readonly db: OracleDB;
  private readonly schemaName?: string;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({
      ...config,
      jsonColumns: [
        'model',
        'tools',
        'defaultOptions',
        'workflows',
        'agents',
        'integrationTools',
        'toolProviders',
        'inputProcessors',
        'outputProcessors',
        'memory',
        'scorers',
        'mcpClients',
        'requestContextSchema',
        'workspace',
        'skills',
        'durable',
        'browser',
        'changedFields',
      ],
    });
    this.schemaName = config.schemaName;
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, AgentsOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_AGENTS,
      schema: TABLE_SCHEMAS[TABLE_AGENTS],
    });
    await this.db.createTable({
      tableName: TABLE_AGENT_VERSIONS,
      schema: ORACLE_AGENT_VERSIONS_SCHEMA,
    });

    await this.db.alterTable({
      tableName: TABLE_AGENTS,
      schema: TABLE_SCHEMAS[TABLE_AGENTS],
      ifNotExists: ['activeVersionId', 'authorId', 'visibility', 'metadata', 'favoriteCount'],
    });
    await this.db.alterTable({
      tableName: TABLE_AGENT_VERSIONS,
      schema: ORACLE_AGENT_VERSIONS_SCHEMA,
      ifNotExists: [
        'tools',
        'mcpClients',
        'requestContextSchema',
        'browser',
        'integrationTools',
        'toolProviders',
        'workspace',
        'skills',
        'skillsFormat',
        'durable',
        'inputProcessors',
        'outputProcessors',
      ],
    });

    // Keep init safe for existing schemas: additive columns are created above,
    // then legacy JSON shapes are normalized before indexes are used by queries.
    await this.migrateToolsToObjectFormat();
    await this.createIndexes();
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.tx(async client => {
      await client.none(`DELETE FROM ${this.table(TABLE_AGENT_VERSIONS)}`);
      await client.none(`DELETE FROM ${this.table(TABLE_AGENTS)}`);
    });
  }

  getDefaultIndexDefinitions(): OracleCreateIndexOptions[] {
    return getDefaultAgentIndexDefinitions(this.indexName.bind(this));
  }

  async getById(id: string): Promise<StorageAgentType | null> {
    return this.getByIdWithClient(this.db.read, id);
  }

  private async getByIdWithClient(client: OracleDB, id: string): Promise<StorageAgentType | null> {
    try {
      const row = await client.oneOrNone<AgentRow>(
        `${this.agentSelect()} FROM ${this.table(TABLE_AGENTS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseAgentRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_AGENT_BY_ID', 'FAILED', { agentId: id }, error);
    }
  }

  async create(input: { agent: StorageCreateAgentInput }): Promise<StorageAgentType> {
    const { agent } = input;

    try {
      const now = new Date();
      const visibility = agent.visibility ?? (agent.authorId ? 'private' : undefined);

      await this.db.tx(async client => {
        const existing = await client.oneOrNone<{ id: string }>(
          `SELECT id AS "id" FROM ${this.table(TABLE_AGENTS)} WHERE id = :id`,
          { id: agent.id },
        );
        if (existing) {
          throw new Error(`Agent with id ${agent.id} already exists`);
        }

        await client.none(
          `INSERT INTO ${this.table(TABLE_AGENTS)} (
            id, status, ${AGENT_ACTIVE_VERSION_ID}, ${AGENT_AUTHOR_ID}, visibility, metadata,
            ${AGENT_FAVORITE_COUNT}, ${AGENT_CREATED_AT}, ${AGENT_UPDATED_AT}
          ) VALUES (
            :id, :status, :activeVersionId, :authorId, :visibility, :metadata,
            :favoriteCount, :createdAt, :updatedAt
          )`,
          {
            id: agent.id,
            status: 'draft',
            activeVersionId: null,
            authorId: agent.authorId ?? null,
            visibility: visibility ?? null,
            metadata: nullableJsonBind(agent.metadata),
            favoriteCount: 0,
            createdAt: now,
            updatedAt: now,
          },
        );

        // The initial version captures the runnable agent config while the parent row stays small and queryable.
        const { id: _id, authorId: _authorId, visibility: _visibility, metadata: _metadata, ...snapshotConfig } = agent;
        await this.insertVersion(client, {
          id: crypto.randomUUID(),
          agentId: agent.id,
          versionNumber: 1,
          ...snapshotConfig,
          changedFields: Object.keys(snapshotConfig),
          changeMessage: 'Initial version',
        });
      });

      return {
        id: agent.id,
        status: 'draft',
        activeVersionId: undefined,
        authorId: agent.authorId,
        visibility,
        metadata: agent.metadata,
        favoriteCount: 0,
        createdAt: now,
        updatedAt: now,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('CREATE_AGENT', 'FAILED', { agentId: agent.id }, error);
    }
  }

  async update(input: StorageUpdateAgentInput): Promise<StorageAgentType> {
    const { id, ...updates } = input;

    try {
      const existingAgent = await this.getByIdWithClient(this.db, id);
      if (!existingAgent) {
        throw new Error(`Agent with id ${id} not found`);
      }

      // Updating an agent only changes parent-row state. Runnable configuration
      // changes go through createVersion so historical versions stay immutable.
      const { authorId, activeVersionId, metadata, status, visibility } = updates;
      const setClauses: string[] = [];
      const binds: Record<string, unknown> = { id };

      if (authorId !== undefined) {
        setClauses.push(`${AGENT_AUTHOR_ID} = :authorId`);
        binds.authorId = authorId;
      }

      if (activeVersionId !== undefined) {
        setClauses.push(`${AGENT_ACTIVE_VERSION_ID} = :activeVersionId`);
        binds.activeVersionId = activeVersionId;
      }

      if (status !== undefined) {
        setClauses.push(`status = :status`);
        binds.status = status;
      }

      if (visibility !== undefined) {
        setClauses.push(`visibility = :visibility`);
        binds.visibility = visibility;
      }

      if (metadata !== undefined) {
        setClauses.push(`metadata = :metadata`);
        binds.metadata = jsonBind(metadata);
      }

      setClauses.push(`${AGENT_UPDATED_AT} = :updatedAt`);
      binds.updatedAt = new Date();

      await this.db.none(`UPDATE ${this.table(TABLE_AGENTS)} SET ${setClauses.join(', ')} WHERE id = :id`, binds);

      const updatedAgent = await this.getByIdWithClient(this.db, id);
      if (!updatedAgent) {
        throw new Error(`Agent with id ${id} not found after update`);
      }
      return updatedAgent;
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('UPDATE_AGENT', 'FAILED', { agentId: id }, error);
    }
  }

  async delete(id: string): Promise<void> {
    try {
      await this.db.tx(async client => {
        await client.none(`DELETE FROM ${this.table(TABLE_AGENT_VERSIONS)} WHERE ${VERSION_AGENT_ID} = :id`, { id });
        await client.none(`DELETE FROM ${this.table(TABLE_AGENTS)} WHERE id = :id`, { id });
      });
    } catch (error) {
      throw this.storageError('DELETE_AGENT', 'FAILED', { agentId: id }, error);
    }
  }

  async list(args?: StorageListAgentsInput): Promise<StorageListAgentsOutput> {
    const {
      page = 0,
      perPage: perPageInput,
      orderBy,
      authorId,
      visibility,
      metadata,
      status,
      entityIds,
      pinFavoritedFor,
      favoritedOnly,
    } = args || {};

    try {
      this.validatePagination(page, perPageInput, 100);
    } catch (error) {
      throw this.storageError('LIST_AGENTS', 'INVALID_INPUT', { page }, error, ErrorCategory.USER);
    }

    const perPage = normalizePerPage(perPageInput, 100);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      if (entityIds && entityIds.length === 0) {
        return {
          agents: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      const { field, direction } = this.parseOrderBy(orderBy);
      const { whereClause, binds } = this.agentWhereClause({
        status,
        authorId,
        visibility,
        metadata,
        entityIds,
        pinFavoritedFor,
        favoritedOnly,
      });
      const favoritesJoin = pinFavoritedFor
        ? `LEFT JOIN ${this.table(TABLE_FAVORITES)} f ON f."entityType" = 'agent' AND f."entityId" = a.id AND f."userId" = :favoriteUser`
        : '';
      if (pinFavoritedFor) binds.favoriteUser = pinFavoritedFor;

      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_AGENTS)} a ${favoritesJoin} ${whereClause}`,
        binds,
      );
      const total = Number(countRow?.count ?? 0);

      if (total === 0) {
        return {
          agents: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      if (perPage === 0) {
        return {
          agents: [],
          total,
          page,
          perPage: perPageForResponse,
          hasMore: total > 0,
        };
      }

      const limit = perPageInput === false ? total : perPage;
      const orderByParts: string[] = [];
      if (pinFavoritedFor) orderByParts.push('CASE WHEN f."userId" IS NULL THEN 1 ELSE 0 END');
      orderByParts.push(`a.${this.agentOrderColumn(field)} ${direction}`);
      orderByParts.push(`a.id ASC`);

      const rows = await this.db.read.manyOrNone<AgentRow>(
        `${this.agentSelect('a')} FROM ${this.table(TABLE_AGENTS)} a ${favoritesJoin} ${whereClause} ORDER BY ${orderByParts.join(
          ', ',
        )} OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { ...binds, offset, limit },
      );

      const agents = rows.flatMap(row => {
        try {
          return [this.parseAgentRow(row)];
        } catch (error) {
          this.logger?.warn?.('[Oracle] Failed to map agent row, skipping', { id: row?.id, error });
          return [];
        }
      });

      return {
        agents,
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('LIST_AGENTS', 'FAILED', {}, error);
    }
  }

  async createVersion(input: CreateVersionInput): Promise<AgentVersion> {
    try {
      const existingById = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(TABLE_AGENT_VERSIONS)} WHERE id = :id`,
        { id: input.id },
      );
      if (existingById) {
        throw new Error(`Version with id ${input.id} already exists`);
      }

      const existingByNumber = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(
          TABLE_AGENT_VERSIONS,
        )} WHERE ${VERSION_AGENT_ID} = :agentId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { agentId: input.agentId, versionNumber: input.versionNumber },
      );
      if (existingByNumber) {
        throw new Error(`Version number ${input.versionNumber} already exists for agent ${input.agentId}`);
      }

      const createdAt = new Date();
      await this.insertVersion(this.db, input, createdAt);

      return this.toStoredVersion(input, createdAt);
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('CREATE_AGENT_VERSION', 'FAILED', { versionId: input.id, agentId: input.agentId }, error);
    }
  }

  async getVersion(id: string): Promise<AgentVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<AgentVersionRow>(
        `${this.versionSelect()} FROM ${this.table(TABLE_AGENT_VERSIONS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_AGENT_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async getVersionByNumber(agentId: string, versionNumber: number): Promise<AgentVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<AgentVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_AGENT_VERSIONS,
        )} WHERE ${VERSION_AGENT_ID} = :agentId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { agentId, versionNumber },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_AGENT_VERSION_BY_NUMBER', 'FAILED', { agentId, versionNumber }, error);
    }
  }

  async getLatestVersion(agentId: string): Promise<AgentVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<AgentVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_AGENT_VERSIONS,
        )} WHERE ${VERSION_AGENT_ID} = :agentId ORDER BY ${VERSION_VERSION_NUMBER} DESC FETCH FIRST 1 ROWS ONLY`,
        { agentId },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_LATEST_AGENT_VERSION', 'FAILED', { agentId }, error);
    }
  }

  async listVersions(input: ListVersionsInput): Promise<ListVersionsOutput> {
    const { agentId, page = 0, perPage: perPageInput, orderBy } = input;

    try {
      this.validatePagination(page, perPageInput, 20);
    } catch (error) {
      throw this.storageError('LIST_AGENT_VERSIONS', 'INVALID_INPUT', { page, agentId }, error, ErrorCategory.USER);
    }

    const perPage = normalizePerPage(perPageInput, 20);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const { field, direction } = this.parseVersionOrderBy(orderBy);
      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_AGENT_VERSIONS)} WHERE ${VERSION_AGENT_ID} = :agentId`,
        { agentId },
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
      const rows = await this.db.read.manyOrNone<AgentVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_AGENT_VERSIONS,
        )} WHERE ${VERSION_AGENT_ID} = :agentId ORDER BY ${this.versionOrderColumn(
          field,
        )} ${direction}, id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { agentId, offset, limit },
      );

      const versions = rows.flatMap(row => {
        try {
          return [this.parseVersionRow(row)];
        } catch (error) {
          this.logger?.warn?.('[Oracle] Failed to map agent version row, skipping', { id: row?.id, error });
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
      throw this.storageError('LIST_AGENT_VERSIONS', 'FAILED', { agentId }, error);
    }
  }

  async deleteVersion(id: string): Promise<void> {
    try {
      await this.db.none(`DELETE FROM ${this.table(TABLE_AGENT_VERSIONS)} WHERE id = :id`, { id });
    } catch (error) {
      throw this.storageError('DELETE_AGENT_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async deleteVersionsByParentId(entityId: string): Promise<void> {
    try {
      await this.db.none(`DELETE FROM ${this.table(TABLE_AGENT_VERSIONS)} WHERE ${VERSION_AGENT_ID} = :entityId`, {
        entityId,
      });
    } catch (error) {
      throw this.storageError('DELETE_AGENT_VERSIONS_BY_AGENT_ID', 'FAILED', { agentId: entityId }, error);
    }
  }

  async countVersions(agentId: string): Promise<number> {
    try {
      const row = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_AGENT_VERSIONS)} WHERE ${VERSION_AGENT_ID} = :agentId`,
        { agentId },
      );
      return Number(row?.count ?? 0);
    } catch (error) {
      throw this.storageError('COUNT_AGENT_VERSIONS', 'FAILED', { agentId }, error);
    }
  }

  private toStoredVersion(input: CreateVersionInput, createdAt: Date): AgentVersion {
    return {
      id: input.id,
      agentId: input.agentId,
      versionNumber: input.versionNumber,
      name: input.name,
      description: input.description,
      instructions: input.instructions,
      model: input.model,
      tools: normalizeLegacyTools(input.tools) as AgentVersion['tools'],
      defaultOptions: input.defaultOptions,
      workflows: input.workflows,
      agents: input.agents,
      integrationTools: input.integrationTools,
      toolProviders: input.toolProviders,
      inputProcessors: input.inputProcessors,
      outputProcessors: input.outputProcessors,
      memory: input.memory,
      scorers: input.scorers,
      mcpClients: input.mcpClients,
      requestContextSchema: input.requestContextSchema,
      workspace: input.workspace,
      skills: input.skills,
      skillsFormat: input.skillsFormat,
      durable: input.durable,
      browser: input.browser,
      changedFields: input.changedFields,
      changeMessage: input.changeMessage,
      createdAt,
    };
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
    input: CreateVersionInput,
    createdAt = new Date(),
  ): Promise<void> {
    // Version rows contain the full runnable snapshot. CLOB binds cover long
    // prompts/change messages, while JSON binds preserve structured tool config.
    await client.none(
      `INSERT INTO ${this.table(TABLE_AGENT_VERSIONS)} (
        id, ${VERSION_AGENT_ID}, ${VERSION_VERSION_NUMBER},
        name, description, instructions, model, tools, ${VERSION_TOOL_PROVIDERS},
        ${VERSION_DEFAULT_OPTIONS}, workflows, agents, ${VERSION_INTEGRATION_TOOLS},
        ${VERSION_INPUT_PROCESSORS}, ${VERSION_OUTPUT_PROCESSORS}, memory, scorers,
        ${VERSION_MCP_CLIENTS}, ${VERSION_REQUEST_CONTEXT_SCHEMA},
        workspace, skills, ${VERSION_SKILLS_FORMAT}, durable,
        browser, ${VERSION_CHANGED_FIELDS}, ${VERSION_CHANGE_MESSAGE}, ${VERSION_CREATED_AT}
      ) VALUES (
        :id, :agentId, :versionNumber,
        :name, :description, :instructions, :model, :tools, :toolProviders,
        :defaultOptions, :workflows, :agents, :integrationTools,
        :inputProcessors, :outputProcessors, :memory, :scorers,
        :mcpClients, :requestContextSchema,
        :workspace, :skills, :skillsFormat, :durable,
        :browser, :changedFields, :changeMessage, :createdAt
      )`,
      {
        id: input.id,
        agentId: input.agentId,
        versionNumber: input.versionNumber,
        name: input.name,
        description: nullableClobBind(input.description),
        instructions: clobBind(this.serializeInstructions(input.instructions)),
        model: jsonBind(input.model),
        tools: nullableJsonBind(normalizeLegacyTools(input.tools)),
        defaultOptions: nullableJsonBind(input.defaultOptions),
        workflows: nullableJsonBind(input.workflows),
        agents: nullableJsonBind(input.agents),
        integrationTools: nullableJsonBind(input.integrationTools),
        toolProviders: nullableJsonBind(input.toolProviders),
        inputProcessors: nullableJsonBind(input.inputProcessors),
        outputProcessors: nullableJsonBind(input.outputProcessors),
        memory: nullableJsonBind(input.memory),
        scorers: nullableJsonBind(input.scorers),
        mcpClients: nullableJsonBind(input.mcpClients),
        requestContextSchema: nullableJsonBind(input.requestContextSchema),
        workspace: nullableJsonBind(input.workspace),
        skills: nullableJsonBind(input.skills),
        skillsFormat: input.skillsFormat ?? null,
        durable: nullableJsonBind(input.durable),
        browser: nullableJsonBind(input.browser),
        changedFields: nullableJsonBind(input.changedFields),
        changeMessage: nullableClobBind(input.changeMessage),
        createdAt,
      },
    );
  }

  private agentWhereClause(args: {
    status?: StorageAgentType['status'];
    authorId?: string;
    visibility?: StorageAgentType['visibility'];
    metadata?: Record<string, unknown>;
    entityIds?: string[];
    pinFavoritedFor?: string;
    favoritedOnly?: boolean;
  }): { whereClause: string; binds: Record<string, unknown> } {
    const conditions: string[] = [];
    const binds: Record<string, unknown> = {};

    if (args.status) {
      conditions.push(`a.status = :status`);
      binds.status = args.status;
    }

    if (args.authorId !== undefined) {
      conditions.push(`a.${AGENT_AUTHOR_ID} = :authorId`);
      binds.authorId = args.authorId;
    }

    if (args.visibility !== undefined) {
      conditions.push(`a.visibility = :visibility`);
      binds.visibility = args.visibility;
    }

    if (args.metadata && Object.keys(args.metadata).length > 0) {
      const metadata = buildOracleJsonContains('a.metadata', args.metadata);
      conditions.push(metadata.sql);
      Object.assign(binds, metadata.binds);
    }

    if (args.entityIds && args.entityIds.length > 0) {
      const bindNames = args.entityIds.map((entityId, index) => {
        const bindName = `entityId${index}`;
        binds[bindName] = entityId;
        return `:${bindName}`;
      });
      conditions.push(`a.id IN (${bindNames.join(', ')})`);
    }

    if (args.favoritedOnly) {
      conditions.push(args.pinFavoritedFor ? 'f."userId" IS NOT NULL' : '1 = 0');
    }

    return {
      whereClause: conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '',
      binds,
    };
  }

  private agentSelect(alias?: string): string {
    const prefix = alias ? `${alias}.` : '';
    return [
      `SELECT ${prefix}id AS "id"`,
      `${prefix}status AS "status"`,
      `${prefix}${AGENT_ACTIVE_VERSION_ID} AS "activeVersionId"`,
      `${prefix}${AGENT_AUTHOR_ID} AS "authorId"`,
      `${prefix}visibility AS "visibility"`,
      `JSON_SERIALIZE(${prefix}metadata RETURNING CLOB) AS "metadataJson"`,
      `${prefix}${AGENT_FAVORITE_COUNT} AS "favoriteCount"`,
      `${prefix}${AGENT_CREATED_AT} AS "createdAt"`,
      `${prefix}${AGENT_UPDATED_AT} AS "updatedAt"`,
    ].join(', ');
  }

  private versionSelect(): string {
    return [
      `SELECT id AS "id"`,
      `${VERSION_AGENT_ID} AS "agentId"`,
      `${VERSION_VERSION_NUMBER} AS "versionNumber"`,
      `name AS "name"`,
      `description AS "description"`,
      `instructions AS "instructions"`,
      `JSON_SERIALIZE(model RETURNING CLOB) AS "model"`,
      `JSON_SERIALIZE(tools RETURNING CLOB) AS "tools"`,
      `JSON_SERIALIZE(${VERSION_DEFAULT_OPTIONS} RETURNING CLOB) AS "defaultOptions"`,
      `JSON_SERIALIZE(workflows RETURNING CLOB) AS "workflows"`,
      `JSON_SERIALIZE(agents RETURNING CLOB) AS "agents"`,
      `JSON_SERIALIZE(${VERSION_INTEGRATION_TOOLS} RETURNING CLOB) AS "integrationTools"`,
      `JSON_SERIALIZE(${VERSION_TOOL_PROVIDERS} RETURNING CLOB) AS "toolProviders"`,
      `JSON_SERIALIZE(${VERSION_INPUT_PROCESSORS} RETURNING CLOB) AS "inputProcessors"`,
      `JSON_SERIALIZE(${VERSION_OUTPUT_PROCESSORS} RETURNING CLOB) AS "outputProcessors"`,
      `JSON_SERIALIZE(memory RETURNING CLOB) AS "memory"`,
      `JSON_SERIALIZE(scorers RETURNING CLOB) AS "scorers"`,
      `JSON_SERIALIZE(${VERSION_MCP_CLIENTS} RETURNING CLOB) AS "mcpClients"`,
      `JSON_SERIALIZE(${VERSION_REQUEST_CONTEXT_SCHEMA} RETURNING CLOB) AS "requestContextSchema"`,
      `JSON_SERIALIZE(workspace RETURNING CLOB) AS "workspace"`,
      `JSON_SERIALIZE(skills RETURNING CLOB) AS "skills"`,
      `${VERSION_SKILLS_FORMAT} AS "skillsFormat"`,
      `JSON_SERIALIZE(durable RETURNING CLOB) AS "durable"`,
      `JSON_SERIALIZE(browser RETURNING CLOB) AS "browser"`,
      `JSON_SERIALIZE(${VERSION_CHANGED_FIELDS} RETURNING CLOB) AS "changedFields"`,
      `${VERSION_CHANGE_MESSAGE} AS "changeMessage"`,
      `${VERSION_CREATED_AT} AS "createdAt"`,
    ].join(', ');
  }

  private parseAgentRow(row: AgentRow): StorageAgentType {
    return {
      id: String(row.id),
      status: row.status,
      activeVersionId: optionalString(row.activeVersionId),
      authorId: optionalString(row.authorId),
      visibility: optionalVisibility(row.visibility),
      metadata: parseOptionalJsonObject(row.metadataJson ?? row.metadata),
      favoriteCount: row.favoriteCount === null || row.favoriteCount === undefined ? 0 : Number(row.favoriteCount),
      createdAt: toDate(row.createdAt),
      updatedAt: toDate(row.updatedAt),
    };
  }

  private parseVersionRow(row: AgentVersionRow): AgentVersion {
    // Oracle can return JSON columns as objects or serialized values depending
    // on fetch mode, so every structured field is normalized before returning.
    return {
      id: String(row.id),
      agentId: String(row.agentId),
      versionNumber: Number(row.versionNumber),
      name: String(row.name),
      description: optionalString(row.description),
      instructions: this.deserializeInstructions(row.instructions),
      model: parseOracleJson(row.model) as AgentVersion['model'],
      tools: normalizeLegacyTools(parseOptionalJson<AgentVersion['tools']>(row.tools)) as AgentVersion['tools'],
      defaultOptions: parseOptionalJson<AgentVersion['defaultOptions']>(row.defaultOptions),
      workflows: parseOptionalJson<AgentVersion['workflows']>(row.workflows),
      agents: parseOptionalJson<AgentVersion['agents']>(row.agents),
      integrationTools: parseOptionalJson<AgentVersion['integrationTools']>(row.integrationTools),
      toolProviders: parseOptionalJson<AgentVersion['toolProviders']>(row.toolProviders),
      inputProcessors: parseOptionalJson<AgentVersion['inputProcessors']>(row.inputProcessors),
      outputProcessors: parseOptionalJson<AgentVersion['outputProcessors']>(row.outputProcessors),
      memory: parseOptionalJson<AgentVersion['memory']>(row.memory),
      scorers: parseOptionalJson<AgentVersion['scorers']>(row.scorers),
      mcpClients: parseOptionalJson<AgentVersion['mcpClients']>(row.mcpClients),
      requestContextSchema: parseOptionalJson<AgentVersion['requestContextSchema']>(row.requestContextSchema),
      workspace: parseOptionalJson<AgentVersion['workspace']>(row.workspace),
      skills: parseOptionalJson<AgentVersion['skills']>(row.skills),
      skillsFormat: row.skillsFormat ?? undefined,
      durable: parseOptionalJson<AgentVersion['durable']>(row.durable),
      browser: parseOptionalJson<AgentVersion['browser']>(row.browser),
      changedFields: parseOptionalJson<AgentVersion['changedFields']>(row.changedFields),
      changeMessage: optionalString(row.changeMessage),
      createdAt: toDate(row.createdAt),
    };
  }

  private serializeInstructions(instructions: string | AgentInstructionBlock[]): string {
    return Array.isArray(instructions) ? JSON.stringify(instructions) : instructions;
  }

  private deserializeInstructions(raw: string | null | undefined): string | AgentInstructionBlock[] {
    if (!raw) return '';
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return parsed as AgentInstructionBlock[];
    } catch {
      // Plain string instructions are the common case.
    }
    return raw;
  }

  private agentOrderColumn(field: string): string {
    return field === 'updatedAt' ? AGENT_UPDATED_AT : AGENT_CREATED_AT;
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

  private async migrateToolsToObjectFormat(): Promise<void> {
    try {
      // Older rows may store tools as string arrays; the current API expects an
      // object map. Migrate lazily during init to avoid a hard breaking migration.
      const rows = await this.db.manyOrNone<{ id: string; tools?: unknown }>(
        `SELECT id AS "id", tools AS "tools" FROM ${this.table(TABLE_AGENT_VERSIONS)} WHERE tools IS NOT NULL`,
      );
      const updates = rows.flatMap(row => {
        const tools = parseOracleJson(row.tools);
        if (!isLegacyToolArray(tools)) return [];
        return [{ id: row.id, tools: Object.fromEntries(tools.map(toolName => [toolName, {}])) }];
      });

      if (updates.length === 0) return;

      await this.db.tx(async client => {
        for (const update of updates) {
          await client.none(`UPDATE ${this.table(TABLE_AGENT_VERSIONS)} SET tools = :tools WHERE id = :id`, {
            id: update.id,
            tools: jsonBind(update.tools),
          });
        }
      });

      this.logger?.info?.(`Migrated ${updates.length} agent version(s) tools from array to object format`);
    } catch (error) {
      this.logger?.warn?.('Failed to migrate agent tools to object format:', error);
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

export function getDefaultAgentIndexDefinitions(indexName: (name: string) => string): OracleCreateIndexOptions[] {
  return [
    {
      name: indexName('MASTRA_AGENTS_STATUS_CREATED'),
      table: TABLE_AGENTS,
      columns: ['status', 'createdAt'],
    },
    {
      name: indexName('MASTRA_AGENTS_AUTHOR_CREATED'),
      table: TABLE_AGENTS,
      columns: ['authorId', 'createdAt'],
    },
    {
      name: indexName('MASTRA_AGENTS_ACTIVE_VERSION'),
      table: TABLE_AGENTS,
      columns: ['activeVersionId'],
    },
    {
      name: indexName('MASTRA_AGENT_VERSIONS_AGENT_VERSION'),
      table: TABLE_AGENT_VERSIONS,
      columns: ['agentId', 'versionNumber'],
      unique: true,
    },
  ];
}

function optionalString(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  return String(value);
}

function optionalVisibility(value: unknown): StorageAgentType['visibility'] | undefined {
  if (value === 'private' || value === 'public') return value;
  return undefined;
}

function normalizeLegacyTools(value: unknown): unknown {
  if (!isLegacyToolArray(value)) return value;
  return Object.fromEntries(value.map(toolName => [toolName, {}]));
}

function isLegacyToolArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every(item => typeof item === 'string');
}
