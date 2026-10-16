import { createHash } from 'node:crypto';

import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import { createStorageErrorId, MastraCompositeStore } from '@mastra/core/storage';
import type { StorageDomains } from '@mastra/core/storage';
import type { Pool } from 'oracledb';

import { OraclePoolManager } from '../shared/connection';
import type { OracleConnectionConfig } from '../shared/connection';
import { normalizeIdentifier } from '../vector/identifiers';
import { OracleDB, filterIndexesForTables } from './db';
import type { OracleCreateIndexOptions } from './db';
import { AgentsOracle } from './domains/agents';
import { BackgroundTasksOracle } from './domains/background-tasks';
import { BlobsOracle } from './domains/blobs';
import { ChannelsOracle } from './domains/channels';
import { DatasetsOracle } from './domains/datasets';
import { ExperimentsOracle } from './domains/experiments';
import { FavoritesOracle } from './domains/favorites';
import { KnowledgeOracle } from './domains/knowledge';
import { MCPClientsOracle } from './domains/mcp-clients';
import { MCPServersOracle } from './domains/mcp-servers';
import { MemoryOracle } from './domains/memory';
import { NotificationsOracle } from './domains/notifications';
import { ObservabilityOracle } from './domains/observability';
import { ObservabilityStorageOracleVNext } from './domains/observability/v-next';
import type { VNextOracleObservabilityConfig } from './domains/observability/v-next';
import { PromptBlocksOracle } from './domains/prompt-blocks';
import { SchedulesOracle } from './domains/schedules';
import { ScorerDefinitionsOracle } from './domains/scorer-definitions';
import { ScoresOracle } from './domains/scores';
import { SkillsOracle } from './domains/skills';
import { ThreadStateOracle } from './domains/thread-state';
import { ToolProviderConnectionsOracle } from './domains/tool-provider-connections';
import { WorkflowDefinitionsOracle } from './domains/workflow-definitions';
import { WorkflowsOracle } from './domains/workflows';
import { WorkspacesOracle } from './domains/workspaces';
import { OracleMigrationRegistry } from './migrations';
import type { OracleMigration, OracleMigrationRecord, OracleMigrationResult } from './migrations';
import type { OracleStoreConfig } from './types';

const STORE_NAME = 'ORACLEDB';
const DOMAIN_SCHEMA_VERSIONS: Record<string, number> = {
  // 3: retention indexes (2 added observational memory supersededBy).
  R001_MEMORY_SCHEMA: 3,
  // 2: workflow snapshot retention index.
  R002_WORKFLOWS_SCHEMA: 2,
  R003_OBSERVABILITY_SCHEMA: 1,
  // 2: score retention index.
  R004_SCORES_SCHEMA: 2,
  // 3: required version uniqueness (2 added organization/project scope).
  R005_SCORER_DEFINITIONS_SCHEMA: 3,
  // 2: required version uniqueness even when performance indexes are skipped.
  R006_MCP_CLIENTS_SCHEMA: 2,
  // 3: required version uniqueness (2 completed stored agent configuration).
  R007_AGENTS_SCHEMA: 3,
  R008_THREAD_STATE_SCHEMA: 1,
  R009_BLOBS_SCHEMA: 1,
  R010_TOOL_PROVIDER_CONNECTIONS_SCHEMA: 1,
  R011_WORKFLOW_DEFINITIONS_SCHEMA: 1,
  // 2: version-number uniqueness is required even when performance indexes are skipped.
  R012_PROMPT_BLOCKS_SCHEMA: 2,
  R013_MCP_SERVERS_SCHEMA: 1,
  R014_WORKSPACES_SCHEMA: 1,
  R015_SKILLS_SCHEMA: 1,
  R016_FAVORITES_SCHEMA: 1,
  R017_CHANNELS_SCHEMA: 1,
  R018_BACKGROUND_TASKS_SCHEMA: 1,
  R019_SCHEDULES_SCHEMA: 1,
  R020_NOTIFICATIONS_SCHEMA: 1,
  R021_DATASETS_SCHEMA: 1,
  R022_EXPERIMENTS_SCHEMA: 1,
};

export class OracleStore extends MastraCompositeStore {
  private readonly poolManager: OraclePoolManager;
  private readonly readPoolManager: OraclePoolManager;
  private readonly ownsPoolManager: boolean;
  private readonly schemaName?: string;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes?: OracleCreateIndexOptions[];
  private readonly dbClient: OracleDB;
  private readonly migrationRegistry: OracleMigrationRegistry;
  private isInitialized = false;
  private initPromise?: Promise<void>;
  private migrationPromise?: Promise<OracleMigrationResult[]>;

  stores: StorageDomains;

  constructor(config: OracleStoreConfig) {
    try {
      super({ id: config.id, name: 'OracleStore', disableInit: config.disableInit, retention: config.retention });
      this.schemaName = config.schemaName ? normalizeIdentifier(config.schemaName, 'schema name') : undefined;
      this.skipDefaultIndexes = config.skipDefaultIndexes;
      this.indexes = config.indexes;
      this.poolManager = config.poolManager ?? new OraclePoolManager(config);
      this.readPoolManager =
        config.readPoolManager ??
        (config.readPool ? new OraclePoolManager({ pool: config.readPool }) : this.poolManager);
      this.ownsPoolManager = !config.poolManager;
      this.dbClient = new OracleDB({
        poolManager: this.poolManager,
        readPoolManager: this.readPoolManager,
        schemaName: this.schemaName,
      });
      this.migrationRegistry = new OracleMigrationRegistry({
        db: this.dbClient,
        tableName: config.migrationTableName,
      });

      const domainConfig = {
        poolManager: this.poolManager,
        readPoolManager: this.readPoolManager,
        schemaName: this.schemaName,
        storageIsolationKey: config.storageIsolationKey,
        messageBatchSize: config.messageBatchSize,
        skipDefaultIndexes: config.skipDefaultIndexes,
        indexes: config.indexes,
        vectorRegistryTableName: config.vectorRegistryTableName
          ? normalizeIdentifier(config.vectorRegistryTableName, 'vector registry table name')
          : undefined,
      };

      this.stores = {
        memory: new MemoryOracle(domainConfig),
        workflows: new WorkflowsOracle(domainConfig),
        observability: new ObservabilityOracle(domainConfig),
        scores: new ScoresOracle(domainConfig),
        scorerDefinitions: new ScorerDefinitionsOracle(domainConfig),
        mcpClients: new MCPClientsOracle(domainConfig),
        agents: new AgentsOracle(domainConfig),
        threadState: new ThreadStateOracle(domainConfig),
        blobs: new BlobsOracle(domainConfig),
        toolProviderConnections: new ToolProviderConnectionsOracle(domainConfig),
        workflowDefinitions: new WorkflowDefinitionsOracle(domainConfig),
        promptBlocks: new PromptBlocksOracle(domainConfig),
        mcpServers: new MCPServersOracle(domainConfig),
        workspaces: new WorkspacesOracle(domainConfig),
        skills: new SkillsOracle(domainConfig),
        favorites: new FavoritesOracle(domainConfig),
        channels: new ChannelsOracle(domainConfig),
        backgroundTasks: new BackgroundTasksOracle(domainConfig),
        schedules: new SchedulesOracle(domainConfig),
        notifications: new NotificationsOracle(domainConfig),
        datasets: new DatasetsOracle(domainConfig),
        experiments: new ExperimentsOracle(domainConfig),
        // Core activates this opt-in domain through getStore('knowledge') or initKnowledge().
        knowledge: new KnowledgeOracle(domainConfig),
      };
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId(STORE_NAME, 'INITIALIZATION', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.USER,
          details: { schemaName: config.schemaName ?? '' },
        },
        error,
      );
    }
  }

  async init(): Promise<void> {
    if (this.isInitialized) return;
    if (this.initPromise) return this.initPromise;

    this.initPromise = this.runMigrationsOnce(false).then(() => undefined);
    return this.initPromise;
  }

  async migrate(): Promise<OracleMigrationResult[]> {
    // If an init() is already in flight, let it settle first so a concurrent
    // migrate() still gets its own forced (forceRepeatable: true) run instead
    // of coalescing into the unforced init promise. runMigrations()'s finally
    // block clears migrationPromise/initPromise on settle, so this always
    // starts a fresh forced run afterward.
    if (this.initPromise) {
      await this.initPromise;
    }
    return this.runMigrationsOnce(true);
  }

  private async runMigrationsOnce(forceRepeatable: boolean): Promise<OracleMigrationResult[]> {
    if (this.migrationPromise) return this.migrationPromise;

    this.migrationPromise = this.runMigrations(forceRepeatable);
    return this.migrationPromise;
  }

  async listMigrations(): Promise<OracleMigrationRecord[]> {
    return this.migrationRegistry.list();
  }

  private async runMigrations(forceRepeatable: boolean): Promise<OracleMigrationResult[]> {
    try {
      const results = await this.migrationRegistry.run(this.storageMigrations(), { forceRepeatable });
      // Unchanged repeatable migrations are skipped, but observational memory rows retired by an
      // older adapter version (which never set supersededBy) must be fenced on every start.
      if (results.find(result => result.id === 'R001_MEMORY_SCHEMA')?.status === 'skipped') {
        const memory = this.stores.memory;
        if (memory instanceof MemoryOracle) await memory.backfillObservationalMemorySupersededBy();
      }
      this.isInitialized = true;
      return results;
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw new MastraError(
        {
          id: createStorageErrorId(STORE_NAME, 'MIGRATION', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
        },
        error,
      );
    } finally {
      this.migrationPromise = undefined;
      this.initPromise = undefined;
    }
  }

  private storageMigrations(): OracleMigration[] {
    const repeatable = (
      id: string,
      name: string,
      description: string,
      managedTables: readonly string[],
      run: () => Promise<void>,
    ): OracleMigration => ({
      id,
      name,
      kind: 'repeatable',
      description,
      checksum: this.domainMigrationChecksum({ id, name, description, managedTables }),
      run,
    });

    return [
      repeatable(
        'R001_MEMORY_SCHEMA',
        'Memory domain schema',
        'Creates or updates Oracle tables and indexes for threads, messages, resources, working memory, and observational memory.',
        MemoryOracle.MANAGED_TABLES,
        async () => {
          await this.stores.memory?.init();
        },
      ),
      repeatable(
        'R002_WORKFLOWS_SCHEMA',
        'Workflow domain schema',
        'Creates or updates Oracle workflow snapshot tables for durable workflow state.',
        WorkflowsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.workflows?.init();
        },
      ),
      repeatable(
        'R003_OBSERVABILITY_SCHEMA',
        'Observability domain schema',
        'Creates or updates Oracle spans and log event tables used by Mastra observability.',
        ObservabilityOracle.MANAGED_TABLES,
        async () => {
          await this.stores.observability?.init();
        },
      ),
      repeatable(
        'R004_SCORES_SCHEMA',
        'Scores domain schema',
        'Creates or updates Oracle scoring and evaluation result tables.',
        ScoresOracle.MANAGED_TABLES,
        async () => {
          await this.stores.scores?.init();
        },
      ),
      repeatable(
        'R005_SCORER_DEFINITIONS_SCHEMA',
        'Scorer definitions domain schema',
        'Creates or updates Oracle scorer definition registry tables and indexes.',
        ScorerDefinitionsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.scorerDefinitions?.init();
        },
      ),
      repeatable(
        'R006_MCP_CLIENTS_SCHEMA',
        'MCP clients domain schema',
        'Creates or updates Oracle MCP client registry tables and indexes.',
        MCPClientsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.mcpClients?.init();
        },
      ),
      repeatable(
        'R007_AGENTS_SCHEMA',
        'Agents domain schema',
        'Creates or updates Oracle agent registry tables and indexes.',
        AgentsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.agents?.init();
        },
      ),
      repeatable(
        'R008_THREAD_STATE_SCHEMA',
        'Thread state domain schema',
        'Creates Oracle tables and indexes for durable per-thread task and goal state.',
        ThreadStateOracle.MANAGED_TABLES,
        async () => {
          await this.stores.threadState?.init();
        },
      ),
      repeatable(
        'R009_BLOBS_SCHEMA',
        'Blobs domain schema',
        'Creates Oracle tables for immutable skill file content keyed by hash.',
        BlobsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.blobs?.init();
        },
      ),
      repeatable(
        'R010_TOOL_PROVIDER_CONNECTIONS_SCHEMA',
        'Tool provider connections domain schema',
        'Creates Oracle tables and indexes for scoped tool provider connection labels.',
        ToolProviderConnectionsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.toolProviderConnections?.init();
        },
      ),
      repeatable(
        'R011_WORKFLOW_DEFINITIONS_SCHEMA',
        'Workflow definitions domain schema',
        'Creates Oracle tables and indexes for persisted workflow graphs and schedules.',
        WorkflowDefinitionsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.workflowDefinitions?.init();
        },
      ),
      repeatable(
        'R012_PROMPT_BLOCKS_SCHEMA',
        'Prompt blocks domain schema',
        'Creates Oracle registry and version tables for reusable prompt content and rules.',
        PromptBlocksOracle.MANAGED_TABLES,
        async () => {
          await this.stores.promptBlocks?.init();
        },
      ),
      repeatable(
        'R013_MCP_SERVERS_SCHEMA',
        'MCP servers domain schema',
        'Creates Oracle registry and version tables for MCP server capabilities and configuration.',
        MCPServersOracle.MANAGED_TABLES,
        async () => {
          await this.stores.mcpServers?.init();
        },
      ),
      repeatable(
        'R014_WORKSPACES_SCHEMA',
        'Workspaces domain schema',
        'Creates Oracle registry and version tables for workspace filesystem and execution configuration.',
        WorkspacesOracle.MANAGED_TABLES,
        async () => {
          await this.stores.workspaces?.init();
        },
      ),
      repeatable(
        'R015_SKILLS_SCHEMA',
        'Skills domain schema',
        'Creates Oracle registry and version tables for skill definitions and file manifests.',
        SkillsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.skills?.init();
        },
      ),
      repeatable(
        'R016_FAVORITES_SCHEMA',
        'Favorites domain schema',
        'Creates Oracle favorite records for agents and skills with entity lookup indexes.',
        FavoritesOracle.MANAGED_TABLES,
        async () => {
          await this.stores.favorites?.init();
        },
      ),
      repeatable(
        'R017_CHANNELS_SCHEMA',
        'Channels domain schema',
        'Creates Oracle channel installation and platform configuration tables with webhook lookup indexes.',
        ChannelsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.channels?.init();
        },
      ),
      repeatable(
        'R018_BACKGROUND_TASKS_SCHEMA',
        'Background tasks domain schema',
        'Creates Oracle background task state, ownership, and retention indexes.',
        BackgroundTasksOracle.MANAGED_TABLES,
        async () => {
          await this.stores.backgroundTasks?.init();
        },
      ),
      repeatable(
        'R019_SCHEDULES_SCHEMA',
        'Schedules domain schema',
        'Creates Oracle schedule definitions and trigger history with due-time and retention indexes.',
        SchedulesOracle.MANAGED_TABLES,
        async () => {
          await this.stores.schedules?.init();
        },
      ),
      repeatable(
        'R020_NOTIFICATIONS_SCHEMA',
        'Notifications domain schema',
        'Creates Oracle notification state, delivery timestamps, and retention indexes.',
        NotificationsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.notifications?.init();
        },
      ),
      repeatable(
        'R021_DATASETS_SCHEMA',
        'Datasets domain schema',
        'Creates Oracle datasets, versioned item history, and snapshot records.',
        DatasetsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.datasets?.init();
        },
      ),
      repeatable(
        'R022_EXPERIMENTS_SCHEMA',
        'Experiments domain schema',
        'Creates Oracle experiment headers and evaluation results with retry identity and retention indexes.',
        ExperimentsOracle.MANAGED_TABLES,
        async () => {
          await this.stores.experiments?.init();
        },
      ),
    ].filter(
      migration =>
        migration.id !== 'R003_OBSERVABILITY_SCHEMA' ||
        !(this.stores.observability instanceof ObservabilityStorageOracleVNext),
    );
  }

  private domainMigrationChecksum(input: {
    id: string;
    name: string;
    description: string;
    managedTables: readonly string[];
  }): string {
    const indexes = filterIndexesForTables(this.indexes, input.managedTables);

    return createHash('sha256')
      .update(
        stableStringify({
          id: input.id,
          name: input.name,
          kind: 'repeatable',
          description: input.description,
          schemaVersion: DOMAIN_SCHEMA_VERSIONS[input.id] ?? 1,
          managedTables: [...input.managedTables],
          indexConfig: {
            skipDefaultIndexes: this.skipDefaultIndexes === true,
            indexes,
          },
        }),
      )
      .digest('hex')
      .toUpperCase();
  }

  async getPool(): Promise<Pool> {
    return this.poolManager.getPool();
  }

  async getReadPool(): Promise<Pool> {
    return this.readPoolManager.getPool();
  }

  get readDb(): OracleDB {
    return this.dbClient.read;
  }

  getPoolManager(): OraclePoolManager {
    return this.poolManager;
  }

  get db(): OracleDB {
    return this.dbClient;
  }

  async disconnect(): Promise<void> {
    if (this.ownsPoolManager) {
      await this.poolManager.close();
    }
  }

  async close(): Promise<void> {
    await this.disconnect();
  }
}

export type OracleStoreVNextObservabilityConfig = OracleConnectionConfig & {
  poolManager?: OraclePoolManager;
  schemaName?: string;
  skipDefaultIndexes?: boolean;
  indexes?: OracleCreateIndexOptions[];
  discovery?: VNextOracleObservabilityConfig['discovery'];
  traceQueryTimeoutMs?: number;
};

/** Explicitly selects advanced observability on the supplied Oracle connection. */
export class OracleStoreVNext extends OracleStore {
  private readonly observabilityPoolManager: OraclePoolManager;
  private readonly ownsObservabilityPoolManager: boolean;
  private advancedInitialized = false;
  private advancedInitPromise?: Promise<void>;

  constructor(config: OracleStoreConfig & { observability: OracleStoreVNextObservabilityConfig }) {
    if (!config.observability) throw new Error('OracleStoreVNext requires an explicit observability connection');
    super(config);
    this.name = 'OracleStoreVNext';
    const obs = config.observability;
    this.observabilityPoolManager = obs.poolManager ?? new OraclePoolManager(obs);
    this.ownsObservabilityPoolManager = !obs.poolManager;
    this.stores = {
      ...this.stores,
      observability: new ObservabilityStorageOracleVNext({
        poolManager: this.observabilityPoolManager,
        schemaName: obs.schemaName,
        skipDefaultIndexes: obs.skipDefaultIndexes,
        indexes: obs.indexes,
        discovery: obs.discovery,
        traceQueryTimeoutMs: obs.traceQueryTimeoutMs,
      }),
    };
  }
  override async init(): Promise<void> {
    if (this.advancedInitialized) return;
    if (this.advancedInitPromise) return this.advancedInitPromise;
    this.advancedInitPromise = super
      .init()
      .then(async () => {
        await this.stores.observability!.init();
        this.advancedInitialized = true;
      })
      .finally(() => {
        this.advancedInitPromise = undefined;
      });
    return this.advancedInitPromise;
  }
  override async migrate(): Promise<OracleMigrationResult[]> {
    if (this.advancedInitPromise) await this.advancedInitPromise;
    const result = await super.migrate();
    await this.stores.observability!.init();
    this.advancedInitialized = true;
    return result;
  }
  override async disconnect(): Promise<void> {
    const results = await Promise.allSettled([
      super.disconnect(),
      this.ownsObservabilityPoolManager ? this.observabilityPoolManager.close() : Promise.resolve(),
    ]);
    const errors = results.flatMap(result => (result.status === 'rejected' ? [result.reason] : []));
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) throw new AggregateError(errors, 'OracleStoreVNext failed to close its connections');
  }
}

export { OracleDB } from './db';
export { OracleFactoryStorage } from './factory-storage';
export type { OracleFactoryStorageConfig } from './factory-storage';
export { AgentsOracle } from './domains/agents';
export { BackgroundTasksOracle } from './domains/background-tasks';
export { BlobsOracle } from './domains/blobs';
export { ChannelsOracle } from './domains/channels';
export { DatasetsOracle } from './domains/datasets';
export { ExperimentsOracle } from './domains/experiments';
export { FavoritesOracle } from './domains/favorites';
export { KnowledgeOracle } from './domains/knowledge';
export { MCPClientsOracle } from './domains/mcp-clients';
export { MCPServersOracle } from './domains/mcp-servers';
export { MemoryOracle } from './domains/memory';
export { NotificationsOracle } from './domains/notifications';
export { ObservabilityOracle } from './domains/observability';
export { ObservabilityStorageOracleVNext } from './domains/observability/v-next';
export type { VNextOracleObservabilityConfig } from './domains/observability/v-next';
export type { DiscoveryConfig as OracleObservabilityDiscoveryConfig } from './domains/observability/v-next/discovery';
export { PromptBlocksOracle } from './domains/prompt-blocks';
export { ScorerDefinitionsOracle } from './domains/scorer-definitions';
export { SchedulesOracle } from './domains/schedules';
export { ScoresOracle } from './domains/scores';
export { SkillsOracle } from './domains/skills';
export { ThreadStateOracle } from './domains/thread-state';
export { ToolProviderConnectionsOracle } from './domains/tool-provider-connections';
export { WorkflowsOracle } from './domains/workflows';
export { WorkspacesOracle } from './domains/workspaces';
export { WorkflowDefinitionsOracle } from './domains/workflow-definitions';
export { DEFAULT_ORACLE_MIGRATIONS_TABLE, OracleMigrationRegistry, oracleMigrationTableSql } from './migrations';
export type {
  OracleMigration,
  OracleMigrationRecord,
  OracleMigrationResult,
  OracleMigrationRunOptions,
} from './migrations';
export type { OracleStoreConfig } from './types';

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  if (!value || typeof value !== 'object') return JSON.stringify(value);

  return `{${Object.keys(value)
    .sort()
    .map(key => `${JSON.stringify(key)}:${stableStringify((value as Record<string, unknown>)[key])}`)
    .join(',')}}`;
}
