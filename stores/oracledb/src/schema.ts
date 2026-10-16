import {
  RUN_FENCING_TABLE_SCHEMAS,
  TABLE_AGENTS,
  TABLE_DATASETS,
  TABLE_DATASET_ITEMS,
  TABLE_DATASET_VERSIONS,
  TABLE_EXPERIMENTS,
  TABLE_EXPERIMENT_RESULTS,
  TABLE_BACKGROUND_TASKS,
  TABLE_NOTIFICATIONS,
  TABLE_CHANNEL_INSTALLATIONS,
  TABLE_CHANNEL_CONFIG,
  TABLE_FAVORITES,
  TABLE_AGENT_VERSIONS,
  TABLE_MCP_CLIENTS,
  TABLE_MCP_CLIENT_VERSIONS,
  TABLE_MCP_SERVERS,
  TABLE_MCP_SERVER_VERSIONS,
  TABLE_MESSAGES,
  TABLE_MEMORY_RUN_FENCES,
  TABLE_OBSERVATIONAL_MEMORY,
  TABLE_RESOURCES,
  TABLE_SCHEMAS,
  TABLE_SCORER_DEFINITION_VERSIONS,
  TABLE_SCORER_DEFINITIONS,
  TABLE_SCORERS,
  TABLE_SCHEDULES,
  TABLE_SCHEDULE_TRIGGERS,
  TABLE_SKILL_BLOBS,
  TABLE_SKILLS,
  TABLE_SKILL_VERSIONS,
  TABLE_SPANS,
  TABLE_THREADS,
  TABLE_THREAD_STATE,
  TABLE_TOOL_PROVIDER_CONNECTIONS,
  TABLE_PROMPT_BLOCKS,
  TABLE_PROMPT_BLOCK_VERSIONS,
  TABLE_WORKFLOW_SNAPSHOT,
  TABLE_WORKFLOW_RUN_OWNERS,
  TABLE_WORKFLOW_DEFINITIONS,
  TABLE_WORKSPACES,
  TABLE_WORKSPACE_VERSIONS,
} from '@mastra/core/storage';
import type { TABLE_NAMES } from '@mastra/core/storage';

import { generateOracleIndexSQL, generateOracleTableSQL } from './storage/db';
import type { OracleCreateIndexOptions } from './storage/db';
import { getDefaultAgentIndexDefinitions, ORACLE_AGENT_VERSIONS_SCHEMA } from './storage/domains/agents';
import { getDefaultBackgroundTaskIndexDefinitions } from './storage/domains/background-tasks';
import { getDefaultChannelIndexDefinitions } from './storage/domains/channels';
import { getDefaultDatasetIndexDefinitions } from './storage/domains/datasets';
import {
  getDefaultExperimentIndexDefinitions,
  ORACLE_EXPERIMENTS_SCHEMA,
  ORACLE_EXPERIMENT_RESULTS_SCHEMA,
} from './storage/domains/experiments';
import { getDefaultFavoriteIndexDefinitions } from './storage/domains/favorites';
import { KnowledgeOracle } from './storage/domains/knowledge';
import { getDefaultMCPClientIndexDefinitions } from './storage/domains/mcp-clients';
import { getDefaultMCPServerIndexDefinitions } from './storage/domains/mcp-servers';
import { getDefaultNotificationIndexDefinitions } from './storage/domains/notifications';
import {
  getDefaultObservabilityIndexDefinitions,
  logEventsTableSql,
  LOG_EVENTS_TABLE,
} from './storage/domains/observability';
import { ObservabilityStorageOracleVNext } from './storage/domains/observability/v-next';
import { getDefaultPromptBlockIndexDefinitions } from './storage/domains/prompt-blocks';
import { getDefaultScheduleIndexDefinitions } from './storage/domains/schedules';
import { getDefaultScorerDefinitionIndexDefinitions } from './storage/domains/scorer-definitions';
import { getDefaultScoreIndexDefinitions, ORACLE_SCORES_SCHEMA } from './storage/domains/scores';
import { getDefaultSkillIndexDefinitions } from './storage/domains/skills';
import { getDefaultThreadStateIndexDefinitions } from './storage/domains/thread-state';
import { getDefaultToolProviderConnectionIndexDefinitions } from './storage/domains/tool-provider-connections';
import { getDefaultWorkflowDefinitionIndexDefinitions } from './storage/domains/workflow-definitions';
import { getDefaultWorkspaceIndexDefinitions } from './storage/domains/workspaces';
import { DEFAULT_ORACLE_MIGRATIONS_TABLE, oracleMigrationTableSql } from './storage/migrations';
import {
  assertJsonPath,
  indexNameForMetadataField,
  indexNameForTable,
  normalizeIdentifier,
  normalizeLogicalIndexName,
  qualifyName,
  tableNameForIndex,
} from './vector/identifiers';
import {
  buildVectorIndexParameterClause,
  DEFAULT_NAMESPACE,
  defaultMetricForFormat,
  metricToken,
  normalizeMetric,
  normalizeVectorFormat,
  validateAccuracy,
  validateDimension,
  validateMetricForFormat,
  validateVectorFormatDimension,
  vectorColumnType,
} from './vector/sql';
import type { OracleMetric, OracleVectorFormat, OracleVectorIndexConfig } from './vector/types';

// Offline DDL export mirrors the runtime migrations so DBA-managed deployments
// can review or apply the exact Oracle schema before initializing a Mastra app.
export type OracleSchemaDomain =
  | 'migrations'
  | 'memory'
  | 'workflows'
  | 'observability'
  | 'observabilityVNext'
  | 'scores'
  | 'scorerDefinitions'
  | 'mcpClients'
  | 'agents'
  | 'threadState'
  | 'blobs'
  | 'toolProviderConnections'
  | 'workflowDefinitions'
  | 'promptBlocks'
  | 'mcpServers'
  | 'workspaces'
  | 'skills'
  | 'favorites'
  | 'channels'
  | 'backgroundTasks'
  | 'schedules'
  | 'notifications'
  | 'datasets'
  | 'experiments'
  | 'knowledge'
  | 'vector';

export interface OracleVectorSchemaIndex {
  indexName: string;
  dimension: number;
  metric?: OracleMetric;
  vectorFormat?: OracleVectorFormat;
  indexConfig?: OracleVectorIndexConfig;
  buildIndex?: boolean;
  metadataIndexes?: string[];
}

export interface ExportOracleSchemasOptions {
  schemaName?: string;
  domains?: OracleSchemaDomain[];
  skipDefaultIndexes?: boolean;
  indexes?: OracleCreateIndexOptions[];
  migrationTableName?: string;
  vector?: {
    tablePrefix?: string;
    registryTableName?: string;
    defaultMetadataIndexes?: string[];
    indexes?: OracleVectorSchemaIndex[];
  };
}

const DEFAULT_DOMAINS: OracleSchemaDomain[] = [
  'migrations',
  'memory',
  'workflows',
  'observability',
  'scores',
  'scorerDefinitions',
  'mcpClients',
  'agents',
  'threadState',
  'blobs',
  'toolProviderConnections',
  'workflowDefinitions',
  'promptBlocks',
  'mcpServers',
  'workspaces',
  'skills',
  'favorites',
  'channels',
  'backgroundTasks',
  'schedules',
  'notifications',
  'datasets',
  'experiments',
  'vector',
];
const DEFAULT_VECTOR_TABLE_PREFIX = 'MASTRA_VEC';
const DEFAULT_VECTOR_REGISTRY_TABLE = 'MASTRA_VECTOR_INDEXES';
const DEFAULT_VECTOR_METADATA_INDEXES = ['thread_id', 'resource_id', 'message_id', 'source_id'];

// Keep domain ordering deterministic: shared metadata first, storage domains next,
// and vector tables last so generated SQL reads like the runtime initialization path.
export function exportSchemas(options: ExportOracleSchemasOptions = {}): string {
  const schemaName = options.schemaName ? normalizeIdentifier(options.schemaName, 'schema name') : undefined;
  const domains = new Set(options.domains ?? DEFAULT_DOMAINS);
  const statements: string[] = [];

  if (needsMigrationTable(domains)) {
    statements.push(...migrationSchemaStatements(schemaName, options.migrationTableName));
  }

  if (domains.has('memory')) {
    statements.push(...memorySchemaStatements(schemaName));
    if (!options.skipDefaultIndexes) statements.push(...memoryDefaultIndexStatements(schemaName));
    statements.push(...customIndexStatements(options.indexes, MemorySchemaTables, schemaName));
  }

  if (domains.has('workflows')) {
    statements.push(...workflowSchemaStatements(schemaName));
    if (!options.skipDefaultIndexes) statements.push(...workflowDefaultIndexStatements(schemaName));
    statements.push(...customIndexStatements(options.indexes, WorkflowSchemaTables, schemaName));
  }

  if (domains.has('observability')) {
    statements.push(...observabilitySchemaStatements(schemaName));
    if (!options.skipDefaultIndexes) statements.push(...observabilityDefaultIndexStatements(schemaName));
    statements.push(...customIndexStatements(options.indexes, ObservabilitySchemaTables, schemaName));
  }

  if (domains.has('observabilityVNext')) {
    statements.push(
      ...ObservabilityStorageOracleVNext.getExportDDL({
        schemaName,
        skipDefaultIndexes: options.skipDefaultIndexes,
        indexes: options.indexes,
      }),
    );
  }

  if (domains.has('scores')) {
    statements.push(...scoreSchemaStatements(schemaName));
    if (!options.skipDefaultIndexes) statements.push(...scoreDefaultIndexStatements(schemaName));
    statements.push(...customIndexStatements(options.indexes, ScoreSchemaTables, schemaName));
  }

  if (domains.has('scorerDefinitions')) {
    statements.push(...scorerDefinitionSchemaStatements(schemaName));
    statements.push(...scorerDefinitionDefaultIndexStatements(schemaName, options.skipDefaultIndexes));
    statements.push(...customIndexStatements(options.indexes, ScorerDefinitionSchemaTables, schemaName));
  }

  if (domains.has('mcpClients')) {
    statements.push(...mcpClientSchemaStatements(schemaName));
    statements.push(...mcpClientDefaultIndexStatements(schemaName, options.skipDefaultIndexes));
    statements.push(...customIndexStatements(options.indexes, MCPClientSchemaTables, schemaName));
  }

  if (domains.has('agents')) {
    statements.push(...agentSchemaStatements(schemaName));
    statements.push(...agentDefaultIndexStatements(schemaName, options.skipDefaultIndexes));
    statements.push(...customIndexStatements(options.indexes, AgentSchemaTables, schemaName));
  }

  if (domains.has('threadState')) {
    statements.push(
      generateOracleTableSQL({
        tableName: TABLE_THREAD_STATE,
        schema: TABLE_SCHEMAS[TABLE_THREAD_STATE],
        schemaName,
        compositePrimaryKey: ['threadId', 'type'],
      }),
    );
    if (!options.skipDefaultIndexes) {
      statements.push(
        ...getDefaultThreadStateIndexDefinitions().map(index => generateOracleIndexSQL(index, schemaName)),
      );
    }
    statements.push(...customIndexStatements(options.indexes, [TABLE_THREAD_STATE], schemaName));
  }

  if (domains.has('blobs')) {
    statements.push(
      generateOracleTableSQL({ tableName: TABLE_SKILL_BLOBS, schema: TABLE_SCHEMAS[TABLE_SKILL_BLOBS], schemaName }),
    );
    statements.push(...customIndexStatements(options.indexes, [TABLE_SKILL_BLOBS], schemaName));
  }

  if (domains.has('toolProviderConnections')) {
    statements.push(
      generateOracleTableSQL({
        tableName: TABLE_TOOL_PROVIDER_CONNECTIONS,
        schema: TABLE_SCHEMAS[TABLE_TOOL_PROVIDER_CONNECTIONS],
        schemaName,
        compositePrimaryKey: ['authorId', 'providerId', 'connectionId'],
      }),
    );
    if (!options.skipDefaultIndexes)
      statements.push(
        ...getDefaultToolProviderConnectionIndexDefinitions().map(index => generateOracleIndexSQL(index, schemaName)),
      );
    statements.push(...customIndexStatements(options.indexes, [TABLE_TOOL_PROVIDER_CONNECTIONS], schemaName));
  }

  if (domains.has('workflowDefinitions')) {
    statements.push(
      generateOracleTableSQL({
        tableName: TABLE_WORKFLOW_DEFINITIONS,
        schema: TABLE_SCHEMAS[TABLE_WORKFLOW_DEFINITIONS],
        schemaName,
      }),
    );
    if (!options.skipDefaultIndexes)
      statements.push(
        ...getDefaultWorkflowDefinitionIndexDefinitions().map(index => generateOracleIndexSQL(index, schemaName)),
      );
    statements.push(...customIndexStatements(options.indexes, [TABLE_WORKFLOW_DEFINITIONS], schemaName));
  }

  if (domains.has('promptBlocks')) {
    for (const tableName of [TABLE_PROMPT_BLOCKS, TABLE_PROMPT_BLOCK_VERSIONS] as const) {
      statements.push(generateOracleTableSQL({ tableName, schema: TABLE_SCHEMAS[tableName], schemaName }));
    }
    for (const index of getDefaultPromptBlockIndexDefinitions(name => indexNameForTable(name, 'IDX'))) {
      if (!options.skipDefaultIndexes || index.unique) statements.push(generateOracleIndexSQL(index, schemaName));
    }
    statements.push(
      ...customIndexStatements(options.indexes, [TABLE_PROMPT_BLOCKS, TABLE_PROMPT_BLOCK_VERSIONS], schemaName),
    );
  }

  if (domains.has('mcpServers')) {
    for (const tableName of [TABLE_MCP_SERVERS, TABLE_MCP_SERVER_VERSIONS] as const) {
      statements.push(generateOracleTableSQL({ tableName, schema: TABLE_SCHEMAS[tableName], schemaName }));
    }
    for (const index of getDefaultMCPServerIndexDefinitions(name => indexNameForTable(name, 'IDX'))) {
      if (!options.skipDefaultIndexes || index.unique) statements.push(generateOracleIndexSQL(index, schemaName));
    }
    statements.push(
      ...customIndexStatements(options.indexes, [TABLE_MCP_SERVERS, TABLE_MCP_SERVER_VERSIONS], schemaName),
    );
  }

  if (domains.has('workspaces')) {
    for (const tableName of [TABLE_WORKSPACES, TABLE_WORKSPACE_VERSIONS] as const) {
      statements.push(generateOracleTableSQL({ tableName, schema: TABLE_SCHEMAS[tableName], schemaName }));
    }
    for (const index of getDefaultWorkspaceIndexDefinitions(name => indexNameForTable(name, 'IDX'))) {
      if (!options.skipDefaultIndexes || index.unique) statements.push(generateOracleIndexSQL(index, schemaName));
    }
    statements.push(
      ...customIndexStatements(options.indexes, [TABLE_WORKSPACES, TABLE_WORKSPACE_VERSIONS], schemaName),
    );
  }

  if (domains.has('skills')) {
    for (const tableName of [TABLE_SKILLS, TABLE_SKILL_VERSIONS] as const) {
      statements.push(generateOracleTableSQL({ tableName, schema: TABLE_SCHEMAS[tableName], schemaName }));
    }
    for (const index of getDefaultSkillIndexDefinitions(name => indexNameForTable(name, 'IDX'))) {
      if (!options.skipDefaultIndexes || index.unique) statements.push(generateOracleIndexSQL(index, schemaName));
    }
    statements.push(...customIndexStatements(options.indexes, [TABLE_SKILLS, TABLE_SKILL_VERSIONS], schemaName));
  }

  if (domains.has('favorites')) {
    statements.push(
      generateOracleTableSQL({
        tableName: TABLE_FAVORITES,
        schema: TABLE_SCHEMAS[TABLE_FAVORITES],
        schemaName,
        compositePrimaryKey: ['userId', 'entityType', 'entityId'],
      }),
    );
    if (!options.skipDefaultIndexes)
      statements.push(...getDefaultFavoriteIndexDefinitions().map(index => generateOracleIndexSQL(index, schemaName)));
    statements.push(...customIndexStatements(options.indexes, [TABLE_FAVORITES], schemaName));
  }

  if (domains.has('channels')) {
    for (const tableName of [TABLE_CHANNEL_INSTALLATIONS, TABLE_CHANNEL_CONFIG] as const)
      statements.push(generateOracleTableSQL({ tableName, schema: TABLE_SCHEMAS[tableName], schemaName }));
    for (const index of getDefaultChannelIndexDefinitions()) {
      if (!options.skipDefaultIndexes || index.unique) statements.push(generateOracleIndexSQL(index, schemaName));
    }
    statements.push(
      ...customIndexStatements(options.indexes, [TABLE_CHANNEL_INSTALLATIONS, TABLE_CHANNEL_CONFIG], schemaName),
    );
  }

  if (domains.has('backgroundTasks')) {
    statements.push(
      generateOracleTableSQL({
        tableName: TABLE_BACKGROUND_TASKS,
        schema: TABLE_SCHEMAS[TABLE_BACKGROUND_TASKS],
        schemaName,
      }),
    );
    if (!options.skipDefaultIndexes)
      statements.push(
        ...getDefaultBackgroundTaskIndexDefinitions().map(index => generateOracleIndexSQL(index, schemaName)),
      );
    statements.push(...customIndexStatements(options.indexes, [TABLE_BACKGROUND_TASKS], schemaName));
  }

  if (domains.has('schedules')) {
    for (const tableName of [TABLE_SCHEDULES, TABLE_SCHEDULE_TRIGGERS] as const)
      statements.push(generateOracleTableSQL({ tableName, schema: TABLE_SCHEMAS[tableName], schemaName }));
    if (!options.skipDefaultIndexes)
      statements.push(...getDefaultScheduleIndexDefinitions().map(index => generateOracleIndexSQL(index, schemaName)));
    statements.push(...customIndexStatements(options.indexes, [TABLE_SCHEDULES, TABLE_SCHEDULE_TRIGGERS], schemaName));
  }

  if (domains.has('notifications')) {
    statements.push(
      generateOracleTableSQL({
        tableName: TABLE_NOTIFICATIONS,
        schema: TABLE_SCHEMAS[TABLE_NOTIFICATIONS],
        schemaName,
        compositePrimaryKey: ['threadId', 'id'],
      }),
    );
    if (!options.skipDefaultIndexes)
      statements.push(
        ...getDefaultNotificationIndexDefinitions().map(index => generateOracleIndexSQL(index, schemaName)),
      );
    statements.push(...customIndexStatements(options.indexes, [TABLE_NOTIFICATIONS], schemaName));
  }

  if (domains.has('datasets')) {
    for (const tableName of [TABLE_DATASETS, TABLE_DATASET_ITEMS, TABLE_DATASET_VERSIONS] as const)
      statements.push(
        generateOracleTableSQL({
          tableName,
          schema: TABLE_SCHEMAS[tableName],
          schemaName,
          ...(tableName === TABLE_DATASET_ITEMS ? { compositePrimaryKey: ['id', 'datasetVersion'] } : {}),
        }),
      );
    if (!options.skipDefaultIndexes)
      statements.push(...getDefaultDatasetIndexDefinitions().map(index => generateOracleIndexSQL(index, schemaName)));
    statements.push(
      ...customIndexStatements(
        options.indexes,
        [TABLE_DATASETS, TABLE_DATASET_ITEMS, TABLE_DATASET_VERSIONS],
        schemaName,
      ),
    );
  }

  if (domains.has('experiments')) {
    for (const tableName of [TABLE_EXPERIMENTS, TABLE_EXPERIMENT_RESULTS] as const)
      statements.push(
        generateOracleTableSQL({
          tableName,
          schema: tableName === TABLE_EXPERIMENTS ? ORACLE_EXPERIMENTS_SCHEMA : ORACLE_EXPERIMENT_RESULTS_SCHEMA,
          schemaName,
        }),
      );
    statements.push(
      ...getDefaultExperimentIndexDefinitions()
        .filter(index => !options.skipDefaultIndexes || index.unique)
        .map(index => generateOracleIndexSQL(index, schemaName)),
    );
    statements.push(
      ...customIndexStatements(options.indexes, [TABLE_EXPERIMENTS, TABLE_EXPERIMENT_RESULTS], schemaName),
    );
  }

  // Knowledge is opt-in at runtime and in offline deployments.
  if (domains.has('knowledge')) {
    statements.push(...KnowledgeOracle.getExportDDL(schemaName, options.skipDefaultIndexes));
    statements.push(...customIndexStatements(options.indexes, KnowledgeOracle.MANAGED_TABLES, schemaName));
  }

  if (domains.has('vector')) {
    statements.push(...vectorSchemaStatements(options.vector, schemaName));
  }

  return statements.map(statement => `${statement.trim()};`).join('\n\n');
}

const MemorySchemaTables = [
  TABLE_THREADS,
  TABLE_MESSAGES,
  TABLE_RESOURCES,
  TABLE_OBSERVATIONAL_MEMORY,
  TABLE_MEMORY_RUN_FENCES,
] as const;
const WorkflowSchemaTables = [TABLE_WORKFLOW_SNAPSHOT, TABLE_WORKFLOW_RUN_OWNERS] as const;
const ObservabilitySchemaTables = [TABLE_SPANS, LOG_EVENTS_TABLE] as const;
const ScoreSchemaTables = [TABLE_SCORERS] as const;
const ScorerDefinitionSchemaTables = [TABLE_SCORER_DEFINITIONS, TABLE_SCORER_DEFINITION_VERSIONS] as const;
const MCPClientSchemaTables = [TABLE_MCP_CLIENTS, TABLE_MCP_CLIENT_VERSIONS] as const;
const AgentSchemaTables = [TABLE_AGENTS, TABLE_AGENT_VERSIONS] as const;

function needsMigrationTable(domains: Set<OracleSchemaDomain>): boolean {
  return (
    domains.has('migrations') ||
    domains.has('memory') ||
    domains.has('workflows') ||
    domains.has('observability') ||
    domains.has('scores') ||
    domains.has('scorerDefinitions') ||
    domains.has('mcpClients') ||
    domains.has('agents') ||
    domains.has('threadState') ||
    domains.has('blobs') ||
    domains.has('toolProviderConnections') ||
    domains.has('workflowDefinitions') ||
    domains.has('promptBlocks') ||
    domains.has('mcpServers') ||
    domains.has('workspaces') ||
    domains.has('skills') ||
    domains.has('favorites') ||
    domains.has('channels') ||
    domains.has('backgroundTasks') ||
    domains.has('schedules') ||
    domains.has('notifications') ||
    domains.has('datasets') ||
    domains.has('experiments')
  );
}

function migrationSchemaStatements(schemaName?: string, tableName = DEFAULT_ORACLE_MIGRATIONS_TABLE): string[] {
  return [oracleMigrationTableSql(qualifyName(normalizeIdentifier(tableName, 'migration table name'), schemaName))];
}

function memorySchemaStatements(schemaName?: string): string[] {
  // Memory DDL is explicit because messages, working memory, and observations
  // need Oracle-specific CLOB choices in addition to native JSON columns.
  return [
    `CREATE TABLE ${qualifyName(TABLE_RESOURCES, schemaName)} (
  id VARCHAR2(512) PRIMARY KEY,
  "workingMemory" CLOB,
  metadata JSON,
  "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL,
  "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL
)`,
    `CREATE TABLE ${qualifyName(TABLE_THREADS, schemaName)} (
  id VARCHAR2(512) PRIMARY KEY,
  "resourceId" VARCHAR2(512) NOT NULL,
  title VARCHAR2(1024),
  metadata JSON,
  "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL,
  "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL
)`,
    `CREATE TABLE ${qualifyName(TABLE_MESSAGES, schemaName)} (
  id VARCHAR2(512) PRIMARY KEY,
  thread_id VARCHAR2(512) NOT NULL,
  content CLOB NOT NULL,
  role VARCHAR2(64) NOT NULL,
  type VARCHAR2(64) NOT NULL,
  "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL,
  "resourceId" VARCHAR2(512)
)`,
    `CREATE TABLE ${qualifyName(TABLE_OBSERVATIONAL_MEMORY, schemaName)} (
  id VARCHAR2(512) PRIMARY KEY,
  "lookupKey" VARCHAR2(1024) NOT NULL,
  "scope" VARCHAR2(32) NOT NULL,
  "resourceId" VARCHAR2(512) NOT NULL,
  "threadId" VARCHAR2(512),
  "activeObservations" CLOB,
  "activeObservationsPendingUpdate" CLOB,
  "originType" VARCHAR2(32) NOT NULL,
  config JSON NOT NULL,
  "generationCount" NUMBER(10) DEFAULT 0 NOT NULL,
  "lastObservedAt" TIMESTAMP WITH TIME ZONE,
  "lastReflectionAt" TIMESTAMP WITH TIME ZONE,
  "pendingMessageTokens" NUMBER(20) DEFAULT 0 NOT NULL,
  "totalTokensObserved" NUMBER(20) DEFAULT 0 NOT NULL,
  "observationTokenCount" NUMBER(20) DEFAULT 0 NOT NULL,
  "isObserving" NUMBER(1) DEFAULT 0 NOT NULL,
  "isReflecting" NUMBER(1) DEFAULT 0 NOT NULL,
  "observedMessageIds" JSON,
  "observedTimezone" VARCHAR2(128),
  "bufferedObservations" CLOB,
  "bufferedObservationTokens" NUMBER(20),
  "bufferedMessageIds" JSON,
  "bufferedReflection" CLOB,
  "bufferedReflectionTokens" NUMBER(20),
  "bufferedReflectionInputTokens" NUMBER(20),
  "reflectedObservationLineCount" NUMBER(20),
  "bufferedObservationChunks" JSON,
  "isBufferingObservation" NUMBER(1) DEFAULT 0 NOT NULL,
  "isBufferingReflection" NUMBER(1) DEFAULT 0 NOT NULL,
  "lastBufferedAtTokens" NUMBER(20) DEFAULT 0 NOT NULL,
  "lastBufferedAtTime" TIMESTAMP WITH TIME ZONE,
  metadata JSON,
  "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL,
  "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL,
  "supersededBy" VARCHAR2(512)
)`,
    generateOracleTableSQL({
      tableName: TABLE_MEMORY_RUN_FENCES as TABLE_NAMES,
      schema: RUN_FENCING_TABLE_SCHEMAS[TABLE_MEMORY_RUN_FENCES],
      schemaName,
    }),
  ];
}

function workflowSchemaStatements(schemaName?: string): string[] {
  return [
    `CREATE TABLE ${qualifyName(TABLE_WORKFLOW_SNAPSHOT, schemaName)} (
  workflow_name VARCHAR2(512) NOT NULL,
  run_id VARCHAR2(512) NOT NULL,
  "resourceId" VARCHAR2(512),
  snapshot JSON NOT NULL,
  "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL,
  "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL,
  CONSTRAINT ${storageConstraintName('MASTRA_WORKFLOW_SNAPSHOT_PK')} PRIMARY KEY (workflow_name, run_id)
)`,
    generateOracleTableSQL({
      tableName: TABLE_WORKFLOW_RUN_OWNERS as TABLE_NAMES,
      schema: RUN_FENCING_TABLE_SCHEMAS[TABLE_WORKFLOW_RUN_OWNERS],
      schemaName,
    }),
  ];
}

function scoreSchemaStatements(schemaName?: string): string[] {
  return [
    generateOracleTableSQL({
      tableName: TABLE_SCORERS,
      schema: ORACLE_SCORES_SCHEMA,
      schemaName,
    }),
  ];
}

function observabilitySchemaStatements(schemaName?: string): string[] {
  return [
    generateOracleTableSQL({
      tableName: TABLE_SPANS,
      schema: TABLE_SCHEMAS[TABLE_SPANS],
      schemaName,
      compositePrimaryKey: ['traceId', 'spanId'],
    }),
    logEventsTableSql(qualifyName(LOG_EVENTS_TABLE, schemaName)),
  ];
}

function agentSchemaStatements(schemaName?: string): string[] {
  return [
    generateOracleTableSQL({
      tableName: TABLE_AGENTS,
      schema: TABLE_SCHEMAS[TABLE_AGENTS],
      schemaName,
    }),
    generateOracleTableSQL({
      tableName: TABLE_AGENT_VERSIONS,
      schema: ORACLE_AGENT_VERSIONS_SCHEMA,
      schemaName,
    }),
  ];
}

function scorerDefinitionSchemaStatements(schemaName?: string): string[] {
  return [
    generateOracleTableSQL({
      tableName: TABLE_SCORER_DEFINITIONS,
      schema: TABLE_SCHEMAS[TABLE_SCORER_DEFINITIONS],
      schemaName,
    }),
    generateOracleTableSQL({
      tableName: TABLE_SCORER_DEFINITION_VERSIONS,
      schema: TABLE_SCHEMAS[TABLE_SCORER_DEFINITION_VERSIONS],
      schemaName,
    }),
  ];
}

function mcpClientSchemaStatements(schemaName?: string): string[] {
  return [
    generateOracleTableSQL({
      tableName: TABLE_MCP_CLIENTS,
      schema: TABLE_SCHEMAS[TABLE_MCP_CLIENTS],
      schemaName,
    }),
    generateOracleTableSQL({
      tableName: TABLE_MCP_CLIENT_VERSIONS,
      schema: TABLE_SCHEMAS[TABLE_MCP_CLIENT_VERSIONS],
      schemaName,
    }),
  ];
}

function vectorSchemaStatements(config: ExportOracleSchemasOptions['vector'], schemaName?: string): string[] {
  // Vector export creates the same registry + physical VECTOR tables used by
  // OracleVector at runtime, so DBAs can pre-provision indexes deterministically.
  const registryTableName = normalizeIdentifier(
    config?.registryTableName ?? DEFAULT_VECTOR_REGISTRY_TABLE,
    'registry table name',
  );
  const tablePrefix = normalizeIdentifier(config?.tablePrefix ?? DEFAULT_VECTOR_TABLE_PREFIX, 'table prefix');
  const statements = [vectorRegistryTableStatement(registryTableName, schemaName)];

  for (const vectorIndex of config?.indexes ?? []) {
    statements.push(
      ...vectorIndexStatements(vectorIndex, tablePrefix, registryTableName, config?.defaultMetadataIndexes, schemaName),
    );
  }

  return statements;
}

function memoryDefaultIndexStatements(schemaName?: string): string[] {
  return [
    { name: storageIndexName('MASTRA_THREADS_RETENTION_IDX'), table: TABLE_THREADS, columns: ['createdAt'] },
    { name: storageIndexName('MASTRA_MESSAGES_RETENTION_IDX'), table: TABLE_MESSAGES, columns: ['createdAt'] },
    { name: storageIndexName('MASTRA_RESOURCES_RETENTION_IDX'), table: TABLE_RESOURCES, columns: ['createdAt'] },
    {
      name: storageIndexName('MASTRA_THREADS_RESOURCE_CREATED_IDX'),
      table: TABLE_THREADS,
      columns: ['resourceId', 'createdAt'],
    },
    {
      name: storageIndexName('MASTRA_MESSAGES_THREAD_CREATED_IDX'),
      table: TABLE_MESSAGES,
      columns: ['thread_id', 'createdAt'],
    },
    {
      name: storageIndexName('MASTRA_MESSAGES_RESOURCE_CREATED_IDX'),
      table: TABLE_MESSAGES,
      columns: ['resourceId', 'createdAt'],
    },
    {
      name: storageIndexName('MASTRA_OM_LOOKUP_GENERATION_IDX'),
      table: TABLE_OBSERVATIONAL_MEMORY,
      columns: ['lookupKey', 'generationCount'],
    },
    {
      name: storageIndexName('MASTRA_OM_RESOURCE_CREATED_IDX'),
      table: TABLE_OBSERVATIONAL_MEMORY,
      columns: ['resourceId', 'createdAt'],
    },
    {
      name: storageIndexName('MASTRA_OM_THREAD_CREATED_IDX'),
      table: TABLE_OBSERVATIONAL_MEMORY,
      columns: ['threadId', 'createdAt'],
    },
  ].map(index => generateOracleIndexSQL(index, schemaName));
}

function workflowDefaultIndexStatements(schemaName?: string): string[] {
  return [
    { name: storageIndexName('MASTRA_WORKFLOW_RETENTION_IDX'), table: TABLE_WORKFLOW_SNAPSHOT, columns: ['updatedAt'] },
    {
      name: storageIndexName('MASTRA_WORKFLOW_NAME_CREATED_IDX'),
      table: TABLE_WORKFLOW_SNAPSHOT,
      columns: ['workflow_name', 'createdAt'],
    },
    {
      name: storageIndexName('MASTRA_WORKFLOW_RESOURCE_CREATED_IDX'),
      table: TABLE_WORKFLOW_SNAPSHOT,
      columns: ['resourceId', 'createdAt'],
    },
    {
      name: storageIndexName('MASTRA_WORKFLOW_STATUS_IDX'),
      table: TABLE_WORKFLOW_SNAPSHOT,
      columns: ["JSON_VALUE(snapshot, '$.status' RETURNING VARCHAR2(64) NULL ON ERROR)"],
    },
  ].map(index => generateOracleIndexSQL(index, schemaName));
}

function scoreDefaultIndexStatements(schemaName?: string): string[] {
  return getDefaultScoreIndexDefinitions(indexName => storageIndexName(indexName)).map(index =>
    generateOracleIndexSQL(index, schemaName),
  );
}

function observabilityDefaultIndexStatements(schemaName?: string): string[] {
  return getDefaultObservabilityIndexDefinitions(indexName => storageIndexName(indexName)).map(index =>
    generateOracleIndexSQL(index, schemaName),
  );
}

function agentDefaultIndexStatements(schemaName?: string, skipDefaultIndexes?: boolean): string[] {
  return getDefaultAgentIndexDefinitions(indexName => storageIndexName(indexName))
    .filter(index => !skipDefaultIndexes || index.unique)
    .map(index => generateOracleIndexSQL(index, schemaName));
}

function scorerDefinitionDefaultIndexStatements(schemaName?: string, skipDefaultIndexes?: boolean): string[] {
  return getDefaultScorerDefinitionIndexDefinitions(indexName => storageIndexName(indexName))
    .filter(index => !skipDefaultIndexes || index.unique)
    .map(index => generateOracleIndexSQL(index, schemaName));
}

function mcpClientDefaultIndexStatements(schemaName?: string, skipDefaultIndexes?: boolean): string[] {
  return getDefaultMCPClientIndexDefinitions(indexName => storageIndexName(indexName))
    .filter(index => !skipDefaultIndexes || index.unique)
    .map(index => generateOracleIndexSQL(index, schemaName));
}

function customIndexStatements(
  indexes: OracleCreateIndexOptions[] | undefined,
  tables: readonly string[],
  schemaName?: string,
): string[] {
  // A single config can contain indexes for every domain; filter here so a
  // domain-specific export never emits DDL for tables it does not own.
  if (!indexes?.length) return [];
  const managedTables = new Set(tables.map(table => normalizeIdentifier(table, 'table name')));
  return indexes
    .filter(index => managedTables.has(normalizeIdentifier(index.table, 'table name')))
    .map(index => generateOracleIndexSQL(index, schemaName));
}

function vectorRegistryTableStatement(registryTableName: string, schemaName?: string): string {
  return `CREATE TABLE ${qualifyName(registryTableName, schemaName)} (
  index_name VARCHAR2(512) PRIMARY KEY,
  table_name VARCHAR2(128) NOT NULL,
  dimension NUMBER(10) NOT NULL,
  metric VARCHAR2(32) NOT NULL,
  index_type VARCHAR2(16) NOT NULL,
  vector_format VARCHAR2(16) DEFAULT 'vector' NOT NULL,
  accuracy NUMBER(3),
  index_config JSON,
  created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
  updated_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
)`;
}

function vectorIndexStatements(
  vectorIndex: OracleVectorSchemaIndex,
  tablePrefix: string,
  registryTableName: string,
  defaultMetadataIndexes: string[] | undefined,
  schemaName?: string,
): string[] {
  // Logical index metadata is stored in the registry table; the data lives in a
  // dedicated VECTOR table so index names map cleanly to Mastra's API surface.
  const indexName = normalizeLogicalIndexName(vectorIndex.indexName);
  const dimension = validateDimension(vectorIndex.dimension);
  const vectorFormat = normalizeVectorFormat(vectorIndex.vectorFormat ?? 'vector');
  validateVectorFormatDimension(vectorFormat, dimension);
  const metric = normalizeMetric(vectorIndex.metric ?? defaultMetricForFormat(vectorFormat));
  const tableName = tableNameForIndex(indexName, tablePrefix);
  const indexConfig: OracleVectorIndexConfig = { type: 'none', accuracy: 95, ...vectorIndex.indexConfig };
  buildVectorIndexParameterClause(indexConfig);
  const registryIndexConfig: OracleVectorIndexConfig =
    vectorIndex.buildIndex === false ? { type: 'none', accuracy: 95 } : indexConfig;
  validateMetricForFormat(metric, vectorFormat, registryIndexConfig.type);
  const statements = [
    `CREATE TABLE ${qualifyName(tableName, schemaName)} (
  vector_id VARCHAR2(512) NOT NULL,
  namespace VARCHAR2(255 CHAR) DEFAULT '${DEFAULT_NAMESPACE}' NOT NULL,
  embedding ${vectorColumnType(dimension, vectorFormat)} NOT NULL,
  metadata JSON NOT NULL,
  created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
  updated_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
  PRIMARY KEY (namespace, vector_id)
)`,
    vectorRegistryMergeStatement({
      registryTableName,
      indexName,
      tableName,
      dimension,
      metric,
      indexType: registryIndexConfig.type ?? 'none',
      vectorFormat,
      accuracy: registryIndexConfig.accuracy ?? 95,
      indexConfig,
      schemaName,
    }),
  ];

  for (const field of vectorIndex.metadataIndexes ?? defaultMetadataIndexes ?? DEFAULT_VECTOR_METADATA_INDEXES) {
    // Metadata indexes are function-based JSON_VALUE indexes, matching the
    // provider's JSON filter path for common resource/thread/document fields.
    const metadataIndexName = indexNameForMetadataField(tableName, field);
    const jsonPath = assertJsonPath(field);
    statements.push(`CREATE INDEX ${qualifyName(metadataIndexName, schemaName)}
ON ${qualifyName(tableName, schemaName)} (
  JSON_VALUE(metadata, '${jsonPath}' RETURNING VARCHAR2(4000) NULL ON ERROR)
)`);
  }

  if (vectorIndex.buildIndex !== false && indexConfig.type && indexConfig.type !== 'none') {
    statements.push(vectorPhysicalIndexStatement({ tableName, metric, indexConfig, schemaName }));
  }

  return statements;
}

function vectorRegistryMergeStatement({
  registryTableName,
  indexName,
  tableName,
  dimension,
  metric,
  indexType,
  vectorFormat,
  accuracy,
  indexConfig,
  schemaName,
}: {
  registryTableName: string;
  indexName: string;
  tableName: string;
  dimension: number;
  metric: OracleMetric;
  indexType: string;
  vectorFormat: OracleVectorFormat;
  accuracy: number;
  indexConfig: OracleVectorIndexConfig;
  schemaName?: string;
}): string {
  // MERGE makes generated SQL re-runnable during review, local setup, or DBA
  // deployment without duplicate registry rows.
  return `MERGE INTO ${qualifyName(registryTableName, schemaName)} target
USING (
  SELECT
    '${escapeSqlLiteral(indexName)}' AS index_name,
    '${tableName}' AS table_name,
    ${dimension} AS dimension,
    '${metric}' AS metric,
    '${indexType}' AS index_type,
    '${vectorFormat}' AS vector_format,
    ${validateAccuracy(accuracy)} AS accuracy,
    JSON('${escapeSqlLiteral(JSON.stringify(indexConfig))}') AS index_config
  FROM dual
) source
ON (target.index_name = source.index_name)
WHEN MATCHED THEN UPDATE SET
  target.table_name = source.table_name,
  target.dimension = source.dimension,
  target.metric = source.metric,
  target.index_type = source.index_type,
  target.vector_format = source.vector_format,
  target.accuracy = source.accuracy,
  target.index_config = source.index_config,
  target.updated_at = SYSTIMESTAMP
WHEN NOT MATCHED THEN INSERT (
  index_name,
  table_name,
  dimension,
  metric,
  index_type,
  vector_format,
  accuracy,
  index_config,
  created_at,
  updated_at
) VALUES (
  source.index_name,
  source.table_name,
  source.dimension,
  source.metric,
  source.index_type,
  source.vector_format,
  source.accuracy,
  source.index_config,
  SYSTIMESTAMP,
  SYSTIMESTAMP
)`;
}

function vectorPhysicalIndexStatement({
  tableName,
  metric,
  indexConfig,
  schemaName,
}: {
  tableName: string;
  metric: OracleMetric;
  indexConfig: OracleVectorIndexConfig;
  schemaName?: string;
}): string {
  // Physical HNSW/IVF indexes are optional. Exact VECTOR_DISTANCE search works
  // without them, while approximate indexes are added when buildIndex is enabled.
  const accuracy = validateAccuracy(indexConfig.accuracy ?? 95);
  const organization =
    indexConfig.type === 'ivf' ? 'ORGANIZATION NEIGHBOR PARTITIONS' : 'ORGANIZATION INMEMORY NEIGHBOR GRAPH';
  const parameters = buildVectorIndexParameterClause(indexConfig);

  return `CREATE VECTOR INDEX ${qualifyName(indexNameForTable(tableName, 'VECTOR_IDX'), schemaName)}
ON ${qualifyName(tableName, schemaName)} (embedding)
${organization}
DISTANCE ${metricToken(metric)}
WITH TARGET ACCURACY ${accuracy}${parameters ? `\n${parameters}` : ''}`;
}

function storageIndexName(indexName: string): string {
  return indexNameForTable(indexName, 'IDX');
}

function storageConstraintName(name: string): string {
  return indexNameForTable(name, 'CONSTRAINT');
}

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}
