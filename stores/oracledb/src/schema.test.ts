import { TABLE_AGENTS, TABLE_MCP_CLIENTS, TABLE_SCORER_DEFINITIONS } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';

import { exportSchemas } from './schema';

describe('Oracle schema export', () => {
  it('exports advanced observability independently of the legacy tables', () => {
    const ddl = exportSchemas({
      domains: ['observabilityVNext'],
      schemaName: 'observability_owner',
      skipDefaultIndexes: true,
    });
    expect(ddl).toContain('CREATE SEQUENCE "OBSERVABILITY_OWNER"."MASTRA_OBS_EVENT_CURSOR"');
    expect(ddl).toContain('CREATE TABLE "OBSERVABILITY_OWNER"."MASTRA_OBS_SPAN_EVENTS"');
    expect(ddl).toContain('CREATE TABLE "OBSERVABILITY_OWNER"."MASTRA_OBS_FEEDBACK_EVENTS"');
    expect(ddl).toContain('ROWDEPENDENCIES');
    expect(ddl).toContain('SYS_EXTRACT_UTC("endedAt")');
    expect(ddl).not.toContain('"MASTRA_AI_SPANS"');
    expect(ddl).not.toContain('"MASTRA_LOG_EVENTS"');
    expect(ddl).not.toContain('"MASTRA_ORACLE_MIGRATIONS"');
  });
  it('retains registry version uniqueness when optional indexes are skipped', () => {
    const ddl = exportSchemas({ domains: ['agents', 'mcpClients', 'scorerDefinitions'], skipDefaultIndexes: true });
    expect(ddl).toContain('CREATE UNIQUE INDEX "MASTRA_AGENT_VERSIONS_AGENT_VERSION_IDX"');
    expect(ddl).toContain('CREATE UNIQUE INDEX "MASTRA_MCP_CLIENT_VERSIONS_CLIENT_VERSION_IDX"');
    expect(ddl).toContain('CREATE UNIQUE INDEX "MASTRA_SCORER_DEFINITION_VERSIONS_DEF_VERSION_IDX"');
    expect(ddl).not.toContain('CREATE INDEX "MASTRA_AGENTS_STATUS_CREATED_IDX"');
  });
  it('exports the opt-in Knowledge V2 schema and its version marker', () => {
    const ddl = exportSchemas({ domains: ['knowledge'], skipDefaultIndexes: true });
    expect(ddl).toContain('CREATE TABLE "MASTRA_KNOWLEDGE_NODES"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_KNOWLEDGE_NODE_SCOPES"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_KNOWLEDGE_SEMANTIC_OUTBOX"');
    expect(ddl).toContain('CREATE UNIQUE INDEX "MASTRA_KNOWLEDGE_NODES_IDENTITY_IDX"');
    expect(ddl).toContain("VALUES ('global', 0, 2)");
    expect(exportSchemas()).not.toContain('CREATE TABLE "MASTRA_KNOWLEDGE_NODES"');
  });
  it('exports experiment retry identity and keeps external dataset identities logical', () => {
    const ddl = exportSchemas({ domains: ['experiments'], skipDefaultIndexes: true });
    expect(ddl).toContain('CREATE TABLE "MASTRA_EXPERIMENTS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_EXPERIMENT_RESULTS"');
    expect(ddl).toContain('"comment" CLOB');
    expect(ddl).toContain('CREATE UNIQUE INDEX "MASTRA_EXPERIMENT_RESULTS_IDENTITY_IDX"');
    expect(ddl).toContain('("experimentId", "itemId", NVL(attempt, 0))');
    expect(ddl).not.toContain('REFERENCES "MASTRA_DATASET_ITEMS"');
    expect(ddl).not.toContain('REFERENCES "MASTRA_DATASETS"');
    expect(ddl).toContain('REFERENCES "MASTRA_EXPERIMENTS"');
  });
  it('exports dataset item history with composite keys and native JSON', () => {
    const ddl = exportSchemas({ domains: ['datasets'] });
    expect(ddl).toContain('CREATE TABLE "MASTRA_DATASETS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_DATASET_ITEMS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_DATASET_VERSIONS"');
    expect(ddl).toContain('PRIMARY KEY (id, "datasetVersion")');
    expect(ddl).toContain('input JSON NOT NULL');
    expect(ddl).toContain('"toolMocks" JSON');
  });
  it('exports notification scope, long text, and delivery indexes', () => {
    const ddl = exportSchemas({ domains: ['notifications'] });
    expect(ddl).toContain('CREATE TABLE "MASTRA_NOTIFICATIONS"');
    expect(ddl).toContain('PRIMARY KEY ("threadId", id)');
    expect(ddl).toContain('summary CLOB NOT NULL');
    expect(ddl).toContain('"lastDeliveryError" CLOB');
    expect(ddl).toContain('CREATE INDEX "MASTRA_NOTIFICATIONS_DUE_IDX"');
  });
  it('exports schedules, trigger history, and numeric retention indexes', () => {
    const ddl = exportSchemas({ domains: ['schedules'] });
    expect(ddl).toContain('CREATE TABLE "MASTRA_SCHEDULES"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_SCHEDULE_TRIGGERS"');
    expect(ddl).toContain('target JSON NOT NULL');
    expect(ddl).toContain('error CLOB');
    expect(ddl).toContain('CREATE INDEX "MASTRA_SCHEDULE_TRIGGERS_RETENTION_IDX"');
  });
  it('exports background task ownership, result JSON, and retention indexes', () => {
    const ddl = exportSchemas({ domains: ['backgroundTasks'] });
    expect(ddl).toContain('CREATE TABLE "MASTRA_BACKGROUND_TASKS"');
    expect(ddl).toContain('"ownerId" VARCHAR2(512)');
    expect(ddl).toContain('"leaseExpiresAt" TIMESTAMP WITH TIME ZONE');
    expect(ddl).toContain('suspend_payload JSON');
    expect(ddl).toContain('CREATE INDEX "MASTRA_BACKGROUND_TASKS_RETENTION_IDX"');
  });
  it('exports channel data and enforces unique webhook routing', () => {
    const ddl = exportSchemas({ domains: ['channels'], skipDefaultIndexes: true });
    expect(ddl).toContain('CREATE TABLE "MASTRA_CHANNEL_INSTALLATIONS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_CHANNEL_CONFIG"');
    expect(ddl).toContain('error CLOB');
    expect(ddl).toContain('CREATE UNIQUE INDEX "MASTRA_CHANNEL_WEBHOOK_IDX"');
  });
  it('exports favorites with a composite key and entity lookup index', () => {
    const ddl = exportSchemas({ domains: ['favorites'] });
    expect(ddl).toContain('CREATE TABLE "MASTRA_FAVORITES"');
    expect(ddl).toContain('PRIMARY KEY ("userId", "entityType", "entityId")');
    expect(ddl).toContain('CREATE INDEX "MASTRA_FAVORITES_ENTITY_IDX"');
  });
  it('exports skill content, file manifests, and required version uniqueness', () => {
    const ddl = exportSchemas({ domains: ['skills'], skipDefaultIndexes: true });
    expect(ddl).toContain('CREATE TABLE "MASTRA_SKILLS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_SKILL_VERSIONS"');
    expect(ddl).toContain('instructions CLOB NOT NULL');
    expect(ddl).toContain('files JSON');
    expect(ddl).toContain('tree JSON');
    expect(ddl).toContain('("skillId", "versionNumber")');
  });
  it('exports workspace configuration and required version uniqueness', () => {
    const ddl = exportSchemas({ domains: ['workspaces'], skipDefaultIndexes: true });
    expect(ddl).toContain('CREATE TABLE "MASTRA_WORKSPACES"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_WORKSPACE_VERSIONS"');
    expect(ddl).toContain('filesystem JSON');
    expect(ddl).toContain('"autoSync" NUMBER(1)');
    expect(ddl).toContain('CREATE UNIQUE INDEX');
    expect(ddl).toContain('("workspaceId", "versionNumber")');
  });
  it('exports MCP server configuration and required version uniqueness', () => {
    const ddl = exportSchemas({ domains: ['mcpServers'], skipDefaultIndexes: true });
    expect(ddl).toContain('CREATE TABLE "MASTRA_MCP_SERVERS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_MCP_SERVER_VERSIONS"');
    expect(ddl).toContain('instructions CLOB');
    expect(ddl).toContain('tools JSON');
    expect(ddl).toContain('CREATE UNIQUE INDEX');
    expect(ddl).toContain('("mcpServerId", "versionNumber")');
  });
  it('exports prompt snapshots and always enforces version-number uniqueness', () => {
    const ddl = exportSchemas({ domains: ['promptBlocks'], skipDefaultIndexes: true });
    expect(ddl).toContain('CREATE TABLE "MASTRA_PROMPT_BLOCKS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_PROMPT_BLOCK_VERSIONS"');
    expect(ddl).toContain('content CLOB NOT NULL');
    expect(ddl).toContain('"requestContextSchema" JSON');
    expect(ddl).toContain('CREATE UNIQUE INDEX');
    expect(ddl).toContain('("blockId", "versionNumber")');
  });
  it('exports persisted workflow graphs, schedules, and their status index', () => {
    const ddl = exportSchemas({ domains: ['workflowDefinitions'] });
    expect(ddl).toContain('CREATE TABLE "MASTRA_WORKFLOW_DEFINITIONS"');
    expect(ddl).toContain('graph JSON NOT NULL');
    expect(ddl).toContain('schedule JSON');
    expect(ddl).toContain('CREATE INDEX "MASTRA_WORKFLOW_DEFINITIONS_STATUS_IDX"');
  });
  it('exports scoped tool provider connections with full labels and lookup indexes', () => {
    const ddl = exportSchemas({ schemaName: 'app_schema', domains: ['toolProviderConnections'] });
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_TOOL_PROVIDER_CONNECTIONS"');
    expect(ddl).toContain('PRIMARY KEY ("authorId", "providerId", "connectionId")');
    expect(ddl).toContain('label CLOB');
    expect(ddl).toContain('CREATE INDEX "APP_SCHEMA"."MASTRA_TOOL_CONNECTIONS_AUTHOR_IDX"');
  });
  it('exports skill blobs as non-null CLOB content keyed by hash', () => {
    const ddl = exportSchemas({ schemaName: 'app_schema', domains: ['blobs'] });
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_SKILL_BLOBS"');
    expect(ddl).toContain('hash VARCHAR2(512) PRIMARY KEY');
    expect(ddl).toContain('content CLOB NOT NULL');
    expect(ddl).toContain('"size" NUMBER');
  });
  it('exports thread state with a composite key and its retention index', () => {
    const ddl = exportSchemas({ schemaName: 'app_schema', domains: ['threadState'] });
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_THREAD_STATE"');
    expect(ddl).toContain('PRIMARY KEY ("threadId", type)');
    expect(ddl).toContain('"updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL');
    expect(ddl).toContain('CREATE INDEX "APP_SCHEMA"."MASTRA_THREAD_STATE_RETENTION_IDX_IDX"');
    expect(exportSchemas({ domains: ['threadState'], skipDefaultIndexes: true })).not.toContain('CREATE INDEX');
  });
  it('includes the memory and workflow ownership schema required at runtime', () => {
    const ddl = exportSchemas({ schemaName: 'app_schema', domains: ['memory', 'workflows'] });

    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_MEMORY_RUN_FENCES"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_WORKFLOW_RUN_OWNERS"');
    expect(ddl).toContain('"supersededBy" VARCHAR2(512)');
  });

  it('exports schema-qualified storage DDL and vector registry DDL by default', () => {
    const ddl = exportSchemas({ schemaName: 'app_schema' });

    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_ORACLE_MIGRATIONS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_THREADS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_WORKFLOW_SNAPSHOT"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_AI_SPANS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_LOG_EVENTS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_SCORERS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_SCORER_DEFINITIONS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_MCP_CLIENTS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_AGENTS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_VECTOR_INDEXES"');
  });

  it('exports vector table, registry row, metadata indexes, and explicit IVF DDL', () => {
    const ddl = exportSchemas({
      schemaName: 'app_schema',
      domains: ['vector'],
      vector: {
        tablePrefix: 'APP_VEC',
        indexes: [
          {
            indexName: 'memory_messages_1536',
            dimension: 1536,
            indexConfig: { type: 'ivf', accuracy: 90, ivf: { neighborPartitions: 4 } },
            metadataIndexes: ['resource_id'],
          },
        ],
      },
    });

    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."APP_VEC_MEMORY_MESSAGES_1536"');
    expect(ddl).toContain('MERGE INTO "APP_SCHEMA"."MASTRA_VECTOR_INDEXES"');
    expect(ddl).toContain("'ivf' AS index_type");
    expect(ddl).toContain("JSON_VALUE(metadata, '$.resource_id' RETURNING VARCHAR2(4000) NULL ON ERROR)");
    expect(ddl).toContain('CREATE VECTOR INDEX "APP_SCHEMA"."APP_VEC_MEMORY_MESSAGES_1536_VECTOR_IDX"');
    expect(ddl).toContain('PARAMETERS (type IVF, neighbor partitions 4)');
    expect(ddl).not.toContain('MASTRA_ORACLE_MIGRATIONS');
  });

  it('exports deferred vector index registry rows as exact physical indexes', () => {
    const ddl = exportSchemas({
      domains: ['vector'],
      vector: {
        indexes: [
          {
            indexName: 'deferred_vectors',
            dimension: 3,
            indexConfig: { type: 'ivf', accuracy: 90, ivf: { neighborPartitions: 1 } },
            buildIndex: false,
          },
        ],
      },
    });

    expect(ddl).toContain("'none' AS index_type");
    expect(ddl).toContain('95 AS accuracy');
    expect(ddl).not.toContain('90 AS accuracy');
    expect(ddl).not.toContain('CREATE VECTOR INDEX');
  });

  it('exports vector DDL without metadata indexes when explicitly disabled', () => {
    const ddl = exportSchemas({
      domains: ['vector'],
      vector: {
        indexes: [
          {
            indexName: 'plain_vectors',
            dimension: 3,
            metadataIndexes: [],
          },
        ],
      },
    });

    expect(ddl).toContain('CREATE TABLE "MASTRA_VEC_PLAIN_VECTORS"');
    expect(ddl).toContain("'cosine' AS metric");
    expect(ddl).not.toContain('JSON_VALUE(metadata');
  });

  it('omits default storage indexes when requested', () => {
    const ddl = exportSchemas({
      domains: ['memory'],
      skipDefaultIndexes: true,
    });

    expect(ddl).toContain('CREATE TABLE "MASTRA_MESSAGES"');
    expect(ddl).not.toContain('CREATE INDEX');
  });

  it('omits optional indexes while retaining required registry uniqueness', () => {
    const ddl = exportSchemas({
      domains: ['workflows', 'observability', 'scores', 'scorerDefinitions', 'mcpClients', 'agents'],
      skipDefaultIndexes: true,
    });

    expect(ddl).toContain('CREATE TABLE "MASTRA_WORKFLOW_SNAPSHOT"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_AI_SPANS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_SCORERS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_SCORER_DEFINITIONS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_MCP_CLIENTS"');
    expect(ddl).toContain('CREATE TABLE "MASTRA_AGENTS"');
    expect(ddl).not.toContain('CREATE INDEX');
    expect(ddl.match(/CREATE UNIQUE INDEX/g)).toHaveLength(3);
  });

  it('exports observability spans, log events, and indexes', () => {
    const ddl = exportSchemas({
      schemaName: 'app_schema',
      domains: ['observability'],
    });

    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_AI_SPANS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_LOG_EVENTS"');
    expect(ddl).toContain('PRIMARY KEY ("traceId", "spanId")');
    expect(ddl).toContain('CREATE INDEX "APP_SCHEMA"."MASTRA_AI_SPANS_TRACEID_STARTEDAT_IDX"');
    expect(ddl).toContain('CREATE INDEX "APP_SCHEMA"."MASTRA_LOG_EVENTS_TIMESTAMP_IDX"');
  });

  it('exports agents DDL, default indexes, and custom indexes', () => {
    const ddl = exportSchemas({
      schemaName: 'app_schema',
      domains: ['agents'],
      indexes: [
        {
          name: 'AGENT_CATEGORY_IDX',
          table: TABLE_AGENTS,
          columns: ["JSON_VALUE(metadata, '$.category' RETURNING VARCHAR2(128) NULL ON ERROR)", 'createdAt DESC'],
        },
      ],
    });

    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_AGENTS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_AGENT_VERSIONS"');
    expect(ddl).toContain('instructions CLOB NOT NULL');
    expect(ddl).toContain('CREATE INDEX "APP_SCHEMA"."MASTRA_AGENTS_STATUS_CREATED_IDX"');
    expect(ddl).toContain('CREATE UNIQUE INDEX "APP_SCHEMA"."MASTRA_AGENT_VERSIONS_AGENT_VERSION_IDX"');
    expect(ddl).toContain('CREATE INDEX "APP_SCHEMA"."AGENT_CATEGORY_IDX"');
  });

  it('exports scorer definitions DDL, default unique index, and custom indexes', () => {
    const ddl = exportSchemas({
      schemaName: 'app_schema',
      domains: ['scorerDefinitions'],
      indexes: [
        {
          name: 'SCORER_DEFINITION_CATEGORY_IDX',
          table: TABLE_SCORER_DEFINITIONS,
          columns: ["JSON_VALUE(metadata, '$.category' RETURNING VARCHAR2(128) NULL ON ERROR)", 'createdAt DESC'],
        },
      ],
    });

    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_SCORER_DEFINITIONS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_SCORER_DEFINITION_VERSIONS"');
    expect(ddl).toContain('CREATE UNIQUE INDEX "APP_SCHEMA"."MASTRA_SCORER_DEFINITION_VERSIONS_DEF_VERSION_IDX"');
    expect(ddl).toContain('CREATE INDEX "APP_SCHEMA"."SCORER_DEFINITION_CATEGORY_IDX"');
  });

  it('exports MCP clients DDL, default unique index, and custom indexes', () => {
    const ddl = exportSchemas({
      schemaName: 'app_schema',
      domains: ['mcpClients'],
      indexes: [
        {
          name: 'MCP_CLIENT_CATEGORY_IDX',
          table: TABLE_MCP_CLIENTS,
          columns: ["JSON_VALUE(metadata, '$.category' RETURNING VARCHAR2(128) NULL ON ERROR)", 'createdAt DESC'],
        },
      ],
    });

    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_MCP_CLIENTS"');
    expect(ddl).toContain('CREATE TABLE "APP_SCHEMA"."MASTRA_MCP_CLIENT_VERSIONS"');
    expect(ddl).toContain('servers JSON NOT NULL');
    expect(ddl).toContain('CREATE UNIQUE INDEX "APP_SCHEMA"."MASTRA_MCP_CLIENT_VERSIONS_CLIENT_VERSION_IDX"');
    expect(ddl).toContain('CREATE INDEX "APP_SCHEMA"."MCP_CLIENT_CATEGORY_IDX"');
  });
});
