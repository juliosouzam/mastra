import { TABLE_SCHEMAS, TABLE_WORKFLOW_DEFINITIONS, WorkflowDefinitionsStorage } from '@mastra/core/storage';
import type {
  CreateWorkflowDefinitionInput,
  ListWorkflowDefinitionsInput,
  ListWorkflowDefinitionsOutput,
  UpdateWorkflowDefinitionInput,
  WorkflowDefinition,
} from '@mastra/core/storage';

import { isOracleErrorCode } from '../../../shared/connection';
import { indexNameForTable } from '../../../vector/identifiers';
import { OracleDB, filterIndexesForTables, parseOracleJson } from '../../db';
import type { OracleCreateIndexOptions } from '../../db';
import { createOracleStorageError, toDate } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

const UPDATE_FIELDS = new Set([
  'description',
  'metadata',
  'inputSchema',
  'outputSchema',
  'stateSchema',
  'requestContextSchema',
  'graph',
  'schedule',
  'status',
  'authorId',
]);

export class WorkflowDefinitionsOracle extends WorkflowDefinitionsStorage {
  static readonly MANAGED_TABLES = [TABLE_WORKFLOW_DEFINITIONS] as const;
  private readonly db: OracleDB;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({
      ...config,
      jsonColumns: [
        'metadata',
        'inputSchema',
        'outputSchema',
        'stateSchema',
        'requestContextSchema',
        'graph',
        'schedule',
      ],
    });
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, WorkflowDefinitionsOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_WORKFLOW_DEFINITIONS,
      schema: TABLE_SCHEMAS[TABLE_WORKFLOW_DEFINITIONS],
    });
    if (!this.skipDefaultIndexes) {
      for (const index of getDefaultWorkflowDefinitionIndexDefinitions()) {
        try {
          await this.db.createIndex(index);
        } catch (error) {
          this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
        }
      }
    }
    for (const index of this.indexes) await this.db.createIndex(index);
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.clearTable(TABLE_WORKFLOW_DEFINITIONS);
  }

  async upsert(input: CreateWorkflowDefinitionInput | UpdateWorkflowDefinitionInput): Promise<WorkflowDefinition> {
    try {
      const now = new Date();
      const existing = await this.getWithClient(this.db, input.id);
      if (!existing) {
        for (const field of ['inputSchema', 'outputSchema', 'graph'] as const) {
          if (input[field] === undefined)
            throw new Error(`Cannot create workflow definition "${input.id}": ${field} is required.`);
        }
        try {
          await this.db.insert({
            tableName: TABLE_WORKFLOW_DEFINITIONS,
            record: {
              id: input.id,
              description: input.description ?? null,
              metadata: input.metadata ?? null,
              inputSchema: input.inputSchema,
              outputSchema: input.outputSchema,
              stateSchema: input.stateSchema ?? null,
              requestContextSchema: input.requestContextSchema ?? null,
              graph: input.graph,
              schedule: input.schedule ?? null,
              status: 'active',
              source: 'storage',
              authorId: input.authorId ?? null,
              createdAt: now,
              updatedAt: now,
            },
          });
          const created = await this.getWithClient(this.db, input.id);
          if (!created) throw new Error(`Failed to persist workflow definition "${input.id}".`);
          return created;
        } catch (error) {
          if (!isOracleErrorCode(error, [-1])) throw error;
        }
      }
      const data = Object.fromEntries(
        Object.entries(input).filter(([field, value]) => UPDATE_FIELDS.has(field) && value !== undefined),
      );
      await this.db.update({
        tableName: TABLE_WORKFLOW_DEFINITIONS,
        keys: { id: input.id },
        data: { ...data, updatedAt: now },
      });
      const updated = await this.getWithClient(this.db, input.id);
      if (!updated) throw new Error(`Failed to update workflow definition "${input.id}".`);
      return updated;
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'WORKFLOW_DEFINITION_UPSERT',
        reason: 'FAILED',
        details: { id: input.id },
        cause,
      });
    }
  }

  async get(id: string): Promise<WorkflowDefinition | null> {
    return this.getWithClient(this.db.read, id);
  }

  private async getWithClient(client: OracleDB, id: string): Promise<WorkflowDefinition | null> {
    const row = await client.oneOrNone(`${this.select()} WHERE id = :id`, { id });
    return row ? rowToDefinition(row) : null;
  }

  async list(args?: ListWorkflowDefinitionsInput): Promise<ListWorkflowDefinitionsOutput> {
    const conditions: string[] = [];
    const binds: Record<string, unknown> = {};
    if (args?.status !== undefined) {
      conditions.push('status = :status');
      binds.status = args.status;
    }
    if (args?.authorId !== undefined) {
      conditions.push('"authorId" = :authorId');
      binds.authorId = args.authorId;
    }
    const rows = await this.db.read.manyOrNone(
      `${this.select()}${conditions.length ? ` WHERE ${conditions.join(' AND ')}` : ''} ORDER BY "updatedAt" DESC, id`,
      binds,
    );
    const definitions = rows.map(rowToDefinition);
    return { definitions, total: definitions.length };
  }

  async delete(id: string): Promise<void> {
    await this.db.none(`DELETE FROM ${this.db.table(TABLE_WORKFLOW_DEFINITIONS)} WHERE id = :id`, { id });
  }

  private select(): string {
    return `SELECT id AS "id", description AS "description", CASE WHEN description IS NULL THEN 1 ELSE 0 END AS "descriptionIsNull", JSON_SERIALIZE(metadata RETURNING CLOB) AS "metadata", JSON_SERIALIZE("inputSchema" RETURNING CLOB) AS "inputSchema", JSON_SERIALIZE("outputSchema" RETURNING CLOB) AS "outputSchema",
      "stateSchema", JSON_SERIALIZE("requestContextSchema" RETURNING CLOB) AS "requestContextSchema", JSON_SERIALIZE(graph RETURNING CLOB) AS "graph", JSON_SERIALIZE(schedule RETURNING CLOB) AS "schedule", status AS "status", source AS "source",
      "authorId", "createdAt", "updatedAt" FROM ${this.db.table(TABLE_WORKFLOW_DEFINITIONS)}`;
  }
}

function rowToDefinition(row: Record<string, unknown>): WorkflowDefinition {
  const inputSchema = parseOracleJson(row.inputSchema);
  const outputSchema = parseOracleJson(row.outputSchema);
  const graph = parseOracleJson<WorkflowDefinition['graph']>(row.graph);
  if (inputSchema === undefined || outputSchema === undefined || graph === undefined)
    throw new Error(`Workflow definition "${String(row.id)}" is missing required JSON columns.`);
  return {
    id: String(row.id),
    inputSchema,
    outputSchema,
    graph,
    status: row.status === 'archived' ? 'archived' : 'active',
    source: 'storage',
    createdAt: toDate(row.createdAt),
    updatedAt: toDate(row.updatedAt),
    description: Number(row.descriptionIsNull) === 1 ? undefined : String(row.description ?? ''),
    metadata: parseOracleJson<WorkflowDefinition['metadata']>(row.metadata),
    stateSchema: parseOracleJson(row.stateSchema),
    requestContextSchema: parseOracleJson(row.requestContextSchema),
    schedule: parseOracleJson<WorkflowDefinition['schedule']>(row.schedule),
    authorId: row.authorId == null ? undefined : String(row.authorId),
  };
}

export function getDefaultWorkflowDefinitionIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    {
      name: indexNameForTable('MASTRA_WORKFLOW_DEFINITIONS_STATUS', 'IDX'),
      table: TABLE_WORKFLOW_DEFINITIONS,
      columns: ['status'],
    },
  ];
}
