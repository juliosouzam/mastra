import { randomUUID } from 'node:crypto';

import type {
  ListKnowledgeScopeMembersInput,
  ListKnowledgeScopeMembersOutput,
  ListKnowledgeScopeNodesInput,
  ListKnowledgeScopeNodesOutput,
} from '@internal/core/knowledge-compat';
import {
  pageKnowledgeScopeMembers,
  pageKnowledgeScopeNodes,
  parseListKnowledgeScopeMembersInput,
  parseListKnowledgeScopeNodesInput,
  KNOWLEDGE_ACCESS_STATE_SCHEMA,
  KNOWLEDGE_IMPORT_RUNS_SCHEMA,
  KNOWLEDGE_IMPORT_STATE_SCHEMA,
  KNOWLEDGE_NODE_ADDRESSES_SCHEMA,
  KNOWLEDGE_NODE_SCOPES_SCHEMA,
  KNOWLEDGE_PROPOSALS_SCHEMA,
  KNOWLEDGE_RECORD_SCOPES_SCHEMA,
  KNOWLEDGE_SCOPE_ADDRESSES_SCHEMA,
  KNOWLEDGE_SCOPE_GRANTS_SCHEMA,
  KNOWLEDGE_STORAGE_CONTRACT_VERSION,
  KNOWLEDGE_STORAGE_SCHEMA_VERSION,
  KNOWLEDGE_TABLE_NAMES,
  RETIRED_KNOWLEDGE_TABLE_NAMES,
  createKnowledgeV2CoreLoader,
  KNOWLEDGE_V2_ACTIVITY_SCHEMA,
  KNOWLEDGE_V2_MENTIONS_SCHEMA,
  KNOWLEDGE_V2_NODES_SCHEMA,
  KNOWLEDGE_V2_RECORDS_SCHEMA,
  TABLE_KNOWLEDGE_ACCESS_STATE,
  TABLE_KNOWLEDGE_IMPORT_RUNS,
  TABLE_KNOWLEDGE_IMPORT_STATE,
  TABLE_KNOWLEDGE_NODE_ADDRESSES,
  TABLE_KNOWLEDGE_NODE_SCOPES,
  TABLE_KNOWLEDGE_PROPOSALS,
  TABLE_KNOWLEDGE_RECORD_SCOPES,
  TABLE_KNOWLEDGE_SCOPE_ADDRESSES,
  TABLE_KNOWLEDGE_SCOPE_GRANTS,
} from '@internal/core/knowledge-compat';
import { coreFeatures } from '@mastra/core/features';
import {
  canonicalizeKnowledgeScope,
  createKnowledgeUlid,
  isKnowledgeScopeVisible,
  KNOWLEDGE_SEMANTIC_OUTBOX_SCHEMA,
  knowledgeScopeKey,
  knowledgeSemanticDocumentId,
  knowledgeSemanticIdempotencyKey,
  KnowledgeConflictError,
  KnowledgeNotFoundError,
  KnowledgeStorage,
  parseKnowledgeNodeCursor,
  parseKnowledgeWikilinks,
  TABLE_KNOWLEDGE_ACTIVITY,
  TABLE_KNOWLEDGE_MENTIONS,
  TABLE_KNOWLEDGE_NODES,
  TABLE_KNOWLEDGE_RECORDS,
  TABLE_KNOWLEDGE_SEMANTIC_OUTBOX,
} from '@mastra/core/storage';
import type {
  AppendKnowledgeInput,
  ClaimKnowledgeSemanticOutboxInput,
  CreateKnowledgeNodeInput,
  KnowledgeActivityAction,
  KnowledgeActivityEvent,
  KnowledgeCurationCursor,
  KnowledgeNode,
  KnowledgeRecord,
  KnowledgeScope,
  KnowledgeSemanticDocumentType,
  KnowledgeStructurePlan,
  KnowledgeStructureReconcileResult,
  KnowledgeSemanticOperation,
  KnowledgeSemanticOutboxEntry,
  ListKnowledgeNodesInput,
  QueryKnowledgeBySourceInput,
  QueryKnowledgeInput,
  QueryKnowledgeOutput,
  SearchKnowledgeInput,
  KNOWLEDGE_TABLE_NAME,
  SearchKnowledgeResult,
  StorageColumn,
  TABLE_NAMES,
  UpdateKnowledgeNodeInput,
} from '@mastra/core/storage';
import type { Connection } from 'oracledb';
import oracledb from 'oracledb';

import { asBindParameters, clobBind, isOracleErrorCode } from '../../../shared/connection';
import { normalizeIdentifier, qualifyName } from '../../../vector/identifiers';
import { OracleDB, generateOracleTableSQL, generateOracleIndexSQL, filterIndexesForTables } from '../../db';
import type { OracleCreateIndexOptions } from '../../db';
import { toDate } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

type QueryValues = unknown[];

const loadKnowledgeV2Core = createKnowledgeV2CoreLoader(coreFeatures, () => import('@mastra/core/storage'));

async function assertKnowledgeDescriptionWithinBoundCompat(description: string | undefined): Promise<void> {
  const { assertKnowledgeDescriptionWithinBound } = await loadKnowledgeV2Core();
  assertKnowledgeDescriptionWithinBound(description);
}

async function assertKnowledgeRecordTextWithinBoundCompat(text: string): Promise<void> {
  const { assertKnowledgeRecordTextWithinBound } = await loadKnowledgeV2Core();
  assertKnowledgeRecordTextWithinBound(text);
}

interface QueryResult {
  rows: Record<string, unknown>[];
  rowsAffected: number;
}

interface Executor {
  execute(statement: string | { sql: string; args?: QueryValues }): Promise<QueryResult>;
}

const camelCaseColumns = [
  'canonicalName',
  'scopeKey',
  'mergedInto',
  'createdAt',
  'updatedAt',
  'sourceThreadId',
  'capturedAt',
  'deletedAt',
  'deletedBy',
  'sourceType',
  'sourceId',
  'recordId',
  'lastKnowledgeId',
  'recordType',
  'idempotencyKey',
  'documentId',
  'documentType',
  'availableAt',
  'claimedAt',
  'claimedBy',
  'completedAt',
  'isScope',
  'nodeId',
  'targetNodeId',
  'scopeNodeId',
  'scopeRefId',
  'canSuggest',
  'schemaVersion',
  'importerId',
  'importKind',
  'triggerKind',
  'transcriptThreadId',
  'traceId',
  'queuedAt',
  'startedAt',
  'targetType',
  'targetId',
  'contextScopeId',
  'importRunId',
  'proposerContextScopeId',
  'expectedVersion',
  'reviewerContextScopeId',
  'reviewedAt',
  'addedAt',
] as const;

function transformSqlCode(sql: string, transform: (code: string) => string): string {
  return sql
    .split(/('(?:''|[^'])*')/g)
    .map((part, index) => (index % 2 === 0 ? transform(part) : part))
    .join('');
}

// This is the same domain-local executor shape used by the other SQL knowledge providers.
// It only normalizes this module's static SQL, never caller-supplied SQL.
function oracleSql(sql: string, schemaName?: string): string {
  return transformSqlCode(sql, code => {
    let result = code;
    for (const column of camelCaseColumns)
      result = result.replace(new RegExp(`(?<!")\\b${column}\\b(?!")`, 'g'), `"${column}"`);
    for (const table of [...KNOWLEDGE_TABLE_NAMES, ...RETIRED_KNOWLEDGE_TABLE_NAMES])
      result = result.replaceAll(`"${table}"`, qualifyName(table, schemaName));
    return result
      .replace(/\bTRUE\b/g, '1')
      .replace(/\bFALSE\b/g, '0')
      .replace(/\bLIMIT (\?|\d+)/g, 'FETCH FIRST $1 ROWS ONLY')
      .replaceAll('COLLATE "C"', 'COLLATE BINARY');
  });
}

function textBind(value: string | undefined | null) {
  return value == null ? null : clobBind(value);
}

function createExecutor(connection: Connection, schemaName?: string): Executor {
  return {
    async execute(statement) {
      const source = typeof statement === 'string' ? statement : statement.sql;
      const args = typeof statement === 'string' ? [] : (statement.args ?? []);
      const ignoreDuplicate = / ON CONFLICT(?: \(id\))? DO NOTHING$/.test(source);
      const binds: Record<string, unknown> = {};
      let index = 0;
      const sql = transformSqlCode(
        oracleSql(source.replace(/ ON CONFLICT(?: \(id\))? DO NOTHING$/, ''), schemaName),
        code =>
          code.replace(/jsonb\(\?\)|\?/g, token => {
            const value = args[index];
            const name = `v${index++}`;
            if (token.startsWith('jsonb')) {
              binds[name] = value == null ? null : clobBind(String(value));
              return `JSON(:${name})`;
            }
            binds[name] = typeof value === 'boolean' ? Number(value) : value;
            return value && typeof value === 'object' && 'val' in value && value.val === ''
              ? `NVL(:${name}, EMPTY_CLOB())`
              : `:${name}`;
          }),
      );
      if (index !== args.length) throw new Error('Oracle knowledge bind count mismatch');
      try {
        const result = await connection.execute<Record<string, unknown>>(sql, asBindParameters(binds), {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
          fetchTypeHandler: metadata =>
            metadata.dbType === oracledb.DB_TYPE_CLOB ? { type: oracledb.STRING } : undefined,
        });
        return {
          rows: (result.rows ?? []).map(row =>
            Object.fromEntries(
              Object.entries(row).map(([key, value]) => [key === key.toUpperCase() ? key.toLowerCase() : key, value]),
            ),
          ),
          rowsAffected: result.rowsAffected ?? 0,
        };
      } catch (error) {
        if (ignoreDuplicate && isOracleErrorCode(error, [-1])) return { rows: [], rowsAffected: 0 };
        throw error;
      }
    },
  };
}

/**
 * Scope keys visible to a caller holding `scope`: a row is visible when its scope is a subset of the
 * caller's (companion entries such as `resource:x:uncurated` make this more than a prefix match).
 * Matching rows by `scopeKey` keeps visibility filters on an index; a caller scope has only a few
 * entries, so the list stays small.
 */
function visibleScopeKeys(scope: KnowledgeScope): string[] {
  const canonical = canonicalizeKnowledgeScope(scope);
  const keys = new Set<string>();
  for (let mask = 1; mask < 1 << canonical.length; mask++) {
    try {
      keys.add(knowledgeScopeKey(canonical.filter((_, index) => mask & (1 << index))));
    } catch {
      // Not a valid scope, so no stored row can carry it.
    }
  }
  return [...keys];
}

const visibleSql = (keys: string[], scopeKeyColumn = 'scopeKey') =>
  `${scopeKeyColumn} IN (${keys.map(() => '?').join(',')})`;

function parseJson<T>(value: unknown): T {
  if (typeof value === 'string') return JSON.parse(value) as T;
  if (value instanceof Uint8Array) return JSON.parse(new TextDecoder().decode(value)) as T;
  if (value instanceof ArrayBuffer) return JSON.parse(new TextDecoder().decode(value)) as T;
  return value as T;
}

function optionalDate(value: unknown): Date | undefined {
  return value == null ? undefined : toDate(value);
}

function canonicalName(name: string): string {
  return name.trim().toLocaleLowerCase();
}

function nodeReferenceId(node: KnowledgeNode | string): string {
  return typeof node === 'string' ? node : node.id;
}

function escapeLikePattern(value: string): string {
  return value.replaceAll('=', '==').replaceAll('%', '=%').replaceAll('_', '=_');
}

function parseNode(row: Record<string, unknown>): KnowledgeNode {
  return {
    id: String(row.id),
    type: 'node',
    name: String(row.name),
    kind: row.kind == null ? '' : String(row.kind),
    content: Number(row.contentIsNull) === 1 ? undefined : row.content == null ? '' : String(row.content),
    description:
      Number(row.descriptionIsNull) === 1 ? undefined : row.description == null ? '' : String(row.description),
    isScope: row.isScope === true || row.isScope === 1 ? true : undefined,
    scope: parseJson(row.scopeJson ?? row.scope),
    version: Number(row.version),
    mergedInto: row.mergedInto == null ? undefined : String(row.mergedInto),
    createdAt: toDate(row.createdAt),
    updatedAt: toDate(row.updatedAt),
  };
}

function parseKnowledge(row: Record<string, unknown>): KnowledgeRecord {
  return {
    id: String(row.id),
    node: String(row.node),
    text: String(row.text),
    scope: parseJson(row.scopeJson ?? row.scope),
    sourceThreadId: String(row.sourceThreadId),
    capturedAt: toDate(row.capturedAt),
    when: optionalDate(row.when),
    metadata: row.metadata == null ? undefined : parseJson<Record<string, unknown>>(row.metadata),
    deletedAt: optionalDate(row.deletedAt),
    deletedBy: row.deletedBy == null ? undefined : String(row.deletedBy),
  };
}

function parseOutbox(row: Record<string, unknown>): KnowledgeSemanticOutboxEntry {
  return {
    id: String(row.id),
    idempotencyKey: String(row.idempotencyKey),
    documentId: String(row.documentId),
    documentType: String(row.documentType) as KnowledgeSemanticDocumentType,
    operation: String(row.operation) as KnowledgeSemanticOperation,
    scope: parseJson(row.scopeJson ?? row.scope),
    status: String(row.status) as KnowledgeSemanticOutboxEntry['status'],
    attempts: Number(row.attempts),
    availableAt: toDate(row.availableAt),
    claimedAt: optionalDate(row.claimedAt),
    claimedBy: row.claimedBy == null ? undefined : String(row.claimedBy),
    createdAt: toDate(row.createdAt),
    completedAt: optionalDate(row.completedAt),
  };
}

export function getDefaultKnowledgeIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    // Structural scopes may share names under different parents; only content nodes have an identity key.
    {
      name: 'MASTRA_KNOWLEDGE_NODES_IDENTITY_IDX',
      table: TABLE_KNOWLEDGE_NODES,
      unique: true,
      where: '"isScope" = 0',
      columns: ['STANDARD_HASH("scopeKey", \'SHA256\')', 'STANDARD_HASH("canonicalName", \'SHA256\')'],
    },
    { name: 'MASTRA_KNOWLEDGE_NODES_SCOPE_IDX', table: TABLE_KNOWLEDGE_NODES, columns: ['"scopeKey"'] },
    { name: 'MASTRA_KNOWLEDGE_NODES_NAME_IDX', table: TABLE_KNOWLEDGE_NODES, columns: ['"canonicalName"'] },
    { name: 'MASTRA_KNOWLEDGE_RECORDS_NODE_IDX', table: TABLE_KNOWLEDGE_RECORDS, columns: ['node', 'id DESC'] },
    {
      name: 'MASTRA_KNOWLEDGE_RECORDS_THREAD_IDX',
      table: TABLE_KNOWLEDGE_RECORDS,
      columns: ['"sourceThreadId"', 'id DESC'],
    },
    { name: 'MASTRA_KNOWLEDGE_RECORDS_SCOPE_IDX', table: TABLE_KNOWLEDGE_RECORDS, columns: ['"scopeKey"', 'id'] },
    {
      name: 'MASTRA_KNOWLEDGE_MENTIONS_SOURCE_IDX',
      table: TABLE_KNOWLEDGE_MENTIONS,
      columns: ['"sourceId"', '"recordId"'],
    },
    {
      name: 'MASTRA_KNOWLEDGE_ACTIVITY_SCOPE_IDX',
      table: TABLE_KNOWLEDGE_ACTIVITY,
      columns: ['"scopeKey"', 'id DESC'],
    },
    {
      name: 'MASTRA_KNOWLEDGE_OUTBOX_IDENTITY_IDX',
      table: TABLE_KNOWLEDGE_SEMANTIC_OUTBOX,
      columns: ['"idempotencyKey"'],
      unique: true,
    },
    {
      name: 'MASTRA_KNOWLEDGE_OUTBOX_CLAIM_IDX',
      table: TABLE_KNOWLEDGE_SEMANTIC_OUTBOX,
      columns: ['status', '"availableAt"', '"createdAt"'],
    },
    {
      name: 'MASTRA_KNOWLEDGE_NODE_SCOPES_IDX',
      table: TABLE_KNOWLEDGE_NODE_SCOPES,
      columns: ['"scopeNodeId"', '"nodeId"'],
    },
    {
      name: 'MASTRA_KNOWLEDGE_RECORD_SCOPES_IDX',
      table: TABLE_KNOWLEDGE_RECORD_SCOPES,
      columns: ['"scopeNodeId"', '"recordId"'],
    },
    {
      name: 'MASTRA_KNOWLEDGE_GRANTS_REF_IDX',
      table: TABLE_KNOWLEDGE_SCOPE_GRANTS,
      columns: ['"scopeRefId"', '"scopeNodeId"'],
    },
    { name: 'MASTRA_KNOWLEDGE_ADDRESSES_NODE_IDX', table: TABLE_KNOWLEDGE_NODE_ADDRESSES, columns: ['"nodeId"'] },
    {
      name: 'MASTRA_KNOWLEDGE_IMPORT_RUNS_IDX',
      table: TABLE_KNOWLEDGE_IMPORT_RUNS,
      columns: ['"importerId"', 'binding', '"queuedAt" DESC'],
    },
    {
      name: 'MASTRA_KNOWLEDGE_ACTIVITY_IMPORT_IDX',
      table: TABLE_KNOWLEDGE_ACTIVITY,
      columns: ['"importRunId"', 'id DESC'],
    },
  ];
}

// Duplicated from Core so this adapter keeps working against Core versions that predate the deprecation.
const KNOWLEDGE_CURATION_CURSOR_REMOVED_MESSAGE =
  'Knowledge curation cursors were removed: observation-time curate is the only Knowledge writer and needs no cursor.';

export const knowledgeTableDefinitions: Array<{
  tableName: TABLE_NAMES | KNOWLEDGE_TABLE_NAME;
  schema: Record<string, StorageColumn>;
  compositePrimaryKey?: string[];
}> = [
  { tableName: TABLE_KNOWLEDGE_NODES, schema: KNOWLEDGE_V2_NODES_SCHEMA },
  { tableName: TABLE_KNOWLEDGE_RECORDS, schema: KNOWLEDGE_V2_RECORDS_SCHEMA },
  {
    tableName: TABLE_KNOWLEDGE_MENTIONS,
    schema: {
      ...KNOWLEDGE_V2_MENTIONS_SCHEMA,
      // The shared SQL API stores the target node in recordId and a polymorphic source in sourceId.
      recordId: {
        ...KNOWLEDGE_V2_MENTIONS_SCHEMA.recordId,
        references: { table: TABLE_KNOWLEDGE_NODES, column: 'id' },
      },
    },
    compositePrimaryKey: ['recordId', 'sourceId'],
  },
  {
    tableName: TABLE_KNOWLEDGE_NODE_SCOPES,
    schema: KNOWLEDGE_NODE_SCOPES_SCHEMA,
    compositePrimaryKey: ['nodeId', 'scopeNodeId'],
  },
  {
    tableName: TABLE_KNOWLEDGE_RECORD_SCOPES,
    schema: KNOWLEDGE_RECORD_SCOPES_SCHEMA,
    compositePrimaryKey: ['recordId', 'scopeNodeId'],
  },
  {
    tableName: TABLE_KNOWLEDGE_SCOPE_GRANTS,
    schema: KNOWLEDGE_SCOPE_GRANTS_SCHEMA,
    compositePrimaryKey: ['scopeNodeId', 'scopeRefId'],
  },
  { tableName: TABLE_KNOWLEDGE_ACCESS_STATE, schema: KNOWLEDGE_ACCESS_STATE_SCHEMA },
  { tableName: TABLE_KNOWLEDGE_SCOPE_ADDRESSES, schema: KNOWLEDGE_SCOPE_ADDRESSES_SCHEMA },
  {
    tableName: TABLE_KNOWLEDGE_NODE_ADDRESSES,
    schema: KNOWLEDGE_NODE_ADDRESSES_SCHEMA,
    compositePrimaryKey: ['source', 'address'],
  },
  {
    tableName: TABLE_KNOWLEDGE_IMPORT_STATE,
    schema: KNOWLEDGE_IMPORT_STATE_SCHEMA,
    compositePrimaryKey: ['importerId', 'binding', 'key'],
  },
  { tableName: TABLE_KNOWLEDGE_IMPORT_RUNS, schema: KNOWLEDGE_IMPORT_RUNS_SCHEMA },
  { tableName: TABLE_KNOWLEDGE_PROPOSALS, schema: KNOWLEDGE_PROPOSALS_SCHEMA },
  { tableName: TABLE_KNOWLEDGE_ACTIVITY, schema: KNOWLEDGE_V2_ACTIVITY_SCHEMA },
  { tableName: TABLE_KNOWLEDGE_SEMANTIC_OUTBOX, schema: KNOWLEDGE_SEMANTIC_OUTBOX_SCHEMA },
];

function selectKnowledgeColumns(table: TABLE_NAMES | KNOWLEDGE_TABLE_NAME, alias?: string): string {
  const definition = knowledgeTableDefinitions.find(item => item.tableName === table)!;
  const prefix = alias ? `${alias}.` : '';
  const fields = Object.entries(definition.schema).map(([field, column]) => {
    const name = field !== field.toLowerCase() || ['when', 'key'].includes(field) ? `"${field}"` : field;
    return column.type === 'jsonb'
      ? `JSON_SERIALIZE(${prefix}${name} RETURNING CLOB) AS "${field}"`
      : `${prefix}${name} AS "${field}"`;
  });
  if (table === TABLE_KNOWLEDGE_NODES)
    for (const field of ['content', 'description'])
      fields.push(`CASE WHEN ${prefix}${field} IS NULL THEN 1 ELSE 0 END AS "${field}IsNull"`);
  return fields.join(', ');
}

export class KnowledgeOracle extends KnowledgeStorage {
  static readonly MANAGED_TABLES = KNOWLEDGE_TABLE_NAMES;
  static getExportDDL(schemaName?: string, skipDefaultIndexes?: boolean): string[] {
    return [
      ...knowledgeTableDefinitions.map(definition =>
        generateOracleTableSQL({ ...definition, tableName: definition.tableName as TABLE_NAMES, schemaName }),
      ),
      ...getDefaultKnowledgeIndexDefinitions()
        .filter(index => index.unique || !skipDefaultIndexes)
        .map(index => generateOracleIndexSQL(index, schemaName)),
      `INSERT INTO ${qualifyName(TABLE_KNOWLEDGE_ACCESS_STATE, schemaName)} (id, epoch, "schemaVersion") VALUES ('global', 0, ${KNOWLEDGE_STORAGE_SCHEMA_VERSION})`,
    ];
  }
  readonly #executor: Executor;
  readonly #readExecutor: Executor;
  readonly #db: OracleDB;
  readonly #schemaName?: string;
  readonly #skipDefaultIndexes?: boolean;
  readonly #indexes: OracleCreateIndexOptions[];
  #initializing?: Promise<void>;

  constructor(config: OracleDomainConfig) {
    // With no resolvable database identity, assume it may be shared, as the reference providers do.
    super({
      storageIsolationKey:
        config.storageIsolationKey ??
        config.poolManager.getStorageIsolationKey?.(config.schemaName) ??
        `oracle:unidentified:schema:${config.schemaName ?? 'current'}`,
    });
    this.#db = new OracleDB(config);
    this.#schemaName = config.schemaName;
    this.#skipDefaultIndexes = config.skipDefaultIndexes;
    this.#indexes = filterIndexesForTables(config.indexes, KnowledgeOracle.MANAGED_TABLES);
    this.#executor = {
      execute: statement =>
        this.#db.withConnection(connection => createExecutor(connection, this.#schemaName).execute(statement)),
    };
    this.#readExecutor = {
      execute: statement =>
        this.#db.read.withConnection(connection => createExecutor(connection, this.#schemaName).execute(statement)),
    };
  }

  override getCapabilities() {
    return {
      contractVersion: KNOWLEDGE_STORAGE_CONTRACT_VERSION,
      schemaVersion: KNOWLEDGE_STORAGE_SCHEMA_VERSION,
      supportsV2: true,
      supportsSchemaInspection: true,
      supportsExplicitReset: true,
    } as const;
  }

  override async inspectSchema() {
    const { inspectKnowledgeSchema } = await loadKnowledgeV2Core();
    try {
      const owner =
        this.#schemaName ??
        String(
          (
            await this.#db.one<{ owner: string }>(
              `SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS "owner" FROM dual`,
            )
          ).owner,
        );
      const tables = await this.#db.manyOrNone<{ tableName: string }>(
        `SELECT table_name AS "tableName" FROM all_tables WHERE owner = :owner AND table_name LIKE 'MASTRA\\_KNOWLEDGE\\_%' ESCAPE '\\'`,
        { owner },
      );
      const tableNames = tables.map(row => row.tableName.toLowerCase());
      const columnRows = await this.#db.manyOrNone<{ tableName: string; columnName: string }>(
        `SELECT table_name AS "tableName", column_name AS "columnName" FROM all_tab_columns WHERE owner = :owner AND table_name LIKE 'MASTRA\\_KNOWLEDGE\\_%' ESCAPE '\\'`,
        { owner },
      );
      const columns = new Map<string, Set<string>>();
      for (const row of columnRows) {
        const table = row.tableName.toLowerCase();
        const names = columns.get(table) ?? new Set<string>();
        names.add(row.columnName === row.columnName.toUpperCase() ? row.columnName.toLowerCase() : row.columnName);
        columns.set(table, names);
      }
      return this.#classifySchema(this.#executor, tableNames, columns);
    } catch (error) {
      return inspectKnowledgeSchema({
        available: false,
        tableNames: [],
        reason: error instanceof Error ? error.message : String(error),
      });
    }
  }

  async #classifySchema(executor: Executor, tableNames: string[], columns: Map<string, Set<string>>) {
    const { inspectKnowledgeSchema } = await loadKnowledgeV2Core();
    if (tableNames.length === 0) return inspectKnowledgeSchema({ available: true, tableNames });
    const missingTables = KNOWLEDGE_TABLE_NAMES.filter(table => !tableNames.includes(table));
    if (missingTables.length > 0) {
      return inspectKnowledgeSchema({
        available: true,
        tableNames,
        reason: `Missing Knowledge v2 tables: ${missingTables.join(', ')}`,
      });
    }

    for (const { tableName: table, schema } of knowledgeTableDefinitions) {
      const actual = columns.get(table) ?? new Set<string>();
      const missing = Object.keys(schema).filter(column => !actual.has(column));
      if (missing.length > 0) {
        return inspectKnowledgeSchema({
          available: true,
          tableNames,
          reason: `Knowledge table ${table} is missing v2 columns: ${missing.join(', ')}`,
        });
      }
    }
    const accessState = await executor.execute(
      `SELECT schemaVersion FROM "${TABLE_KNOWLEDGE_ACCESS_STATE}" WHERE id='global'`,
    );
    if (Number(accessState.rows[0]?.schemaVersion) !== KNOWLEDGE_STORAGE_SCHEMA_VERSION) {
      return inspectKnowledgeSchema({
        available: true,
        tableNames,
        reason: 'Knowledge schema version marker is missing or incompatible',
      });
    }
    return inspectKnowledgeSchema({
      available: true,
      tableNames,
      schemaVersion: KNOWLEDGE_STORAGE_SCHEMA_VERSION,
    });
  }

  async init(): Promise<void> {
    if (this.#initializing) return this.#initializing;
    this.#initializing = this.#initialize();
    try {
      await this.#initializing;
    } finally {
      this.#initializing = undefined;
    }
  }
  async #initialize(): Promise<void> {
    const { assertKnowledgeSchemaCompatible } = await loadKnowledgeV2Core();
    let inspection = await this.inspectSchema();
    let initializing = await this.#hasInitializationMarker();
    if (!initializing && inspection.status === 'incompatible-reset-required') {
      // Another initializer may have finished between catalog inspection and marker lookup.
      inspection = await this.inspectSchema();
      initializing = await this.#hasInitializationMarker();
    }
    if (!initializing) assertKnowledgeSchemaCompatible(inspection);
    if (inspection.status === 'uninitialized') {
      // Oracle DDL commits independently. An explicit marker lets a failed first setup resume
      // additively; an older or unmarked incompatible schema still requires explicit reset.
      // CTAS publishes the table and marker together, so a concurrent startup
      // cannot mistake an empty access-state table for an incompatible schema.
      await this.#db.executeDdl(
        `CREATE TABLE ${this.#db.table(TABLE_KNOWLEDGE_ACCESS_STATE)} (
          id PRIMARY KEY, epoch NOT NULL, "schemaVersion" NOT NULL
        ) AS SELECT CAST('initializing' AS VARCHAR2(512)) AS id,
          CAST(0 AS NUMBER(10)) AS epoch,
          CAST(${KNOWLEDGE_STORAGE_SCHEMA_VERSION} AS NUMBER(10)) AS "schemaVersion" FROM dual`,
        [-955],
      );
    }
    for (const definition of knowledgeTableDefinitions)
      await this.#db.createTable({ ...definition, tableName: definition.tableName as TABLE_NAMES });
    for (const index of getDefaultKnowledgeIndexDefinitions()) {
      if (this.#skipDefaultIndexes && !index.unique) continue;
      try {
        await this.#db.createIndex(index);
      } catch (error) {
        if (index.unique) throw error;
        this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
      }
    }
    for (const index of this.#indexes) await this.#db.createIndex(index);
    await this.#db.tx(async (_client, connection) => {
      await createExecutor(connection, this.#schemaName).execute({
        sql: `INSERT INTO "${TABLE_KNOWLEDGE_ACCESS_STATE}" (id, epoch, schemaVersion) VALUES ('global', 0, ?) ON CONFLICT (id) DO NOTHING`,
        args: [KNOWLEDGE_STORAGE_SCHEMA_VERSION],
      });
      await createExecutor(connection, this.#schemaName).execute(
        `DELETE FROM "${TABLE_KNOWLEDGE_ACCESS_STATE}" WHERE id='initializing'`,
      );
    });
  }

  async #hasInitializationMarker(): Promise<boolean> {
    try {
      const row = await this.#db.oneOrNone<{ schemaVersion: number }>(
        `SELECT "schemaVersion" FROM ${this.#db.table(TABLE_KNOWLEDGE_ACCESS_STATE)} WHERE id='initializing'`,
      );
      return Number(row?.schemaVersion) === KNOWLEDGE_STORAGE_SCHEMA_VERSION;
    } catch (error) {
      if (isOracleErrorCode(error, [-942, -904])) return false;
      throw error;
    }
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.#transaction(async tx => {
      for (const table of [
        TABLE_KNOWLEDGE_RECORD_SCOPES,
        TABLE_KNOWLEDGE_NODE_SCOPES,
        TABLE_KNOWLEDGE_SCOPE_GRANTS,
        TABLE_KNOWLEDGE_SCOPE_ADDRESSES,
        TABLE_KNOWLEDGE_NODE_ADDRESSES,
        TABLE_KNOWLEDGE_MENTIONS,
        TABLE_KNOWLEDGE_PROPOSALS,
        TABLE_KNOWLEDGE_ACTIVITY,
        TABLE_KNOWLEDGE_IMPORT_STATE,
        TABLE_KNOWLEDGE_IMPORT_RUNS,
        TABLE_KNOWLEDGE_SEMANTIC_OUTBOX,
        TABLE_KNOWLEDGE_RECORDS,
        TABLE_KNOWLEDGE_NODES,
      ]) {
        await tx.execute(`DELETE FROM "${table}"`);
      }
      await tx.execute(`UPDATE "${TABLE_KNOWLEDGE_ACCESS_STATE}" SET epoch = 0 WHERE id = 'global'`);
    });
  }

  override async dangerouslyReset(): Promise<void> {
    for (const table of [...RETIRED_KNOWLEDGE_TABLE_NAMES, ...[...KNOWLEDGE_TABLE_NAMES].reverse()])
      await this.#db.executeDdl(`DROP TABLE ${this.#db.table(table)} CASCADE CONSTRAINTS PURGE`, [-942]);
    await this.init();
  }

  override async reconcileStructure(plan: KnowledgeStructurePlan): Promise<KnowledgeStructureReconcileResult> {
    return this.#transaction(async tx => {
      const scopes: Record<string, string> = {};
      const createdScopeIds: string[] = [];
      const createdAddresses = new Set<string>();
      const retrofit = plan.retrofit ?? true;
      let structureChanged = false;
      const deletedScopeAddresses = new Set<string>();
      const resolveAddress = async (address: string): Promise<string | undefined> => {
        if (scopes[address]) return scopes[address];
        const result = await tx.execute({
          sql: `SELECT a."scopeNodeId",n."isScope",n."deletedAt" FROM "${TABLE_KNOWLEDGE_SCOPE_ADDRESSES}" a JOIN "${TABLE_KNOWLEDGE_NODES}" n ON n.id=a."scopeNodeId" WHERE a.address=?`,
          args: [address],
        });
        const row = result.rows[0];
        if (!row) return undefined;
        if (!row.isScope) throw new Error(`Knowledge address ${address} does not reference a scope`);
        if (row.deletedAt) deletedScopeAddresses.add(address);
        scopes[address] = String(row.scopeNodeId);
        return scopes[address];
      };

      for (const scope of plan.scopes) {
        const existingId = await resolveAddress(scope.address);
        if (existingId) continue;
        const id = randomUUID();
        const now = new Date();
        await tx.execute({
          sql: `INSERT INTO "${TABLE_KNOWLEDGE_NODES}" (id,type,name,"canonicalName",kind,description,"isScope",metadata,version,"createdAt","updatedAt") VALUES (?,'node',?,?,?,?,TRUE,jsonb(?),1,?,?)`,
          args: [
            id,
            scope.name,
            canonicalName(scope.name),
            scope.kind ?? null,
            textBind(scope.description),
            scope.metadata == null ? null : JSON.stringify(scope.metadata),
            now,
            now,
          ],
        });
        await tx.execute({
          sql: `INSERT INTO "${TABLE_KNOWLEDGE_SCOPE_ADDRESSES}" (address,"scopeNodeId") VALUES (?,?)`,
          args: [scope.address, id],
        });
        scopes[scope.address] = id;
        createdAddresses.add(scope.address);
        createdScopeIds.push(id);
        structureChanged = true;
      }

      for (const scope of plan.scopes) {
        if (!createdAddresses.has(scope.address) && (!retrofit || deletedScopeAddresses.has(scope.address))) continue;
        const scopeNodeId = scopes[scope.address]!;
        for (const parentAddress of scope.parentAddresses ?? []) {
          const parentId = await resolveAddress(parentAddress);
          if (!parentId || deletedScopeAddresses.has(parentAddress)) {
            const deletedParentId = scopes[parentAddress];
            const existing = deletedParentId
              ? await tx.execute({
                  sql: `SELECT 1 FROM "${TABLE_KNOWLEDGE_NODE_SCOPES}" WHERE "nodeId"=? AND "scopeNodeId"=?`,
                  args: [scopeNodeId, deletedParentId],
                })
              : undefined;
            if (existing?.rows.length) continue;
            throw new Error(`Knowledge parent scope does not exist: ${parentAddress}`);
          }
          const edge = await tx.execute({
            sql: `SELECT 1 FROM "${TABLE_KNOWLEDGE_NODE_SCOPES}" WHERE "nodeId"=? AND "scopeNodeId"=?`,
            args: [scopeNodeId, parentId],
          });
          if (edge.rows.length) continue;
          // Only scope children can collide by name; content nodes placed under the parent share its edges table.
          const sibling = await tx.execute({
            sql: `SELECT n.id FROM "${TABLE_KNOWLEDGE_NODE_SCOPES}" ns JOIN "${TABLE_KNOWLEDGE_NODES}" n ON n.id=ns."nodeId" WHERE ns."scopeNodeId"=? AND n."isScope" = 1 AND n."canonicalName"=? AND n."deletedAt" IS NULL AND n.id<>? LIMIT 1`,
            args: [parentId, canonicalName(scope.name), scopeNodeId],
          });
          if (sibling.rows.length) {
            throw new Error(`Knowledge scope name ${scope.name} already exists under ${parentAddress}`);
          }
          const inserted = await tx.execute({
            sql: `INSERT INTO "${TABLE_KNOWLEDGE_NODE_SCOPES}" ("nodeId","scopeNodeId","addedAt") VALUES (?,?,?) ON CONFLICT DO NOTHING`,
            args: [scopeNodeId, parentId, new Date()],
          });
          structureChanged ||= inserted.rowsAffected > 0;
        }
        for (const grant of scope.grants ?? []) {
          const scopeRefId = await resolveAddress(grant.scopeRefAddress);
          if (!scopeRefId || deletedScopeAddresses.has(grant.scopeRefAddress)) {
            const deletedScopeRefId = scopes[grant.scopeRefAddress];
            const existing = deletedScopeRefId
              ? await tx.execute({
                  sql: `SELECT 1 FROM "${TABLE_KNOWLEDGE_SCOPE_GRANTS}" WHERE "scopeNodeId"=? AND "scopeRefId"=?`,
                  args: [scopeNodeId, deletedScopeRefId],
                })
              : undefined;
            if (existing?.rows.length) continue;
            throw new Error(`Knowledge grant scope does not exist: ${grant.scopeRefAddress}`);
          }
          const inserted = await tx.execute({
            sql: `INSERT INTO "${TABLE_KNOWLEDGE_SCOPE_GRANTS}" ("scopeNodeId","scopeRefId",role,"canSuggest") VALUES (?,?,?,?) ON CONFLICT DO NOTHING`,
            args: [scopeNodeId, scopeRefId, grant.role, grant.canSuggest ?? null],
          });
          structureChanged ||= inserted.rowsAffected > 0;
        }
      }

      if (structureChanged) {
        await tx.execute(`UPDATE "${TABLE_KNOWLEDGE_ACCESS_STATE}" SET epoch=epoch+1 WHERE id='global'`);
      }
      const state = await tx.execute(`SELECT epoch FROM "${TABLE_KNOWLEDGE_ACCESS_STATE}" WHERE id='global'`);
      return {
        scopes,
        createdScopeIds,
        deletedScopeAddresses: [...deletedScopeAddresses],
        changed: structureChanged,
        accessEpoch: Number(state.rows[0]?.epoch ?? 0),
      };
    });
  }

  override async listScopeNodes(input: ListKnowledgeScopeNodesInput = {}): Promise<ListKnowledgeScopeNodesOutput> {
    const { limit, after } = parseListKnowledgeScopeNodesInput(input);
    if (input.addresses?.length === 0 || input.ids?.length === 0) return { scopes: [], nextCursor: null };
    const args: Array<string | number> = [];
    const where = [`n."isScope" = 1`, `n."deletedAt" IS NULL`];
    let within = '';
    if (input.withinAddress !== undefined) {
      // The subtree follows parent edges transitively; UNION dedupes scopes reachable through several parents.
      within = `WITH within_scope(id) AS (SELECT r.id FROM "${TABLE_KNOWLEDGE_NODES}" r JOIN "${TABLE_KNOWLEDGE_SCOPE_ADDRESSES}" ra ON ra."scopeNodeId"=r.id WHERE ra.address=? AND r."isScope" = 1 AND r."deletedAt" IS NULL UNION ALL SELECT ns."nodeId" FROM "${TABLE_KNOWLEDGE_NODE_SCOPES}" ns JOIN within_scope w ON ns."scopeNodeId"=w.id JOIN "${TABLE_KNOWLEDGE_NODES}" c ON c.id=ns."nodeId" WHERE c."isScope" = 1 AND c."deletedAt" IS NULL) CYCLE id SET is_cycle TO 1 DEFAULT 0 `;
      args.push(input.withinAddress);
      where.push('n.id IN (SELECT id FROM within_scope)');
    }
    if (input.addresses) {
      where.push(`a.address IN (${input.addresses.map(() => '?').join(',')})`);
      args.push(...input.addresses);
    }
    if (input.ids) {
      where.push(`n.id IN (${input.ids.map(() => '?').join(',')})`);
      args.push(...input.ids);
    }
    if (after) {
      where.push('(n.name COLLATE "C" > ? OR (n.name = ? AND n.id > ?))');
      args.push(after.name, after.name, after.id);
    }
    args.push(limit + 1);
    const scopes = await this.#readExecutor.execute({
      sql: `${within}SELECT n.id,n.name,n.kind,n.description,CASE WHEN n.description IS NULL THEN 1 ELSE 0 END AS "descriptionIsNull",a.address FROM "${TABLE_KNOWLEDGE_NODES}" n LEFT JOIN "${TABLE_KNOWLEDGE_SCOPE_ADDRESSES}" a ON a."scopeNodeId"=n.id WHERE ${where.join(' AND ')} ORDER BY n.name COLLATE "C", n.id LIMIT ?`,
      args,
    });
    const rows = scopes.rows as Array<Record<string, unknown>>;
    if (rows.length === 0) return { scopes: [], nextCursor: null };
    // Parent edges for this page only, so the read stays bounded by the page size.
    const parents = await this.#readExecutor.execute({
      sql: `SELECT ns."nodeId",ns."scopeNodeId" FROM "${TABLE_KNOWLEDGE_NODE_SCOPES}" ns WHERE ns."nodeId" IN (${rows.map(() => '?').join(',')}) ORDER BY ns."scopeNodeId" COLLATE "C"`,
      args: rows.map(row => String(row.id)),
    });
    const parentsByScopeId = new Map<string, string[]>();
    for (const row of parents.rows as Array<Record<string, unknown>>) {
      const scopeId = String(row.nodeId);
      const parentId = String(row.scopeNodeId);
      const existing = parentsByScopeId.get(scopeId);
      if (existing) existing.push(parentId);
      else parentsByScopeId.set(scopeId, [parentId]);
    }
    return pageKnowledgeScopeNodes(
      rows.map(row => ({
        id: String(row.id),
        address: String(row.address),
        name: String(row.name),
        ...(row.kind == null ? {} : { kind: String(row.kind) }),
        ...(Number(row.descriptionIsNull) === 1 ? {} : { description: String(row.description ?? '') }),
        parentIds: parentsByScopeId.get(String(row.id)) ?? [],
      })),
      limit,
      input,
    );
  }

  override async listScopeMembers(input: ListKnowledgeScopeMembersInput): Promise<ListKnowledgeScopeMembersOutput> {
    const { limit, after } = parseListKnowledgeScopeMembersInput(input);
    const args: unknown[] = [input.scopeNodeId];
    let keyset = '';
    if (after) {
      const updatedAt = after.updatedAt;
      keyset = ` AND (n."updatedAt" < ? OR (n."updatedAt" = ? AND (n.name > ? OR (n.name = ? AND n.id > ?))))`;
      args.push(updatedAt, updatedAt, after.name, after.name, after.id);
    }
    args.push(limit + 1);
    const result = await this.#readExecutor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_NODES)} FROM "${TABLE_KNOWLEDGE_NODES}" n WHERE EXISTS (SELECT 1 FROM "${TABLE_KNOWLEDGE_NODE_SCOPES}" ns WHERE ns."nodeId"=n.id AND ns."scopeNodeId"=?) AND n."deletedAt" IS NULL AND n."mergedInto" IS NULL${keyset} ORDER BY n."updatedAt" DESC, n.name ASC, n.id ASC LIMIT ?`,
      args,
    });
    return pageKnowledgeScopeMembers((result.rows as Array<Record<string, unknown>>).map(parseNode), limit, input);
  }

  async createNode(input: CreateKnowledgeNodeInput): Promise<KnowledgeNode> {
    await assertKnowledgeDescriptionWithinBoundCompat(input.description);
    const scope = canonicalizeKnowledgeScope(input.scope);
    return this.#transaction(async tx => {
      const existing = await this.#getNodeByName(tx, input.name, scope);
      if (existing) {
        const terminal = (await this.#resolveTerminalNode(tx, existing.id))!;
        if (!isKnowledgeScopeVisible(terminal.scope, scope)) {
          throw new Error(`Merged knowledge node is not visible from scope: ${input.name}`);
        }
        // Writing about a node that already exists still places it where the caller asked.
        await this.#placeNodeInScopes(tx, terminal.id, input.scopeAddresses, new Date());
        return terminal;
      }
      const now = new Date();
      const node: KnowledgeNode = {
        id: input.id ?? crypto.randomUUID(),
        type: 'node',
        name: input.name.trim(),
        kind: input.kind,
        content: input.content,
        description: input.description,
        scope,
        version: 1,
        createdAt: now,
        updatedAt: now,
      };
      await tx.execute({
        sql: `INSERT INTO "${TABLE_KNOWLEDGE_NODES}" (id,type,name,canonicalName,kind,content,description,isScope,scope,scopeKey,version,mergedInto,createdAt,updatedAt) VALUES (?,?,?,?,?,?,?,FALSE,jsonb(?),?,?,NULL,?,?)`,
        args: [
          node.id,
          'node',
          node.name,
          canonicalName(node.name),
          node.kind,
          textBind(node.content),
          textBind(node.description),
          JSON.stringify(scope),
          knowledgeScopeKey(scope),
          node.version,
          now,
          now,
        ],
      });
      await this.#replaceNodeScopes(tx, node.id, scope, now);
      await this.#placeNodeInScopes(tx, node.id, input.scopeAddresses, now);
      await this.#replaceMentions(tx, 'node', node.id, node.content ?? '', input.resolutionScope ?? scope, scope);
      await this.#activity(tx, 'node-created', 'node', node.id, scope);
      await this.#outbox(tx, 'node', node.id, 'upsert', node.version, scope);
      return node;
    });
  }

  async getNode(id: string): Promise<KnowledgeNode | null> {
    return this.#getNode(this.#readExecutor, id);
  }

  async getNodeByName(input: { name: string; scope: KnowledgeScope }): Promise<KnowledgeNode | null> {
    return this.#getNodeByName(this.#readExecutor, input.name, canonicalizeKnowledgeScope(input.scope));
  }

  async resolveNode(input: { name: string; scope: KnowledgeScope }): Promise<KnowledgeNode | null> {
    return this.#resolveNode(this.#readExecutor, input.name, canonicalizeKnowledgeScope(input.scope));
  }

  async listNodes(input: ListKnowledgeNodesInput): Promise<KnowledgeNode[]> {
    const scope = canonicalizeKnowledgeScope(input.scope);
    const keys = visibleScopeKeys(scope);
    const clauses = [`type = 'node'`, 'mergedInto IS NULL', visibleSql(keys)];
    const args: QueryValues = [...keys];
    if (input.namePrefix) {
      clauses.push("canonicalName LIKE ? ESCAPE '='");
      args.push(`${escapeLikePattern(canonicalName(input.namePrefix))}%`);
    }
    if (input.kind) {
      clauses.push('kind = ?');
      args.push(input.kind);
    }
    if (input.hasContent !== undefined)
      clauses.push(
        input.hasContent ? 'DBMS_LOB.GETLENGTH(content) > 0' : '(content IS NULL OR DBMS_LOB.GETLENGTH(content) = 0)',
      );
    if (input.cursor) {
      const cursor = parseKnowledgeNodeCursor(input.cursor, {
        namePrefix: input.namePrefix,
        kind: input.kind,
        hasContent: input.hasContent,
      });
      const updatedAt = cursor.updatedAt;
      clauses.push('(updatedAt < ? OR (updatedAt = ? AND (name > ? OR (name = ? AND id > ?))))');
      args.push(updatedAt, updatedAt, cursor.name, cursor.name, cursor.id);
    }
    args.push(input.limit ?? 100);
    const result = await this.#readExecutor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_NODES)} FROM "${TABLE_KNOWLEDGE_NODES}" WHERE ${clauses.join(' AND ')} ORDER BY updatedAt DESC, name ASC, id ASC LIMIT ?`,
      args,
    });
    return result.rows.map(parseNode);
  }

  async updateNode(input: UpdateKnowledgeNodeInput): Promise<KnowledgeNode> {
    await assertKnowledgeDescriptionWithinBoundCompat(input.description);
    return this.#transaction(async tx => {
      const existing = await this.#getNode(tx, input.id);
      if (!existing) throw new KnowledgeNotFoundError('node', input.id);
      if (existing.mergedInto) throw new Error(`Cannot update merged knowledge node: ${input.id}`);
      const scope = canonicalizeKnowledgeScope(input.scope ?? existing.scope);
      const name = (input.name ?? existing.name).trim();
      const content = input.content ?? existing.content;
      const description = input.description ?? existing.description;
      const now = new Date();
      const result = await tx.execute({
        sql: `UPDATE "${TABLE_KNOWLEDGE_NODES}" SET name=?,canonicalName=?,kind=?,content=?,description=?,scope=jsonb(?),scopeKey=?,version=version+1,updatedAt=? WHERE id=? AND type='node' AND version=?`,
        args: [
          name,
          canonicalName(name),
          input.kind ?? existing.kind,
          textBind(content),
          textBind(description),
          JSON.stringify(scope),
          knowledgeScopeKey(scope),
          now,
          input.id,
          input.version,
        ],
      });
      if (result.rowsAffected === 0) throw new KnowledgeConflictError(input.id);
      if (knowledgeScopeKey(existing.scope) !== knowledgeScopeKey(scope)) {
        await this.#swapIdentityScopes(tx, input.id, existing.scope, scope, now);
      }
      if (input.content !== undefined || input.name !== undefined || input.scope !== undefined) {
        await this.#replaceMentions(tx, 'node', input.id, content ?? '', input.resolutionScope ?? scope, scope);
      }
      await this.#activity(tx, 'node-updated', 'node', input.id, scope);
      if (knowledgeScopeKey(existing.scope) !== knowledgeScopeKey(scope)) {
        await this.#outbox(tx, 'node', input.id, 'delete', createKnowledgeUlid(), existing.scope);
        const records = await tx.execute({
          sql: `SELECT id,JSON_SERIALIZE(scope RETURNING CLOB) AS "scopeJson",deletedAt FROM "${TABLE_KNOWLEDGE_RECORDS}" WHERE node=?`,
          args: [input.id],
        });
        for (const row of records.rows) {
          const factScope = parseJson<KnowledgeScope>(row.scopeJson ?? row.scope);
          await this.#outbox(tx, 'record', String(row.id), 'delete', createKnowledgeUlid(), factScope);
          if (row.deletedAt == null) {
            await this.#outbox(tx, 'record', String(row.id), 'upsert', createKnowledgeUlid(), factScope);
          }
        }
      }
      await this.#outbox(tx, 'node', input.id, 'upsert', input.version + 1, scope);
      return {
        ...existing,
        name,
        kind: input.kind ?? existing.kind,
        content,
        description,
        scope,
        version: input.version + 1,
        updatedAt: now,
      };
    });
  }

  async mergeNodes(input: { sourceId: string; targetId: string; sourceVersion: number }): Promise<KnowledgeNode> {
    if (input.sourceId === input.targetId) throw new Error('Cannot merge a knowledge node into itself');
    return this.#transaction(async tx => {
      const source = await this.#getNode(tx, input.sourceId);
      if (!source) throw new KnowledgeNotFoundError('node', input.sourceId);
      const target = await this.#resolveTerminalNode(tx, input.targetId);
      if (!target) throw new KnowledgeNotFoundError('node', input.targetId);
      if (target.id === source.id) throw new Error('Cannot create a knowledge merge cycle');
      if (!isKnowledgeScopeVisible(target.scope, source.scope)) {
        throw new Error('Cannot merge a knowledge node into a target that is narrower than its source scope');
      }
      const affected = await tx.execute({
        sql: `SELECT m.sourceType,m.sourceId,JSON_SERIALIZE(COALESCE(f.scope,r.scope) RETURNING CLOB) AS "scopeJson",CASE WHEN f.deletedAt IS NULL THEN 0 ELSE 1 END AS deleted FROM "${TABLE_KNOWLEDGE_MENTIONS}" m LEFT JOIN "${TABLE_KNOWLEDGE_RECORDS}" f ON m.sourceType='record' AND f.id=m.sourceId LEFT JOIN "${TABLE_KNOWLEDGE_NODES}" r ON m.sourceType='node' AND r.id=m.sourceId WHERE m.recordId=?`,
        args: [source.id],
      });
      const movedFacts = await tx.execute({
        sql: `SELECT id,JSON_SERIALIZE(scope RETURNING CLOB) AS "scopeJson",deletedAt FROM "${TABLE_KNOWLEDGE_RECORDS}" WHERE node=?`,
        args: [source.id],
      });
      const updated = await tx.execute({
        sql: `UPDATE "${TABLE_KNOWLEDGE_NODES}" SET mergedInto=?,version=version+1,updatedAt=? WHERE id=? AND type='node' AND version=? AND mergedInto IS NULL`,
        args: [target.id, new Date(), source.id, input.sourceVersion],
      });
      if (updated.rowsAffected === 0) throw new KnowledgeConflictError(source.id);
      await tx.execute({
        sql: `UPDATE "${TABLE_KNOWLEDGE_RECORDS}" SET node=?,version=version+1,updatedAt=? WHERE node=?`,
        args: [target.id, new Date(), source.id],
      });
      await tx.execute({
        sql: `DELETE FROM "${TABLE_KNOWLEDGE_MENTIONS}" WHERE recordId=? AND EXISTS (SELECT 1 FROM "${TABLE_KNOWLEDGE_MENTIONS}" target WHERE target.sourceType="${TABLE_KNOWLEDGE_MENTIONS}".sourceType AND target.sourceId="${TABLE_KNOWLEDGE_MENTIONS}".sourceId AND target.recordId=?)`,
        args: [source.id, target.id],
      });
      await tx.execute({
        sql: `UPDATE "${TABLE_KNOWLEDGE_MENTIONS}" SET recordId=? WHERE recordId=?`,
        args: [target.id, source.id],
      });
      for (const row of movedFacts.rows)
        await this.#outbox(
          tx,
          'record',
          String(row.id),
          row.deletedAt == null ? 'upsert' : 'delete',
          createKnowledgeUlid(),
          parseJson(row.scopeJson ?? row.scope),
        );
      for (const row of affected.rows)
        await this.#outbox(
          tx,
          String(row.sourceType) as 'record' | 'node',
          String(row.sourceId),
          Number(row.deleted) ? 'delete' : 'upsert',
          createKnowledgeUlid(),
          parseJson<KnowledgeScope>(row.scopeJson ?? row.scope),
        );
      // Merge matrix: a target that NEVER had a description (undefined — '' is an explicit curator
      // clear and wins) adopts the source's; otherwise the target's state is preserved.
      let mergedTarget = target;
      if (target.description === undefined && source.description) {
        const adoptedAt = new Date();
        // Adoption is conditional on the target state this merge observed: a concurrent write (a new
        // description, or an intentional '' clear) bumps the version and loses the predicate, so the
        // merge leaves that newer value alone instead of clobbering it with the source's synopsis.
        const adopted = await tx.execute({
          sql: `UPDATE "${TABLE_KNOWLEDGE_NODES}" SET description=?,version=version+1,updatedAt=? WHERE id=? AND type='node' AND version=? AND description IS NULL`,
          args: [textBind(source.description), adoptedAt, target.id, target.version],
        });
        if (adopted.rowsAffected > 0) {
          mergedTarget = {
            ...target,
            description: source.description,
            version: target.version + 1,
            updatedAt: adoptedAt,
          };
          await this.#activity(tx, 'node-updated', 'node', target.id, target.scope);
        }
      }
      await this.#activity(tx, 'node-merged', 'node', source.id, source.scope);
      await this.#outbox(tx, 'node', source.id, 'delete', input.sourceVersion + 1, source.scope);
      await this.#outbox(tx, 'node', target.id, 'upsert', createKnowledgeUlid(), mergedTarget.scope);
      return mergedTarget;
    });
  }

  async appendKnowledge(input: AppendKnowledgeInput): Promise<KnowledgeRecord> {
    await assertKnowledgeRecordTextWithinBoundCompat(input.text);
    const scope = canonicalizeKnowledgeScope(input.scope);
    const resolutionScope = canonicalizeKnowledgeScope(input.resolutionScope);
    const defaultScope = canonicalizeKnowledgeScope(input.defaultScope);
    return this.#transaction(async tx => {
      const parent = await this.#resolveTerminalNode(tx, nodeReferenceId(input.node));
      if (!parent) throw new KnowledgeNotFoundError('node', nodeReferenceId(input.node));
      const metadataJson = input.metadata ? JSON.stringify(input.metadata) : null;
      const record: KnowledgeRecord = {
        id: input.id ?? createKnowledgeUlid(),
        node: parent.id,
        text: input.text,
        scope,
        sourceThreadId: input.sourceThreadId,
        capturedAt: new Date(),
        when: input.when ? new Date(input.when) : undefined,
        metadata: input.metadata,
      };
      await tx.execute({
        sql: `INSERT INTO "${TABLE_KNOWLEDGE_RECORDS}" (id,node,text,scope,scopeKey,sourceThreadId,capturedAt,"when",metadata,version,updatedAt,deletedAt,deletedBy) VALUES (?,?,?,jsonb(?),?,?,?,?,jsonb(?),?,?,NULL,NULL)`,
        args: [
          record.id,
          record.node,
          record.text,
          JSON.stringify(scope),
          knowledgeScopeKey(scope),
          record.sourceThreadId,
          record.capturedAt,
          record.when ?? null,
          metadataJson,
          1,
          record.capturedAt,
        ],
      });
      await this.#replaceRecordScopes(tx, record.id, scope, record.capturedAt);
      await this.#replaceMentions(tx, 'record', record.id, record.text, resolutionScope, defaultScope);
      await this.#activity(tx, 'record-created', 'record', record.id, scope, record.sourceThreadId);
      await this.#outbox(tx, 'record', record.id, 'upsert', record.id, scope);
      return metadataJson ? { ...record, metadata: JSON.parse(metadataJson) } : record;
    });
  }

  async getKnowledge(input: { id: string; includeDeleted?: boolean }): Promise<KnowledgeRecord | null> {
    const result = await this.#readExecutor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_RECORDS)} FROM "${TABLE_KNOWLEDGE_RECORDS}" WHERE id=?${input.includeDeleted ? '' : ' AND deletedAt IS NULL'}`,
      args: [input.id],
    });
    return result.rows[0] ? parseKnowledge(result.rows[0]) : null;
  }

  async listKnowledgeAbout(input: QueryKnowledgeInput): Promise<QueryKnowledgeOutput> {
    return this.#queryKnowledge(input, 'about');
  }
  async listKnowledgeMentioning(input: QueryKnowledgeInput): Promise<QueryKnowledgeOutput> {
    return this.#queryKnowledge(input, 'mentioning');
  }

  async listKnowledgeRelatedTo(input: QueryKnowledgeInput): Promise<QueryKnowledgeOutput> {
    return this.#queryKnowledge(input, 'related');
  }

  async knowledgeBySource(input: QueryKnowledgeBySourceInput): Promise<QueryKnowledgeOutput> {
    const scope = canonicalizeKnowledgeScope(input.scope);
    const keys = visibleScopeKeys(scope);
    const args: QueryValues = [input.sourceThreadId, ...keys];
    if (input.after) args.push(input.after);
    const limit = input.limit ?? 100;
    args.push(limit + 1);
    const result = await this.#readExecutor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_RECORDS)} FROM "${TABLE_KNOWLEDGE_RECORDS}" WHERE sourceThreadId=? AND ${visibleSql(keys)}${input.includeDeleted ? '' : ' AND deletedAt IS NULL'}${input.after ? ' AND id > ?' : ''} ORDER BY id ASC LIMIT ?`,
      args,
    });
    const records = result.rows.map(parseKnowledge);
    return {
      records: records.slice(0, limit),
      nextCursor: records.length > limit ? records[limit - 1]?.id : undefined,
    };
  }

  async removeKnowledge(input: { id: string; deletedBy: string }): Promise<KnowledgeRecord> {
    return this.#transaction(async tx => {
      const record = await this.#getKnowledge(tx, input.id, true);
      if (!record) throw new KnowledgeNotFoundError('record', input.id);
      if (record.deletedAt) return record;
      const deletedAt = new Date();
      await tx.execute({
        sql: `UPDATE "${TABLE_KNOWLEDGE_RECORDS}" SET deletedAt=?,deletedBy=?,version=version+1,updatedAt=? WHERE id=? AND deletedAt IS NULL`,
        args: [deletedAt, input.deletedBy, deletedAt, input.id],
      });
      await this.#activity(tx, 'record-deleted', 'record', input.id, record.scope, record.sourceThreadId);
      await this.#outbox(tx, 'record', input.id, 'delete', deletedAt.toISOString(), record.scope);
      return { ...record, deletedAt, deletedBy: input.deletedBy };
    });
  }

  async restoreKnowledge(input: { id: string }): Promise<KnowledgeRecord> {
    return this.#transaction(async tx => {
      const record = await this.#getKnowledge(tx, input.id, true);
      if (!record) throw new KnowledgeNotFoundError('record', input.id);
      if (!record.deletedAt) return record;
      await tx.execute({
        sql: `UPDATE "${TABLE_KNOWLEDGE_RECORDS}" SET deletedAt=NULL,deletedBy=NULL,version=version+1,updatedAt=? WHERE id=?`,
        args: [new Date(), input.id],
      });
      await this.#activity(tx, 'record-restored', 'record', input.id, record.scope, record.sourceThreadId);
      await this.#outbox(tx, 'record', input.id, 'upsert', createKnowledgeUlid(), record.scope);
      return { ...record, deletedAt: undefined, deletedBy: undefined };
    });
  }

  async rescopeKnowledge(input: { id: string; scope: KnowledgeScope }): Promise<KnowledgeRecord> {
    const scope = canonicalizeKnowledgeScope(input.scope);
    return this.#transaction(async tx => {
      const record = await this.#getKnowledge(tx, input.id, true);
      if (!record) throw new KnowledgeNotFoundError('record', input.id);
      const updatedAt = new Date();
      await tx.execute({
        sql: `UPDATE "${TABLE_KNOWLEDGE_RECORDS}" SET scope=jsonb(?),scopeKey=?,version=version+1,updatedAt=? WHERE id=?`,
        args: [JSON.stringify(scope), knowledgeScopeKey(scope), updatedAt, input.id],
      });
      await this.#replaceRecordScopes(tx, input.id, scope, updatedAt);
      await this.#activity(tx, 'record-rescoped', 'record', input.id, scope, record.sourceThreadId);
      if (knowledgeScopeKey(record.scope) !== knowledgeScopeKey(scope))
        await this.#outbox(tx, 'record', input.id, 'delete', createKnowledgeUlid(), record.scope);
      if (!record.deletedAt) await this.#outbox(tx, 'record', input.id, 'upsert', createKnowledgeUlid(), scope);
      return { ...record, scope };
    });
  }

  async search(input: SearchKnowledgeInput): Promise<SearchKnowledgeResult[]> {
    const scope = canonicalizeKnowledgeScope(input.scope);
    const keys = visibleScopeKeys(scope);
    const normalizedQuery = input.query.trim().toLocaleLowerCase();
    if (!normalizedQuery) return [];
    const query = `%${escapeLikePattern(normalizedQuery)}%`;
    const records = await this.#readExecutor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_NODES)} FROM "${TABLE_KNOWLEDGE_NODES}" WHERE mergedInto IS NULL AND ${visibleSql(keys)} AND (canonicalName LIKE ? ESCAPE '=' OR lower(COALESCE(kind,'')) LIKE ? ESCAPE '=' OR lower(content) LIKE ? ESCAPE '=' OR lower(description) LIKE ? ESCAPE '=') ORDER BY updatedAt DESC LIMIT ?`,
      args: [...keys, query, query, query, query, input.limit ?? 20],
    });
    const results: SearchKnowledgeResult[] = records.rows.map(row => ({
      type: String(row.type) as 'node',
      id: String(row.id),
      recordId: String(row.id),
      name: String(row.name),
      // Description joins the snippet only when present so description-less results stay byte-identical.
      text: [
        String(row.name),
        ...(row.description ? [String(row.description)] : []),
        ...(row.content ? [String(row.content)] : []),
      ].join('\n'),
      scope: parseJson<KnowledgeScope>(row.scopeJson ?? row.scope),
    }));
    if (results.length < (input.limit ?? 20)) {
      const records = await this.#readExecutor.execute({
        sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_RECORDS, 'f')},r.name,JSON_SERIALIZE(r.scope RETURNING CLOB) AS "parentScopeJson" FROM "${TABLE_KNOWLEDGE_RECORDS}" f JOIN "${TABLE_KNOWLEDGE_NODES}" r ON r.id=f.node AND r.type='node' AND r.mergedInto IS NULL WHERE f.deletedAt IS NULL AND ${visibleSql(keys, 'f.scopeKey')} AND lower(f.text) LIKE ? ESCAPE '=' ORDER BY f.id DESC LIMIT ?`,
        args: [...keys, query, (input.limit ?? 20) - results.length],
      });
      results.push(
        ...records.rows.map(row => {
          const parentVisible = isKnowledgeScopeVisible(parseJson<KnowledgeScope>(row.parentScopeJson), scope);
          return {
            type: 'record' as const,
            id: String(row.id),
            recordId: parentVisible ? String(row.node) : String(row.id),
            name: parentVisible ? String(row.name) : '(private node)',
            text: String(row.text),
            scope: parseJson<KnowledgeScope>(row.scopeJson ?? row.scope),
          };
        }),
      );
    }
    return results;
  }

  /**
   * @deprecated Curation cursors were removed. Observation-time curate is the only Knowledge writer and needs no
   * cursor. Always throws.
   */
  async getCurationCursor(_input: { sourceThreadId: string; agent: string }): Promise<KnowledgeCurationCursor | null> {
    throw new Error(KNOWLEDGE_CURATION_CURSOR_REMOVED_MESSAGE);
  }

  /**
   * @deprecated Curation cursors were removed. Observation-time curate is the only Knowledge writer and needs no
   * cursor. Always throws.
   */
  async advanceCurationCursor(_input: {
    sourceThreadId: string;
    agent: string;
    lastKnowledgeId: string;
  }): Promise<KnowledgeCurationCursor> {
    throw new Error(KNOWLEDGE_CURATION_CURSOR_REMOVED_MESSAGE);
  }

  async listActivity(input: {
    scope: KnowledgeScope;
    after?: string;
    limit?: number;
  }): Promise<KnowledgeActivityEvent[]> {
    const scope = canonicalizeKnowledgeScope(input.scope);
    const keys = visibleScopeKeys(scope);
    const result = await this.#readExecutor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_ACTIVITY)} FROM "${TABLE_KNOWLEDGE_ACTIVITY}" WHERE ${visibleSql(keys)}${input.after ? ' AND id < ?' : ''} ORDER BY id DESC LIMIT ?`,
      args: [...keys, ...(input.after ? [input.after] : []), input.limit ?? 100],
    });
    return result.rows.map(row => ({
      id: String(row.id),
      action: String(row.action) as KnowledgeActivityAction,
      recordType: String(row.recordType) as KnowledgeSemanticDocumentType,
      recordId: String(row.recordId),
      scope: parseJson<KnowledgeScope>(row.scopeJson ?? row.scope),
      sourceThreadId: row.sourceThreadId == null ? undefined : String(row.sourceThreadId),
      createdAt: toDate(row.createdAt),
    }));
  }

  async listSemanticOutbox(
    input: { status?: KnowledgeSemanticOutboxEntry['status']; scope?: KnowledgeScope; limit?: number } = {},
  ): Promise<KnowledgeSemanticOutboxEntry[]> {
    const clauses: string[] = [];
    const args: QueryValues = [];
    if (input.status) {
      clauses.push('status=?');
      args.push(input.status);
    }
    if (input.scope) {
      const keys = visibleScopeKeys(input.scope);
      clauses.push(visibleSql(keys));
      args.push(...keys);
    }
    args.push(input.limit ?? 100);
    const result = await this.#executor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_SEMANTIC_OUTBOX)} FROM "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}"${clauses.length ? ` WHERE ${clauses.join(' AND ')}` : ''} ORDER BY createdAt ASC,id ASC LIMIT ?`,
      args,
    });
    return result.rows.map(parseOutbox);
  }

  async claimSemanticOutbox(input: ClaimKnowledgeSemanticOutboxInput): Promise<KnowledgeSemanticOutboxEntry[]> {
    const now = input.now ?? new Date();
    const stale = new Date(now.getTime() - (input.claimTimeoutMs ?? 60_000));
    return this.#transaction(async tx => {
      const clauses = [
        `availableAt <= ?`,
        `(status='pending' OR (status='processing' AND claimedAt <= ?))`,
        `NOT EXISTS (SELECT 1 FROM "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}" earlier WHERE earlier.documentId = "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}".documentId AND earlier.status != 'completed' AND (earlier.createdAt < "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}".createdAt OR (earlier.createdAt = "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}".createdAt AND earlier.id < "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}".id)))`,
      ];
      const args: QueryValues = [now, stale];
      if (input.scope) {
        const keys = visibleScopeKeys(input.scope);
        clauses.push(visibleSql(keys));
        args.push(...keys);
      }
      args.push(input.limit ?? 100);
      const selected = await tx.execute({
        sql: `SELECT id FROM "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}" WHERE ${clauses.join(' AND ')} ORDER BY createdAt ASC,id ASC LIMIT ?`,
        args,
      });
      const ids = selected.rows.map(row => String(row.id));
      for (const id of ids)
        await tx.execute({
          sql: `UPDATE "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}" SET status='processing',attempts=attempts+1,claimedAt=?,claimedBy=? WHERE id=?`,
          args: [now, input.workerId, id],
        });
      if (!ids.length) return [];
      const result = await tx.execute({
        sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_SEMANTIC_OUTBOX)} FROM "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}" WHERE id IN (${ids.map(() => '?').join(',')}) ORDER BY createdAt ASC,id ASC`,
        args: ids,
      });
      return result.rows.map(parseOutbox);
    });
  }

  async completeSemanticOutbox(input: { ids: string[]; workerId: string }): Promise<void> {
    if (!input.ids.length) return;
    const now = new Date();
    await this.#transaction(async tx => {
      for (const id of input.ids)
        await tx.execute({
          sql: `UPDATE "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}" SET status='completed',completedAt=? WHERE id=? AND status='processing' AND claimedBy=?`,
          args: [now, id, input.workerId],
        });
    });
  }
  async releaseSemanticOutbox(input: { ids: string[]; workerId: string; retryAt?: Date }): Promise<void> {
    if (!input.ids.length) return;
    await this.#transaction(async tx => {
      for (const id of input.ids)
        await tx.execute({
          sql: `UPDATE "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}" SET status='pending',availableAt=?,claimedAt=NULL,claimedBy=NULL WHERE id=? AND status='processing' AND claimedBy=?`,
          args: [input.retryAt ?? new Date(), id, input.workerId],
        });
    });
  }

  async #transaction<T>(operation: (tx: Executor) => Promise<T>): Promise<T> {
    return this.#db.tx(async (_client, connection) => {
      const executor = createExecutor(connection, this.#schemaName);
      // A short schema-local write lock orders structural changes, identity creation, and outbox claims.
      // Reads remain concurrent. This also works with poolMax=1 and needs no DBMS_LOCK privileges.
      await executor.execute(`SELECT id FROM "${TABLE_KNOWLEDGE_ACCESS_STATE}" WHERE id='global' FOR UPDATE`);
      return operation(executor);
    });
  }
  async #getNode(executor: Executor, id: string): Promise<KnowledgeNode | null> {
    const result = await executor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_NODES)} FROM "${TABLE_KNOWLEDGE_NODES}" WHERE id=? AND type='node'`,
      args: [id],
    });
    return result.rows[0] ? parseNode(result.rows[0]) : null;
  }
  async #getNodeByName(executor: Executor, name: string, scope: KnowledgeScope): Promise<KnowledgeNode | null> {
    const result = await executor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_NODES)} FROM "${TABLE_KNOWLEDGE_NODES}" WHERE type='node' AND scopeKey=? AND canonicalName=?`,
      args: [knowledgeScopeKey(scope), canonicalName(name)],
    });
    return result.rows[0] ? parseNode(result.rows[0]) : null;
  }
  async #resolveNode(executor: Executor, name: string, scope: KnowledgeScope): Promise<KnowledgeNode | null> {
    // An unmerged node outside the caller's scope resolves to itself and can never be visible, so only
    // visible-scope rows and merged aliases (whose terminal may be visible) are candidates.
    const keys = visibleScopeKeys(scope);
    const result = await executor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_NODES)} FROM "${TABLE_KNOWLEDGE_NODES}" WHERE type='node' AND canonicalName=? AND ("scopeKey" IN (${keys.map(() => '?').join(',')}) OR "mergedInto" IS NOT NULL)`,
      args: [canonicalName(name), ...keys],
    });
    const candidates = result.rows.map(parseNode).sort((left, right) => right.scope.length - left.scope.length);
    for (const candidate of candidates) {
      const terminal = await this.#resolveTerminalNode(executor, candidate.id);
      if (terminal && isKnowledgeScopeVisible(terminal.scope, scope)) return terminal;
    }
    return null;
  }
  async #resolveTerminalNode(executor: Executor, id: string): Promise<KnowledgeNode | null> {
    let node = await this.#getNode(executor, id);
    const seen = new Set<string>();
    while (node?.mergedInto) {
      if (seen.has(node.id)) throw new Error(`Knowledge merge cycle detected at ${node.id}`);
      seen.add(node.id);
      node = await this.#getNode(executor, node.mergedInto);
    }
    return node;
  }
  async #getKnowledge(executor: Executor, id: string, includeDeleted: boolean): Promise<KnowledgeRecord | null> {
    const result = await executor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_RECORDS)} FROM "${TABLE_KNOWLEDGE_RECORDS}" WHERE id=?${includeDeleted ? '' : ' AND deletedAt IS NULL'}`,
      args: [id],
    });
    return result.rows[0] ? parseKnowledge(result.rows[0]) : null;
  }

  async #queryKnowledge(
    input: QueryKnowledgeInput,
    relationship: 'about' | 'mentioning' | 'related',
  ): Promise<QueryKnowledgeOutput> {
    const scope = canonicalizeKnowledgeScope(input.scope);
    const node = await this.#resolveTerminalNode(this.#readExecutor, nodeReferenceId(input.node));
    if (!node) return { records: [] };
    const keys = visibleScopeKeys(scope);
    const args: QueryValues = [node.id, ...(relationship === 'related' ? [node.id] : []), ...keys];
    if (input.after) args.push(input.after);
    args.push((input.limit ?? 100) + 1);
    const result = await this.#readExecutor.execute({
      sql: `SELECT ${selectKnowledgeColumns(TABLE_KNOWLEDGE_RECORDS, 'f')} FROM "${TABLE_KNOWLEDGE_RECORDS}" f WHERE ${relationship === 'about' ? 'f.node=?' : relationship === 'mentioning' ? `EXISTS (SELECT 1 FROM "${TABLE_KNOWLEDGE_MENTIONS}" m WHERE m.sourceType='record' AND m.sourceId=f.id AND m.recordId=?)` : `(f.node=? OR EXISTS (SELECT 1 FROM "${TABLE_KNOWLEDGE_MENTIONS}" m WHERE m.sourceType='record' AND m.sourceId=f.id AND m.recordId=?))`} AND ${visibleSql(keys, 'f.scopeKey')}${input.includeDeleted ? '' : ' AND f.deletedAt IS NULL'}${input.after ? ' AND f.id < ?' : ''} ORDER BY f.id DESC LIMIT ?`,
      args,
    });
    const records = result.rows.map(parseKnowledge);
    const limit = input.limit ?? 100;
    return {
      records: records.slice(0, limit),
      nextCursor: records.length > limit ? records[limit - 1]?.id : undefined,
    };
  }

  async #resolveScopeNodeIds(executor: Executor, addresses: KnowledgeScope): Promise<string[]> {
    const scopeNodeIds = new Set<string>();
    for (const address of addresses) {
      const result = await executor.execute({
        sql: `SELECT scopeNodeId FROM "${TABLE_KNOWLEDGE_SCOPE_ADDRESSES}" WHERE address=?`,
        args: [address],
      });
      if (result.rows[0]?.scopeNodeId != null) scopeNodeIds.add(String(result.rows[0].scopeNodeId));
    }
    return [...scopeNodeIds];
  }

  async #replaceNodeScopes(
    executor: Executor,
    nodeId: string,
    addresses: KnowledgeScope,
    addedAt: Date,
  ): Promise<void> {
    await executor.execute({ sql: `DELETE FROM "${TABLE_KNOWLEDGE_NODE_SCOPES}" WHERE nodeId=?`, args: [nodeId] });
    for (const scopeNodeId of await this.#resolveScopeNodeIds(executor, addresses)) {
      await executor.execute({
        sql: `INSERT INTO "${TABLE_KNOWLEDGE_NODE_SCOPES}" (nodeId,scopeNodeId,addedAt) VALUES (?,?,?) ON CONFLICT DO NOTHING`,
        args: [nodeId, scopeNodeId, addedAt],
      });
    }
  }

  /**
   * Move a node's identity memberships from `from` to `to` without touching structural
   * placements added by `#placeNodeInScopes`, which a scope change must not erase.
   */
  async #swapIdentityScopes(
    executor: Executor,
    nodeId: string,
    from: KnowledgeScope,
    to: KnowledgeScope,
    addedAt: Date,
  ): Promise<void> {
    const next = await this.#resolveScopeNodeIds(executor, to);
    for (const scopeNodeId of await this.#resolveScopeNodeIds(executor, from)) {
      if (next.includes(scopeNodeId)) continue;
      await executor.execute({
        sql: `DELETE FROM "${TABLE_KNOWLEDGE_NODE_SCOPES}" WHERE nodeId=? AND scopeNodeId=?`,
        args: [nodeId, scopeNodeId],
      });
    }
    for (const scopeNodeId of next) {
      await executor.execute({
        sql: `INSERT INTO "${TABLE_KNOWLEDGE_NODE_SCOPES}" (nodeId,scopeNodeId,addedAt) VALUES (?,?,?) ON CONFLICT DO NOTHING`,
        args: [nodeId, scopeNodeId, addedAt],
      });
    }
  }

  /**
   * Additive structural placement: every address must resolve to a live scope node
   * (unlike identity membership, an unknown address is a caller error, not a lazy
   * materialization miss). Runs inside the caller's transaction, so a throw aborts
   * the whole mutation.
   */
  async #placeNodeInScopes(
    executor: Executor,
    nodeId: string,
    addresses: string[] | undefined,
    addedAt: Date,
  ): Promise<void> {
    for (const address of addresses ?? []) {
      const result = await executor.execute({
        sql: `SELECT sa.scopeNodeId AS "scopeNodeId" FROM "${TABLE_KNOWLEDGE_SCOPE_ADDRESSES}" sa JOIN "${TABLE_KNOWLEDGE_NODES}" n ON n.id=sa."scopeNodeId" WHERE sa.address=? AND n."isScope" = 1 AND n."deletedAt" IS NULL`,
        args: [address],
      });
      const scopeNodeId = result.rows[0]?.scopeNodeId;
      if (scopeNodeId == null) throw new KnowledgeNotFoundError('scope', address);
      await executor.execute({
        sql: `INSERT INTO "${TABLE_KNOWLEDGE_NODE_SCOPES}" ("nodeId","scopeNodeId","addedAt") VALUES (?,?,?) ON CONFLICT DO NOTHING`,
        args: [nodeId, String(scopeNodeId), addedAt],
      });
    }
  }

  async #replaceRecordScopes(
    executor: Executor,
    recordId: string,
    addresses: KnowledgeScope,
    addedAt: Date,
  ): Promise<void> {
    await executor.execute({
      sql: `DELETE FROM "${TABLE_KNOWLEDGE_RECORD_SCOPES}" WHERE recordId=?`,
      args: [recordId],
    });
    for (const scopeNodeId of await this.#resolveScopeNodeIds(executor, addresses)) {
      await executor.execute({
        sql: `INSERT INTO "${TABLE_KNOWLEDGE_RECORD_SCOPES}" (recordId,scopeNodeId,addedAt) VALUES (?,?,?) ON CONFLICT DO NOTHING`,
        args: [recordId, scopeNodeId, addedAt],
      });
    }
  }

  async #replaceMentions(
    tx: Executor,
    sourceType: 'record' | 'node',
    sourceId: string,
    text: string,
    resolutionScope: KnowledgeScope,
    defaultScope: KnowledgeScope,
  ): Promise<void> {
    await tx.execute({
      sql: `DELETE FROM "${TABLE_KNOWLEDGE_MENTIONS}" WHERE sourceType=? AND sourceId=?`,
      args: [sourceType, sourceId],
    });
    for (const name of parseKnowledgeWikilinks(text)) {
      let node = await this.#resolveNode(tx, name, resolutionScope);
      if (!node) {
        node = await this.#getNodeByName(tx, name, defaultScope);
        if (node) node = await this.#resolveTerminalNode(tx, node.id);
        if (!node) {
          const now = new Date();
          node = {
            id: crypto.randomUUID(),
            type: 'node',
            name,
            kind: 'node',
            scope: defaultScope,
            version: 1,
            createdAt: now,
            updatedAt: now,
          };
          await tx.execute({
            sql: `INSERT INTO "${TABLE_KNOWLEDGE_NODES}" (id,type,name,canonicalName,kind,content,isScope,scope,scopeKey,version,mergedInto,createdAt,updatedAt) VALUES (?,?,?,?,?,NULL,FALSE,jsonb(?),?,?,NULL,?,?)`,
            args: [
              node.id,
              'node',
              node.name,
              canonicalName(node.name),
              node.kind,
              JSON.stringify(defaultScope),
              knowledgeScopeKey(defaultScope),
              1,
              now,
              now,
            ],
          });
          await this.#replaceNodeScopes(tx, node.id, defaultScope, now);
          await this.#activity(tx, 'node-created', 'node', node.id, defaultScope);
          await this.#outbox(tx, 'node', node.id, 'upsert', 1, defaultScope);
        }
      }
      await tx.execute({
        sql: `INSERT INTO "${TABLE_KNOWLEDGE_MENTIONS}" (sourceType,sourceId,recordId) VALUES (?,?,?) ON CONFLICT DO NOTHING`,
        args: [sourceType, sourceId, node.id],
      });
    }
  }

  async #activity(
    executor: Executor,
    action: KnowledgeActivityAction,
    recordType: KnowledgeSemanticDocumentType,
    recordId: string,
    scope: KnowledgeScope,
    sourceThreadId?: string,
  ): Promise<void> {
    const now = new Date();
    const [contextScopeId] = await this.#resolveScopeNodeIds(executor, scope);
    await executor.execute({
      sql: `INSERT INTO "${TABLE_KNOWLEDGE_ACTIVITY}" (id,action,targetType,targetId,contextScopeId,recordType,recordId,scope,scopeKey,sourceThreadId,createdAt) VALUES (?,?,?,?,?,?,?,jsonb(?),?,?,?)`,
      args: [
        createKnowledgeUlid(),
        action,
        recordType,
        recordId,
        contextScopeId ?? null,
        recordType,
        recordId,
        JSON.stringify(scope),
        knowledgeScopeKey(scope),
        sourceThreadId ?? null,
        now,
      ],
    });
  }
  async #outbox(
    executor: Executor,
    documentType: KnowledgeSemanticDocumentType,
    id: string,
    operation: KnowledgeSemanticOperation,
    version: number | string,
    scope: KnowledgeScope,
  ): Promise<void> {
    const documentId = knowledgeSemanticDocumentId(documentType, id);
    const idempotencyKey = knowledgeSemanticIdempotencyKey(documentId, operation, version);
    const now = new Date();
    await executor.execute({
      sql: `INSERT INTO "${TABLE_KNOWLEDGE_SEMANTIC_OUTBOX}" (id,idempotencyKey,documentId,documentType,operation,scope,scopeKey,status,attempts,availableAt,claimedAt,claimedBy,createdAt,completedAt) VALUES (?,?,?,?,?,jsonb(?),?,'pending',0,?,NULL,NULL,?,NULL) ON CONFLICT DO NOTHING`,
      args: [
        createKnowledgeUlid(),
        idempotencyKey,
        documentId,
        documentType,
        operation,
        JSON.stringify(scope),
        knowledgeScopeKey(scope),
        now,
        now,
      ],
    });
  }
}
