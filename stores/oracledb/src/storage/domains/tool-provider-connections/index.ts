import { ToolProviderConnectionsStorage, TABLE_TOOL_PROVIDER_CONNECTIONS, TABLE_SCHEMAS } from '@mastra/core/storage';
import type {
  StorageDeleteToolProviderConnectionInput,
  StorageListToolProviderConnectionsInput,
  StorageToolProviderConnection,
  StorageToolProviderConnectionKey,
  StorageUpsertToolProviderConnectionInput,
} from '@mastra/core/storage';
import oracledb from 'oracledb';

import { clobBind, isOracleErrorCode } from '../../../shared/connection';
import { indexNameForTable } from '../../../vector/identifiers';
import { OracleDB, filterIndexesForTables } from '../../db';
import type { OracleCreateIndexOptions } from '../../db';
import { createOracleStorageError, toDate } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

const KEY_WHERE = '"authorId" = :authorId AND "providerId" = :providerId AND "connectionId" = :connectionId';

export class ToolProviderConnectionsOracle extends ToolProviderConnectionsStorage {
  static readonly MANAGED_TABLES = [TABLE_TOOL_PROVIDER_CONNECTIONS] as const;
  private readonly db: OracleDB;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB(config);
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, ToolProviderConnectionsOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_TOOL_PROVIDER_CONNECTIONS,
      schema: TABLE_SCHEMAS[TABLE_TOOL_PROVIDER_CONNECTIONS],
      compositePrimaryKey: ['authorId', 'providerId', 'connectionId'],
    });
    if (!this.skipDefaultIndexes) {
      for (const index of getDefaultToolProviderConnectionIndexDefinitions()) {
        try {
          await this.db.createIndex(index);
        } catch (error) {
          this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
        }
      }
    }
    for (const index of this.indexes) await this.db.createIndex(index);
  }

  async getConnectionById(key: StorageToolProviderConnectionKey): Promise<StorageToolProviderConnection | null> {
    try {
      const row = await this.db.read.oneOrNone(`${this.select()} WHERE ${KEY_WHERE}`, key);
      return row ? rowToConnection(row) : null;
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'TOOL_PROVIDER_CONNECTION_GET',
        reason: 'FAILED',
        details: key,
        cause,
      });
    }
  }

  async upsertConnection(input: StorageUpsertToolProviderConnectionInput): Promise<StorageToolProviderConnection> {
    const { authorId, providerId, connectionId, toolkit } = input;
    const key = { authorId, providerId, connectionId };
    const binds = {
      ...key,
      toolkit,
      connectionLabel: input.label == null ? null : clobBind(input.label),
      emptyLabel: input.label === '' ? 1 : 0,
      connectionScope: input.scope ?? null,
      now: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: new Date() },
    };
    const labelValue = 'CASE WHEN :emptyLabel = 1 THEN EMPTY_CLOB() ELSE :connectionLabel END';
    try {
      return await this.db.tx(async client => {
        try {
          await client.none(
            `INSERT INTO ${this.db.table(TABLE_TOOL_PROVIDER_CONNECTIONS)}
             ("authorId", "providerId", "connectionId", toolkit, label, scope, "createdAt", "updatedAt")
             VALUES (:authorId, :providerId, :connectionId, :toolkit, ${labelValue}, NVL(:connectionScope, 'per-author'), :now, :now)`,
            binds,
          );
        } catch (error) {
          if (!isOracleErrorCode(error, [-1])) throw error;
          await client.none(
            `UPDATE ${this.db.table(TABLE_TOOL_PROVIDER_CONNECTIONS)} SET toolkit = :toolkit, label = ${labelValue},
             scope = NVL(:connectionScope, scope), "updatedAt" = :now WHERE ${KEY_WHERE}`,
            binds,
          );
        }
        return rowToConnection(await client.one(`${this.select()} WHERE ${KEY_WHERE}`, key));
      });
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'TOOL_PROVIDER_CONNECTION_UPSERT',
        reason: 'FAILED',
        details: key,
        cause,
      });
    }
  }

  async listConnectionsByAuthor(
    input: StorageListToolProviderConnectionsInput,
  ): Promise<StorageToolProviderConnection[]> {
    const conditions: string[] = [];
    const binds: Record<string, unknown> = {};
    for (const [field, column] of [
      ['authorId', '"authorId"'],
      ['providerId', '"providerId"'],
      ['toolkit', 'toolkit'],
      ['scope', 'scope'],
    ] as const) {
      if (input[field] === undefined) continue;
      conditions.push(`${column} = :${field}`);
      binds[field] = input[field];
    }
    try {
      const rows = await this.db.read.manyOrNone(
        `${this.select()}${conditions.length ? ` WHERE ${conditions.join(' AND ')}` : ''}`,
        binds,
      );
      return rows.map(rowToConnection);
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'TOOL_PROVIDER_CONNECTION_LIST',
        reason: 'FAILED',
        details: { ...input },
        cause,
      });
    }
  }

  async deleteConnection(key: StorageDeleteToolProviderConnectionInput): Promise<void> {
    try {
      await this.db.none(`DELETE FROM ${this.db.table(TABLE_TOOL_PROVIDER_CONNECTIONS)} WHERE ${KEY_WHERE}`, key);
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'TOOL_PROVIDER_CONNECTION_DELETE',
        reason: 'FAILED',
        details: key,
        cause,
      });
    }
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.clearTable(TABLE_TOOL_PROVIDER_CONNECTIONS);
  }

  private select(): string {
    return `SELECT "authorId", "providerId", "connectionId", toolkit AS "toolkit", label AS "label",
      CASE WHEN label IS NULL THEN 1 ELSE 0 END AS "labelIsNull", scope AS "scope", "createdAt", "updatedAt" FROM ${this.db.table(TABLE_TOOL_PROVIDER_CONNECTIONS)}`;
  }
}

function rowToConnection(row: Record<string, unknown>): StorageToolProviderConnection {
  return {
    authorId: String(row.authorId),
    providerId: String(row.providerId),
    connectionId: String(row.connectionId),
    toolkit: String(row.toolkit),
    label: Number(row.labelIsNull) === 1 ? null : String(row.label ?? ''),
    scope: row.scope === 'shared' || row.scope === 'caller-supplied' ? row.scope : 'per-author',
    createdAt: toDate(row.createdAt),
    updatedAt: toDate(row.updatedAt),
  };
}

export function getDefaultToolProviderConnectionIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    {
      name: indexNameForTable('MASTRA_TOOL_CONNECTIONS_AUTHOR', 'IDX'),
      table: TABLE_TOOL_PROVIDER_CONNECTIONS,
      columns: ['authorId', 'providerId', 'toolkit'],
    },
  ];
}
