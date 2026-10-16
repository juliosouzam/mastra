import {
  ThreadStateStorage,
  TABLE_THREAD_STATE,
  TABLE_SCHEMAS,
  executeRetentionPrune,
  resolveRetentionTargets,
  retentionCutoffMs,
} from '@mastra/core/storage';
import type { PruneOptions, PruneResult, RetentionTablesDescriptor, TableRetentionPolicy } from '@mastra/core/storage';
import oracledb from 'oracledb';

import { clobBind, isOracleErrorCode, jsonBindText } from '../../../shared/connection';
import { indexNameForTable } from '../../../vector/identifiers';
import { OracleDB, filterIndexesForTables } from '../../db';
import type { OracleCreateIndexOptions } from '../../db';
import { createOracleStorageError } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

export class ThreadStateOracle extends ThreadStateStorage {
  static readonly MANAGED_TABLES = [TABLE_THREAD_STATE] as const;
  static override readonly retentionTables: RetentionTablesDescriptor = {
    threadState: { table: TABLE_THREAD_STATE, column: 'updatedAt', indexed: true },
  };
  private readonly db: OracleDB;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB(config);
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, ThreadStateOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_THREAD_STATE,
      schema: TABLE_SCHEMAS[TABLE_THREAD_STATE],
      compositePrimaryKey: ['threadId', 'type'],
    });
    if (!this.skipDefaultIndexes) await this.ensureRetentionIndex();
    for (const index of this.indexes) await this.db.createIndex(index);
  }

  private async ensureRetentionIndex(): Promise<void> {
    try {
      for (const index of getDefaultThreadStateIndexDefinitions()) await this.db.createIndex(index);
    } catch (error) {
      this.logger?.warn?.(`Failed to create retention index for ${TABLE_THREAD_STATE}:`, error);
    }
  }

  async prune(policies: Record<string, TableRetentionPolicy>, options?: PruneOptions): Promise<PruneResult[]> {
    const targets = resolveRetentionTargets({
      policies,
      descriptor: ThreadStateOracle.retentionTables,
      order: ['threadState'],
    });
    if (targets.length && !options?.signal?.aborted) await this.ensureRetentionIndex();
    return executeRetentionPrune({
      domain: 'threadState',
      targets,
      options,
      cutoffFor: (target, now) => new Date(retentionCutoffMs(target.policy, now)),
      deleteBatch: (target, cutoff, limit) =>
        this.db.pruneBatch({ tableName: target.table, column: target.column, cutoff, limit }),
    });
  }

  async getState<T = unknown>({ threadId, type }: { threadId: string; type: string }): Promise<T | undefined> {
    try {
      return await this.db.withConnection(async connection => {
        // Serialize on the server so JSON strings, null, and large values keep their types.
        const result = await connection.execute<{ value: string }>(
          `SELECT JSON_SERIALIZE(value RETURNING CLOB) AS "value"
           FROM ${this.db.table(TABLE_THREAD_STATE)} WHERE "threadId" = :threadId AND type = :type`,
          { threadId, type },
          { outFormat: oracledb.OUT_FORMAT_OBJECT, fetchInfo: { value: { type: oracledb.STRING } } },
        );
        const row = result.rows?.[0];
        return row ? (JSON.parse(row.value) as T) : undefined;
      });
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'THREAD_STATE_GET',
        reason: 'FAILED',
        details: { threadId, type },
        cause,
      });
    }
  }

  async setState<T = unknown>({ threadId, type, value }: { threadId: string; type: string; value: T }): Promise<void> {
    try {
      const binds = {
        threadId,
        type,
        value: clobBind(jsonBindText(value) ?? 'null'),
        now: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: new Date() },
      };
      await this.db.tx(async client => {
        try {
          await client.none(
            `INSERT INTO ${this.db.table(TABLE_THREAD_STATE)} ("threadId", type, value, "createdAt", "updatedAt")
             VALUES (:threadId, :type, JSON(:value), :now, :now)`,
            binds,
          );
          return;
        } catch (error) {
          // Like atomic working memory, concurrent first writes coordinate on the primary key.
          if (!isOracleErrorCode(error, [-1])) throw error;
        }
        await client.none(
          `UPDATE ${this.db.table(TABLE_THREAD_STATE)} SET value = JSON(:value), "updatedAt" = :now
           WHERE "threadId" = :threadId AND type = :type`,
          binds,
        );
      });
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'THREAD_STATE_SET',
        reason: 'FAILED',
        details: { threadId, type },
        cause,
      });
    }
  }

  async deleteState({ threadId, type }: { threadId: string; type: string }): Promise<void> {
    try {
      await this.db.none(
        `DELETE FROM ${this.db.table(TABLE_THREAD_STATE)} WHERE "threadId" = :threadId AND type = :type`,
        { threadId, type },
      );
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'THREAD_STATE_DELETE',
        reason: 'FAILED',
        details: { threadId, type },
        cause,
      });
    }
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.clearTable(TABLE_THREAD_STATE);
  }
}

export function getDefaultThreadStateIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    {
      name: indexNameForTable('MASTRA_THREAD_STATE_RETENTION_IDX', 'IDX'),
      table: TABLE_THREAD_STATE,
      columns: ['updatedAt'],
    },
  ];
}
