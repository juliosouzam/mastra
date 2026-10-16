import { ErrorCategory, MastraError } from '@mastra/core/error';
import { FavoritesStorage, TABLE_AGENTS, TABLE_SKILLS, TABLE_FAVORITES, TABLE_SCHEMAS } from '@mastra/core/storage';
import type {
  StorageDeleteFavoritesForEntityInput,
  StorageIsFavoritedBatchInput,
  StorageListFavoritesInput,
  StorageFavoriteKey,
} from '@mastra/core/storage';
import type { FavoriteToggleResult } from '@mastra/core/storage/domains/favorites';
import oracledb from 'oracledb';

import { rollbackQuietly } from '../../../shared/connection';
import { indexNameForTable } from '../../../vector/identifiers';
import { OracleDB, filterIndexesForTables } from '../../db';
import type { OracleCreateIndexOptions } from '../../db';
import { createOracleStorageError } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

const ENTITY_TABLE = { agent: TABLE_AGENTS, skill: TABLE_SKILLS } as const;
const KEY_WHERE = '"userId" = :userId AND "entityType" = :entityType AND "entityId" = :entityId';

export class FavoritesOracle extends FavoritesStorage {
  static readonly MANAGED_TABLES = [TABLE_FAVORITES] as const;
  private readonly db: OracleDB;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB(config);
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, FavoritesOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_FAVORITES,
      schema: TABLE_SCHEMAS[TABLE_FAVORITES],
      compositePrimaryKey: ['userId', 'entityType', 'entityId'],
    });
    if (!this.skipDefaultIndexes) {
      for (const index of getDefaultFavoriteIndexDefinitions()) {
        try {
          await this.db.createIndex(index);
        } catch (error) {
          this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
        }
      }
    }
    for (const index of this.indexes) await this.db.createIndex(index);
  }

  async favorite(input: StorageFavoriteKey): Promise<FavoriteToggleResult> {
    return this.setFavorite(input, true);
  }
  async unfavorite(input: StorageFavoriteKey): Promise<FavoriteToggleResult> {
    return this.setFavorite(input, false);
  }

  private async setFavorite(input: StorageFavoriteKey, favorited: boolean): Promise<FavoriteToggleResult> {
    const operation = favorited ? 'FAVORITE' : 'UNFAVORITE';
    try {
      const parent = this.db.table(ENTITY_TABLE[input.entityType]);
      return await this.db.tx(async client => {
        // The parent lock coordinates the row and its counter across all users and pools.
        const entity = await client.oneOrNone<{ count: number }>(
          `SELECT NVL("favoriteCount", 0) AS "count" FROM ${parent} WHERE id = :entityId FOR UPDATE`,
          { entityId: input.entityId },
        );
        if (!entity)
          throw createOracleStorageError({
            operation,
            reason: 'ENTITY_NOT_FOUND',
            details: input,
            category: ErrorCategory.USER,
            cause: new Error(`${input.entityType} ${input.entityId} not found`),
          });
        const existing = await client.oneOrNone(
          `SELECT 1 AS "present" FROM ${this.db.table(TABLE_FAVORITES)} WHERE ${KEY_WHERE}`,
          { ...input },
        );
        let favoriteCount = Number(entity.count);
        if (favorited && !existing) {
          await client.none(
            `INSERT INTO ${this.db.table(TABLE_FAVORITES)} ("userId", "entityType", "entityId", "createdAt") VALUES (:userId, :entityType, :entityId, :createdAt)`,
            { ...input, createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: new Date() } },
          );
          favoriteCount++;
        } else if (!favorited && existing) {
          await client.none(`DELETE FROM ${this.db.table(TABLE_FAVORITES)} WHERE ${KEY_WHERE}`, { ...input });
          favoriteCount = Math.max(0, favoriteCount - 1);
        }
        await client.none(`UPDATE ${parent} SET "favoriteCount" = :favoriteCount WHERE id = :entityId`, {
          favoriteCount,
          entityId: input.entityId,
        });
        return { favorited, favoriteCount };
      });
    } catch (cause) {
      if (cause instanceof MastraError) throw cause;
      throw createOracleStorageError({ operation, reason: 'FAILED', details: input, cause });
    }
  }

  async isFavorited(input: StorageFavoriteKey): Promise<boolean> {
    return (
      (await this.db.read.oneOrNone(`SELECT 1 AS "present" FROM ${this.db.table(TABLE_FAVORITES)} WHERE ${KEY_WHERE}`, {
        ...input,
      })) !== null
    );
  }

  async isFavoritedBatch({ userId, entityType, entityIds }: StorageIsFavoritedBatchInput): Promise<Set<string>> {
    const result = new Set<string>();
    for (let start = 0; start < entityIds.length; start += 500) {
      const binds: Record<string, unknown> = { userId, entityType };
      const names = entityIds.slice(start, start + 500).map((id, index) => {
        const key = `entity${index}`;
        binds[key] = id;
        return `:${key}`;
      });
      const rows = await this.db.read.manyOrNone<{ entityId: string }>(
        `SELECT "entityId" FROM ${this.db.table(TABLE_FAVORITES)} WHERE "userId" = :userId AND "entityType" = :entityType AND "entityId" IN (${names.join(', ')})`,
        binds,
      );
      for (const row of rows) result.add(row.entityId);
    }
    return result;
  }

  async listFavoritedIds(input: StorageListFavoritesInput): Promise<string[]> {
    const rows = await this.db.read.manyOrNone<{ entityId: string }>(
      `SELECT "entityId" FROM ${this.db.table(TABLE_FAVORITES)} WHERE "userId" = :userId AND "entityType" = :entityType ORDER BY "createdAt" DESC, "entityId"`,
      { ...input },
    );
    return rows.map(row => row.entityId);
  }

  async deleteFavoritesForEntity(input: StorageDeleteFavoritesForEntityInput): Promise<number> {
    const parent = this.db.table(ENTITY_TABLE[input.entityType]);
    return this.db.withConnection(async connection => {
      try {
        await connection.execute(`SELECT id FROM ${parent} WHERE id = :entityId FOR UPDATE`, {
          entityId: input.entityId,
        });
        const deleted = await connection.execute(
          `DELETE FROM ${this.db.table(TABLE_FAVORITES)} WHERE "entityType" = :entityType AND "entityId" = :entityId`,
          { ...input },
        );
        await connection.execute(`UPDATE ${parent} SET "favoriteCount" = 0 WHERE id = :entityId`, {
          entityId: input.entityId,
        });
        await connection.commit();
        return deleted.rowsAffected ?? 0;
      } catch (error) {
        await rollbackQuietly(connection);
        throw error;
      }
    });
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.tx(async client => {
      await client.none(`DELETE FROM ${this.db.table(TABLE_FAVORITES)}`);
      for (const table of [TABLE_AGENTS, TABLE_SKILLS])
        await client.none(`UPDATE ${this.db.table(table)} SET "favoriteCount" = 0 WHERE "favoriteCount" > 0`);
    });
  }
}

export function getDefaultFavoriteIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    {
      name: indexNameForTable('MASTRA_FAVORITES_ENTITY', 'IDX'),
      table: TABLE_FAVORITES,
      columns: ['entityType', 'entityId'],
    },
  ];
}
