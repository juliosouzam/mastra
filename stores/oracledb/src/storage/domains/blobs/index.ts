import { BlobStore, TABLE_SKILL_BLOBS, TABLE_SCHEMAS } from '@mastra/core/storage';
import type { StorageBlobEntry } from '@mastra/core/storage';
import oracledb from 'oracledb';

import { clobBind, isOracleErrorCode } from '../../../shared/connection';
import { OracleDB, filterIndexesForTables } from '../../db';
import type { OracleCreateIndexOptions } from '../../db';
import { createOracleStorageError, toDate } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

type BlobRow = { hash: string; content: string | null; size: number; mimeType: string | null; createdAt: Date };

export class BlobsOracle extends BlobStore {
  static readonly MANAGED_TABLES = [TABLE_SKILL_BLOBS] as const;
  private readonly db: OracleDB;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB(config);
    this.indexes = filterIndexesForTables(config.indexes, BlobsOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({ tableName: TABLE_SKILL_BLOBS, schema: TABLE_SCHEMAS[TABLE_SKILL_BLOBS] });
    for (const index of this.indexes) await this.db.createIndex(index);
  }

  async put(entry: StorageBlobEntry): Promise<void> {
    await this.putMany([entry]);
  }

  async putMany(entries: StorageBlobEntry[]): Promise<void> {
    if (!entries.length) return;
    try {
      await this.db.tx(async client => {
        for (const entry of entries) {
          try {
            await client.none(
              `INSERT INTO ${this.db.table(TABLE_SKILL_BLOBS)} (hash, content, "size", "mimeType", "createdAt")
               VALUES (:hash, NVL(:content, EMPTY_CLOB()), :blobSize, :mimeType, :createdAt)`,
              {
                hash: entry.hash,
                content: clobBind(entry.content),
                blobSize: entry.size,
                mimeType: entry.mimeType ?? null,
                createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: entry.createdAt ?? new Date() },
              },
            );
          } catch (error) {
            // The first writer owns the immutable content, including across pools.
            if (!isOracleErrorCode(error, [-1])) throw error;
          }
        }
      });
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'BLOBS_PUT_MANY',
        reason: 'FAILED',
        details: { count: entries.length },
        cause,
      });
    }
  }

  async get(hash: string): Promise<StorageBlobEntry | null> {
    return (await this.getMany([hash])).get(hash) ?? null;
  }

  async getMany(hashes: string[]): Promise<Map<string, StorageBlobEntry>> {
    const result = new Map<string, StorageBlobEntry>();
    try {
      for (let start = 0; start < hashes.length; start += 500) {
        const batch = hashes.slice(start, start + 500);
        const binds = Object.fromEntries(batch.map((hash, index) => [`hash${index}`, hash]));
        const rows = await this.db.read.manyOrNone<BlobRow>(
          `SELECT hash AS "hash", content AS "content", "size", "mimeType", "createdAt"
           FROM ${this.db.table(TABLE_SKILL_BLOBS)} WHERE hash IN (${Object.keys(binds)
             .map(key => `:${key}`)
             .join(', ')})`,
          binds,
        );
        for (const row of rows)
          result.set(row.hash, {
            hash: row.hash,
            content: row.content ?? '',
            size: Number(row.size),
            mimeType: row.mimeType ?? undefined,
            createdAt: toDate(row.createdAt),
          });
      }
      return result;
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'BLOBS_GET_MANY',
        reason: 'FAILED',
        details: { count: hashes.length },
        cause,
      });
    }
  }

  async has(hash: string): Promise<boolean> {
    return (
      (await this.db.read.oneOrNone(
        `SELECT 1 AS "present" FROM ${this.db.table(TABLE_SKILL_BLOBS)} WHERE hash = :hash`,
        {
          hash,
        },
      )) !== null
    );
  }

  async delete(hash: string): Promise<boolean> {
    try {
      return await this.db.withConnection(async connection => {
        const result = await connection.execute(
          `DELETE FROM ${this.db.table(TABLE_SKILL_BLOBS)} WHERE hash = :hash`,
          { hash },
          { autoCommit: true },
        );
        return (result.rowsAffected ?? 0) > 0;
      });
    } catch (cause) {
      throw createOracleStorageError({ operation: 'BLOBS_DELETE', reason: 'FAILED', details: { hash }, cause });
    }
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.clearTable(TABLE_SKILL_BLOBS);
  }
}
