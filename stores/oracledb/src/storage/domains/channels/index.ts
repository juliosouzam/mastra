import {
  ChannelsStorage,
  TABLE_CHANNEL_INSTALLATIONS,
  TABLE_CHANNEL_CONFIG,
  TABLE_SCHEMAS,
} from '@mastra/core/storage';
import type { ChannelInstallation, ChannelConfig } from '@mastra/core/storage';
import oracledb from 'oracledb';

import { clobBind, isOracleErrorCode, jsonBindText } from '../../../shared/connection';
import { indexNameForTable } from '../../../vector/identifiers';
import { OracleDB, filterIndexesForTables, parseOracleJson } from '../../db';
import type { OracleCreateIndexOptions } from '../../db';
import { createOracleStorageError, toDate } from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

export class ChannelsOracle extends ChannelsStorage {
  static readonly MANAGED_TABLES = [TABLE_CHANNEL_INSTALLATIONS, TABLE_CHANNEL_CONFIG] as const;
  private readonly db: OracleDB;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({ ...config, jsonColumns: ['data'] });
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, ChannelsOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    for (const tableName of ChannelsOracle.MANAGED_TABLES)
      await this.db.createTable({ tableName, schema: TABLE_SCHEMAS[tableName] });
    for (const index of getDefaultChannelIndexDefinitions()) {
      if (this.skipDefaultIndexes && !index.unique) continue;
      try {
        await this.db.createIndex(index);
      } catch (error) {
        if (index.unique) throw error;
        this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
      }
    }
    for (const index of this.indexes) await this.db.createIndex(index);
  }

  async saveInstallation(installation: ChannelInstallation): Promise<void> {
    const now = new Date();
    const binds = {
      id: installation.id,
      platform: installation.platform,
      agentId: installation.agentId,
      status: installation.status,
      webhookId: installation.webhookId ?? null,
      payload: clobBind(jsonBindText(installation.data)!),
      configHash: installation.configHash ?? null,
      errorText: installation.error === undefined ? null : clobBind(installation.error),
      emptyError: installation.error === '' ? 1 : 0,
      createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: installation.createdAt ?? now },
      updatedAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: now },
    };
    try {
      await this.db.tx(async client => {
        try {
          await client.none(
            `INSERT INTO ${this.db.table(TABLE_CHANNEL_INSTALLATIONS)}
            (id, platform, "agentId", status, "webhookId", data, "configHash", error, "createdAt", "updatedAt")
            VALUES (:id, :platform, :agentId, :status, :webhookId, JSON(:payload), :configHash,
            CASE WHEN :emptyError = 1 THEN EMPTY_CLOB() ELSE :errorText END, :createdAt, :updatedAt)`,
            binds,
          );
          return;
        } catch (error) {
          if (!isOracleErrorCode(error, [-1])) throw error;
          // A webhook collision on another installation is not an upsert conflict.
          const existing = await client.oneOrNone(
            `SELECT id AS "id" FROM ${this.db.table(TABLE_CHANNEL_INSTALLATIONS)} WHERE id = :id FOR UPDATE`,
            { id: installation.id },
          );
          if (!existing) throw error;
        }
        const { createdAt: _createdAt, ...updates } = binds;
        await client.none(
          `UPDATE ${this.db.table(TABLE_CHANNEL_INSTALLATIONS)} SET platform = :platform, "agentId" = :agentId,
          status = :status, "webhookId" = :webhookId, data = JSON(:payload), "configHash" = :configHash,
          error = CASE WHEN :emptyError = 1 THEN EMPTY_CLOB() ELSE :errorText END, "updatedAt" = :updatedAt WHERE id = :id`,
          updates,
        );
      });
    } catch (cause) {
      throw createOracleStorageError({
        operation: 'CHANNEL_SAVE_INSTALLATION',
        reason: 'FAILED',
        details: { id: installation.id },
        cause,
      });
    }
  }

  async getInstallation(id: string): Promise<ChannelInstallation | null> {
    const row = await this.db.read.oneOrNone(`${this.select()} WHERE id = :id`, { id });
    return row ? parseInstallation(row) : null;
  }

  async getInstallationByAgent(platform: string, agentId: string): Promise<ChannelInstallation | null> {
    const row = await this.db.read.oneOrNone(
      `${this.select()} WHERE platform = :platform AND "agentId" = :agentId
      ORDER BY CASE status WHEN 'active' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END, "updatedAt" DESC, id FETCH FIRST 1 ROW ONLY`,
      { platform, agentId },
    );
    return row ? parseInstallation(row) : null;
  }

  async getInstallationByWebhookId(webhookId: string): Promise<ChannelInstallation | null> {
    const row = await this.db.read.oneOrNone(`${this.select()} WHERE "webhookId" = :webhookId`, { webhookId });
    return row ? parseInstallation(row) : null;
  }

  async listInstallations(platform: string): Promise<ChannelInstallation[]> {
    return (
      await this.db.read.manyOrNone(`${this.select()} WHERE platform = :platform ORDER BY "createdAt" DESC, id`, {
        platform,
      })
    ).map(parseInstallation);
  }

  async deleteInstallation(id: string): Promise<void> {
    await this.db.none(`DELETE FROM ${this.db.table(TABLE_CHANNEL_INSTALLATIONS)} WHERE id = :id`, { id });
  }

  async saveConfig(config: ChannelConfig): Promise<void> {
    const binds = {
      platform: config.platform,
      payload: clobBind(jsonBindText(config.data)!),
      updatedAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: config.updatedAt },
    };
    await this.db.tx(async client => {
      try {
        await client.none(
          `INSERT INTO ${this.db.table(TABLE_CHANNEL_CONFIG)} (platform, data, "updatedAt") VALUES (:platform, JSON(:payload), :updatedAt)`,
          binds,
        );
        return;
      } catch (error) {
        if (!isOracleErrorCode(error, [-1])) throw error;
      }
      await client.none(
        `UPDATE ${this.db.table(TABLE_CHANNEL_CONFIG)} SET data = JSON(:payload), "updatedAt" = :updatedAt WHERE platform = :platform`,
        binds,
      );
    });
  }

  async getConfig(platform: string): Promise<ChannelConfig | null> {
    const row = await this.db.read.oneOrNone(
      `SELECT platform AS "platform", JSON_SERIALIZE(data RETURNING CLOB) AS "data", "updatedAt" FROM ${this.db.table(TABLE_CHANNEL_CONFIG)} WHERE platform = :platform`,
      { platform },
    );
    return row
      ? {
          platform: String(row.platform),
          data: parseOracleJson<Record<string, unknown>>(row.data) ?? {},
          updatedAt: toDate(row.updatedAt),
        }
      : null;
  }

  async deleteConfig(platform: string): Promise<void> {
    await this.db.none(`DELETE FROM ${this.db.table(TABLE_CHANNEL_CONFIG)} WHERE platform = :platform`, { platform });
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.tx(async client => {
      for (const table of ChannelsOracle.MANAGED_TABLES) await client.none(`DELETE FROM ${this.db.table(table)}`);
    });
  }

  private select(): string {
    return `SELECT id AS "id", platform AS "platform", "agentId", status AS "status", "webhookId", JSON_SERIALIZE(data RETURNING CLOB) AS "data", "configHash", error AS "errorText",
      CASE WHEN error IS NULL THEN 1 ELSE 0 END AS "errorIsNull", "createdAt", "updatedAt" FROM ${this.db.table(TABLE_CHANNEL_INSTALLATIONS)}`;
  }
}

function parseInstallation(row: Record<string, unknown>): ChannelInstallation {
  return {
    id: String(row.id),
    platform: String(row.platform),
    agentId: String(row.agentId),
    status: row.status as ChannelInstallation['status'],
    webhookId: row.webhookId == null ? undefined : String(row.webhookId),
    data: parseOracleJson<Record<string, unknown>>(row.data) ?? {},
    configHash: row.configHash == null ? undefined : String(row.configHash),
    error: Number(row.errorIsNull) === 1 ? undefined : String(row.errorText ?? ''),
    createdAt: toDate(row.createdAt),
    updatedAt: toDate(row.updatedAt),
  };
}

export function getDefaultChannelIndexDefinitions(): OracleCreateIndexOptions[] {
  return [
    {
      name: indexNameForTable('MASTRA_CHANNEL_WEBHOOK', 'IDX'),
      table: TABLE_CHANNEL_INSTALLATIONS,
      columns: ['webhookId'],
      unique: true,
    },
    {
      name: indexNameForTable('MASTRA_CHANNEL_PLATFORM_AGENT', 'IDX'),
      table: TABLE_CHANNEL_INSTALLATIONS,
      columns: ['platform', 'agentId'],
    },
  ];
}
