import type { Connection, ExecuteManyOptions, BindParameters } from 'oracledb';
import oracledb from 'oracledb';

import { asBindParameters, rollbackQuietly } from '../../../../shared/connection';
import { OracleDB } from '../../../db';
import type { OracleDomainConfig } from '../../../types';

// Domain-local query facade follows the existing SQL observability providers and uses OracleDB transactions.
export class OracleObservabilityClient {
  private readonly db: OracleDB;
  constructor(
    config: OracleDomainConfig,
    private readonly connection?: Connection,
  ) {
    this.db = new OracleDB(config);
    this.config = config;
  }
  private readonly config: OracleDomainConfig;
  snapshotHorizon?: string;
  setCallTimeout(timeoutMs: number): void {
    if (!this.connection) throw new Error('A statement timeout requires an observability transaction');
    this.connection.callTimeout = timeoutMs;
  }
  async query<T extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    values: unknown[] = [],
  ): Promise<{ rows: T[]; rowCount: number }> {
    const run = async (connection: Connection) => {
      try {
        const result = await connection.execute<T>(
          sql,
          asBindParameters(Object.fromEntries(values.map((value, index) => [`p${index + 1}`, value]))),
          {
            outFormat: oracledb.OUT_FORMAT_OBJECT,
            autoCommit: !this.connection,
            fetchTypeHandler: metadata =>
              metadata.dbType === oracledb.DB_TYPE_CLOB || ['xactId', 'cursorId'].includes(metadata.name)
                ? { type: oracledb.STRING }
                : undefined,
          },
        );
        return {
          rows: (result.rows ?? []).map(
            row =>
              Object.fromEntries(
                Object.entries(row).map(([key, value]) => [key === key.toUpperCase() ? key.toLowerCase() : key, value]),
              ) as T,
          ),
          rowCount: result.rowsAffected ?? 0,
        };
      } catch (error) {
        if (!this.connection) await rollbackQuietly(connection);
        throw error;
      }
    };
    return this.connection ? run(this.connection) : this.db.withConnection(run);
  }
  async manyOrNone<T extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    values?: unknown[],
  ): Promise<T[]> {
    return (await this.query<T>(sql, values)).rows;
  }
  async any<T extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    values?: unknown[],
  ): Promise<T[]> {
    return this.manyOrNone<T>(sql, values);
  }
  async oneOrNone<T extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    values?: unknown[],
  ): Promise<T | null> {
    const rows = await this.manyOrNone<T>(sql, values);
    if (rows.length > 1) throw new Error(`Expected at most one row, got ${rows.length}`);
    return rows[0] ?? null;
  }
  async one<T extends Record<string, unknown> = Record<string, unknown>>(sql: string, values?: unknown[]): Promise<T> {
    const row = await this.oneOrNone<T>(sql, values);
    if (!row) throw new Error('Expected one row');
    return row;
  }
  async none(sql: string, values?: unknown[]): Promise<void> {
    await this.query(sql, values);
  }
  async batch(sql: string, rows: BindParameters[], options: ExecuteManyOptions): Promise<void> {
    if (!this.connection) return this.tx(client => client.batch(sql, rows, options));
    await this.connection.executeMany(sql, rows, { ...options, autoCommit: false });
  }
  async tx<T>(callback: (client: OracleObservabilityClient) => Promise<T>): Promise<T> {
    if (this.connection) return callback(this);
    return this.db.tx(async (_client, connection) => {
      const previousTimeout = connection.callTimeout;
      try {
        return await callback(new OracleObservabilityClient(this.config, connection));
      } finally {
        connection.callTimeout = previousTimeout;
      }
    });
  }
}
export type DbClient = OracleObservabilityClient;
export type TxClient = OracleObservabilityClient;
