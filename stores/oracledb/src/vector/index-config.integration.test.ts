import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { exportSchemas } from '../schema';
import { tableNameForIndex } from './identifiers';
import { OracleVector } from '.';

describe.runIf(process.env.RUN_ORACLE_VECTOR_INTEGRATION === 'true')('Oracle deferred vector indexes', () => {
  const indexName = `deferred_${randomUUID().replaceAll('-', '')}`;
  const config = {
    user: process.env.ORACLE_DATABASE_USER,
    password: process.env.ORACLE_DATABASE_PASSWORD,
    connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
  };
  let vector: OracleVector;

  beforeAll(() => {
    vector = new OracleVector({ id: 'oracle-deferred-config', ...config });
  });

  afterAll(async () => {
    await vector?.deleteIndex({ indexName });
    await vector?.disconnect();
  });

  it('builds the requested deferred index after opening a new provider instance', async () => {
    const indexConfig = { type: 'ivf' as const, accuracy: 90, ivf: { neighborPartitions: 2 } };
    await vector.createIndex({ indexName, dimension: 3, indexConfig, buildIndex: false });
    await vector.upsert({
      indexName,
      vectors: Array.from({ length: 32 }, (_, index) => [1, index / 32, 0.5]),
    });
    expect(await vector.getIndexInfo({ indexName })).toMatchObject({ indexType: 'none' });

    const reopened = new OracleVector({ id: 'oracle-deferred-reopened', ...config });
    try {
      await reopened.buildIndex({ indexName });
      expect(await reopened.getIndexInfo({ indexName })).toMatchObject({
        indexType: 'ivf',
        accuracy: 90,
        indexConfig,
        count: 32,
      });
      expect(await reopened.getIndexStatus({ indexName })).toMatch(/^VALID\b/);
    } finally {
      await reopened.disconnect();
    }
  });

  it('exports the same deferred configuration for externally managed schemas', async () => {
    const registryTableName = `ORACLE_EXPORT_${randomUUID().replaceAll('-', '').slice(0, 12)}`.toUpperCase();
    const exportedName = 'deferred_export';
    const tablePrefix = registryTableName;
    const indexConfig = { type: 'ivf' as const, accuracy: 90, ivf: { neighborPartitions: 2 } };
    const ddl = exportSchemas({
      domains: ['vector'],
      vector: {
        registryTableName,
        tablePrefix,
        indexes: [{ indexName: exportedName, dimension: 3, indexConfig, buildIndex: false, metadataIndexes: [] }],
      },
    });
    const pool = await vector.getPool();
    const connection = await pool.getConnection();
    const reader = new OracleVector({
      id: 'oracle-export-config',
      pool,
      registryTableName,
      tablePrefix,
      disableInit: true,
    });
    try {
      for (const sql of ddl
        .split(';')
        .map(statement => statement.trim())
        .filter(Boolean)) {
        await connection.execute(sql);
      }
      await connection.commit();
      expect(await reader.getIndexInfo({ indexName: exportedName })).toMatchObject({ indexType: 'none', indexConfig });
    } finally {
      await reader.disconnect();
      await connection
        .execute(`DROP TABLE "${tableNameForIndex(exportedName, tablePrefix)}" PURGE`)
        .catch(() => undefined);
      await connection.execute(`DROP TABLE "${registryTableName}" PURGE`).catch(() => undefined);
      await connection.close();
    }
  });

  it('requires an explicit rebuild for changed parameters and preserves vectors', async () => {
    const rebuildName = `rebuild_${randomUUID().replaceAll('-', '')}`;
    const initial = { type: 'ivf' as const, accuracy: 90, ivf: { neighborPartitions: 2 } };
    const changed = { type: 'ivf' as const, accuracy: 90, ivf: { neighborPartitions: 3 } };
    try {
      await vector.createIndex({ indexName: rebuildName, dimension: 3, indexConfig: initial, buildIndex: false });
      await vector.upsert({
        indexName: rebuildName,
        vectors: Array.from({ length: 32 }, (_, index) => [1, index / 32, 0.5]),
      });
      await vector.buildIndex({ indexName: rebuildName });
      await expect(vector.createIndex({ indexName: rebuildName, dimension: 3, indexConfig: changed })).rejects.toThrow(
        /already exists/,
      );
      expect(await vector.getIndexInfo({ indexName: rebuildName })).toMatchObject({ indexConfig: initial, count: 32 });

      await vector.rebuildIndex({ indexName: rebuildName, indexConfig: changed });
      const info = await vector.getIndexInfo({ indexName: rebuildName });
      expect(info).toMatchObject({ indexConfig: changed, count: 32 });
      info.indexConfig!.ivf!.neighborPartitions = 100;
      expect(await vector.getIndexInfo({ indexName: rebuildName })).toMatchObject({ indexConfig: changed, count: 32 });
    } finally {
      await vector.deleteIndex({ indexName: rebuildName });
    }
  });

  it('keeps existing indexes usable when older registry rows have no recorded build parameters', async () => {
    const legacyName = `legacy_config_${randomUUID().replaceAll('-', '')}`;
    const indexConfig = { type: 'ivf' as const, accuracy: 90, ivf: { neighborPartitions: 2 } };
    const reopened = new OracleVector({ id: 'oracle-legacy-config', ...config });
    try {
      await vector.createIndex({ indexName: legacyName, dimension: 3, indexConfig, buildIndex: false });
      await vector.upsert({
        indexName: legacyName,
        vectors: Array.from({ length: 32 }, (_, index) => [1, index / 32, 0.5]),
      });
      await vector.buildIndex({ indexName: legacyName });
      const connection = await (await vector.getPool()).getConnection();
      try {
        await connection.execute(
          'UPDATE MASTRA_VECTOR_INDEXES SET index_config = NULL WHERE index_name = :name',
          { name: legacyName },
          { autoCommit: true },
        );
      } finally {
        await connection.close();
      }

      await expect(reopened.createIndex({ indexName: legacyName, dimension: 3, indexConfig })).resolves.toBeUndefined();
      const info = await reopened.getIndexInfo({ indexName: legacyName });
      expect(info).toMatchObject({ indexType: 'ivf', count: 32 });
      expect(info.indexConfig).toBeUndefined();
      await reopened.rebuildIndex({ indexName: legacyName, indexConfig });
      expect(await reopened.getIndexInfo({ indexName: legacyName })).toMatchObject({ indexConfig, count: 32 });
    } finally {
      await reopened.disconnect();
      await vector.deleteIndex({ indexName: legacyName });
    }
  });
});
