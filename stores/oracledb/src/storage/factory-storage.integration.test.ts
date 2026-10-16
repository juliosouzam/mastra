import { randomUUID } from 'node:crypto';
import { UniqueViolationError } from '@mastra/core/storage';
import type { CollectionSchema } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';

import { describeFactoryStorageContract } from '../../../_test-utils/src/factory-storage-contract';
import { OracleFactoryStorage } from './factory-storage';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle application collections', () => {
  const config = {
    user: process.env.ORACLE_DATABASE_USER,
    password: process.env.ORACLE_DATABASE_PASSWORD,
    connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
  };
  describeFactoryStorageContract('oracle', async () => {
    const storage = new OracleFactoryStorage({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    return { storage, close: () => storage.close() };
  });
  it('preserves long text, empty strings, JSON primitives, and full-value pagination', async () => {
    const storage = new OracleFactoryStorage(config);
    const name = `factory_values_${randomUUID().replaceAll('-', '')}`;
    try {
      await storage.ensureCollections([
        { name, columns: { id: { type: 'uuid-pk' }, text: { type: 'text' }, data: { type: 'json' } } },
      ]);
      const prefix = 'Texto 🌎\u0000 '.repeat(10_000);
      const values = ['', '\u0000', '\u0001', prefix + 'a', prefix + 'b'];
      for (const [index, text] of values.entries()) {
        const data = [null, false, 0, 'false', JSON.parse('{"__proto__":{"literal":true}}')][index];
        const inserted = await storage.ops.insertOne(name, { text, data });
        expect(inserted).toMatchObject({ text, data });
        expect(await storage.ops.findOne(name, { text })).toMatchObject({ text, data });
      }
      const first = await storage.ops.findMany(name, {}, { orderBy: [['text', 'asc']], limit: 4 });
      expect(first.map(row => row.text)).toEqual(values.slice(0, 4));
      const second = await storage.ops.findMany(
        name,
        {},
        { orderBy: [['text', 'asc']], cursor: { values: [prefix + 'a'] }, limit: 4 },
      );
      expect(second.map(row => row.text)).toEqual([prefix + 'b']);
      expect(
        (await storage.ops.findMany(name, { data: { in: [false, 'false'] } })).map(row => row.data).sort(),
      ).toEqual([false, 'false'].sort());
    } finally {
      await storage.getMastraStorage().db?.executeDdl?.(`DROP TABLE "${name.toUpperCase()}" PURGE`, [-942]);
      await storage.close();
    }
  });

  it('prevents write skew across replicas and rolls back the complete callback', async () => {
    const first = new OracleFactoryStorage({ ...config, poolMax: 1 });
    const second = new OracleFactoryStorage({ ...config, poolMax: 1 });
    const name = `factory_tx_${randomUUID().replaceAll('-', '')}`;
    const schema: CollectionSchema = {
      name,
      columns: { id: { type: 'uuid-pk' }, label: { type: 'text' }, count: { type: 'integer' } },
    };
    try {
      await first.ensureCollections([schema]);
      await second.ensureCollections([schema]);
      const outcomes = await Promise.all(
        [first, second].map(storage =>
          storage.withTransaction(
            async ops => {
              if (await ops.count!(name, {})) return false;
              await new Promise(resolve => setTimeout(resolve, 15));
              await ops.insertOne(name, { label: 'exclusive invariant', count: 0 });
              return true;
            },
            { isolationLevel: 'serializable' },
          ),
        ),
      );
      expect(outcomes.sort()).toEqual([false, true]);
      expect(await first.ops.count!(name, {})).toBe(1);
      const sentinel = new Error('callback failed');
      await expect(
        first.withTransaction(async ops => {
          await ops.insertOne(name, { label: 'rolled back', count: 0 });
          await ops.updateAtomic(name, { label: 'exclusive invariant' }, row => ({ count: Number(row.count) + 1 }));
          throw sentinel;
        }),
      ).rejects.toBe(sentinel);
      expect(await second.ops.count!(name, {})).toBe(1);
      expect(await second.ops.findOne(name, { label: 'exclusive invariant' })).toMatchObject({ count: 0 });
    } finally {
      await first.getMastraStorage().db.executeDdl(`DROP TABLE "${name.toUpperCase()}" PURGE`, [-942]);
      await Promise.all([first.close(), second.close()]);
    }
  });

  it('continues keyset pages across NULL values in either sort direction', async () => {
    const storage = new OracleFactoryStorage(config);
    const name = `factory_cursor_${randomUUID().replaceAll('-', '')}`;
    try {
      await storage.ensureCollections([
        {
          name,
          columns: { id: { type: 'uuid-pk' }, label: { type: 'text', nullable: true }, rank: { type: 'integer' } },
        },
      ]);
      for (const [rank, label] of ['a', null, 'b', null].entries()) await storage.ops.insertOne(name, { label, rank });
      for (const direction of ['asc', 'desc'] as const) {
        const expected = direction === 'asc' ? [0, 2, 1, 3] : [1, 3, 2, 0];
        const actual: unknown[] = [];
        let cursor: { values: (string | number | null)[] } | undefined;
        for (let page = 0; page < 5; page++) {
          const rows = await storage.ops.findMany<{ label: string | null; rank: number }>(
            name,
            {},
            {
              orderBy: [
                ['label', direction],
                ['rank', 'asc'],
              ],
              limit: 1,
              cursor,
            },
          );
          if (!rows.length) break;
          actual.push(rows[0]!.rank);
          cursor = { values: [rows[0]!.label, rows[0]!.rank] };
        }
        expect(actual).toEqual(expected);
      }
    } finally {
      await storage.getMastraStorage().db.executeDdl(`DROP TABLE "${name.toUpperCase()}" PURGE`, [-942]);
      await storage.close();
    }
  });

  it('evolves collection defaults, nullability, integer width, and indexes without losing rows', async () => {
    const storage = new OracleFactoryStorage(config);
    const name = `factory_schema_${randomUUID().replaceAll('-', '')}`;
    const schema: CollectionSchema = {
      name,
      columns: { id: { type: 'uuid-pk' }, label: { type: 'text' }, stamp: { type: 'integer' } },
    };
    try {
      await storage.ensureCollections([schema]);
      const first = await storage.ops.insertOne(name, { label: 'existing', stamp: 1 });
      const next: CollectionSchema = {
        ...schema,
        columns: {
          ...schema.columns,
          label: { type: 'text', nullable: true },
          stamp: { type: 'bigint' },
          added: { type: 'text', default: '' },
        },
        uniqueIndexes: [{ name: `${name}_label`, columns: ['label'] }],
      };
      await storage.ensureCollections([next]);
      await storage.ensureCollections([next]);
      expect(await storage.ops.findOne(name, { id: first.id as string })).toMatchObject({
        label: 'existing',
        stamp: 1,
        added: '',
      });
      const updated = await storage.ops.updateAtomic(name, { id: first.id as string }, () => ({
        label: null,
        stamp: 1_780_000_000_000,
      }));
      expect(updated).toMatchObject({ label: null, stamp: 1_780_000_000_000, added: '' });
      await storage.ops.insertOne(name, { label: 'unique', stamp: 2 });
      await expect(storage.ops.insertOne(name, { label: 'unique', stamp: 3 })).rejects.toBeInstanceOf(
        UniqueViolationError,
      );
    } finally {
      await storage.getMastraStorage().db.executeDdl(`DROP TABLE "${name.toUpperCase()}" PURGE`, [-942]);
      await storage.close();
    }
  });

  it('keeps retry identity and selects only rows covered by a partial unique index', async () => {
    const first = new OracleFactoryStorage(config);
    const second = new OracleFactoryStorage(config);
    const name = `factory_retry_${randomUUID().replaceAll('-', '')}`;
    const schema: CollectionSchema = {
      name,
      columns: {
        id: { type: 'uuid-pk' },
        key: { type: 'text' },
        owner: { type: 'text', nullable: true },
        value: { type: 'integer' },
      },
      uniqueIndexes: [{ name: `${name}_shared`, columns: ['key'], whereNull: 'owner' }],
    };
    try {
      await first.ensureCollections([schema]);
      await second.ensureCollections([schema]);
      const owned = await first.ops.insertOne(name, { key: 'same', owner: 'user', value: -1 });
      const results = await Promise.all(
        Array.from({ length: 16 }, (_, value) =>
          (value % 2 ? first : second).ops.upsertOne(name, ['key'], { key: 'same', owner: null, value }),
        ),
      );
      expect(new Set(results.map(row => row.id)).size).toBe(1);
      expect(results[0]!.id).not.toBe(owned.id);
      expect(await first.ops.findOne(name, { id: owned.id as string })).toMatchObject({ owner: 'user', value: -1 });
    } finally {
      await first.getMastraStorage().db.executeDdl(`DROP TABLE "${name.toUpperCase()}" PURGE`, [-942]);
      await Promise.all([first.close(), second.close()]);
    }
  });
});
