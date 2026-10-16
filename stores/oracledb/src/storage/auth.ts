import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import type { CollectionColumnSpec, CollectionSchema } from '@mastra/core/storage';
import type { BetterAuthOptions } from 'better-auth';
import type { AdapterFactory, CustomAdapter } from 'better-auth/adapters';
import type { OracleFactoryStorage, OracleFactoryStorageOps } from './factory-storage';

const require = createRequire(import.meta.url);

function tableName(model: string): string {
  const suffix = createHash('sha256').update(model).digest('hex').slice(0, 8);
  return `MASTRA_AUTH_${model
    .replace(/[^A-Za-z0-9_]/g, '_')
    .slice(0, 64)
    .toUpperCase()}_${suffix}`;
}

/** Loads the optional auth dependency only when Better Auth invokes this database factory. */
export function createOracleAuthAdapter(storage: OracleFactoryStorage): AdapterFactory<BetterAuthOptions> {
  return (options: BetterAuthOptions) => {
    const { createAdapterFactory } = require('better-auth/adapters') as typeof import('better-auth/adapters');
    let initialize: (() => Promise<void>) | undefined;
    let initialized: Promise<void> | undefined;
    const ready = () =>
      (initialized ??= initialize!().catch(error => {
        initialized = undefined;
        throw error;
      }));

    const build = (ops: OracleFactoryStorageOps, transactional = false): AdapterFactory<BetterAuthOptions> =>
      createAdapterFactory({
        config: {
          adapterId: 'mastra-oracle',
          adapterName: 'Mastra Oracle',
          supportsJSON: true,
          supportsDates: true,
          supportsBooleans: true,
          supportsArrays: true,
          supportsNumericIds: false,
          transaction: transactional
            ? false
            : async callback => {
                await ready();
                return storage.withTransaction(tx => callback(build(tx as OracleFactoryStorageOps, true)(options)));
              },
        },
        adapter: ({ schema, getModelName, getFieldName }) => {
          if (!initialize) {
            const collections: CollectionSchema[] = Object.entries(schema).map(([model, definition]) => {
              const name = tableName(getModelName(model));
              const columns: Record<string, CollectionColumnSpec> = Object.create(null);
              columns[getFieldName({ model, field: 'id' })] = { type: 'text', primaryKey: true };
              const uniqueIndexes: NonNullable<CollectionSchema['uniqueIndexes']> = [];
              const indexes: NonNullable<CollectionSchema['indexes']> = [];
              for (const [key, field] of Object.entries(definition.fields)) {
                const column = getFieldName({ model, field: key });
                const type =
                  field.type === 'date'
                    ? 'timestamp'
                    : field.type === 'boolean'
                      ? 'boolean'
                      : field.type === 'number'
                        ? 'bigint'
                        : field.type === 'json' || field.type === 'string[]' || field.type === 'number[]'
                          ? 'json'
                          : 'text';
                const value = typeof field.defaultValue === 'function' ? field.defaultValue() : field.defaultValue;
                const literal = value instanceof Date ? value.toISOString() : value;
                columns[column] = {
                  type,
                  nullable: field.required === false,
                  ...(typeof literal === 'string' || typeof literal === 'number' || typeof literal === 'boolean'
                    ? { default: literal }
                    : {}),
                };
                const indexName = `${name}_${createHash('sha256').update(column).digest('hex').slice(0, 8)}`;
                if (field.unique) uniqueIndexes.push({ name: `${indexName}_UQ`, columns: [column] });
                else if (field.index || field.references) indexes.push({ name: `${indexName}_IDX`, columns: [column] });
              }
              for (const index of definition.indexes ?? []) {
                const columns = index.fields.map(field => getFieldName({ model, field }));
                const target = index.unique ? uniqueIndexes : indexes;
                if (target.some(existing => existing.columns.join('\0') === columns.join('\0'))) continue;
                const suffix = createHash('sha256').update(JSON.stringify(columns)).digest('hex').slice(0, 8);
                target.push({ name: index.name ?? `${name}_${suffix}_${index.unique ? 'UQ' : 'IDX'}`, columns });
              }
              return { name, columns, uniqueIndexes, indexes };
            });
            initialize = () => storage.ensureCollections(collections);
          }
          const adapter: CustomAdapter = {
            async create<T extends Record<string, unknown>>({ model, data }: { model: string; data: T }) {
              await ready();
              return ops.insertOne<T>(tableName(model), data);
            },
            async findOne<T>({ model, where }: Parameters<CustomAdapter['findOne']>[0]) {
              await ready();
              return (await ops.findOne(tableName(model), where)) as T | null;
            },
            async findMany<T>({ model, where, limit, offset, sortBy }: Parameters<CustomAdapter['findMany']>[0]) {
              await ready();
              return (await ops.findMany(tableName(model), where ?? [], {
                limit,
                offset,
                orderBy: sortBy ? [[getFieldName({ model, field: sortBy.field }), sortBy.direction]] : undefined,
              })) as T[];
            },
            async count({ model, where }) {
              await ready();
              return ops.count(tableName(model), where ?? []);
            },
            async update<T>({ model, where, update }: Parameters<CustomAdapter['update']>[0]) {
              await ready();
              return (await ops.updateAtomic(
                tableName(model),
                where,
                () => update as Record<string, unknown>,
              )) as T | null;
            },
            async updateMany({ model, where, update }) {
              await ready();
              return ops.updateMany(tableName(model), where, update);
            },
            async delete({ model, where }) {
              await ready();
              await ops.consumeOne(tableName(model), where);
            },
            async deleteMany({ model, where }) {
              await ready();
              return ops.deleteMany(tableName(model), where);
            },
            async consumeOne<T>({ model, where }: Parameters<NonNullable<CustomAdapter['consumeOne']>>[0]) {
              await ready();
              return (await ops.consumeOne(tableName(model), where)) as T | null;
            },
            async incrementOne<T>({
              model,
              where,
              increment,
              set,
            }: Parameters<NonNullable<CustomAdapter['incrementOne']>>[0]) {
              await ready();
              return (await ops.updateAtomic(tableName(model), where, row => {
                const patch: Record<string, unknown> = { ...set };
                for (const [key, delta] of Object.entries(increment)) {
                  if (typeof row[key] !== 'number' || !Number.isFinite(delta))
                    throw new Error(`Invalid auth counter: ${key}`);
                  patch[key] = row[key] + delta;
                }
                return patch;
              })) as T | null;
            },
          };
          return adapter;
        },
      });
    return build(storage.ops as OracleFactoryStorageOps)(options);
  };
}
