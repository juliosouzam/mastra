import { FactoryStorage, UniqueViolationError } from '@mastra/core/storage';
import type {
  CollectionColumnSpec,
  CollectionListOptions,
  CollectionSchema,
  CollectionValue,
  CollectionWhere,
  FactoryAuthDatabase,
  FactoryStorageOps,
} from '@mastra/core/storage';
import type { Connection } from 'oracledb';
import oracledb from 'oracledb';

import { asBindParameters, clobBind, isOracleErrorCode } from '../shared/connection';
import type { OraclePoolManager } from '../shared/connection';
import { qualifyName, normalizeIdentifier } from '../vector/identifiers';
import { createOracleAuthAdapter } from './auth';
import type { OracleStoreConfig } from './types';
import { OracleStore } from './index';

export type OracleFactoryStorageConfig =
  | (Omit<OracleStoreConfig, 'id'> & { id?: string; authDatabase?: unknown })
  | { store: OracleStore; authDatabase?: unknown };

// Structural auth predicates keep the optional Better Auth dependency out of the main package declarations.
type OracleAuthWhere = {
  field: string;
  value: string | number | boolean | string[] | number[] | Date | null;
  operator: 'eq' | 'ne' | 'lt' | 'lte' | 'gt' | 'gte' | 'in' | 'not_in' | 'contains' | 'starts_with' | 'ends_with';
  connector: 'AND' | 'OR';
  mode: 'sensitive' | 'insensitive';
};
type OracleCollectionWhere = CollectionWhere | OracleAuthWhere[];
type OracleCollectionListOptions = CollectionListOptions & { offset?: number };

const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
const TEXT_PREFIX = '\u0001';
const COLUMN_DDL: Record<CollectionColumnSpec['type'], string> = {
  text: 'CLOB',
  bigint: 'NUMBER(20)',
  integer: 'NUMBER(10)',
  boolean: 'NUMBER(1)',
  json: 'JSON',
  timestamp: 'TIMESTAMP WITH TIME ZONE',
  'uuid-pk': 'VARCHAR2(36)',
};

function assertIdentifier(kind: string, name: string): void {
  if (!IDENTIFIER_RE.test(name)) {
    throw new Error(`OracleFactoryStorage: invalid ${kind} identifier '${name}'`);
  }
}

function isUniqueViolation(error: unknown): boolean {
  return isOracleErrorCode(error, [-1]);
}

function primaryKeyOf(schema: CollectionSchema): string {
  const pks = Object.entries(schema.columns).filter(([, spec]) => spec.type === 'uuid-pk' || spec.primaryKey);
  if (pks.length !== 1) {
    throw new Error(
      `OracleFactoryStorage: collection '${schema.name}' must declare exactly one primary key (found ${pks.length})`,
    );
  }
  return pks[0]![0];
}

function isClob(spec: CollectionColumnSpec): boolean {
  return spec.type === 'text' && !spec.primaryKey;
}

function compareValues(left: unknown, right: unknown): number {
  if (left === right) return 0;
  if (left == null) return 1;
  if (right == null) return -1;
  if (typeof left === 'string' && typeof right === 'string')
    return Buffer.compare(Buffer.from(left), Buffer.from(right));
  const first = left instanceof Date ? left.getTime() : left;
  const second = right instanceof Date ? right.getTime() : right;
  return first < second ? -1 : first > second ? 1 : 0;
}

function assertUpsertConflict(schema: CollectionSchema, conflictKeys: string[], row: Record<string, unknown>) {
  const keys = new Set(conflictKeys);
  if (keys.size !== conflictKeys.length || conflictKeys.some(key => row[key] === undefined)) {
    throw new Error(
      `OracleFactoryStorage: upsert conflict keys for '${schema.name}' must be unique and present in the row`,
    );
  }

  const primaryKey = primaryKeyOf(schema);
  const candidates = [{ columns: [primaryKey] }, ...(schema.uniqueIndexes ?? [])];
  const matches = candidates.find(candidate => {
    if (candidate.columns.length !== keys.size || candidate.columns.some(column => !keys.has(column))) return false;
    if ('whereNotNull' in candidate && candidate.whereNotNull && row[candidate.whereNotNull] == null) return false;
    if ('whereNull' in candidate && candidate.whereNull && row[candidate.whereNull] != null) return false;
    return true;
  });
  if (!matches) {
    throw new Error(
      `OracleFactoryStorage: upsert conflict keys [${conflictKeys.join(', ')}] do not match an applicable primary key or unique index on '${schema.name}'`,
    );
  }
  return matches;
}

type Queryable = {
  query(sql: string, args?: unknown[]): Promise<{ rows: Record<string, unknown>[]; rowCount: number }>;
};

function createQueryable(connection: Connection): Queryable {
  return {
    async query(sql, args = []) {
      const result = await connection.execute<Record<string, unknown>>(
        sql,
        asBindParameters(Object.fromEntries(args.map((value, index) => [`v${index + 1}`, value]))),
        {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
          fetchTypeHandler: meta => (meta.dbType === oracledb.DB_TYPE_CLOB ? { type: oracledb.STRING } : undefined),
        },
      );
      return { rows: result.rows ?? [], rowCount: result.rowsAffected ?? 0 };
    },
  };
}

async function transaction<T>(manager: OraclePoolManager, run: (connection: Connection) => Promise<T>): Promise<T> {
  const connection = await (await manager.getPool()).getConnection();
  let drop = false;
  try {
    const result = await run(connection);
    await connection.commit();
    return result;
  } catch (error) {
    try {
      await connection.rollback();
    } catch (rollbackError) {
      drop = true;
      throw new AggregateError([error, rollbackError], 'Factory transaction and rollback both failed');
    }
    throw error;
  } finally {
    await connection.close({ drop });
  }
}

export class OracleFactoryStorageOps implements FactoryStorageOps {
  readonly #queryable: Queryable;
  readonly #transactionClient?: Connection;
  readonly #manager: OraclePoolManager;
  readonly #table: (collection: string) => string;
  readonly #schemas: Map<string, CollectionSchema>;

  constructor(
    manager: OraclePoolManager,
    schemas: Map<string, CollectionSchema>,
    table: (collection: string) => string,
    connection?: Connection,
  ) {
    this.#manager = manager;
    this.#schemas = schemas;
    this.#table = table;
    this.#transactionClient = connection;
    this.#queryable = connection
      ? createQueryable(connection)
      : { query: (sql, args) => manager.withConnection(conn => createQueryable(conn).query(sql, args)) };
  }
  async #write<T>(fn: (ops: OracleFactoryStorageOps) => Promise<T>): Promise<T> {
    return this.#transactionClient
      ? fn(this)
      : transaction(this.#manager, connection =>
          fn(new OracleFactoryStorageOps(this.#manager, this.#schemas, this.#table, connection)),
        );
  }

  #schema(collection: string): CollectionSchema {
    const schema = this.#schemas.get(collection);
    if (!schema) {
      throw new Error(
        `OracleFactoryStorage: unknown collection '${collection}' — register it via ensureCollections() first`,
      );
    }
    return schema;
  }

  #column(schema: CollectionSchema, column: string): CollectionColumnSpec {
    const spec = Object.hasOwn(schema.columns, column) ? schema.columns[column] : undefined;
    if (!spec) {
      throw new Error(`OracleFactoryStorage: unknown column '${column}' on collection '${schema.name}'`);
    }
    return spec;
  }

  #serialize(spec: CollectionColumnSpec, value: unknown): unknown {
    if (value === undefined || (value === null && spec.type !== 'json')) return null;
    switch (spec.type) {
      case 'timestamp':
        return value instanceof Date ? value : new Date(String(value));
      case 'boolean':
        return Number(Boolean(value));
      case 'json':
        return clobBind(JSON.stringify(value));
      case 'bigint':
      case 'integer':
        return Number(value);
      case 'text':
        // A common prefix preserves empty text separately from NULL and preserves binary ordering.
        return isClob(spec) ? clobBind(TEXT_PREFIX + String(value)) : TEXT_PREFIX + String(value);
      default:
        return String(value);
    }
  }

  #deserializeRow<T extends Record<string, unknown>>(schema: CollectionSchema, raw: Record<string, unknown>): T {
    const row: Record<string, unknown> = Object.create(null);
    for (const [name, spec] of Object.entries(schema.columns)) {
      const value = raw[name];
      if (value === null || value === undefined) {
        row[name] = null;
        continue;
      }
      switch (spec.type) {
        case 'timestamp':
          row[name] = value instanceof Date ? value : new Date(String(value));
          break;
        case 'boolean':
          row[name] = Boolean(value);
          break;
        case 'json':
          row[name] = JSON.parse(String(value));
          break;
        case 'bigint':
        case 'integer':
          row[name] = Number(value);
          break;
        case 'text':
          row[name] = String(value).slice(TEXT_PREFIX.length);
          break;
        default:
          row[name] = String(value);
      }
    }
    return row as T;
  }

  /** Builds `WHERE` SQL (without the keyword). `{}` yields a match-all clause. */
  #equal(column: string, spec: CollectionColumnSpec, bind: string): string {
    if (spec.type === 'json') return `JSON_EQUAL("${column}", JSON(${bind}))`;
    if (isClob(spec)) return `DBMS_LOB.COMPARE("${column}", ${bind}) = 0`;
    return `"${column}" = ${bind}`;
  }

  #orderedColumn(schema: CollectionSchema, column: string): string {
    return isClob(this.#column(schema, column))
      ? `CASE WHEN "${column}" IS NULL THEN NULL ELSE JSON_SCALAR("${column}") END`
      : `"${column}"`;
  }

  #buildWhere(
    schema: CollectionSchema,
    where: OracleCollectionWhere,
    startIndex = 1,
  ): { sql: string; args: unknown[]; nextIndex: number } {
    if (Array.isArray(where)) return this.#buildAuthWhere(schema, where, startIndex);
    const clauses: string[] = [];
    const args: unknown[] = [];
    let index = startIndex;
    for (const [column, condition] of Object.entries(where)) {
      const spec = this.#column(schema, column);
      if (condition !== null && typeof condition === 'object' && !(condition instanceof Date) && 'in' in condition) {
        if (condition.in.length === 0) {
          clauses.push('1 = 0');
          continue;
        }
        const nonNull = condition.in.filter(value => value !== null);
        const includesNull = nonNull.length !== condition.in.length;
        const inClause =
          nonNull.length > 0
            ? isClob(spec) || spec.type === 'json'
              ? `(${nonNull.map(() => this.#equal(column, spec, `:v${index++}`)).join(' OR ')})`
              : `"${column}" IN (${nonNull.map(() => `:v${index++}`).join(', ')})`
            : undefined;
        clauses.push(
          inClause && includesNull ? `(${inClause} OR "${column}" IS NULL)` : (inClause ?? `"${column}" IS NULL`),
        );
        args.push(...nonNull.map(value => this.#serialize(spec, value)));
      } else if (condition === null) {
        clauses.push(`"${column}" IS NULL`);
      } else {
        clauses.push(this.#equal(column, spec, `:v${index++}`));
        args.push(this.#serialize(spec, condition));
      }
    }
    return { sql: clauses.length > 0 ? clauses.join(' AND ') : '1 = 1', args, nextIndex: index };
  }

  /** Better Auth's predicates stay local to this adapter; Factory's public equality contract is unchanged. */
  #buildAuthWhere(schema: CollectionSchema, where: OracleAuthWhere[], startIndex: number) {
    const args: unknown[] = [];
    const bind = (value: unknown) => {
      args.push(value);
      return `:v${startIndex + args.length - 1}`;
    };
    const predicate = (clause: OracleAuthWhere): string => {
      const spec = this.#column(schema, clause.field);
      const column = `"${clause.field}"`;
      const insensitive = clause.mode === 'insensitive' && spec.type === 'text';
      const fold = (expression: string) => (insensitive ? `LOWER(${expression})` : expression);
      if (clause.operator === 'in' || clause.operator === 'not_in') {
        if (!Array.isArray(clause.value)) throw new Error('Auth membership predicates require an array');
        if (!clause.value.length) return clause.operator === 'in' ? '1 = 0' : '1 = 1';
        const match = clause.value.map(value => predicate({ ...clause, operator: 'eq', value })).join(' OR ');
        return clause.operator === 'in' ? `(${match})` : `NOT (${match})`;
      }
      if (clause.value === null) return `${column} IS ${clause.operator === 'ne' ? 'NOT ' : ''}NULL`;
      if (clause.operator === 'contains' || clause.operator === 'starts_with' || clause.operator === 'ends_with') {
        if (typeof clause.value !== 'string' || spec.type !== 'text')
          throw new Error('Auth text predicate requires text');
        if (clause.value === '') return `${column} IS NOT NULL`;
        const value = fold(bind(clobBind(clause.value)));
        const text = fold(`SUBSTR(${column}, ${TEXT_PREFIX.length + 1})`);
        if (clause.operator === 'contains') return `INSTR(${text}, ${value}) > 0`;
        if (clause.operator === 'starts_with') return `INSTR(${text}, ${value}) = 1`;
        return `DBMS_LOB.COMPARE(TO_CLOB(SUBSTR(${text}, -LENGTH(${value}))), ${value}) = 0`;
      }
      const value = fold(bind(this.#serialize(spec, clause.value)));
      if (clause.operator === 'eq' || clause.operator === 'ne') {
        const equal =
          spec.type === 'json'
            ? `JSON_EQUAL(${column}, JSON(${value}))`
            : isClob(spec)
              ? `DBMS_LOB.COMPARE(${fold(column)}, ${value}) = 0`
              : `${fold(column)} = ${value}`;
        return clause.operator === 'ne' ? `NOT (${equal})` : equal;
      }
      const operator = { gt: '>', gte: '>=', lt: '<', lte: '<=' }[clause.operator];
      if (!operator) throw new Error(`Unsupported auth predicate: ${clause.operator}`);
      return isClob(spec)
        ? `JSON_SCALAR(${fold(column)}) ${operator} JSON_SCALAR(${value})`
        : `${fold(column)} ${operator} ${value}`;
    };
    // Better Auth evaluates connector lists from left to right; explicit parentheses preserve that contract.
    const sql = where.reduce((previous, clause) => {
      const next = predicate(clause);
      return previous ? `(${previous} ${clause.connector === 'OR' ? 'OR' : 'AND'} ${next})` : next;
    }, '');
    return { sql: sql || '1 = 1', args, nextIndex: startIndex + args.length };
  }

  /** Keyset condition: rows strictly after `cursor.values` in the `orderBy` order. */
  #buildCursor(
    schema: CollectionSchema,
    orderBy: NonNullable<CollectionListOptions['orderBy']>,
    values: CollectionValue[],
    startIndex: number,
  ): { sql: string; args: unknown[]; nextIndex: number } {
    if (values.length !== orderBy.length) {
      throw new Error(
        `OracleFactoryStorage: cursor has ${values.length} values but orderBy has ${orderBy.length} columns`,
      );
    }
    const branches: string[] = [];
    const args: unknown[] = [];
    let index = startIndex;
    for (let i = 0; i < orderBy.length; i++) {
      const [column, dir] = orderBy[i]!;
      // Native ordering is ASC NULLS LAST / DESC NULLS FIRST.
      if (values[i] === null && dir !== 'desc') continue;
      const parts: string[] = [];
      for (let j = 0; j < i; j++) {
        const [column] = orderBy[j]!;
        if (values[j] === null) {
          parts.push(`"${column}" IS NULL`);
          continue;
        }
        parts.push(this.#equal(column, this.#column(schema, column), `:v${index++}`));
        args.push(this.#serialize(this.#column(schema, column), values[j]));
      }
      if (values[i] === null) {
        parts.push(`"${column}" IS NOT NULL`);
        branches.push(`(${parts.join(' AND ')})`);
        continue;
      }
      const bind = `:v${index++}`;
      parts.push(
        `(${this.#orderedColumn(schema, column)} ${dir === 'desc' ? '<' : '>'} ${isClob(this.#column(schema, column)) ? `JSON_SCALAR(${bind})` : bind}${dir === 'desc' ? '' : ` OR "${column}" IS NULL`})`,
      );
      args.push(this.#serialize(this.#column(schema, column), values[i]));
      branches.push(`(${parts.join(' AND ')})`);
    }
    return { sql: branches.length ? `(${branches.join(' OR ')})` : '1 = 0', args, nextIndex: index };
  }

  #buildSelect(
    schema: CollectionSchema,
    where: OracleCollectionWhere,
    opts?: OracleCollectionListOptions,
    forUpdate = false,
  ): { sql: string; args: unknown[] } {
    const filter = this.#buildWhere(schema, where);
    const projection = Object.entries(schema.columns)
      .map(([name, spec]) =>
        spec.type === 'json' ? `JSON_SERIALIZE("${name}" RETURNING CLOB) AS "${name}"` : `"${name}"`,
      )
      .join(', ');
    let sql = `SELECT ${projection} FROM ${this.#table(schema.name)} WHERE ${filter.sql}`;
    const args = [...filter.args];
    let index = filter.nextIndex;

    if (opts?.cursor) {
      if (!opts.orderBy || opts.orderBy.length === 0) {
        throw new Error('OracleFactoryStorage: cursor pagination requires orderBy');
      }
      const cursor = this.#buildCursor(schema, opts.orderBy, opts.cursor.values, index);
      sql += ` AND ${cursor.sql}`;
      args.push(...cursor.args);
      index = cursor.nextIndex;
    }
    if (opts?.orderBy && opts.orderBy.length > 0) {
      const order = opts.orderBy
        .map(([column, dir]) => {
          this.#column(schema, column);
          return `${this.#orderedColumn(schema, column)} ${dir === 'desc' ? 'DESC' : 'ASC'}`;
        })
        .join(', ');
      sql += ` ORDER BY ${order}`;
    }
    if (opts?.offset !== undefined) {
      if (!Number.isSafeInteger(opts.offset) || opts.offset < 0 || forUpdate) throw new Error('Invalid auth offset');
      sql += ` OFFSET :v${index++} ROWS`;
      args.push(opts.offset);
    }
    if (opts?.limit !== undefined) {
      if (!Number.isSafeInteger(opts.limit) || opts.limit < 0)
        throw new Error('OracleFactoryStorage: limit must be a non-negative safe integer');
      sql += forUpdate ? ` AND ROWNUM <= :v${index++}` : ` FETCH FIRST :v${index++} ROWS ONLY`;
      args.push(opts.limit);
    }
    if (forUpdate) sql += ' FOR UPDATE';
    return { sql, args };
  }

  async #select<T extends Record<string, unknown>>(
    queryable: Queryable,
    collection: string,
    where: OracleCollectionWhere,
    opts?: OracleCollectionListOptions,
    forUpdate = false,
  ): Promise<T[]> {
    const schema = this.#schema(collection);
    const { sql, args } = this.#buildSelect(schema, where, opts, forUpdate);
    try {
      const result = await queryable.query(sql, args);
      return result.rows.map(raw => this.#deserializeRow<T>(schema, raw));
    } catch (error) {
      if (
        !isOracleErrorCode(error, [-40180, -22835]) ||
        !opts?.orderBy?.some(([column]) => isClob(this.#column(schema, column)))
      )
        throw error;
      // Very large CLOBs exceed Oracle's JSON scalar comparison representation.
      // Keep their full values, using the same database filters before ordering the result.
      const all = this.#buildSelect(schema, where, undefined, forUpdate);
      let rows = (await queryable.query(all.sql, all.args)).rows.map(row => this.#deserializeRow<T>(schema, row));
      const compare = (left: Record<string, unknown>, right: Record<string, unknown>) => {
        for (const [column, direction] of opts.orderBy!) {
          const order = compareValues(left[column], right[column]);
          if (order) return direction === 'desc' ? -order : order;
        }
        return 0;
      };
      rows.sort(compare);
      if (opts.cursor) {
        const cursor = Object.fromEntries(opts.orderBy.map(([column], index) => [column, opts.cursor!.values[index]]));
        rows = rows.filter(row => compare(row, cursor) > 0);
      }
      return opts.limit === undefined
        ? rows.slice(opts.offset ?? 0)
        : rows.slice(opts.offset ?? 0, (opts.offset ?? 0) + opts.limit);
    }
  }

  async findOne<T extends Record<string, unknown>>(
    collection: string,
    where: OracleCollectionWhere,
  ): Promise<T | null> {
    const rows = await this.#select<T>(this.#queryable, collection, where, { limit: 1 });
    return rows[0] ?? null;
  }

  async findMany<T extends Record<string, unknown>>(
    collection: string,
    where: OracleCollectionWhere,
    opts?: OracleCollectionListOptions,
  ): Promise<T[]> {
    return this.#select<T>(this.#queryable, collection, where, opts);
  }

  async count(collection: string, where: OracleCollectionWhere): Promise<number> {
    const schema = this.#schema(collection);
    const filter = this.#buildWhere(schema, where);
    const result = await this.#queryable.query(
      `SELECT COUNT(*) AS "count" FROM ${this.#table(schema.name)} WHERE ${filter.sql}`,
      filter.args,
    );
    return Number(result.rows[0]?.count ?? 0);
  }

  async insertOne<T extends Record<string, unknown>>(collection: string, row: Partial<T>): Promise<T> {
    if (!this.#transactionClient) return this.#write(ops => ops.insertOne(collection, row));
    const schema = this.#schema(collection);
    const pk = primaryKeyOf(schema);

    const values: Record<string, unknown> = { ...row };
    if (schema.columns[pk]!.type === 'uuid-pk' && values[pk] === undefined) {
      values[pk] = globalThis.crypto.randomUUID();
    }

    const columns = Object.keys(values).filter(column => values[column] !== undefined);
    for (const column of columns) this.#column(schema, column);

    const sql = `INSERT INTO ${this.#table(schema.name)} (${columns.map(c => `"${c}"`).join(', ')}) VALUES (${columns
      .map((column, i) => (schema.columns[column]!.type === 'json' ? `JSON(:v${i + 1})` : `:v${i + 1}`))
      .join(', ')})`;
    const args = columns.map(column => this.#serialize(this.#column(schema, column), values[column]));

    try {
      await this.#queryable.query(sql, args);
      const inserted = await this.findOne<T>(collection, { [pk]: values[pk] as CollectionValue });
      if (!inserted) throw new Error(`OracleFactoryStorage: inserted row in '${collection}' was not found`);
      return inserted;
    } catch (error) {
      if (isUniqueViolation(error)) throw new UniqueViolationError(collection, { cause: error });
      throw error;
    }
  }

  async upsertOne<T extends Record<string, unknown>>(
    collection: string,
    conflictKeys: string[],
    row: Partial<T>,
  ): Promise<T> {
    if (!this.#transactionClient) return this.#write(ops => ops.upsertOne(collection, conflictKeys, row));
    const schema = this.#schema(collection);
    const pk = primaryKeyOf(schema);
    const unique = assertUpsertConflict(schema, conflictKeys, row);
    const keyWhere = Object.fromEntries(conflictKeys.map(key => [key, row[key] as CollectionValue])) as CollectionWhere;
    if ('whereNull' in unique && unique.whereNull) keyWhere[unique.whereNull] = null;

    let lastError: unknown;
    for (let attempt = 0; attempt < 2; attempt++) {
      const candidates = await this.#select<T>(this.#queryable, collection, keyWhere, undefined, true);
      const existing = candidates.find(
        candidate => !('whereNotNull' in unique && unique.whereNotNull) || candidate[unique.whereNotNull] != null,
      );
      if (existing) {
        const set = Object.fromEntries(
          Object.entries(row).filter(
            ([column, value]) => value !== undefined && column !== pk && !conflictKeys.includes(column),
          ),
        );
        if (Object.keys(set).length > 0) {
          await this.updateMany(collection, { [pk]: existing[pk] as CollectionValue }, set);
        }
        const updated = await this.findOne<T>(collection, { [pk]: existing[pk] as CollectionValue });
        if (!updated) continue; // deleted concurrently; retry
        return updated;
      }
      try {
        return await this.insertOne<T>(collection, row);
      } catch (error) {
        if (!(error instanceof UniqueViolationError)) throw error;
        lastError = error; // lost an insert race; retry as update
      }
    }
    throw lastError ?? new Error(`OracleFactoryStorage: upsert into '${collection}' did not converge`);
  }

  async updateMany(collection: string, where: OracleCollectionWhere, set: Record<string, unknown>): Promise<number> {
    return this.#write(ops => ops.#updateMany(ops.#queryable, collection, where, set));
  }

  async #updateMany(
    queryable: Queryable,
    collection: string,
    where: OracleCollectionWhere,
    set: Record<string, unknown>,
  ): Promise<number> {
    const schema = this.#schema(collection);
    const columns = Object.keys(set).filter(column => set[column] !== undefined);
    if (columns.length === 0) return 0;
    const assignments = columns
      .map(
        (column, i) =>
          `"${column}" = ${this.#column(schema, column).type === 'json' ? `JSON(:v${i + 1})` : `:v${i + 1}`}`,
      )
      .join(', ');
    const filter = this.#buildWhere(schema, where, columns.length + 1);
    const args = [...columns.map(column => this.#serialize(this.#column(schema, column), set[column])), ...filter.args];
    try {
      const result = await queryable.query(
        `UPDATE ${this.#table(schema.name)} SET ${assignments} WHERE ${filter.sql}`,
        args,
      );
      return result.rowCount ?? 0;
    } catch (error) {
      if (isUniqueViolation(error)) throw new UniqueViolationError(collection, { cause: error });
      throw error;
    }
  }

  async deleteMany(collection: string, where: OracleCollectionWhere): Promise<number> {
    if (!this.#transactionClient) return this.#write(ops => ops.deleteMany(collection, where));
    const schema = this.#schema(collection);
    const filter = this.#buildWhere(schema, where);
    const result = await this.#queryable.query(
      `DELETE FROM ${this.#table(schema.name)} WHERE ${filter.sql}`,
      filter.args,
    );
    return result.rowCount ?? 0;
  }

  async updateAtomic<T extends Record<string, unknown>>(
    collection: string,
    where: OracleCollectionWhere,
    fn: (row: T) => Partial<T> | null | Promise<Partial<T> | null>,
  ): Promise<T | null> {
    const run = async (connection: Connection): Promise<T | null> => {
      const client = createQueryable(connection);
      const schema = this.#schema(collection);
      const pk = primaryKeyOf(schema);
      const rows = await this.#select<T>(client, collection, where, { limit: 1 }, true);
      const row = rows[0];
      if (!row) return null;
      if (Array.isArray(where)) {
        const guarded = [
          ...where,
          {
            field: pk,
            value: row[pk] as string,
            operator: 'eq' as const,
            connector: 'AND' as const,
            mode: 'sensitive' as const,
          },
        ];
        if (!(await this.#select(client, collection, guarded, { limit: 1 })).length) return null;
      }
      const patch = await fn(row);
      if (patch === null) return row;
      const pkWhere = { [pk]: row[pk] as CollectionValue } as CollectionWhere;
      await this.#updateMany(client, collection, pkWhere, patch);
      const updated = await this.#select<T>(client, collection, pkWhere, { limit: 1 });
      return updated[0] ?? null;
    };

    if (this.#transactionClient) return run(this.#transactionClient);

    return transaction(this.#manager, run);
  }

  /** Atomic destructive read for single-use auth verification values. */
  async consumeOne(collection: string, where: OracleAuthWhere[]): Promise<Record<string, unknown> | null> {
    if (!where.length) return null;
    return this.#write(async ops => {
      const schema = ops.#schema(collection);
      const pk = primaryKeyOf(schema);
      const row = (await ops.#select(ops.#queryable, collection, where, { limit: 1 }, true))[0];
      if (!row) return null;
      const guarded: OracleAuthWhere[] = [
        ...where,
        { field: pk, value: row[pk] as string, operator: 'eq', connector: 'AND', mode: 'sensitive' },
      ];
      if (!(await ops.findOne(collection, guarded))) return null;
      await ops.deleteMany(collection, { [pk]: row[pk] as CollectionValue });
      return row;
    });
  }
}

/** Oracle backend for Mastra agent state and application-owned collections, sharing one pool. */
export class OracleFactoryStorage extends FactoryStorage {
  readonly ops: FactoryStorageOps;
  readonly authDatabase: () => FactoryAuthDatabase;
  readonly #store: OracleStore;
  readonly #ownsStore: boolean;
  readonly #schemas = new Map<string, CollectionSchema>();
  readonly #table: (collection: string) => string;

  constructor(config: OracleFactoryStorageConfig) {
    super();
    this.#ownsStore = !('store' in config);
    this.#store = 'store' in config ? config.store : new OracleStore({ ...config, id: config.id ?? 'oracle-factory' });
    this.#table = name => this.#store.db.table(name);
    this.ops = new OracleFactoryStorageOps(this.#store.getPoolManager(), this.#schemas, this.#table);
    const database = config.authDatabase ?? createOracleAuthAdapter(this);
    this.authDatabase = () => ({ dialect: 'custom', database });
  }
  getMastraStorage(): OracleStore {
    return this.#store;
  }
  protected async initStorage(): Promise<void> {
    await this.#store.db.one(`SELECT 1 AS "ready" FROM dual`);
  }

  async withTransaction<T>(
    fn: (ops: FactoryStorageOps) => Promise<T>,
    options?: { isolationLevel?: 'serializable' },
  ): Promise<T> {
    return transaction(this.#store.getPoolManager(), async connection => {
      if (options?.isolationLevel === 'serializable' && this.#schemas.size) {
        // Oracle's SERIALIZABLE isolation is snapshot isolation and permits write skew.
        // Lock registered app collections in a stable order to enforce the Factory contract,
        // including predicates about rows that do not exist yet. Agent-state tables stay independent.
        const tables = [...this.#schemas.keys()].sort().map(this.#table);
        await connection.execute(`LOCK TABLE ${tables.join(', ')} IN EXCLUSIVE MODE`);
      }
      return fn(new OracleFactoryStorageOps(this.#store.getPoolManager(), this.#schemas, this.#table, connection));
    });
  }
  async ensureCollections(schemas: CollectionSchema[]): Promise<void> {
    for (const schema of schemas) {
      await this.#ensureCollection(schema);
      this.#schemas.set(schema.name, schema);
    }
  }
  async close(): Promise<void> {
    if (this.#ownsStore) await this.#store.close();
  }

  #columnDdl(name: string, spec: CollectionColumnSpec): string {
    assertIdentifier('column', name);
    let ddl = `"${name}" ${spec.type === 'text' && spec.primaryKey ? 'VARCHAR2(4000)' : COLUMN_DDL[spec.type]}`;
    if (!COLUMN_DDL[spec.type]) throw new Error(`OracleFactoryStorage: unsupported type '${spec.type}'`);
    if (spec.default !== undefined) {
      const value = spec.type === 'text' ? TEXT_PREFIX + String(spec.default) : spec.default;
      if (spec.type === 'timestamp') {
        if (typeof value === 'boolean') throw new Error('A timestamp default must be a date string or epoch number');
        const instant = new Date(value).toISOString().replace('Z', '+00:00');
        ddl += ` DEFAULT TO_TIMESTAMP_TZ('${instant}', 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM')`;
      } else {
        ddl += ` DEFAULT ${typeof value === 'string' ? "'" + value.replaceAll("'", "''") + "'" : typeof value === 'boolean' ? Number(value) : value}`;
      }
    }
    if (spec.type === 'uuid-pk' || spec.primaryKey) ddl += ' PRIMARY KEY';
    else if (!spec.nullable) ddl += ' NOT NULL';
    return ddl;
  }
  async #ensureCollection(schema: CollectionSchema): Promise<void> {
    assertIdentifier('collection', schema.name);
    primaryKeyOf(schema);
    const db = this.#store.db;
    const columns = Object.entries(schema.columns).map(([name, spec]) => this.#columnDdl(name, spec));
    // DDL commits independently in Oracle; it is idempotent and only expands the declared schema.
    await db.executeDdl(`CREATE TABLE ${this.#table(schema.name)} (${columns.join(', ')})`, [-955]);
    const owner = this.#table(schema.name).includes('.')
      ? this.#table(schema.name).split('.')[0]!.replaceAll('"', '')
      : undefined;
    const existing = await db.manyOrNone<{ name: string; nullable: string; precision: number | null }>(
      `SELECT column_name AS "name", nullable AS "nullable", data_precision AS "precision" FROM all_tab_columns WHERE table_name = :tableName AND owner = NVL(:owner, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))`,
      { tableName: normalizeIdentifier(schema.name, 'collection'), owner: owner ?? null },
    );
    const byName = new Map(existing.map(column => [column.name, column]));
    for (const [name, spec] of Object.entries(schema.columns)) {
      const current = byName.get(name);
      if (!current)
        await db.executeDdl(`ALTER TABLE ${this.#table(schema.name)} ADD (${this.#columnDdl(name, spec)})`, [-1430]);
      else {
        if (spec.nullable && current.nullable === 'N' && spec.type !== 'uuid-pk' && !spec.primaryKey)
          await db.executeDdl(`ALTER TABLE ${this.#table(schema.name)} MODIFY ("${name}" NULL)`, [-1451]);
        if (spec.type === 'bigint' && current.precision !== null && current.precision < 20)
          await db.executeDdl(`ALTER TABLE ${this.#table(schema.name)} MODIFY ("${name}" NUMBER(20))`);
      }
    }
    const declared = (name: string) => {
      assertIdentifier('column', name);
      if (!Object.hasOwn(schema.columns, name)) throw new Error(`OracleFactoryStorage: unknown index column '${name}'`);
      return `"${name}"`;
    };
    for (const index of schema.uniqueIndexes ?? []) {
      assertIdentifier('index', index.name);
      if (!index.columns.length || (index.whereNotNull && index.whereNull))
        throw new Error('OracleFactoryStorage: invalid unique index');
      const guards: string[] = [];
      // PostgreSQL unique indexes treat each NULL as distinct, including composite keys.
      // Oracle's composite NULL handling differs, so omit any row with a NULL key from this index.
      for (const column of index.columns) guards.push(`${declared(column)} IS NOT NULL`);
      if (index.whereNotNull) guards.push(`${declared(index.whereNotNull)} IS NOT NULL`);
      if (index.whereNull) guards.push(`${declared(index.whereNull)} IS NULL`);
      const expressions = index.columns.map(
        column =>
          `CASE WHEN ${guards.join(' AND ')} THEN ${isClob(schema.columns[column]!) ? `CAST(${declared(column)} AS VARCHAR2(4000))` : declared(column)} END`,
      );
      await db.executeDdl(
        `CREATE UNIQUE INDEX ${qualifyName(index.name, owner)} ON ${this.#table(schema.name)} (${expressions.join(', ')})`,
        [-955],
      );
    }
    for (const index of schema.indexes ?? []) {
      assertIdentifier('index', index.name);
      await db.executeDdl(
        `CREATE INDEX ${qualifyName(index.name, owner)} ON ${this.#table(schema.name)} (${index.columns.map(column => (isClob(schema.columns[column]!) ? `CAST(${declared(column)} AS VARCHAR2(4000))` : declared(column))).join(', ')})`,
        [-955],
      );
    }
  }
}
