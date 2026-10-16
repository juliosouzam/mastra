import * as coreStorage from '@mastra/core/storage';
import oracledb from 'oracledb';
import type { BindParameters } from 'oracledb';

import { safeJsonStringify } from '../../../../shared/connection';
import type { DbClient } from './client';
import {
  CURSOR_SEQUENCE,
  rawTable,
  SIGNAL_COLUMNS,
  SIGNAL_KEYS,
  TABLE_SPAN_EVENTS,
  TABLE_FEEDBACK_EVENTS,
} from './ddl';
import type { SignalTable } from './ddl';
import {
  buildNamedSelectColumns,
  buildSelectColumns,
  SPAN_EVENT_COLUMNS,
  METRIC_EVENT_COLUMNS,
  LOG_EVENT_COLUMNS,
  SCORE_EVENT_COLUMNS,
  FEEDBACK_EVENT_COLUMNS,
  SPAN_LIGHT_SELECT_COLUMN_NAMES,
  LARGE_TEXT_COLUMNS,
} from './signal-schema';
import type { SignalColumn } from './signal-schema';

export class EncodedJsonb {
  constructor(readonly json: string) {}
}
export function encodeJsonbValue(value: unknown): EncodedJsonb | null {
  return value == null ? null : value instanceof EncodedJsonb ? value : new EncodedJsonb(safeJsonStringify(value));
}

function searchText(row: Record<string, unknown>): Record<string, unknown> {
  const words: Record<string, string> = Object.create(null);
  for (const key of ['name', 'comment'])
    if (typeof row[key] === 'string') words[key] = ` ${coreStorage.normalizeTraceQueryText(row[key])} `;
  const raw = row.metadataRaw instanceof EncodedJsonb ? JSON.parse(row.metadataRaw.json) : row.metadataRaw;
  const metadataKeys =
    raw && typeof raw === 'object' && !Array.isArray(raw)
      ? Object.entries(raw).flatMap(([key, value]) =>
          key !== '' &&
          !key.includes('.') &&
          typeof value === 'string' &&
          value.trim() !== '' &&
          Buffer.byteLength('metadata.' + key) <= coreStorage.TRACE_QUERY_MAX_PATH_BYTES &&
          Buffer.byteLength(value) <= coreStorage.TRACE_QUERY_MAX_STRING_BYTES
            ? ['metadata.' + key]
            : [],
        )
      : [];
  return { words, metadataKeys };
}

/** Atomic batches use the existing Oracle executeMany path, including duplicate insert recovery. */
export async function writeSignals(
  client: DbClient,
  schema: string,
  table: SignalTable,
  records: Record<string, unknown>[],
  conflict: 'ignore' | 'replace' | 'span' = 'ignore',
): Promise<void> {
  if (!records.length) return;
  if (([TABLE_SPAN_EVENTS, TABLE_FEEDBACK_EVENTS] as readonly string[]).includes(table))
    records = records.map(row => {
      const record = table === TABLE_SPAN_EVENTS ? { ...row, metadataRaw: encodeJsonbValue(row.metadataRaw) } : row;
      return { ...record, textSearch: searchText(record) };
    });
  const definition = new Map<string, SignalColumn>(SIGNAL_COLUMNS[table].map(column => [column.name, column]));
  const keys = SIGNAL_KEYS[table];
  await client.tx(async tx => {
    for (let start = 0; start < records.length; start += 200) {
      const batch = records.slice(start, start + 200);
      const columns = Object.keys(batch[0]!);
      const bindDefs: Record<string, oracledb.BindDefinition> = {};
      const expressions: string[] = [];
      for (const [index, name] of columns.entries()) {
        const column = definition.get(name);
        if (!column || ['xactId', 'cursorId'].includes(name)) throw new Error(`Invalid signal column: ${name}`);
        const bind = `v${index}`;
        if (column.type === 'jsonb' || column.type === 'text[]') {
          expressions.push(`JSON(:${bind})`);
          bindDefs[bind] = { type: oracledb.DB_TYPE_CLOB };
        } else if (column.type === 'timestamp') {
          expressions.push(`:${bind}`);
          bindDefs[bind] = { type: oracledb.DB_TYPE_TIMESTAMP_TZ };
        } else if (column.type === 'boolean' || column.type === 'float') {
          expressions.push(`:${bind}`);
          bindDefs[bind] = { type: column.type === 'boolean' ? oracledb.NUMBER : oracledb.DB_TYPE_BINARY_DOUBLE };
        } else if (LARGE_TEXT_COLUMNS.has(name)) {
          expressions.push(`CASE WHEN :${bind}Null = 1 THEN NULL ELSE NVL(:${bind}, EMPTY_CLOB()) END`);
          bindDefs[bind] = { type: oracledb.DB_TYPE_CLOB };
          bindDefs[`${bind}Null`] = { type: oracledb.NUMBER };
        } else {
          expressions.push(`:${bind}`);
          bindDefs[bind] = {
            type: oracledb.STRING,
            maxSize: Math.max(1, ...batch.map(row => (row[name] == null ? 0 : Buffer.byteLength(String(row[name]))))),
          };
        }
      }
      const rows: BindParameters[] = batch.map(row => {
        const values: Record<string, unknown> = {};
        for (const [index, name] of columns.entries()) {
          const column = definition.get(name)!;
          const value = row[name];
          const bind = `v${index}`;
          if (column.type === 'jsonb' || column.type === 'text[]')
            values[bind] = value == null ? null : value instanceof EncodedJsonb ? value.json : safeJsonStringify(value);
          else if (column.type === 'timestamp')
            values[bind] = value == null ? null : value instanceof Date ? value : new Date(value as string | number);
          else if (column.type === 'boolean') values[bind] = Number(Boolean(value));
          else if (column.type === 'float') values[bind] = value == null ? null : Number(value);
          else {
            values[bind] = value == null ? null : String(value);
            if (LARGE_TEXT_COLUMNS.has(name)) values[`${bind}Null`] = Number(value == null);
          }
        }
        return values as BindParameters;
      });
      const insert = `INSERT INTO ${rawTable(schema, table)} (${columns.map(name => `"${name}"`).join(', ')}) VALUES (${expressions.join(', ')})`;
      let recovery = 'NULL';
      if (conflict !== 'ignore') {
        const assignments = columns.flatMap((name, index) =>
          keys.includes(name) ? [] : [`"${name}" = ${expressions[index]}`],
        );
        assignments.push(`"cursorId" = ${rawTable(schema, CURSOR_SEQUENCE)}.NEXTVAL`);
        const where = keys.map(name => `"${name}" = :v${columns.indexOf(name)}`).join(' AND ');
        const guard = conflict === 'span' ? ` AND "isPending" = 1 AND :v${columns.indexOf('isPending')} = 0` : '';
        recovery = `UPDATE ${rawTable(schema, table)} SET ${assignments.join(', ')} WHERE ${where}${guard}`;
      }
      await tx.batch(`BEGIN ${insert}; EXCEPTION WHEN DUP_VAL_ON_INDEX THEN ${recovery}; END;`, rows, { bindDefs });
    }
  });
}

export const SPAN_COLLAPSE_ORDER = '"isPending" ASC, "endedAt" DESC, "cursorId" DESC';
export const SPAN_SELECT_COLUMNS = buildSelectColumns(SPAN_EVENT_COLUMNS);
export const SPAN_LIGHT_SELECT_COLUMNS = buildNamedSelectColumns(SPAN_LIGHT_SELECT_COLUMN_NAMES);
export const METRIC_SELECT_COLUMNS = buildSelectColumns(METRIC_EVENT_COLUMNS);
export const LOG_SELECT_COLUMNS = buildSelectColumns(LOG_EVENT_COLUMNS);
export const SCORE_SELECT_COLUMNS = buildSelectColumns(SCORE_EVENT_COLUMNS);
export const FEEDBACK_SELECT_COLUMNS = buildSelectColumns(FEEDBACK_EVENT_COLUMNS);
