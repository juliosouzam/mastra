import { qualifyName } from '../../../../vector/identifiers';
import { filterIndexesForTables } from '../../../db';
import type { OracleCreateIndexOptions } from '../../../db';
import {
  buildColumnDefinitions,
  SPAN_EVENT_COLUMNS,
  METRIC_EVENT_COLUMNS,
  LOG_EVENT_COLUMNS,
  SCORE_EVENT_COLUMNS,
  FEEDBACK_EVENT_COLUMNS,
} from './signal-schema';

// Separate names preserve legacy observability when both modes use one Oracle schema.
export const TABLE_SPAN_EVENTS = 'MASTRA_OBS_SPAN_EVENTS';
export const TABLE_METRIC_EVENTS = 'MASTRA_OBS_METRIC_EVENTS';
export const TABLE_LOG_EVENTS = 'MASTRA_OBS_LOG_EVENTS';
export const TABLE_SCORE_EVENTS = 'MASTRA_OBS_SCORE_EVENTS';
export const TABLE_FEEDBACK_EVENTS = 'MASTRA_OBS_FEEDBACK_EVENTS';
export const TABLE_DISCOVERY = 'MASTRA_OBS_DISCOVERY';
export const CURSOR_SEQUENCE = 'MASTRA_OBS_EVENT_CURSOR';
export const ALL_SIGNAL_TABLES = [
  TABLE_SPAN_EVENTS,
  TABLE_METRIC_EVENTS,
  TABLE_LOG_EVENTS,
  TABLE_SCORE_EVENTS,
  TABLE_FEEDBACK_EVENTS,
] as const;
export const ALL_TABLE_NAMES = [...ALL_SIGNAL_TABLES, TABLE_DISCOVERY] as const;
export const SIGNAL_COLUMNS = {
  [TABLE_SPAN_EVENTS]: SPAN_EVENT_COLUMNS,
  [TABLE_METRIC_EVENTS]: METRIC_EVENT_COLUMNS,
  [TABLE_LOG_EVENTS]: LOG_EVENT_COLUMNS,
  [TABLE_SCORE_EVENTS]: SCORE_EVENT_COLUMNS,
  [TABLE_FEEDBACK_EVENTS]: FEEDBACK_EVENT_COLUMNS,
} as const;
export type SignalTable = keyof typeof SIGNAL_COLUMNS;
export const SIGNAL_KEYS: Record<SignalTable, readonly string[]> = {
  [TABLE_SPAN_EVENTS]: ['traceId', 'spanId', 'endedAt'],
  [TABLE_METRIC_EVENTS]: ['metricId', 'timestamp'],
  [TABLE_LOG_EVENTS]: ['logId', 'timestamp'],
  [TABLE_SCORE_EVENTS]: ['scoreId', 'timestamp'],
  [TABLE_FEEDBACK_EVENTS]: ['feedbackId', 'timestamp'],
};
export const SIGNAL_TIME_COLUMN: Record<SignalTable, string> = {
  [TABLE_SPAN_EVENTS]: 'endedAt',
  [TABLE_METRIC_EVENTS]: 'timestamp',
  [TABLE_LOG_EVENTS]: 'timestamp',
  [TABLE_SCORE_EVENTS]: 'timestamp',
  [TABLE_FEEDBACK_EVENTS]: 'timestamp',
};
export function rawTable(schema: string, table: string): string {
  return qualifyName(table, schema || undefined);
}
export function qualifiedTable(schema: string, table: string): string {
  if (!(ALL_SIGNAL_TABLES as readonly string[]).includes(table)) return rawTable(schema, table);
  // SCNs reflect commits, whereas sequence values can become visible out of order.
  return `(SELECT signal.*, ORA_ROWSCN AS "xactId" FROM ${rawTable(schema, table)} signal)`;
}
export function allTableDDL(schema: string): string[] {
  const sequence = rawTable(schema, CURSOR_SEQUENCE);
  return [
    `CREATE SEQUENCE ${sequence} START WITH 1 INCREMENT BY 1 MAXVALUE 18446744073709551615 CACHE 100 ORDER`,
    ...ALL_SIGNAL_TABLES.map(
      table =>
        `CREATE TABLE ${rawTable(schema, table)} (\n${buildColumnDefinitions(SIGNAL_COLUMNS[table], sequence)},\n PRIMARY KEY ("cursorId")\n) ROWDEPENDENCIES`,
    ),
    `CREATE TABLE ${rawTable(schema, TABLE_DISCOVERY)} ("cacheKey" VARCHAR2(512) PRIMARY KEY, "refreshedAt" TIMESTAMP WITH TIME ZONE NOT NULL, "values" JSON NOT NULL)`,
  ];
}
export function allIndexDDL(schema: string, skipDefaultIndexes?: boolean): string[] {
  return [
    // Oracle cannot use TIMESTAMP WITH TIME ZONE in a primary key. UTC expressions
    // preserve the same logical identity, including timestamps supplied with different offsets.
    ...ALL_SIGNAL_TABLES.map(
      table =>
        `CREATE UNIQUE INDEX ${rawTable(schema, `${table}_IDENTITY_IDX`)} ON ${rawTable(schema, table)} (${SIGNAL_KEYS[table].map(name => (name === SIGNAL_TIME_COLUMN[table] ? `SYS_EXTRACT_UTC("${name}")` : `"${name}"`)).join(', ')})`,
    ),
    ...(skipDefaultIndexes
      ? []
      : [
          ...ALL_SIGNAL_TABLES.map(
            table =>
              `CREATE INDEX ${rawTable(schema, `${table}_TIME_IDX`)} ON ${rawTable(schema, table)} ("${SIGNAL_TIME_COLUMN[table]}")`,
          ),
          ...ALL_SIGNAL_TABLES.map(
            table =>
              `CREATE INDEX ${rawTable(schema, `${table}_TRACE_IDX`)} ON ${rawTable(schema, table)} ("traceId", "spanId")`,
          ),
          ...[TABLE_SCORE_EVENTS, TABLE_FEEDBACK_EVENTS].map(
            table =>
              `CREATE INDEX ${rawTable(schema, `${table}_CURRENT_IDX`)} ON ${rawTable(schema, table)} ("${table === TABLE_SCORE_EVENTS ? 'scoreId' : 'feedbackId'}", "cursorId" DESC)`,
          ),
          `CREATE INDEX ${rawTable(schema, 'MASTRA_OBS_ROOT_SCOPE_IDX')} ON ${rawTable(schema, TABLE_SPAN_EVENTS)} (CASE WHEN "parentSpanId" IS NULL THEN "organizationId" END, CASE WHEN "parentSpanId" IS NULL THEN "resourceId" END, CASE WHEN "parentSpanId" IS NULL THEN "startedAt" END)`,
        ]),
  ];
}

export function customIndexes(indexes?: OracleCreateIndexOptions[]): OracleCreateIndexOptions[] {
  return filterIndexesForTables(indexes, ALL_TABLE_NAMES).map(index => {
    const table = index.table.toUpperCase();
    const names = new Set(
      table === TABLE_DISCOVERY
        ? ['cacheKey', 'refreshedAt', 'values']
        : (SIGNAL_COLUMNS[table as SignalTable]?.map(column => column.name) ?? []),
    );
    return {
      ...index,
      columns: index.columns.map(column => {
        const match = /^([A-Za-z][A-Za-z0-9_]*)(\s+(?:ASC|DESC))?$/i.exec(column.trim());
        return match && names.has(match[1]!) ? `"${match[1]}"${match[2] ?? ''}` : column;
      }),
    };
  });
}
