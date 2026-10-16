type ColumnType = 'sequence' | 'boolean' | 'float' | 'jsonb' | 'text' | 'text[]' | 'timestamp' | 'scn';

export interface SignalColumn {
  name: string;
  type: ColumnType;
  nullable?: boolean;
}

const CURSOR_ID_COLUMN = { name: 'cursorId', type: 'sequence' } as const satisfies SignalColumn;
// Read-only projection of ORA_ROWSCN; no physical transaction-ID column.
const XACT_ID_COLUMN = {
  name: 'xactId',
  type: 'scn',
} as const satisfies SignalColumn;
// Oracle regex patterns are byte-bounded; materialized words preserve Core's full Unicode matching contract.
const TEXT_SEARCH_COLUMN = { name: 'textSearch', type: 'jsonb', nullable: true } as const satisfies SignalColumn;

const COMMON_CONTEXT_COLUMNS = [
  { name: 'traceId', type: 'text', nullable: true },
  { name: 'spanId', type: 'text', nullable: true },
  { name: 'experimentId', type: 'text', nullable: true },
  { name: 'entityType', type: 'text', nullable: true },
  { name: 'entityId', type: 'text', nullable: true },
  { name: 'entityName', type: 'text', nullable: true },
  { name: 'entityVersionId', type: 'text', nullable: true },
  { name: 'parentEntityType', type: 'text', nullable: true },
  { name: 'parentEntityId', type: 'text', nullable: true },
  { name: 'parentEntityName', type: 'text', nullable: true },
  { name: 'parentEntityVersionId', type: 'text', nullable: true },
  { name: 'rootEntityType', type: 'text', nullable: true },
  { name: 'rootEntityId', type: 'text', nullable: true },
  { name: 'rootEntityName', type: 'text', nullable: true },
  { name: 'rootEntityVersionId', type: 'text', nullable: true },
  { name: 'userId', type: 'text', nullable: true },
  { name: 'organizationId', type: 'text', nullable: true },
  { name: 'resourceId', type: 'text', nullable: true },
  { name: 'runId', type: 'text', nullable: true },
  { name: 'sessionId', type: 'text', nullable: true },
  { name: 'threadId', type: 'text', nullable: true },
  { name: 'requestId', type: 'text', nullable: true },
  { name: 'environment', type: 'text', nullable: true },
  { name: 'executionSource', type: 'text', nullable: true },
  { name: 'serviceName', type: 'text', nullable: true },
] as const satisfies readonly SignalColumn[];

export const SPAN_EVENT_COLUMNS = [
  CURSOR_ID_COLUMN,
  XACT_ID_COLUMN,
  TEXT_SEARCH_COLUMN,
  { name: 'traceId', type: 'text' },
  { name: 'spanId', type: 'text' },
  { name: 'parentSpanId', type: 'text', nullable: true },
  { name: 'experimentId', type: 'text', nullable: true },
  { name: 'entityType', type: 'text', nullable: true },
  { name: 'entityId', type: 'text', nullable: true },
  { name: 'entityName', type: 'text', nullable: true },
  { name: 'entityVersionId', type: 'text', nullable: true },
  { name: 'parentEntityType', type: 'text', nullable: true },
  { name: 'parentEntityId', type: 'text', nullable: true },
  { name: 'parentEntityName', type: 'text', nullable: true },
  { name: 'parentEntityVersionId', type: 'text', nullable: true },
  { name: 'rootEntityType', type: 'text', nullable: true },
  { name: 'rootEntityId', type: 'text', nullable: true },
  { name: 'rootEntityName', type: 'text', nullable: true },
  { name: 'rootEntityVersionId', type: 'text', nullable: true },
  { name: 'userId', type: 'text', nullable: true },
  { name: 'organizationId', type: 'text', nullable: true },
  { name: 'resourceId', type: 'text', nullable: true },
  { name: 'runId', type: 'text', nullable: true },
  { name: 'sessionId', type: 'text', nullable: true },
  { name: 'threadId', type: 'text', nullable: true },
  { name: 'requestId', type: 'text', nullable: true },
  { name: 'environment', type: 'text', nullable: true },
  { name: 'executionSource', type: 'text', nullable: true },
  { name: 'serviceName', type: 'text', nullable: true },
  { name: 'name', type: 'text' },
  { name: 'spanType', type: 'text' },
  { name: 'isEvent', type: 'boolean' },
  // True for a span that has started but not yet ended. `endedAt` is part of
  // the required identity index, so it can never be NULL in storage;
  // this flag is what distinguishes a running span (whose `endedAt` column
  // holds a synthesized copy of `startedAt`) from a genuinely ended one.
  { name: 'isPending', type: 'boolean' },
  { name: 'startedAt', type: 'timestamp' },
  { name: 'endedAt', type: 'timestamp' },
  { name: 'tags', type: 'text[]' },
  { name: 'metadataSearch', type: 'jsonb' },
  { name: 'attributes', type: 'jsonb', nullable: true },
  { name: 'scope', type: 'jsonb', nullable: true },
  { name: 'links', type: 'jsonb', nullable: true },
  { name: 'input', type: 'jsonb', nullable: true },
  { name: 'output', type: 'jsonb', nullable: true },
  { name: 'error', type: 'jsonb', nullable: true },
  { name: 'metadataRaw', type: 'jsonb', nullable: true },
  { name: 'requestContext', type: 'jsonb', nullable: true },
  // Short previews of `input` / `output` / `error` in the shared `buildPreview` format, written
  // on every span so list reads skip the payloads. `''` means nothing to preview; NULL only means
  // the row was written before these columns existed (readers then build the preview from the
  // payload). Initialization adds missing preview columns to existing tables.
  { name: 'inputPreview', type: 'text', nullable: true },
  { name: 'outputPreview', type: 'text', nullable: true },
  { name: 'errorPreview', type: 'text', nullable: true },
] as const satisfies readonly SignalColumn[];

export const METRIC_EVENT_COLUMNS = [
  CURSOR_ID_COLUMN,
  XACT_ID_COLUMN,
  { name: 'metricId', type: 'text' },
  { name: 'timestamp', type: 'timestamp' },
  { name: 'name', type: 'text' },
  { name: 'value', type: 'float' },
  ...COMMON_CONTEXT_COLUMNS,
  { name: 'provider', type: 'text', nullable: true },
  { name: 'model', type: 'text', nullable: true },
  { name: 'estimatedCost', type: 'float', nullable: true },
  { name: 'costUnit', type: 'text', nullable: true },
  { name: 'tags', type: 'text[]' },
  { name: 'labels', type: 'jsonb' },
  { name: 'costMetadata', type: 'jsonb', nullable: true },
  { name: 'metadata', type: 'jsonb', nullable: true },
  { name: 'scope', type: 'jsonb', nullable: true },
] as const satisfies readonly SignalColumn[];

export const LOG_EVENT_COLUMNS = [
  CURSOR_ID_COLUMN,
  XACT_ID_COLUMN,
  { name: 'logId', type: 'text' },
  { name: 'timestamp', type: 'timestamp' },
  { name: 'level', type: 'text' },
  { name: 'message', type: 'text' },
  ...COMMON_CONTEXT_COLUMNS,
  { name: 'tags', type: 'text[]' },
  { name: 'data', type: 'jsonb', nullable: true },
  { name: 'metadata', type: 'jsonb', nullable: true },
  { name: 'scope', type: 'jsonb', nullable: true },
] as const satisfies readonly SignalColumn[];

export const SCORE_EVENT_COLUMNS = [
  CURSOR_ID_COLUMN,
  XACT_ID_COLUMN,
  { name: 'scoreId', type: 'text' },
  { name: 'timestamp', type: 'timestamp' },
  { name: 'scorerId', type: 'text' },
  { name: 'scorerVersion', type: 'text', nullable: true },
  { name: 'scoreSource', type: 'text', nullable: true },
  { name: 'score', type: 'float' },
  { name: 'reason', type: 'text', nullable: true },
  ...COMMON_CONTEXT_COLUMNS,
  { name: 'scoreTraceId', type: 'text', nullable: true },
  { name: 'tags', type: 'text[]' },
  { name: 'metadata', type: 'jsonb', nullable: true },
  { name: 'scope', type: 'jsonb', nullable: true },
] as const satisfies readonly SignalColumn[];

export const FEEDBACK_EVENT_COLUMNS = [
  CURSOR_ID_COLUMN,
  XACT_ID_COLUMN,
  TEXT_SEARCH_COLUMN,
  { name: 'feedbackId', type: 'text' },
  { name: 'timestamp', type: 'timestamp' },
  { name: 'feedbackSource', type: 'text' },
  { name: 'feedbackType', type: 'text' },
  { name: 'valueString', type: 'text', nullable: true },
  { name: 'valueNumber', type: 'float', nullable: true },
  { name: 'comment', type: 'text', nullable: true },
  { name: 'feedbackUserId', type: 'text', nullable: true },
  { name: 'sourceId', type: 'text', nullable: true },
  { name: 'reviewStatus', type: 'text' },
  ...COMMON_CONTEXT_COLUMNS,
  { name: 'tags', type: 'text[]' },
  { name: 'metadata', type: 'jsonb', nullable: true },
  { name: 'scope', type: 'jsonb', nullable: true },
] as const satisfies readonly SignalColumn[];

export const SPAN_LIGHT_SELECT_COLUMN_NAMES = [
  'cursorId',
  'xactId',
  'traceId',
  'spanId',
  'parentSpanId',
  'name',
  'entityType',
  'entityId',
  'entityName',
  'spanType',
  'error',
  'isEvent',
  'isPending',
  'startedAt',
  'endedAt',
] as const;

function quotedColumnName(name: string): string {
  return `"${name}"`;
}

export const LARGE_TEXT_COLUMNS = new Set([
  'message',
  'reason',
  'comment',
  'inputPreview',
  'outputPreview',
  'errorPreview',
]);
export function columnType(column: SignalColumn): string {
  switch (column.type) {
    case 'sequence':
    case 'scn':
      return 'NUMBER(20)';
    case 'boolean':
      return 'NUMBER(1)';
    case 'float':
      return 'BINARY_DOUBLE';
    case 'timestamp':
      return 'TIMESTAMP WITH TIME ZONE';
    case 'jsonb':
    case 'text[]':
      return 'JSON';
    case 'text':
      return LARGE_TEXT_COLUMNS.has(column.name)
        ? 'CLOB'
        : column.name.endsWith('Id')
          ? 'VARCHAR2(512)'
          : 'VARCHAR2(4000)';
  }
}
export function buildColumnDefinitions(columns: readonly SignalColumn[], sequence: string): string {
  return columns
    .filter(column => column.name !== 'xactId')
    .map(column => {
      const value = column.type === 'sequence' ? `${sequence}.NEXTVAL` : column.type === 'boolean' ? '0' : undefined;
      const nullable = column.nullable || column.name === 'feedbackSource';
      return `"${column.name}" ${columnType(column)}${value ? ` DEFAULT ${value}` : ''}${nullable ? '' : ' NOT NULL'}`;
    })
    .join(',\n');
}
export function buildNamedSelectColumns(names: readonly string[], prefix = ''): string {
  return names
    .map(name =>
      JSONB_COLUMNS.has(name) || TEXT_ARRAY_COLUMNS.has(name)
        ? `JSON_SERIALIZE(${prefix}"${name}" RETURNING CLOB) AS "${name}"`
        : `${prefix}"${name}" AS "${name}"`,
    )
    .join(', ');
}
export function buildSelectColumns(columns: readonly SignalColumn[], prefix = ''): string {
  return buildNamedSelectColumns(
    columns.map(column => column.name),
    prefix,
  );
}

function columnNamesByType(columns: readonly SignalColumn[], type: ColumnType): string[] {
  return columns.filter(column => column.type === type).map(column => column.name);
}

function typedColumnNames(columns: readonly SignalColumn[]): Set<string> {
  return new Set(
    columns.filter(column => !['sequence', 'jsonb', 'text[]', 'scn'].includes(column.type)).map(column => column.name),
  );
}

const ALL_EVENT_COLUMNS = [
  ...SPAN_EVENT_COLUMNS,
  ...METRIC_EVENT_COLUMNS,
  ...LOG_EVENT_COLUMNS,
  ...SCORE_EVENT_COLUMNS,
  ...FEEDBACK_EVENT_COLUMNS,
] as const;

export const JSONB_COLUMNS = new Set(columnNamesByType(ALL_EVENT_COLUMNS, 'jsonb'));
export const TEXT_ARRAY_COLUMNS = new Set(columnNamesByType(ALL_EVENT_COLUMNS, 'text[]'));

export const METRIC_TYPED_COLUMNS = typedColumnNames(METRIC_EVENT_COLUMNS);
export const SCORE_TYPED_COLUMNS = typedColumnNames(SCORE_EVENT_COLUMNS);
export const FEEDBACK_TYPED_COLUMNS = typedColumnNames(FEEDBACK_EVENT_COLUMNS);
