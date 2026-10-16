import type {
  OracleMetric,
  OracleScoreMode,
  OracleVectorFormat,
  OracleVectorIndexConfig,
  OracleVectorIndexType,
} from './types';

export const DEFAULT_NAMESPACE = 'default';

export function normalizeScoreMode(mode = 'native'): OracleScoreMode {
  const normalized = mode.toLowerCase();
  if (normalized !== 'native' && normalized !== 'postgres') throw new Error('scoreMode must be "native" or "postgres"');
  return normalized;
}

export function normalizeNamespace(namespace = DEFAULT_NAMESPACE): string {
  if (typeof namespace !== 'string' || [...namespace].length > 255 || namespace.includes('\0')) {
    throw new Error('namespace must be a string of at most 255 characters without null bytes');
  }
  // Oracle treats empty VARCHAR2 values as NULL. NUL cannot occur in a PostgreSQL
  // namespace, so it preserves the distinct empty namespace without a collision.
  return namespace || '\0';
}

export function namespaceWhereClause(whereClause: string): string {
  return whereClause
    ? `WHERE (${whereClause.replace(/^WHERE\s+/i, '')}) AND namespace = :namespace`
    : 'WHERE namespace = :namespace';
}

// Shared Oracle VECTOR SQL helpers used by both runtime queries and offline DDL export.
// Keeping these in one place prevents the schema exporter from drifting away from OracleVector behavior.
export function normalizeMetric(metric: string): OracleMetric {
  const normalized = metric.toLowerCase();
  if (
    normalized !== 'cosine' &&
    normalized !== 'euclidean' &&
    normalized !== 'dotproduct' &&
    normalized !== 'hamming' &&
    normalized !== 'jaccard'
  ) {
    throw new Error('metric must be one of "cosine", "euclidean", "dotproduct", "hamming", or "jaccard"');
  }
  return normalized;
}

export function normalizeVectorFormat(format: string): OracleVectorFormat {
  const normalized = format.toLowerCase();
  if (
    normalized !== 'vector' &&
    normalized !== 'halfvec' &&
    normalized !== 'sparsevec' &&
    normalized !== 'bit' &&
    normalized !== 'int8'
  ) {
    throw new Error('vectorFormat must be one of "vector", "halfvec", "sparsevec", "bit", or "int8"');
  }
  return normalized;
}

export function metricToken(metric: OracleMetric): string {
  switch (metric) {
    case 'euclidean':
      return 'EUCLIDEAN';
    case 'dotproduct':
      return 'DOT';
    case 'hamming':
      return 'HAMMING';
    case 'jaccard':
      return 'JACCARD';
    case 'cosine':
    default:
      return 'COSINE';
  }
}

export function defaultMetricForFormat(vectorFormat: OracleVectorFormat): OracleMetric {
  return vectorFormat === 'bit' ? 'hamming' : 'cosine';
}

export function vectorFormatToken(vectorFormat: OracleVectorFormat): string {
  switch (vectorFormat) {
    case 'bit':
      return 'BINARY';
    case 'int8':
      return 'INT8';
    case 'vector':
    default:
      return 'FLOAT32';
  }
}

export function vectorColumnType(dimension: number, vectorFormat: OracleVectorFormat): string {
  const storedDimension = vectorFormat === 'bit' ? Math.ceil(dimension / 8) * 8 : dimension;
  return `VECTOR(${storedDimension}, ${vectorFormatToken(vectorFormat)}${vectorFormat === 'sparsevec' ? ', SPARSE' : ''})`;
}

// `indexType` is optional so callers that only know the vector format (e.g. the offline schema
// exporter) keep validating format/metric compatibility without being forced to resolve an index type.
export function validateMetricForFormat(
  metric: OracleMetric,
  vectorFormat: OracleVectorFormat,
  indexType?: OracleVectorIndexType,
): void {
  if (vectorFormat === 'sparsevec' && indexType === 'ivf') {
    throw new Error('sparsevec indexes require HNSW or exact search; Oracle IVF does not support sparse vectors');
  }
  if (vectorFormat === 'bit') {
    if (metric !== 'hamming' && metric !== 'jaccard') {
      throw new Error('bit vector indexes support only "hamming" or "jaccard" metrics');
    }
  } else if (metric === 'hamming' || metric === 'jaccard') {
    throw new Error(`${metric} metric requires vectorFormat "bit"`);
  }

  if (metric === 'jaccard' && indexType === 'ivf') {
    throw new Error('jaccard metric requires HNSW or exact search; IVF does not support jaccard indexes');
  }
}

export function buildVectorIndexParameterClause(indexConfig: OracleVectorIndexConfig): string {
  if (indexConfig.type === 'ivf') {
    const neighborPartitions = indexConfig.ivf?.neighborPartitions;
    // `!== undefined` (rather than truthy) so an explicit 0 reaches validatePositiveInteger and is
    // rejected instead of being silently treated as "unset".
    return neighborPartitions !== undefined
      ? `PARAMETERS (type IVF, neighbor partitions ${validatePositiveInteger(neighborPartitions, 1, 10_000_000)})`
      : '';
  }

  const parts: string[] = ['type HNSW'];
  if (indexConfig.hnsw?.neighbors !== undefined) {
    parts.push(`neighbors ${validatePositiveInteger(indexConfig.hnsw.neighbors, 2, 2048)}`);
  }
  if (indexConfig.hnsw?.efConstruction !== undefined) {
    parts.push(`efconstruction ${validatePositiveInteger(indexConfig.hnsw.efConstruction, 1, 65535)}`);
  }
  return parts.length > 1 ? `PARAMETERS (${parts.join(', ')})` : '';
}

export function validateDimension(dimension: number): number {
  if (!Number.isInteger(dimension) || dimension <= 0 || dimension > 65535) {
    throw new Error('dimension must be a positive integer at most 65535');
  }
  return dimension;
}

export function validateVectorFormatDimension(vectorFormat: OracleVectorFormat, dimension: number): void {
  if (vectorFormat === 'bit' && dimension > 65528) {
    throw new Error('bit vector dimensions must not exceed 65528');
  }
}

export function validateAccuracy(accuracy: number): number {
  if (!Number.isInteger(accuracy) || accuracy <= 0 || accuracy > 100) {
    throw new Error('vector index accuracy must be an integer between 1 and 100');
  }
  return accuracy;
}

function validatePositiveInteger(value: number, minimum: number, maximum: number): number {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`vector index parameter must be a positive integer between ${minimum} and ${maximum}`);
  }
  return value;
}
