import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import { createVectorErrorId } from '@mastra/core/storage';
import { validateUpsertInput } from '@mastra/core/vector';
import type { SparseVector } from '@mastra/core/vector';
import oracledb from 'oracledb';

import { asBindParameters, jsonBind, jsonBindText, rollbackQuietly } from '../shared/connection';
import type { IndexRegistry, WithConnection } from './ddl';
import { buildMetadataWhereClause } from './filter';
import { namespaceWhereClause, normalizeNamespace } from './sql';
import type {
  OracleDeleteVectorParams,
  OracleDeleteVectorsParams,
  OracleIndexStats,
  OracleUpdateVectorParams,
  OracleUpsertVectorParams,
  OracleVectorFormat,
} from './types';

export const VECTOR_ID_DELETE_CHUNK_SIZE = 900;
export const DEFAULT_VECTOR_UPSERT_BATCH_SIZE = 200;

// This module is the dependency-free base for the vector store split: it owns every operation that
// writes vector rows (upsert/update/delete), the vector-format encode/decode + validation helpers
// shared with query.ts and stats.ts, and the generic MastraError-wrapping helper every sibling
// module uses. Its only import from ddl.ts is type-only (erased at build time), so there is no
// runtime circular dependency even though ddl.ts imports asMastraError/withCause from here.
export const STORE_NAME = 'ORACLE';

type BindParameters = oracledb.BindParameters;

export async function upsert(
  registry: IndexRegistry,
  withConnection: WithConnection,
  upsertBatchSize: number,
  { indexName, vectors = [], metadata, ids, sparseVectors, deleteFilter, namespace }: OracleUpsertVectorParams,
): Promise<string[]> {
  try {
    namespace = normalizeNamespace(namespace);
    if (sparseVectors !== undefined) {
      if (!Array.isArray(sparseVectors)) throw new Error('sparseVectors must be an array');
      if (vectors.length > 0) throw new Error('Provide either vectors or sparseVectors, not both');
      sparseVectors.forEach(vector => validateSparseVector(vector));
      validateUpsertInput(
        STORE_NAME,
        sparseVectors.map(vector => vector.values),
        metadata,
        ids,
      );
    } else {
      validateUpsertInput(STORE_NAME, vectors, metadata, ids);
      validateVectors(vectors);
    }
  } catch (error) {
    if (error instanceof MastraError) throw error;
    throw asMastraError('UPSERT', 'INVALID_INPUT', { indexName }, error, ErrorCategory.USER);
  }

  const vectorIds = ids ?? (sparseVectors ?? vectors).map(() => globalThis.crypto.randomUUID());

  return withConnection(async connection => {
    const indexInfo = await registry.getIndexMetadata(connection, indexName);

    try {
      // deleteFilter + upsert is treated as one transaction to avoid partially refreshed indexes.
      if (deleteFilter && Object.keys(deleteFilter).length > 0) {
        const filter = buildMetadataWhereClause(deleteFilter);
        await connection.execute(
          `DELETE FROM ${indexInfo.qualifiedTableName} ${namespaceWhereClause(filter.sql)}`,
          asBindParameters({ ...filter.binds, namespace }),
        );
      }

      const mergeSql = `
        MERGE INTO ${indexInfo.qualifiedTableName} target
        USING (
          SELECT
            :vector_id AS vector_id,
            :namespace AS namespace,
            :embedding AS embedding,
            :metadata AS metadata
          FROM dual
        ) source
        ON (target.vector_id = source.vector_id AND target.namespace = source.namespace)
        WHEN MATCHED THEN UPDATE SET
          target.embedding = source.embedding,
          target.metadata = source.metadata,
          target.updated_at = SYSTIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
          vector_id,
          namespace,
          embedding,
          metadata,
          created_at,
          updated_at
        ) VALUES (
          source.vector_id,
          source.namespace,
          source.embedding,
          source.metadata,
          SYSTIMESTAMP,
          SYSTIMESTAMP
        )`;

      const executeManyOptions: oracledb.ExecuteManyOptions = {
        bindDefs: {
          vector_id: { type: oracledb.STRING, maxSize: 512 },
          namespace: { type: oracledb.STRING, maxSize: 1020 },
          embedding: { type: oracledb.DB_TYPE_VECTOR },
          // JSON bound as text (see jsonBindText): server-side OSON encoding keeps the rows
          // readable by JDBC-based tools (DBeaver/DataGrip/SQL Developer). See ../shared/connection.
          metadata: { type: oracledb.DB_TYPE_CLOB },
        },
      };
      const upsertBinds = vectorIds.map((id, i) => {
        let embedding: unknown;
        if (sparseVectors) {
          embedding = sparseVectorBind(sparseVectors[i]!, indexInfo, indexName).val;
        } else {
          validateVectorForIndex(vectors[i]!, indexInfo, indexName);
          embedding = vectorValue(vectors[i]!, indexInfo.vectorFormat);
        }

        return {
          vector_id: id,
          namespace,
          embedding,
          metadata: jsonBindText(metadata?.[i] ?? {}),
        };
      });

      // Execute in chunks to avoid very large bind arrays while still using Oracle array DML.
      for (const chunk of chunkArray(upsertBinds, upsertBatchSize)) {
        await connection.executeMany(mergeSql, chunk as BindParameters[], executeManyOptions);
      }

      await connection.commit();
      return vectorIds;
    } catch (error) {
      await rollbackQuietly(connection);
      throw error;
    }
  }).catch(error => {
    if (error instanceof MastraError) throw error;
    throw asMastraError('UPSERT', 'FAILED', { indexName }, error);
  });
}

export async function updateVector(
  registry: IndexRegistry,
  withConnection: WithConnection,
  { indexName, id, filter, update, namespace }: OracleUpdateVectorParams,
): Promise<void> {
  try {
    namespace = normalizeNamespace(namespace);
    if (!update.vector && !update.metadata) {
      throw new Error('No updates provided');
    }
    if (!id && !filter) {
      throw new Error('Either id or filter must be provided');
    }
    if (id && filter) {
      throw new Error('id and filter are mutually exclusive');
    }
    if (update.vector) {
      validateVectors([update.vector]);
    }
  } catch (error) {
    throw asMastraError('UPDATE_VECTOR', 'INVALID_INPUT', { indexName, id: id ?? '' }, error, ErrorCategory.USER);
  }

  return withConnection(async connection => {
    const indexInfo = await registry.getIndexMetadata(connection, indexName);
    if (update.vector) {
      validateVectorForIndex(update.vector, indexInfo, indexName);
    }

    const setParts: string[] = ['updated_at = SYSTIMESTAMP'];
    const binds: BindParameters = {};
    if (update.vector) {
      setParts.unshift('embedding = :embedding');
      binds.embedding = vectorBind(update.vector, indexInfo.vectorFormat);
    }
    if (update.metadata) {
      setParts.unshift('metadata = :metadata');
      binds.metadata = jsonBind(update.metadata);
    }

    const where = id ? { sql: 'WHERE vector_id = :id', binds: { id } } : buildMetadataWhereClause(filter);
    if (!where.sql) {
      throw asMastraError(
        'UPDATE_VECTOR',
        'EMPTY_FILTER',
        { indexName },
        new Error('Cannot update with an empty filter'),
        ErrorCategory.USER,
      );
    }

    try {
      await connection.execute(
        `
        UPDATE ${indexInfo.qualifiedTableName}
        SET ${setParts.join(', ')}
        ${namespaceWhereClause(where.sql)}`,
        asBindParameters({ ...binds, ...where.binds, namespace }),
        { autoCommit: true },
      );
    } catch (error) {
      await rollbackQuietly(connection);
      throw error;
    }
  }).catch(error => {
    if (error instanceof MastraError) throw error;
    throw asMastraError('UPDATE_VECTOR', 'FAILED', { indexName, id: id ?? '' }, error);
  });
}

export async function deleteVector(
  registry: IndexRegistry,
  withConnection: WithConnection,
  { indexName, id, namespace }: OracleDeleteVectorParams,
): Promise<void> {
  try {
    namespace = normalizeNamespace(namespace);
  } catch (error) {
    throw asMastraError('DELETE_VECTOR', 'INVALID_INPUT', { indexName, id }, error, ErrorCategory.USER);
  }
  return withConnection(async connection => {
    const indexInfo = await registry.getIndexMetadata(connection, indexName);
    try {
      await connection.execute(
        `DELETE FROM ${indexInfo.qualifiedTableName} WHERE vector_id = :id AND namespace = :namespace`,
        { id, namespace },
        { autoCommit: true },
      );
    } catch (error) {
      await rollbackQuietly(connection);
      throw error;
    }
  }).catch(error => {
    if (error instanceof MastraError) throw error;
    throw asMastraError('DELETE_VECTOR', 'FAILED', { indexName, id }, error);
  });
}

export async function deleteVectors(
  registry: IndexRegistry,
  withConnection: WithConnection,
  { indexName, ids, filter, namespace }: OracleDeleteVectorsParams,
): Promise<void> {
  const explicitNamespace = namespace !== undefined;
  try {
    namespace = normalizeNamespace(namespace);
    const hasFilter = filter !== undefined && filter !== null;
    if (ids !== undefined && hasFilter) {
      throw new Error('ids and filter are mutually exclusive');
    }
    if (ids !== undefined && ids.length === 0) {
      throw new Error('empty ids array');
    }
    if (hasFilter && Object.keys(filter).length === 0) {
      throw new Error('empty filter');
    }
    if (ids === undefined && !hasFilter && !explicitNamespace) {
      throw new Error('Either filter or ids must be provided');
    }
  } catch (error) {
    throw asMastraError('DELETE_VECTORS', 'INVALID_INPUT', { indexName }, error, ErrorCategory.USER);
  }

  return withConnection(async connection => {
    const indexInfo = await registry.getIndexMetadata(connection, indexName);
    try {
      if (ids?.length) {
        for (const idChunk of chunkArray(ids, VECTOR_ID_DELETE_CHUNK_SIZE)) {
          await connection.executeMany(
            `DELETE FROM ${indexInfo.qualifiedTableName} WHERE vector_id = :id AND namespace = :namespace`,
            idChunk.map(id => ({ id, namespace })) as BindParameters[],
            {
              bindDefs: {
                id: { type: oracledb.STRING, maxSize: 512 },
                namespace: { type: oracledb.STRING, maxSize: 1020 },
              },
            },
          );
        }
      } else {
        const metadataFilter = buildMetadataWhereClause(filter);
        await connection.execute(
          `DELETE FROM ${indexInfo.qualifiedTableName} ${namespaceWhereClause(metadataFilter.sql)}`,
          asBindParameters({ ...metadataFilter.binds, namespace }),
        );
      }
      await connection.commit();
    } catch (error) {
      await rollbackQuietly(connection);
      throw error;
    }
  }).catch(error => {
    if (error instanceof MastraError) throw error;
    throw asMastraError('DELETE_VECTORS', 'FAILED', { indexName }, error);
  });
}

export function validateVectors(vectors: number[][]): void {
  for (let vectorIndex = 0; vectorIndex < vectors.length; vectorIndex += 1) {
    const vector = vectors[vectorIndex];
    if (!Array.isArray(vector) || vector.length === 0) {
      throw new Error(`Vector at index ${vectorIndex} must be a non-empty number array`);
    }
    for (let componentIndex = 0; componentIndex < vector.length; componentIndex += 1) {
      const value = vector[componentIndex];
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        throw new Error(`Vector at index ${vectorIndex} has a non-finite value at component ${componentIndex}`);
      }
    }
  }
}

function validateVectorDimension(vector: number[], dimension: number, indexName: string): void {
  if (vector.length !== dimension) {
    throw asMastraError(
      'UPSERT',
      'INVALID_INPUT',
      { indexName, expected: String(dimension), actual: String(vector.length) },
      new Error(`Vector dimension mismatch: expected ${dimension}, received ${vector.length}`),
      ErrorCategory.USER,
    );
  }
}

export function validateVectorForIndex(
  vector: number[],
  indexInfo: Pick<OracleIndexStats, 'dimension' | 'vectorFormat'>,
  indexName: string,
): void {
  validateVectorDimension(vector, indexInfo.dimension, indexName);

  if (indexInfo.vectorFormat === 'bit') {
    for (const [index, value] of vector.entries()) {
      if (value !== 0 && value !== 1) {
        throw asMastraError(
          'VECTOR_FORMAT',
          'INVALID_BIT_VALUE',
          { indexName, index, value },
          new Error('bit vectors must contain only 0 or 1 values'),
          ErrorCategory.USER,
        );
      }
    }
  }

  if (indexInfo.vectorFormat === 'int8') {
    for (const [index, value] of vector.entries()) {
      if (!Number.isInteger(value) || value < -128 || value > 127) {
        throw asMastraError(
          'VECTOR_FORMAT',
          'INVALID_INT8_VALUE',
          { indexName, index, value },
          new Error('int8 vectors must contain integers from -128 to 127'),
          ErrorCategory.USER,
        );
      }
    }
  }

  if (indexInfo.vectorFormat === 'halfvec') {
    for (const [index, value] of vector.entries()) {
      const rounded = roundToHalfPrecision(value);
      if (!Number.isFinite(rounded) || Math.abs(rounded) > 65504) {
        throw asMastraError(
          'VECTOR_FORMAT',
          'INVALID_HALF_VALUE',
          { indexName, index, value },
          new Error('halfvec values must round to finite half-precision numbers'),
          ErrorCategory.USER,
        );
      }
    }
  }
}

export function vectorBind(vector: number[], vectorFormat: OracleVectorFormat): oracledb.BindParameter {
  return { type: oracledb.DB_TYPE_VECTOR, val: vectorValue(vector, vectorFormat) };
}

export function validateSparseVector(vector: SparseVector, dimension?: number): void {
  if (
    !vector ||
    !Array.isArray(vector.indices) ||
    !Array.isArray(vector.values) ||
    vector.indices.length !== vector.values.length
  ) {
    throw new Error('Sparse vectors require equally sized indices and values arrays');
  }
  const seen = new Set<number>();
  vector.indices.forEach((index, position) => {
    if (
      !Number.isSafeInteger(index) ||
      index < 0 ||
      (dimension !== undefined && index >= dimension) ||
      seen.has(index)
    ) {
      throw new Error('Sparse vector indices must be unique nonnegative integers within the index dimension');
    }
    if (typeof vector.values[position] !== 'number' || !Number.isFinite(Math.fround(vector.values[position]!))) {
      throw new Error('Sparse vector values must be finite FLOAT32 numbers');
    }
    seen.add(index);
  });
}

export function sparseVectorBind(
  vector: SparseVector,
  indexInfo: Pick<OracleIndexStats, 'dimension' | 'vectorFormat'>,
  indexName: string,
): oracledb.BindParameter {
  try {
    if (indexInfo.vectorFormat !== 'sparsevec')
      throw new Error('sparseVector and sparseVectors require an index with vectorFormat "sparsevec"');
    validateSparseVector(vector, indexInfo.dimension);
    const entries = vector.indices
      .map((index, position) => ({ index, value: vector.values[position]! }))
      .filter(entry => entry.value !== 0)
      .sort((left, right) => left.index - right.index);
    // Oracle 23ai loses the declared dimension for an empty sparse payload.
    // A zero-valued entry preserves the same vector on both supported release lines.
    if (entries.length === 0) entries.push({ index: 0, value: 0 });
    return {
      type: oracledb.DB_TYPE_VECTOR,
      val: new oracledb.SparseVector<Float32Array>({
        numDimensions: indexInfo.dimension,
        indices: entries.map(entry => entry.index),
        values: Float32Array.from(entries.map(entry => entry.value)),
      }),
    };
  } catch (error) {
    throw asMastraError('VECTOR_FORMAT', 'INVALID_SPARSE_VECTOR', { indexName }, error, ErrorCategory.USER);
  }
}

export function vectorValue(
  vector: number[],
  vectorFormat: OracleVectorFormat,
): Float32Array | Int8Array | Uint8Array | oracledb.SparseVector<Float32Array> {
  // node-oracledb binds VECTOR columns through typed arrays, with bit vectors packed by byte.
  switch (vectorFormat) {
    case 'bit':
      return packBitVector(vector);
    case 'int8':
      return Int8Array.from(vector);
    case 'halfvec':
      return Float32Array.from(vector, roundToHalfPrecision);
    case 'sparsevec': {
      const indices: number[] = [];
      const values: number[] = [];
      vector.forEach((value, index) => {
        if (value !== 0) {
          indices.push(index);
          values.push(value);
        }
      });
      if (indices.length === 0) {
        indices.push(0);
        values.push(0);
      }
      return new oracledb.SparseVector<Float32Array>({
        numDimensions: vector.length,
        indices,
        values: Float32Array.from(values),
      });
    }
    case 'vector':
    default:
      return Float32Array.from(vector);
  }
}

function roundToHalfPrecision(value: number): number {
  // pgvector parses through float32 before converting to binary16. Store those
  // representable half values in Oracle FLOAT32, which has no native half column.
  const single = Math.fround(value);
  if (!Number.isFinite(single) || single === 0) return single;
  const magnitude = Math.abs(single);
  const step = 2 ** (Math.max(-14, Math.floor(Math.log2(magnitude))) - 10);
  const scaled = magnitude / step;
  const lower = Math.floor(scaled);
  const fraction = scaled - lower;
  const rounded = fraction > 0.5 || (fraction === 0.5 && lower % 2 !== 0) ? lower + 1 : lower;
  return Math.sign(single) * rounded * step;
}

function packBitVector(vector: number[]): Uint8Array {
  // Oracle BINARY vectors store eight user-facing 0/1 dimensions per byte.
  const bytes = new Uint8Array(Math.ceil(vector.length / 8));
  for (let i = 0; i < vector.length; i += 1) {
    if (vector[i]) {
      const byteIndex = Math.floor(i / 8);
      bytes[byteIndex] = (bytes[byteIndex] ?? 0) | (1 << (7 - (i % 8)));
    }
  }
  return bytes;
}

export function chunkArray<T>(values: T[], chunkSize: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }
  return chunks;
}

export function withCause(error: Error, cause: unknown): Error {
  (error as Error & { cause?: unknown }).cause = cause;
  return error;
}

export function asMastraError(
  operation: string,
  reason: string,
  details: Record<string, string | number | boolean | undefined>,
  cause: unknown,
  category: ErrorCategory = ErrorCategory.THIRD_PARTY,
): MastraError {
  const safeDetails = Object.fromEntries(
    Object.entries(details).filter((entry): entry is [string, string | number | boolean] => entry[1] !== undefined),
  );

  return new MastraError(
    {
      id: createVectorErrorId(STORE_NAME, operation, reason),
      domain: ErrorDomain.MASTRA_VECTOR,
      category,
      details: safeDetails,
    },
    cause,
  );
}
