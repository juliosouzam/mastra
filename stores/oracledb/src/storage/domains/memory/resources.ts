import { MastraError } from '@mastra/core/error';
import type { RunFence, StorageResourceType } from '@mastra/core/storage';
import { TABLE_RESOURCES } from '@mastra/core/storage';
import type { Connection } from 'oracledb';

import { executeOptions, isOracleErrorCode, jsonBind, rows } from '../../../shared/connection';
import type { ObjectRow } from '../../../shared/connection';
import { toDate } from '../../domain-utils';
import { RESOURCE_CREATED_AT, RESOURCE_UPDATED_AT, RESOURCE_WORKING_MEMORY } from './schema';
import {
  assertMemoryFence,
  optionalClobStringBind,
  parseJson,
  parseOptionalString,
  storageError,
  table,
  timestampBind,
} from './utils';
import type { MemoryContext } from './utils';

// Resource working memory: one row per resourceId, upserted the same way
// threads are (title/metadata may not be known until after the first message).

type ResourceRow = {
  id: string;
  workingMemory?: unknown;
  metadata?: unknown;
  metadataJson?: unknown;
  createdAt: Date | string;
  updatedAt: Date | string;
};

export async function getResourceById(
  ctx: MemoryContext,
  { resourceId }: { resourceId: string },
): Promise<StorageResourceType | null> {
  try {
    return await ctx.db.withConnection(async connection => {
      const result = await connection.execute<ObjectRow>(
        `${resourceSelect()} FROM ${table(ctx, TABLE_RESOURCES)} WHERE id = :resourceId`,
        { resourceId },
        executeOptions(),
      );
      const row = rows(result)[0] as ResourceRow | undefined;
      return row ? parseResource(row) : null;
    });
  } catch (error) {
    throw storageError('GET_RESOURCE_BY_ID', 'FAILED', { resourceId }, error);
  }
}

export async function saveResource(
  ctx: MemoryContext,
  { resource }: { resource: StorageResourceType },
): Promise<StorageResourceType> {
  try {
    await ctx.db.none(
      `
          MERGE INTO ${table(ctx, TABLE_RESOURCES)} target
          USING (
            SELECT
              :id AS id,
              :workingMemory AS working_memory,
              :metadata AS metadata,
              :createdAt AS created_at,
              :updatedAt AS updated_at
            FROM dual
          ) source
          ON (target.id = source.id)
          WHEN MATCHED THEN UPDATE SET
            target.${RESOURCE_WORKING_MEMORY} = source.working_memory,
            target.metadata = source.metadata,
            target.${RESOURCE_UPDATED_AT} = source.updated_at
          WHEN NOT MATCHED THEN INSERT (
            id,
            ${RESOURCE_WORKING_MEMORY},
            metadata,
            ${RESOURCE_CREATED_AT},
            ${RESOURCE_UPDATED_AT}
          ) VALUES (
            source.id,
            source.working_memory,
            source.metadata,
            source.created_at,
            source.updated_at
          )`,
      {
        id: resource.id,
        workingMemory: optionalClobStringBind(resource.workingMemory),
        metadata: jsonBind(resource.metadata ?? {}),
        createdAt: resource.createdAt ?? new Date(),
        updatedAt: resource.updatedAt ?? new Date(),
      },
    );
    return resource;
  } catch (error) {
    throw storageError('SAVE_RESOURCE', 'FAILED', { resourceId: resource.id }, error);
  }
}

export async function updateResource(
  ctx: MemoryContext,
  {
    resourceId,
    workingMemory,
    metadata,
    fence,
  }: {
    resourceId: string;
    workingMemory?: string;
    metadata?: Record<string, unknown>;
    fence?: RunFence;
  },
): Promise<StorageResourceType> {
  const existing = await ctx.getResourceById({ resourceId });
  const newResource = (): StorageResourceType => ({
    id: resourceId,
    workingMemory,
    metadata: metadata ?? {},
    createdAt: new Date(),
    updatedAt: new Date(),
  });
  // An unfenced create goes through `saveResource` so overrides of it apply;
  // a fenced one has to run behind the fence check in a transaction.
  if (!existing && !fence) return ctx.saveResource({ resource: newResource() });

  try {
    return await ctx.db.tx(async (client, connection) => {
      await assertMemoryFence(ctx, client, fence, 'updateResource');
      if (!existing) {
        const resource = newResource();
        await mergeResourceRow(ctx, connection, resource);
        return resource;
      }
      // Re-read under FOR UPDATE so a concurrent updateResource on the same id
      // cannot interleave between this lock and the MERGE below, which would
      // otherwise let one writer's workingMemory/metadata clobber the other's.
      const result = await connection.execute<ObjectRow>(
        `${resourceSelect()} FROM ${table(ctx, TABLE_RESOURCES)} WHERE id = :resourceId FOR UPDATE`,
        { resourceId },
        executeOptions(),
      );
      const row = rows(result)[0] as ResourceRow | undefined;
      const locked = row ? parseResource(row) : existing;

      const updated: StorageResourceType = {
        ...locked,
        workingMemory: workingMemory !== undefined ? workingMemory : locked.workingMemory,
        metadata: metadata ? { ...(locked.metadata ?? {}), ...metadata } : locked.metadata,
        updatedAt: new Date(),
      };

      await mergeResourceRow(ctx, connection, updated);
      return updated;
    });
  } catch (error) {
    if (error instanceof MastraError) throw error;
    throw storageError('UPDATE_RESOURCE', 'FAILED', { resourceId }, error);
  }
}

export async function mergeResourceWorkingMemory(
  ctx: MemoryContext,
  { resourceId, merge }: { resourceId: string; merge: (existing: string | undefined) => string },
  fence?: RunFence,
): Promise<StorageResourceType> {
  const callbackError: { error?: unknown } = {};
  try {
    return await ctx.db.tx(async client => {
      await assertMemoryFence(ctx, client, fence, 'mergeResourceWorkingMemory');
      const now = new Date();
      // Concurrent first writes wait on the primary key. Once the winning insert commits,
      // the other writer can lock and merge the newly committed resource.
      try {
        await client.none(
          `INSERT INTO ${table(ctx, TABLE_RESOURCES)} (id, metadata, ${RESOURCE_CREATED_AT}, ${RESOURCE_UPDATED_AT})
           VALUES (:resourceId, :metadata, :createdAt, :updatedAt)`,
          {
            resourceId,
            metadata: jsonBind({}),
            createdAt: timestampBind(now),
            updatedAt: timestampBind(now),
          },
        );
      } catch (error) {
        if (!isOracleErrorCode(error, [-1])) throw error;
      }

      const row = await client.one<ResourceRow>(
        `${resourceSelect()} FROM ${table(ctx, TABLE_RESOURCES)} WHERE id = :resourceId FOR UPDATE`,
        { resourceId },
      );
      const existing = parseResource(row);
      let workingMemory: string;
      try {
        workingMemory = merge(existing.workingMemory);
      } catch (error) {
        callbackError.error = error;
        throw error;
      }
      const updatedAt = new Date();
      await client.none(
        `UPDATE ${table(ctx, TABLE_RESOURCES)}
         SET ${RESOURCE_WORKING_MEMORY} = :workingMemory, ${RESOURCE_UPDATED_AT} = :updatedAt
         WHERE id = :resourceId`,
        { resourceId, workingMemory: optionalClobStringBind(workingMemory), updatedAt: timestampBind(updatedAt) },
      );
      return { ...existing, workingMemory, updatedAt };
    });
  } catch (error) {
    if (error === callbackError.error || error instanceof MastraError) throw error;
    throw storageError('MERGE_RESOURCE_WORKING_MEMORY', 'FAILED', { resourceId }, error);
  }
}

/**
 * Upserts one resource row against the given raw connection. Shared by
 * `saveResource` (auto-commit) and `updateResource`, which runs this inside
 * the SAME transaction as the FOR UPDATE lock above, so the merge sees and
 * replaces exactly the row it locked.
 */
async function mergeResourceRow(
  ctx: Pick<MemoryContext, 'schemaName'>,
  connection: Connection,
  resource: StorageResourceType,
): Promise<void> {
  await connection.execute(
    `
        MERGE INTO ${table(ctx, TABLE_RESOURCES)} target
        USING (
          SELECT
            :id AS id,
            :workingMemory AS working_memory,
            :metadata AS metadata,
            :createdAt AS created_at,
            :updatedAt AS updated_at
          FROM dual
        ) source
        ON (target.id = source.id)
        WHEN MATCHED THEN UPDATE SET
          target.${RESOURCE_WORKING_MEMORY} = source.working_memory,
          target.metadata = source.metadata,
          target.${RESOURCE_UPDATED_AT} = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
          id,
          ${RESOURCE_WORKING_MEMORY},
          metadata,
          ${RESOURCE_CREATED_AT},
          ${RESOURCE_UPDATED_AT}
        ) VALUES (
          source.id,
          source.working_memory,
          source.metadata,
          source.created_at,
          source.updated_at
        )`,
    {
      id: resource.id,
      workingMemory: optionalClobStringBind(resource.workingMemory),
      metadata: jsonBind(resource.metadata ?? {}),
      createdAt: resource.createdAt ?? new Date(),
      updatedAt: resource.updatedAt ?? new Date(),
    },
  );
}

function parseResource(row: ResourceRow): StorageResourceType {
  const workingMemory = parseOptionalString(row.workingMemory);
  return {
    id: String(row.id),
    workingMemory,
    metadata: parseJson(row.metadataJson ?? row.metadata),
    createdAt: toDate(row.createdAt),
    updatedAt: toDate(row.updatedAt),
  };
}

function resourceSelect(): string {
  return `SELECT id AS "id", ${RESOURCE_WORKING_MEMORY} AS "workingMemory", JSON_SERIALIZE(metadata RETURNING CLOB) AS "metadataJson", ${RESOURCE_CREATED_AT} AS "createdAt", ${RESOURCE_UPDATED_AT} AS "updatedAt"`;
}
