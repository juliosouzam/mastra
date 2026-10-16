import { randomUUID } from 'node:crypto';
import type { MemoryStorage } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle atomic working memory', () => {
  let store: OracleStore;
  let otherStore: OracleStore;
  let memory: MemoryStorage;
  let otherMemory: MemoryStorage;

  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-working-memory-integration',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
    });
    await store.init();
    memory = (await store.getStore('memory'))!;
    otherStore = new OracleStore({
      id: 'oracle-working-memory-other-process',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
    });
    await otherStore.init();
    otherMemory = (await otherStore.getStore('memory'))!;
  });

  afterAll(async () => {
    await Promise.all([store?.close(), otherStore?.close()]);
  });

  it('creates missing resource working memory through the atomic merge contract', async () => {
    const resourceId = randomUUID();
    const result = await memory.mergeResourceWorkingMemory({
      resourceId,
      merge: existing => {
        expect(existing).toBeUndefined();
        return 'Lives in Fortaleza';
      },
    });

    expect(result).toMatchObject({ id: resourceId, workingMemory: 'Lives in Fortaleza', metadata: {} });
    expect(await memory.getResourceById({ resourceId })).toEqual(result);
    expect(memory.supportsAtomicWorkingMemoryMerge).toBe(true);
  });

  it('keeps every concurrent change across pools, including the first resource write', async () => {
    const resourceId = randomUUID();
    const keys = Array.from({ length: 24 }, (_, index) => `field${index}`);
    await Promise.all(
      keys.map((key, index) =>
        (index % 2 ? memory : otherMemory).mergeResourceWorkingMemory({
          resourceId,
          merge: existing => JSON.stringify({ ...(existing ? JSON.parse(existing) : {}), [key]: true }),
        }),
      ),
    );

    const resource = await memory.getResourceById({ resourceId });
    expect(Object.keys(JSON.parse(resource!.workingMemory!)).sort()).toEqual([...keys].sort());
  });

  it('preserves resource metadata and creation time when the merge returns an empty string', async () => {
    const resourceId = randomUUID();
    const original = await memory.updateResource({
      resourceId,
      workingMemory: 'Old notes',
      metadata: { owner: 'team-one' },
    });
    await otherMemory.mergeResourceWorkingMemory({ resourceId, merge: () => '' });

    expect(await memory.getResourceById({ resourceId })).toMatchObject({
      workingMemory: '',
      metadata: { owner: 'team-one' },
      createdAt: original.createdAt,
    });
  });

  it.each([false, true])('rolls back a failed merge when the resource exists: %s', async exists => {
    const resourceId = randomUUID();
    const original = exists
      ? await memory.updateResource({ resourceId, workingMemory: 'Keep these notes', metadata: { owner: 'team-one' } })
      : null;
    const failure = new Error('Merge failed');

    await expect(
      otherMemory.mergeResourceWorkingMemory({
        resourceId,
        merge: () => {
          throw failure;
        },
      }),
    ).rejects.toBe(failure);
    expect(await memory.getResourceById({ resourceId })).toEqual(original);
  });
});
