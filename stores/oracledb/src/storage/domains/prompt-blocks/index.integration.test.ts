import { randomUUID } from 'node:crypto';
import type { CreatePromptBlockVersionInput } from '@mastra/core/storage/domains/prompt-blocks';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle prompt blocks', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-prompt-blocks',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
    });
    await store.init();
  });
  afterAll(async () => {
    await store?.close();
  });

  it('creates a draft and resolves its initial prompt version', async () => {
    const blocks = await store.getStore('promptBlocks');
    expect(blocks).toBeDefined();
    const promptBlock = {
      id: randomUUID(),
      authorId: 'author-1',
      name: 'Research instructions',
      content: 'Use primary sources for {{topic}}.',
    };
    try {
      expect(await blocks!.create({ promptBlock })).toMatchObject({ id: promptBlock.id, status: 'draft' });
      expect(await blocks!.getByIdResolved(promptBlock.id, { status: 'draft' })).toMatchObject(promptBlock);
      expect(await blocks!.countVersions(promptBlock.id)).toBe(1);
    } finally {
      await blocks!.delete(promptBlock.id);
    }
  });

  it('filters nested metadata and array subsets', async () => {
    const blocks = (await store.getStore('promptBlocks'))!;
    const id = randomUUID();
    try {
      await blocks.create({
        promptBlock: {
          id,
          name: 'Metadata',
          content: 'Prompt',
          authorId: id,
          metadata: { profile: { tier: 'gold', enabled: true }, tags: ['a', 'b'], nullable: null },
        },
      });
      const page = await blocks.list({
        authorId: id,
        status: 'draft',
        metadata: { profile: { tier: 'gold' }, tags: ['a'], nullable: null },
      });
      expect(page.total).toBe(1);
      expect(page.promptBlocks[0]!.id).toBe(id);
    } finally {
      await blocks.delete(id);
    }
  });

  it('preserves large templates, conditional rules, and immutable version selection', async () => {
    const blocks = (await store.getStore('promptBlocks'))!;
    const id = randomUUID();
    try {
      await blocks.create({
        promptBlock: { id, name: 'Research', content: 'Original', authorId: id, metadata: { category: 'research' } },
      });
      const first = (await blocks.getLatestVersion(id))!;
      const next: CreatePromptBlockVersionInput = {
        id: randomUUID(),
        blockId: id,
        versionNumber: 2,
        name: 'Research revised',
        description: '',
        content: 'Use fontes confiáveis para {{topic}}. 🌎\n'.repeat(3000),
        rules: { operator: 'AND', conditions: [{ field: 'role', operator: 'equals', value: 'researcher' }] },
        requestContextSchema: { type: 'object', properties: { topic: { type: 'string' } } },
        changedFields: ['content', 'rules'],
        changeMessage: '',
      };
      await blocks.createVersion(next);
      expect(await blocks.getVersion(next.id)).toMatchObject(next);
      expect((await blocks.getVersion(first.id))!.content).toBe('Original');
      await blocks.update({ id, status: 'published', activeVersionId: first.id });
      expect((await blocks.getByIdResolved(id))!.content).toBe('Original');
      expect((await blocks.getByIdResolved(id, { status: 'draft' }))!.content).toBe(next.content);
      expect((await blocks.getByIdResolved(id, { versionId: next.id }))!.content).toBe(next.content);
      const list = await blocks.list({ authorId: id, metadata: { category: 'research' }, perPage: 1 });
      expect(list).toMatchObject({ total: 1, hasMore: false });
      expect(list.promptBlocks[0]!.id).toBe(id);
      const page = await blocks.listVersions({
        blockId: id,
        perPage: 1,
        orderBy: { field: 'versionNumber', direction: 'ASC' },
      });
      expect(page).toMatchObject({ total: 2, hasMore: true });
      expect(page.versions[0]!.id).toBe(first.id);
      expect((await blocks.getVersions([first.id, next.id, 'missing'])).map(version => version.id).sort()).toEqual(
        [first.id, next.id].sort(),
      );
    } finally {
      await blocks.delete(id);
    }
  });

  it('allows only one concurrent writer for a version number with default indexes skipped', async () => {
    const blocks = (await store.getStore('promptBlocks'))!;
    const id = randomUUID();
    const pool = await store.getPool();
    const borrowed = await Promise.all(Array.from({ length: 4 }, () => pool.getConnection()));
    await Promise.all(borrowed.map(connection => connection.close()));
    try {
      await blocks.create({ promptBlock: { id, name: 'Concurrent prompt', content: 'Original' } });
      const writes = await Promise.allSettled(
        Array.from({ length: 8 }, (_, index) =>
          blocks.createVersion({
            id: randomUUID(),
            blockId: id,
            versionNumber: 2,
            name: 'Concurrent prompt',
            content: `Revision ${index}`,
          }),
        ),
      );
      expect(writes.filter(write => write.status === 'fulfilled')).toHaveLength(1);
      expect(await blocks.countVersions(id)).toBe(2);
      expect((await blocks.getVersionByNumber(id, 2))!.versionNumber).toBe(2);
    } finally {
      await blocks.delete(id);
    }
  });
});
