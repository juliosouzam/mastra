import { randomUUID } from 'node:crypto';
import type { StorageCreateSkillInput } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle skill snapshots', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-skill-snapshots',
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

  it('preserves large file trees, empty content, and primitive JSON compatibility', async () => {
    const skills = (await store.getStore('skills'))!;
    const skill: StorageCreateSkillInput = {
      id: randomUUID(),
      name: 'research',
      description: '',
      instructions: '',
      authorId: 'author-1',
      visibility: 'public',
      license: 'MIT',
      compatibility: 'false',
      source: { type: 'managed', mastraPath: 'skills/research' },
      references: ['references/guide.md'],
      scripts: ['scripts/check.ts'],
      assets: ['assets/logo.svg'],
      files: [{ name: 'guide.md', type: 'file', content: 'Conteúdo 🌎\n'.repeat(30_000) }],
      metadata: { audience: { type: 'research', locale: 'pt' }, tags: ['docs', 'research'] },
      tree: { entries: { 'SKILL.md': { blobHash: 'content-hash', size: 0, mimeType: 'text/markdown' } } },
    };
    try {
      await skills.create({ skill });
      expect(await skills.getByIdResolved(skill.id)).toMatchObject(skill);
      const latest = (await skills.getLatestVersion(skill.id))!;
      await skills.update({ id: skill.id, activeVersionId: latest.id });
      expect(
        (
          await skills.list({ authorId: skill.authorId, metadata: { audience: { locale: 'pt' }, tags: ['docs'] } })
        ).skills.map(row => row.id),
      ).toContain(skill.id);
      await skills.update({ id: skill.id, compatibility: false });
      expect((await skills.getLatestVersion(skill.id))!.compatibility).toBe(false);
      expect((await skills.getVersion(latest.id))!.compatibility).toBe('false');
    } finally {
      await skills.delete(skill.id);
    }
  });

  it('keeps all fields when configuration patches race', async () => {
    const skills = (await store.getStore('skills'))!;
    const id = randomUUID();
    try {
      await skills.create({
        skill: { id, name: 'concurrent', description: 'Description', instructions: 'Instructions' },
      });
      await Promise.all([
        skills.update({ id, description: 'New description' }),
        skills.update({ id, instructions: 'New instructions' }),
        skills.update({ id, metadata: { source: 'research' } }),
      ]);
      expect(await skills.countVersions(id)).toBe(4);
      expect(await skills.getLatestVersion(id)).toMatchObject({
        versionNumber: 4,
        description: 'New description',
        instructions: 'New instructions',
        metadata: { source: 'research' },
      });
    } finally {
      await skills.delete(id);
    }
  });
});
