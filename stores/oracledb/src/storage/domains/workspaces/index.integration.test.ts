import { randomUUID } from 'node:crypto';
import type { StorageCreateWorkspaceInput } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle workspaces', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-workspaces',
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

  it('preserves filesystem, sandbox, search, skills, and tool configuration', async () => {
    const workspaces = await store.getStore('workspaces');
    expect(workspaces).toBeDefined();
    const workspace: StorageCreateWorkspaceInput = {
      id: randomUUID(),
      name: 'Research workspace',
      description: '',
      authorId: 'author-1',
      filesystem: { provider: 'local', config: { basePath: '/work' }, readOnly: false },
      sandbox: { provider: 'local', config: { env: { LANGUAGE: 'pt' } } },
      mounts: { '/docs': { provider: 'local', config: { basePath: '/documents' }, readOnly: true } },
      search: {
        vectorProvider: 'oracledb',
        vectorConfig: { indexName: 'documents' },
        bm25: { k1: 1.2, b: 0.75 },
        autoIndexPaths: ['/docs'],
      },
      skills: ['research'],
      tools: { enabled: false, tools: { write_file: { enabled: true, requireApproval: true } } },
      autoSync: false,
      operationTimeout: 0,
      metadata: { category: 'research' },
    };
    try {
      await workspaces!.create({ workspace });
      expect(await workspaces!.getByIdResolved(workspace.id, { status: 'draft' })).toMatchObject(workspace);
      expect(await workspaces!.countVersions(workspace.id)).toBe(1);
    } finally {
      await workspaces!.delete(workspace.id);
    }
  });

  it('versions changed configuration, ignores equivalent patches, and publishes explicitly', async () => {
    const workspaces = (await store.getStore('workspaces'))!;
    const id = randomUUID();
    try {
      await workspaces.create({
        workspace: {
          id,
          name: 'Workspace',
          filesystem: { provider: 'local', config: { basePath: '/work', readOnly: false } },
          autoSync: false,
        },
      });
      const first = (await workspaces.getLatestVersion(id))!;
      await workspaces.update({ id, description: 'Updated', operationTimeout: 0 });
      expect(await workspaces.countVersions(id)).toBe(2);
      const second = (await workspaces.getLatestVersion(id))!;
      expect(second).toMatchObject({
        name: 'Workspace',
        description: 'Updated',
        autoSync: false,
        operationTimeout: 0,
        changedFields: ['description', 'operationTimeout'],
      });
      await workspaces.update({
        id,
        description: undefined,
        filesystem: { config: { readOnly: false, basePath: '/work' }, provider: 'local' },
      });
      expect(await workspaces.countVersions(id)).toBe(2);
      expect((await workspaces.getVersion(first.id))!.description).toBeUndefined();
      expect(await workspaces.update({ id, activeVersionId: first.id })).toMatchObject({ status: 'published' });
      expect((await workspaces.getByIdResolved(id))!.resolvedVersionId).toBe(first.id);
      expect((await workspaces.getByIdResolved(id, { status: 'draft' }))!.resolvedVersionId).toBe(second.id);
      expect((await workspaces.list()).workspaces.map(workspace => workspace.id)).toContain(id);
    } finally {
      await workspaces.delete(id);
    }
  });

  it('serializes concurrent configuration patches without losing fields or versions', async () => {
    const workspaces = (await store.getStore('workspaces'))!;
    const id = randomUUID();
    try {
      await workspaces.create({ workspace: { id, name: 'Concurrent workspace' } });
      await Promise.all([
        workspaces.update({ id, filesystem: { provider: 'local', config: { basePath: '/work' } } }),
        workspaces.update({ id, sandbox: { provider: 'local', config: {} } }),
        workspaces.update({ id, operationTimeout: 1234 }),
      ]);
      expect(await workspaces.countVersions(id)).toBe(4);
      expect(await workspaces.getLatestVersion(id)).toMatchObject({
        versionNumber: 4,
        filesystem: { provider: 'local', config: { basePath: '/work' } },
        sandbox: { provider: 'local', config: {} },
        operationTimeout: 1234,
      });
    } finally {
      await workspaces.delete(id);
    }
  });

  it('filters nested metadata and array subsets without mixing JSON types', async () => {
    const workspaces = (await store.getStore('workspaces'))!;
    const authorId = randomUUID();
    const first = randomUUID();
    const second = randomUUID();
    const arrays = randomUUID();
    try {
      await workspaces.create({
        workspace: {
          id: first,
          authorId,
          name: 'Matching',
          metadata: {
            category: { name: "team's docs", rank: 1 },
            tags: ['a', 'b'],
            enabled: true,
            nullable: null,
            empty: '',
            nodes: [{ id: 1, label: 'node' }],
            'team.info': { 'quoted"key': 'value' },
          },
        },
      });
      await workspaces.create({
        workspace: {
          id: second,
          authorId,
          name: 'Different type',
          metadata: { category: { name: "team's docs", rank: '1' }, tags: ['c'], enabled: 'true' },
        },
      });
      await workspaces.create({
        workspace: {
          id: arrays,
          authorId,
          name: 'Arrays',
          metadata: { nullable: [null], enabled: [true], category: [{ rank: 1 }] },
        },
      });
      const page = await workspaces.list({
        authorId,
        metadata: { category: { name: "team's docs", rank: 1 }, tags: ['a'], enabled: true, nullable: null },
      });
      expect(page.total).toBe(1);
      expect(page.workspaces.map(workspace => workspace.id)).toEqual([first]);
      for (const metadata of [
        { nullable: null },
        { enabled: true },
        { category: { rank: 1 } },
        { empty: '' },
        { nodes: [{ id: 1 }] },
        { 'team.info': { 'quoted"key': 'value' } },
      ]) {
        expect((await workspaces.list({ authorId, metadata })).workspaces.map(workspace => workspace.id)).toEqual([
          first,
        ]);
      }
    } finally {
      await workspaces.delete(first);
      await workspaces.delete(second);
      await workspaces.delete(arrays);
    }
  });
});
