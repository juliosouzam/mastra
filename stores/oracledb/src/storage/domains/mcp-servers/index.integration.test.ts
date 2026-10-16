import { randomUUID } from 'node:crypto';
import type { StorageCreateMCPServerInput } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle MCP servers', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-mcp-servers',
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

  it('round-trips an MCP server snapshot with all exposed capability bindings', async () => {
    const servers = await store.getStore('mcpServers');
    expect(servers).toBeDefined();
    const mcpServer: StorageCreateMCPServerInput = {
      id: randomUUID(),
      name: 'Research server',
      version: '1.0.0',
      authorId: 'author-1',
      description: '',
      instructions: 'Use primary sources. 🌎\n'.repeat(3000),
      repository: { url: 'https://example.com/research', type: 'git', directory: 'server' },
      releaseDate: '2026-10-16',
      isLatest: false,
      packageCanonical: 'npm',
      tools: { search: { description: 'Search documents' } },
      agents: { research: {} },
      workflows: { summarize: {} },
      metadata: { category: 'research' },
    };
    try {
      await servers!.create({ mcpServer });
      expect(await servers!.getByIdResolved(mcpServer.id, { status: 'draft' })).toMatchObject(mcpServer);
      expect(await servers!.countVersions(mcpServer.id)).toBe(1);
    } finally {
      await servers!.delete(mcpServer.id);
    }
  });

  it('filters nested metadata and array subsets', async () => {
    const servers = (await store.getStore('mcpServers'))!;
    const id = randomUUID();
    try {
      await servers.create({
        mcpServer: {
          id,
          name: 'Metadata',
          version: '1.0.0',
          authorId: id,
          metadata: { profile: { tier: 'gold', enabled: true }, tags: ['a', 'b'], nullable: null },
        },
      });
      const page = await servers.list({
        authorId: id,
        status: 'draft',
        metadata: { profile: { tier: 'gold' }, tags: ['a'], nullable: null },
      });
      expect(page.total).toBe(1);
      expect(page.mcpServers[0]!.id).toBe(id);
    } finally {
      await servers.delete(id);
    }
  });

  it('publishes a selected version and retains earlier server configuration', async () => {
    const servers = (await store.getStore('mcpServers'))!;
    const id = randomUUID();
    try {
      await servers.create({ mcpServer: { id, name: 'Server', version: '1.0.0', isLatest: false, authorId: id } });
      const first = (await servers.getLatestVersion(id))!;
      const second = await servers.createVersion({
        id: randomUUID(),
        mcpServerId: id,
        versionNumber: 2,
        name: 'Server',
        version: '2.0.0',
        instructions: '',
        isLatest: true,
        tools: {},
        changeMessage: '',
      });
      await servers.update({ id, status: 'published', activeVersionId: first.id, metadata: { category: 'research' } });
      expect((await servers.getByIdResolved(id))!.version).toBe('1.0.0');
      expect(await servers.getVersionByNumber(id, 2)).toMatchObject({ ...second, instructions: '', changeMessage: '' });
      expect((await servers.getByIdResolved(id, { status: 'draft' }))!.version).toBe('2.0.0');
      await servers.update({ id, activeVersionId: second.id });
      const published = await servers.listResolved({
        authorId: id,
        metadata: { category: 'research' },
        perPage: false,
      });
      expect(published.mcpServers).toEqual([expect.objectContaining({ id, version: '2.0.0', isLatest: true })]);
      const versions = await servers.listVersions({ mcpServerId: id, page: 0, perPage: 1 });
      expect(versions).toMatchObject({ total: 2, hasMore: true });
      await expect(
        servers.createVersion({
          id: randomUUID(),
          mcpServerId: id,
          versionNumber: 2,
          name: 'Duplicate',
          version: '3.0.0',
        }),
      ).rejects.toThrow();
      await servers.deleteVersion(first.id);
      expect(await servers.getVersion(first.id)).toBeNull();
      expect(await servers.countVersions(id)).toBe(1);
      await servers.deleteVersionsByParentId(id);
      expect(await servers.countVersions(id)).toBe(0);
    } finally {
      await servers.delete(id);
    }
  });
});
