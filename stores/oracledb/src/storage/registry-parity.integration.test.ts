import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { OracleCreateIndexOptions } from './db';
import { OracleStore } from '.';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle existing registry parity', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-existing-registries',
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

  for (const kind of ['agents', 'mcpClients', 'scorerDefinitions'] as const) {
    it(`supports nested metadata containment and literal keys for ${kind}`, async () => {
      const id = randomUUID();
      const authorId = randomUUID();
      const metadata = {
        category: { name: 'support', extra: true },
        labels: ['first', 'second'],
        enabled: false,
        optional: null,
        'literal.key': 'value',
        __proto__: undefined,
      };
      Object.defineProperty(metadata, '__proto__', { value: { authored: true }, enumerable: true });
      const domain = store.stores[kind]!;
      try {
        if (kind === 'agents')
          await store.stores.agents!.create({
            agent: {
              id,
              authorId,
              name: 'Registry metadata',
              instructions: 'Help customers.',
              model: { provider: 'openai', name: 'gpt-5-mini' },
              metadata,
            },
          });
        else if (kind === 'mcpClients')
          await store.stores.mcpClients!.create({
            mcpClient: { id, authorId, name: 'Registry metadata', servers: {}, metadata },
          });
        else
          await store.stores.scorerDefinitions!.create({
            scorerDefinition: { id, authorId, name: 'Registry metadata', type: 'llm-judge', metadata },
          });
        for (const filter of [
          { category: { name: 'support' } },
          { labels: ['second'] },
          { enabled: false, optional: null },
          { 'literal.key': 'value' },
        ]) {
          const listed = await domain.list({ authorId, status: 'draft', metadata: filter });
          expect(listed.total).toBe(1);
        }
        expect((await domain.list({ authorId, status: 'draft', metadata: { enabled: 'false' } })).total).toBe(0);
        expect((await domain.getById(id))!.metadata).toEqual(metadata);
      } finally {
        await domain.delete(id);
      }
    });
  }

  it('enforces version identity even when default performance indexes are disabled', async () => {
    const indexes: OracleCreateIndexOptions[] = [
      {
        name: 'MASTRA_AGENT_VERSIONS_AGENT_VERSION_IDX',
        table: 'mastra_agent_versions',
        columns: ['agentId', 'versionNumber'],
        unique: true,
      },
      {
        name: 'MASTRA_MCP_CLIENT_VERSIONS_CLIENT_VERSION_IDX',
        table: 'mastra_mcp_client_versions',
        columns: ['mcpClientId', 'versionNumber'],
        unique: true,
      },
      {
        name: 'MASTRA_SCORER_DEFINITION_VERSIONS_DEF_VERSION_IDX',
        table: 'mastra_scorer_definition_versions',
        columns: ['scorerDefinitionId', 'versionNumber'],
        unique: true,
      },
    ];
    try {
      // Fixture-owned database: remove the three indexes to reproduce initialization from that state.
      for (const index of indexes) await store.db.executeDdl(`DROP INDEX "${index.name}"`, [-1418]);
      await store.stores.agents!.init();
      await store.stores.mcpClients!.init();
      await store.stores.scorerDefinitions!.init();
      const existing = await store.db.manyOrNone<{ name: string }>(
        `SELECT index_name AS "name" FROM user_indexes WHERE uniqueness = 'UNIQUE'`,
      );
      expect(existing.map(row => row.name)).toEqual(expect.arrayContaining(indexes.map(index => index.name)));
    } finally {
      for (const index of indexes) await store.db.createIndex(index);
    }
  });
});
