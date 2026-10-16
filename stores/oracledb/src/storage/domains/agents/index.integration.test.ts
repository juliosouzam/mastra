import { randomUUID } from 'node:crypto';
import type { AgentsStorage, StorageCreateAgentInput } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle agent configuration', () => {
  let store: OracleStore;
  let agents: AgentsStorage;

  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-agent-configuration-integration',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
    });
    await store.init();
    agents = (await store.getStore('agents'))!;
  });

  afterAll(async () => {
    await store?.close();
  });

  it('preserves workspace, skills, tool providers, and durable configuration in agent snapshots', async () => {
    const agent: StorageCreateAgentInput = {
      id: randomUUID(),
      name: 'Stored assistant',
      instructions: 'Answer the user question.',
      model: { provider: 'openai', name: 'gpt-5-mini' },
      workspace: { type: 'id', workspaceId: 'workspace-one' },
      skills: { 'research-skill': { strategy: 'latest', description: 'Research available evidence' } },
      skillsFormat: 'markdown',
      toolProviders: { composio: { tools: { search: { toolkit: 'search' } }, connections: {} } },
      durable: { maxSteps: 20, cleanupTimeoutMs: 0 },
    };
    try {
      await agents.create({ agent });
      const stored = await agents.getByIdResolved(agent.id);
      expect(stored).toMatchObject(agent);
    } finally {
      await agents.delete(agent.id);
    }
  });

  it('preserves large JSON and authored property names without changing their content', async () => {
    const id = randomUUID();
    const metadata = JSON.parse(`{"__proto__":{"authored":true},"large":"${'a'.repeat(200_000)}"}`);
    try {
      await agents.create({
        agent: {
          id,
          name: 'Large metadata',
          instructions: 'Read documents.',
          model: { provider: 'openai', name: 'gpt-5-mini' },
          metadata,
        },
      });
      expect((await agents.getById(id))!.metadata).toEqual(metadata);
    } finally {
      await agents.delete(id);
    }
  });

  it('preserves agent and version timestamp instants with a different session timezone', async () => {
    const zoned = new OracleStore({
      id: 'oracle-agent-timezone',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      disableInit: true,
      poolMax: 1,
    });
    const scoped = (await zoned.getStore('agents'))!;
    const id = randomUUID();
    try {
      await zoned.db.none("ALTER SESSION SET TIME_ZONE = '+09:00'");
      const created = await scoped.create({
        agent: {
          id,
          name: 'Timestamp fidelity',
          instructions: 'Read documents.',
          model: { provider: 'openai', name: 'gpt-5-mini' },
        },
      });
      expect((await scoped.getById(id))!.createdAt).toEqual(created.createdAt);
      const latest = (await scoped.getLatestVersion(id))!;
      expect(Math.abs(latest.createdAt.getTime() - created.createdAt.getTime())).toBeLessThan(1000);
    } finally {
      await scoped.delete(id);
      await zoned.close();
    }
  });
});
