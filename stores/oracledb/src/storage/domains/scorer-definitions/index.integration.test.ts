import { randomUUID } from 'node:crypto';
import type { ScorerDefinitionsStorage, StorageCreateScorerDefinitionInput } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle scorer definition scope', () => {
  let store: OracleStore;
  let scorers: ScorerDefinitionsStorage;

  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-scorer-scope-integration',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
    });
    await store.init();
    scorers = (await store.getStore('scorerDefinitions'))!;
  });

  afterAll(async () => {
    await store?.close();
  });

  it('persists and filters organization and project without mixing scopes', async () => {
    const authorId = randomUUID();
    const definitions: StorageCreateScorerDefinitionInput[] = [
      {
        id: randomUUID(),
        name: 'Shared name',
        type: 'llm-judge',
        organizationId: 'org-a',
        projectId: 'project-a',
        authorId,
      },
      {
        id: randomUUID(),
        name: 'Shared name',
        type: 'llm-judge',
        organizationId: 'org-a',
        projectId: 'project-b',
        authorId,
      },
      {
        id: randomUUID(),
        name: 'Shared name',
        type: 'llm-judge',
        organizationId: 'org-b',
        projectId: 'project-a',
        authorId,
      },
    ];
    try {
      for (const scorerDefinition of definitions) await scorers.create({ scorerDefinition });
      const scoped = await scorers.list({ authorId, organizationId: 'org-a', projectId: 'project-a', status: 'draft' });
      expect(scoped.total).toBe(1);
      expect(scoped.scorerDefinitions).toEqual([
        expect.objectContaining({ id: definitions[0]!.id, organizationId: 'org-a', projectId: 'project-a' }),
      ]);
      expect(await scorers.getById(definitions[0]!.id)).toMatchObject({
        organizationId: 'org-a',
        projectId: 'project-a',
      });
    } finally {
      for (const definition of definitions) await scorers.delete(definition.id);
    }
  });
});
