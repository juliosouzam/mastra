import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';
import { createSampleAgent, createSampleSkill } from '../../../../../_test-utils/src/domains/favorites/data';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle concurrent favorites', () => {
  const config = {
    id: 'oracle-concurrent-favorites',
    user: process.env.ORACLE_DATABASE_USER,
    password: process.env.ORACLE_DATABASE_PASSWORD,
    connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    skipDefaultIndexes: true,
  };
  let store: OracleStore;
  let other: OracleStore;
  beforeAll(async () => {
    store = new OracleStore(config);
    await store.init();
    other = new OracleStore({ ...config, id: 'oracle-concurrent-favorites-other', disableInit: true });
    for (const provider of [store, other]) {
      const pool = await provider.getPool();
      const borrowed = await Promise.all(Array.from({ length: 4 }, () => pool.getConnection()));
      await Promise.all(borrowed.map(connection => connection.close()));
    }
  });
  afterAll(async () => {
    await store?.close();
    await other?.close();
  });

  for (const entityType of ['agent', 'skill'] as const) {
    it(`keeps ${entityType} counters consistent across pools and repeated writes`, async () => {
      const entityId = randomUUID();
      const favorites = (await store.getStore('favorites'))!;
      const second = (await other.getStore('favorites'))!;
      const agents = (await store.getStore('agents'))!;
      const skills = (await store.getStore('skills'))!;
      if (entityType === 'agent') await agents.create({ agent: createSampleAgent({ id: entityId }) });
      else await skills.create({ skill: createSampleSkill({ id: entityId }) });
      try {
        const inserts = await Promise.allSettled(
          Array.from({ length: 48 }, (_, index) =>
            (index % 2 ? favorites : second).favorite({
              userId: `user-${Math.floor(index / 2)}`,
              entityType,
              entityId,
            }),
          ),
        );
        expect(inserts.every(result => result.status === 'fulfilled')).toBe(true);
        const entity = entityType === 'agent' ? agents : skills;
        expect((await entity.getById(entityId))!.favoriteCount).toBe(24);
        const removals = await Promise.allSettled(
          Array.from({ length: 24 }, (_, index) =>
            (index % 2 ? favorites : second).unfavorite({
              userId: `user-${Math.floor(index / 3)}`,
              entityType,
              entityId,
            }),
          ),
        );
        expect(removals.every(result => result.status === 'fulfilled')).toBe(true);
        expect((await entity.getById(entityId))!.favoriteCount).toBe(16);
        expect(await favorites.isFavorited({ userId: 'user-0', entityType, entityId })).toBe(false);
        expect(await favorites.isFavorited({ userId: 'user-23', entityType, entityId })).toBe(true);
        expect(await favorites.deleteFavoritesForEntity({ entityType, entityId })).toBe(16);
        expect((await entity.getById(entityId))!.favoriteCount).toBe(0);
      } finally {
        await favorites.deleteFavoritesForEntity({ entityType, entityId });
        if (entityType === 'agent') await agents.delete(entityId);
        else await skills.delete(entityId);
      }
    });
  }
});
