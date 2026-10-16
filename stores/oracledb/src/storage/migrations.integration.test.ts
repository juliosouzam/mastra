import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { OraclePoolManager } from '../shared/connection';
import { OracleDB } from './db';
import { OracleMigrationRegistry } from './migrations';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle migration ledger concurrency', () => {
  const tableName = `ORACLE_MIGRATIONS_${randomUUID().replaceAll('-', '').slice(0, 12).toUpperCase()}`;
  let first: OraclePoolManager;
  let second: OraclePoolManager;
  let db: OracleDB;
  let registry: OracleMigrationRegistry;
  beforeAll(async () => {
    const config = {
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      poolMax: 1,
    };
    first = new OraclePoolManager(config);
    second = new OraclePoolManager(config);
    db = new OracleDB({ poolManager: second });
    registry = new OracleMigrationRegistry({ db, tableName });
    await registry.init();
  });
  afterAll(async () => {
    await db?.executeDdl(`DROP TABLE "${tableName}" PURGE`, [-942]);
    await Promise.all([first?.close(), second?.close()]);
  });

  it.each([
    {
      name: 'matching repeatable',
      winnerKind: 'repeatable',
      kind: 'repeatable',
      winnerChecksum: 'stable',
      checksum: 'stable',
      error: undefined,
    },
    {
      name: 'changed repeatable',
      winnerKind: 'repeatable',
      kind: 'repeatable',
      winnerChecksum: 'old',
      checksum: 'new',
      error: undefined,
    },
    {
      name: 'matching versioned',
      winnerKind: 'versioned',
      kind: 'versioned',
      winnerChecksum: 'stable',
      checksum: 'stable',
      error: undefined,
    },
    {
      name: 'changed versioned',
      winnerKind: 'versioned',
      kind: 'versioned',
      winnerChecksum: 'old',
      checksum: 'new',
      error: /already applied with checksum/,
    },
    {
      name: 'conflicting kind',
      winnerKind: 'versioned',
      kind: 'repeatable',
      winnerChecksum: 'stable',
      checksum: 'stable',
      error: /already applied as versioned/,
    },
  ] as const)(
    'handles a competing $name ledger insert that commits while MERGE waits',
    async testCase => {
      const connection = await (await first.getPool()).getConnection();
      const id = `R_CONCURRENT_${randomUUID()}`;
      let reachedMigration!: () => void;
      const migrationReached = new Promise<void>(resolve => {
        reachedMigration = resolve;
      });
      try {
        await connection.execute(
          `INSERT INTO "${tableName}" (id, name, kind, checksum) VALUES (:id, 'Concurrent schema', :kind, :checksum)`,
          { id, kind: testCase.winnerKind, checksum: testCase.winnerChecksum },
        );
        const run = registry.run([
          {
            id,
            name: 'Concurrent schema',
            kind: testCase.kind,
            checksum: testCase.checksum,
            run: async () => {
              reachedMigration();
            },
          },
        ]);
        const outcome = run.then(
          results => ({ results, error: undefined }),
          error => ({ results: undefined, error }),
        );
        await migrationReached;
        // Give the second Oracle session time to enter its blocked MERGE before releasing the unique key.
        await new Promise(resolve => setTimeout(resolve, 50));
        await connection.commit();
        const result = await outcome;
        if (testCase.error) expect(result.error?.message).toMatch(testCase.error);
        else {
          expect(result.error).toBeUndefined();
          expect(result.results).toMatchObject([{ id, checksum: testCase.checksum }]);
        }
        expect((await registry.list()).find(record => record.id === id)).toMatchObject({
          kind: testCase.winnerKind,
          checksum: testCase.error ? testCase.winnerChecksum : testCase.checksum,
        });
      } finally {
        await connection.rollback();
        await connection.close();
      }
    },
    10_000,
  );
});
