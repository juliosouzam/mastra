import { randomUUID } from 'node:crypto';
import { MastraAuthBetterAuth } from '@mastra/auth-better-auth';
import { betterAuth } from 'better-auth';
import { deviceAuthorization, organization } from 'better-auth/plugins';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { OracleFactoryStorage } from './factory-storage';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle Better Auth integration', () => {
  let storage: OracleFactoryStorage;
  let auth: ReturnType<typeof makeAuth>;
  let otherStorage: OracleFactoryStorage;
  let otherAuth: ReturnType<typeof makeAuth>;
  const identities: string[] = [];
  const organizations: string[] = [];
  const makeAuth = (database: unknown) =>
    betterAuth({
      database: database as Parameters<typeof betterAuth>[0]['database'],
      secret: 'oracle-auth-integration-secret-at-least-32-characters',
      baseURL: 'http://localhost:3000',
      emailAndPassword: { enabled: true },
      plugins: [organization()],
      user: { additionalFields: { credits: { type: 'number', required: false } } },
    });
  beforeAll(async () => {
    storage = new OracleFactoryStorage({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      poolMax: 1,
    });
    const database = storage.authDatabase?.();
    if (database?.dialect !== 'custom') throw new Error('Oracle must expose a custom auth adapter');
    auth = makeAuth(database.database);
    otherStorage = new OracleFactoryStorage({
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      poolMax: 1,
    });
    const second = otherStorage.authDatabase();
    if (second.dialect !== 'custom') throw new Error('Expected custom adapter');
    otherAuth = makeAuth(second.database);
    // Initialize both schemas before testing transaction races.
    await (await auth.$context).adapter.findOne({ model: 'user', where: [{ field: 'id', value: 'fixture-absent' }] });
    await (
      await otherAuth.$context
    ).adapter.findOne({ model: 'user', where: [{ field: 'id', value: 'fixture-absent' }] });
  });
  afterAll(async () => {
    if (auth) {
      const adapter = (await auth.$context).adapter;
      for (const id of organizations) {
        for (const model of ['invitation', 'member'])
          await adapter.deleteMany({ model, where: [{ field: 'organizationId', value: id }] });
        await adapter.delete({ model: 'organization', where: [{ field: 'id', value: id }] });
      }
      for (const id of identities) {
        for (const model of ['session', 'account'])
          await adapter.deleteMany({ model, where: [{ field: 'userId', value: id }] });
        await adapter.delete({ model: 'user', where: [{ field: 'id', value: id }] });
      }
    }
    await storage?.close();
    await otherStorage?.close();
  });

  it('persists signup, login, session and organization membership with one pooled connection', async () => {
    const email = `oracle-${randomUUID()}@example.test`;
    const password = 'synthetic-oracle-test-password-123';
    const signup = await auth.api.signUpEmail({ body: { email, password, name: 'Oracle test' } });
    identities.push(signup.user.id);
    const signin = await auth.api.signInEmail({ body: { email, password }, asResponse: true });
    expect(signin.status).toBe(200);
    const cookie = signin.headers
      .getSetCookie()
      .map(value => value.split(';')[0])
      .join('; ');
    const headers = new Headers({ cookie });
    expect((await auth.api.getSession({ headers }))?.user.id).toBe(signup.user.id);
    const org = await auth.api.createOrganization({
      body: { name: 'Oracle organization', slug: `oracle-${randomUUID()}` },
      headers,
    });
    organizations.push(org!.id);
    expect((await auth.api.listOrganizations({ headers })).map(value => value.id)).toContain(org!.id);
    await auth.api.signOut({ headers });
    expect(await auth.api.getSession({ headers })).toBeNull();
  });

  it('consumes a verification token exactly once under concurrent requests', async () => {
    const adapter = (await auth.$context).adapter;
    const other = (await otherAuth.$context).adapter;
    const id = randomUUID();
    await adapter.create({
      model: 'verification',
      data: {
        id,
        identifier: id,
        value: 'one-use',
        expiresAt: new Date(Date.now() + 60_000),
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      forceAllowId: true,
    });
    const results = await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        (index % 2 ? other : adapter).consumeOne({
          model: 'verification',
          where: [
            { field: 'id', value: id },
            { field: 'expiresAt', operator: 'gt', value: new Date() },
          ],
        }),
      ),
    );
    expect(results.filter(Boolean)).toHaveLength(1);
  });

  it('honors a counter guard when two pools contend for the last credit', async () => {
    const adapter = (await auth.$context).adapter;
    const other = (await otherAuth.$context).adapter;
    const id = randomUUID();
    await adapter.create({
      model: 'user',
      forceAllowId: true,
      data: {
        id,
        name: 'Counter fixture',
        email: `${id}@example.test`,
        emailVerified: false,
        credits: 1,
        createdAt: new Date(),
        updatedAt: new Date(),
      },
    });
    identities.push(id);
    const results = await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        (index % 2 ? adapter : other).incrementOne({
          model: 'user',
          where: [
            { field: 'id', value: id },
            { field: 'credits', operator: 'gt', value: 0 },
          ],
          increment: { credits: -1 },
        }),
      ),
    );
    expect(results.filter(Boolean)).toHaveLength(1);
    expect(await adapter.findOne({ model: 'user', where: [{ field: 'id', value: id }] })).toMatchObject({ credits: 0 });
  });

  it('supports text predicates, membership, ordering and offset pagination', async () => {
    const adapter = (await auth.$context).adapter;
    const identifier = randomUUID();
    try {
      for (const value of ['Alpha', 'Beta', 'Gamma'])
        await adapter.create({
          model: 'verification',
          data: {
            identifier,
            value,
            expiresAt: new Date(Date.now() + 60_000),
            createdAt: new Date(),
            updatedAt: new Date(),
          },
        });
      for (const [operator, value, expected] of [
        ['contains', 'PH', 'Alpha'],
        ['starts_with', 'BE', 'Beta'],
        ['ends_with', 'MA', 'Gamma'],
      ] as const) {
        const result = await adapter.findMany<{ value: string }>({
          model: 'verification',
          where: [
            { field: 'identifier', value: identifier },
            { field: 'value', operator, value, mode: 'insensitive' },
          ],
        });
        expect(result.map(row => row.value)).toEqual([expected]);
      }
      const where = [{ field: 'identifier', value: identifier }];
      expect(
        await adapter.count({
          model: 'verification',
          where: [...where, { field: 'value', operator: 'not_in', value: ['beta'], mode: 'insensitive' }],
        }),
      ).toBe(2);
      const page = await adapter.findMany<{ value: string }>({
        model: 'verification',
        where,
        sortBy: { field: 'value', direction: 'asc' },
        offset: 1,
        limit: 1,
      });
      expect(page.map(row => row.value)).toEqual(['Beta']);
    } finally {
      await adapter.deleteMany({ model: 'verification', where: [{ field: 'identifier', value: identifier }] });
    }
  });

  it('integrates with the Mastra deferred auth provider without Kysely migrations', async () => {
    const provider = new MastraAuthBetterAuth({ secret: 'oracle-auth-integration-secret-at-least-32-characters' });
    await provider.init({ database: storage.authDatabase(), publicUrl: 'http://localhost:3000' });
    const email = `oracle-provider-${randomUUID()}@example.test`;
    const password = 'synthetic-oracle-provider-password';
    const request = new Request('http://localhost:3000/auth/api/sign-up/email');
    const result = await provider.signUp(email, password, 'Provider fixture', request);
    identities.push(result.user.id);
    expect((await provider.signIn(email, password, request)).user.id).toBe(result.user.id);
    expect((await provider.getUser(result.user.id))?.email).toBe(email);
    expect((await provider.handleAuthRequest(new Request('http://localhost:3000/auth/api/get-session'))).status).toBe(
      200,
    );
  });

  it('rolls back multi-step auth transactions', async () => {
    const adapter = (await auth.$context).adapter;
    const id = randomUUID();
    await expect(
      adapter.transaction(async tx => {
        await tx.create({
          model: 'verification',
          data: {
            id,
            identifier: id,
            value: 'rollback',
            expiresAt: new Date(Date.now() + 60_000),
            createdAt: new Date(),
            updatedAt: new Date(),
          },
          forceAllowId: true,
        });
        throw new Error('rollback fixture');
      }),
    ).rejects.toThrow('rollback fixture');
    expect(await adapter.findOne({ model: 'verification', where: [{ field: 'id', value: id }] })).toBeNull();
  });

  it('honors Better Auth model and field mappings through its adapter factory', async () => {
    const database = storage.authDatabase();
    if (database.dialect !== 'custom') throw new Error('Expected custom adapter');
    const mapped = betterAuth({
      database: database.database as Parameters<typeof betterAuth>[0]['database'],
      secret: 'oracle-auth-integration-secret-at-least-32-characters',
      baseURL: 'http://localhost:3000',
      user: { modelName: 'oracle_auth_people', fields: { name: 'display_name', email: 'email_address' } },
    });
    const adapter = (await mapped.$context).adapter;
    const email = `${randomUUID()}@example.test`;
    const row = await adapter.create<{ id: string }>({
      model: 'user',
      data: { name: 'Mapped user', email, emailVerified: false, createdAt: new Date(), updatedAt: new Date() },
    });
    try {
      expect(await adapter.findOne({ model: 'user', where: [{ field: 'email', value: email }] })).toMatchObject({
        id: row.id,
        name: 'Mapped user',
        email,
      });
      await adapter.update({
        model: 'user',
        where: [{ field: 'id', value: row.id }],
        update: { name: 'Updated user' },
      });
      const listed = await adapter.findMany<{ name: string }>({
        model: 'user',
        where: [{ field: 'email', value: email }],
        sortBy: { field: 'name', direction: 'asc' },
      });
      expect(listed.map(user => user.name)).toEqual(['Updated user']);
    } finally {
      await adapter.delete({ model: 'user', where: [{ field: 'id', value: row.id }] });
    }
  });

  it('enforces indexes declared at plugin table level', async () => {
    const database = storage.authDatabase();
    if (database.dialect !== 'custom') throw new Error('Expected custom adapter');
    const deviceAuth = betterAuth({
      database: database.database as Parameters<typeof betterAuth>[0]['database'],
      secret: 'oracle-auth-integration-secret-at-least-32-characters',
      baseURL: 'http://localhost:3000',
      plugins: [deviceAuthorization({ verificationUri: 'http://localhost:3000/device' })],
    });
    const adapter = (await deviceAuth.$context).adapter;
    const deviceCode = randomUUID();
    const data = { deviceCode, userCode: randomUUID(), expiresAt: new Date(Date.now() + 60_000), status: 'pending' };
    try {
      await adapter.create({ model: 'deviceCode', data });
      await expect(
        adapter.create({ model: 'deviceCode', data: { ...data, userCode: randomUUID() } }),
      ).rejects.toThrow();
    } finally {
      await adapter.deleteMany({ model: 'deviceCode', where: [{ field: 'deviceCode', value: deviceCode }] });
    }
  });

  it('adds a required timestamp with a dynamic default to an existing auth table', async () => {
    const database = storage.authDatabase();
    if (database.dialect !== 'custom') throw new Error('Expected custom adapter');
    const modelName = `evolution_${randomUUID().replaceAll('-', '')}`;
    const options = {
      database: database.database as Parameters<typeof betterAuth>[0]['database'],
      secret: 'oracle-auth-integration-secret-at-least-32-characters',
      baseURL: 'http://localhost:3000',
    };
    const first = betterAuth({ ...options, user: { modelName } });
    const id = randomUUID();
    try {
      await (
        await first.$context
      ).adapter.create({
        model: 'user',
        forceAllowId: true,
        data: {
          id,
          name: 'Before upgrade',
          email: `${id}@example.test`,
          emailVerified: false,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
      });
      const defaultDate = new Date('2026-01-01T00:00:00.000Z');
      const next = betterAuth({
        ...options,
        user: {
          modelName,
          additionalFields: { joinedAt: { type: 'date', required: true, defaultValue: () => defaultDate } },
        },
      });
      expect(
        await (await next.$context).adapter.findOne({ model: 'user', where: [{ field: 'id', value: id }] }),
      ).toMatchObject({ id, joinedAt: defaultDate });
    } finally {
      const db = storage.getMastraStorage().db;
      const tables = await db.manyOrNone<{ name: string }>(
        'SELECT table_name AS "name" FROM user_tables WHERE table_name LIKE :prefix',
        { prefix: `MASTRA_AUTH_${modelName.toUpperCase()}_%` },
      );
      for (const table of tables) await db.executeDdl(`DROP TABLE "${table.name}" PURGE`);
    }
  });
});
