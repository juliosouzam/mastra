import { Hono } from 'hono';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// ── Mocks ────────────────────────────────────────────────────────────────
vi.mock('drizzle-orm', async () => {
  const actual = (await vi.importActual('drizzle-orm')) as Record<string, unknown>;
  return {
    ...actual,
    eq: (column: any, value: any) => ({ kind: 'eq', column: column?.name, value }),
    and: (...conds: any[]) => ({ kind: 'and', conds: conds.filter(Boolean) }),
  };
});

// In-memory tables keyed by their Postgres names.
let tables: Record<string, Array<Record<string, any>>> = {};
let nextId = 1;

function columnJsKey(table: any, columnName: string): string | undefined {
  for (const [jsKey, col] of Object.entries(table)) {
    if ((col as any)?.name === columnName) return jsKey;
  }
  return undefined;
}

function matches(table: any, row: any, cond: any): boolean {
  if (!cond) return true;
  if (cond.kind === 'and') return cond.conds.every((c: any) => matches(table, row, c));
  if (cond.kind === 'eq') {
    const jsKey = columnJsKey(table, cond.column);
    return jsKey !== undefined && row[jsKey] === cond.value;
  }
  return true;
}

function rowsOf(table: any): Array<Record<string, any>> {
  const name = table?.[Symbol.for('drizzle:Name')] ?? table?.name;
  // Resolve by matching a known column set instead when the symbol isn't set.
  if (typeof name === 'string' && tables[name]) return tables[name];
  if (columnJsKey(table, 'source_key')) return (tables['work_items'] ??= []);
  if (columnJsKey(table, 'repo_full_name')) return (tables['github_projects'] ??= []);
  return (tables['unknown'] ??= []);
}

// Serialize transactions the way row locks would: each `db.transaction` waits
// for the previous one to finish, so a locked read always sees prior writes.
let txTail: Promise<unknown> = Promise.resolve();
let advisoryLockCalls = 0;

// Capture audit events at the store boundary so the real `emitAudit` path
// (actor resolution, request context, never-throws) is exercised end to end.
let auditRecorded: Array<Record<string, any>> = [];
let auditFailure: Error | undefined;

vi.mock('../audit/store', () => ({
  recordAuditEvent: async (input: any) => {
    if (auditFailure) throw auditFailure;
    auditRecorded.push(input);
    return {
      id: `00000000-0000-4000-9000-${String(auditRecorded.length).padStart(12, '0')}`,
      occurredAt: new Date(),
      ...input,
      githubProjectId: input.githubProjectId ?? null,
      metadata: input.metadata ?? {},
      context: input.context ?? {},
    };
  },
  listAuditEvents: async () => ({ events: [] }),
}));

vi.mock('../github/db', () => {
  const makeDbClient = (): any => ({
    select: () => ({
      from: (table: any) => ({
        where: (cond: any) => {
          const result = (async () => {
            // Yield a macrotask so unlocked concurrent read-modify-writes
            // genuinely interleave (regression coverage for the row lock).
            await new Promise(resolve => setTimeout(resolve, 0));
            return rowsOf(table).filter(row => matches(table, row, cond));
          })();
          // Support the chained `.for('update')` row lock as a no-op; locking
          // is emulated by the serialized `transaction` queue below.
          return Object.assign(result, { for: () => result });
        },
      }),
    }),
    insert: (table: any) => ({
      values: (vals: any) => ({
        returning: async () => {
          const rows = rowsOf(table);
          if (vals.sourceKey != null) {
            const dupe = rows.find(r => r.githubProjectId === vals.githubProjectId && r.sourceKey === vals.sourceKey);
            if (dupe) throw new Error('duplicate key value violates unique constraint');
          }
          const row = { id: `00000000-0000-4000-8000-${String(nextId++).padStart(12, '0')}`, ...vals };
          rows.push(row);
          return [row];
        },
      }),
    }),
    update: (table: any) => ({
      set: (set: any) => ({
        where: (cond: any) => ({
          returning: async () => {
            // Yield like the select does so read-modify-write pairs from
            // concurrent callers interleave unless serialized by transaction.
            await new Promise(resolve => setTimeout(resolve, 0));
            const updated: any[] = [];
            for (const row of rowsOf(table)) {
              if (matches(table, row, cond)) {
                Object.assign(row, set);
                updated.push(row);
              }
            }
            return updated;
          },
        }),
      }),
    }),
    delete: (table: any) => ({
      where: (cond: any) => ({
        returning: async () => {
          // Yield like the select does so concurrent deletes interleave and
          // only one caller wins the row (regression coverage for atomicity).
          await new Promise(resolve => setTimeout(resolve, 0));
          const rows = rowsOf(table);
          const deleted = rows.filter(row => matches(table, row, cond));
          const remaining = rows.filter(row => !matches(table, row, cond));
          rows.length = 0;
          rows.push(...remaining);
          return deleted;
        },
      }),
    }),
    execute: async () => {
      advisoryLockCalls++;
    },
    transaction: (fn: (tx: any) => Promise<unknown>) => {
      const run = txTail.then(() => fn(makeDbClient()));
      txTail = run.catch(() => undefined);
      return run;
    },
  });
  return { getAppDb: () => makeDbClient() };
});

import { mountApiRoutes } from '../test-utils';
import { buildFactoryRoutes } from './routes';
import { FACTORY_MIGRATION_SQL } from './schema';
import { parseCreateWorkItem, parseUpdateWorkItem } from './store';

// ── Test harness ─────────────────────────────────────────────────────────
function buildApp(user: { workosId: string; organizationId?: string } | null) {
  const app = new Hono();
  app.use('*', async (c, next) => {
    if (user) c.set('webAuthUser' as never, user as never);
    await next();
  });
  mountApiRoutes(app as any, buildFactoryRoutes());
  return app;
}

const orgUser = { workosId: 'u1', organizationId: 'org1' };
const PROJECT_ID = '11111111-2222-4333-8444-555555555555';

function seedProject(orgId = 'org1', id = PROJECT_ID) {
  (tables['github_projects'] ??= []).push({ id, orgId, repoFullName: 'acme/app' });
}

function json(method: string, path: string, body?: unknown, user: typeof orgUser | null = orgUser) {
  return buildApp(user).request(path, {
    method,
    ...(body !== undefined ? { headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) } : {}),
  });
}

const createBody = (overrides: Record<string, unknown> = {}) => ({
  source: 'github-issue',
  sourceKey: 'github-issue:42',
  title: 'Fix the login flow',
  url: 'https://github.com/acme/app/issues/42',
  stages: ['intake'],
  metadata: { number: 42 },
  ...overrides,
});

beforeEach(() => {
  tables = {};
  nextId = 1;
  txTail = Promise.resolve();
  advisoryLockCalls = 0;
  auditRecorded = [];
  auditFailure = undefined;
  seedProject();
});

afterEach(() => {
  vi.clearAllMocks();
});

// ── Auth / scoping ───────────────────────────────────────────────────────
describe('auth and scoping', () => {
  it('401s without a user', async () => {
    const res = await json('GET', `/web/factory/projects/${PROJECT_ID}/work-items`, undefined, null);
    expect(res.status).toBe(401);
  });

  it('403s without an organization', async () => {
    const res = await buildApp({ workosId: 'u1' }).request(`/web/factory/projects/${PROJECT_ID}/work-items`);
    expect(res.status).toBe(403);
  });

  it('404s when the project belongs to another org', async () => {
    tables = {};
    seedProject('other-org');
    const res = await json('GET', `/web/factory/projects/${PROJECT_ID}/work-items`);
    expect(res.status).toBe(404);
  });

  it('404s on a non-uuid project id', async () => {
    const res = await json('GET', `/web/factory/projects/not-a-uuid/work-items`);
    expect(res.status).toBe(404);
  });

  it('is org-wide: another member of the same org sees the item', async () => {
    await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody());
    const res = await buildApp({ workosId: 'u2', organizationId: 'org1' }).request(
      `/web/factory/projects/${PROJECT_ID}/work-items`,
    );
    const body = await res.json();
    expect(body.workItems).toHaveLength(1);
    expect(body.workItems[0].createdBy).toBe('u1');
  });
});

// ── Create / upsert ──────────────────────────────────────────────────────
describe('POST /web/factory/projects/:id/work-items', () => {
  it('creates a work item with server-stamped history', async () => {
    const res = await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody());
    expect(res.status).toBe(200);
    const { workItem } = await res.json();
    expect(workItem).toMatchObject({
      orgId: 'org1',
      createdBy: 'u1',
      githubProjectId: PROJECT_ID,
      source: 'github-issue',
      sourceKey: 'github-issue:42',
      title: 'Fix the login flow',
      stages: ['intake'],
      metadata: { number: 42 },
    });
    expect(workItem.stageHistory).toHaveLength(1);
    expect(workItem.stageHistory[0]).toMatchObject({ stage: 'intake', by: 'u1' });
    expect(workItem.stageHistory[0].enteredAt).toBeTruthy();
    expect(workItem.stageHistory[0].exitedAt).toBeUndefined();
  });

  it('upserts on sourceKey instead of duplicating', async () => {
    await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody());
    const res = await json(
      'POST',
      `/web/factory/projects/${PROJECT_ID}/work-items`,
      createBody({
        stages: ['execute'],
        sessions: { work: { projectPath: '/sb/wt/issue-42', branch: 'factory/issue-42', threadId: 't-1' } },
      }),
    );
    const { workItem } = await res.json();
    expect(tables['work_items']).toHaveLength(1);
    expect(workItem.stages).toEqual(['execute']);
    // History: intake entered+exited, execute entered.
    expect(workItem.stageHistory.map((e: any) => [e.stage, e.exitedAt !== undefined])).toEqual([
      ['intake', true],
      ['execute', false],
    ]);
    // Session got the acting user stamped server-side.
    expect(workItem.sessions.work).toMatchObject({
      projectPath: '/sb/wt/issue-42',
      branch: 'factory/issue-42',
      threadId: 't-1',
      startedBy: 'u1',
    });
  });

  it('never dedupes manual cards (null sourceKey)', async () => {
    await json(
      'POST',
      `/web/factory/projects/${PROJECT_ID}/work-items`,
      createBody({ source: 'manual', sourceKey: null }),
    );
    await json(
      'POST',
      `/web/factory/projects/${PROJECT_ID}/work-items`,
      createBody({ source: 'manual', sourceKey: null }),
    );
    expect(tables['work_items']).toHaveLength(2);
  });

  it('400s on an invalid body', async () => {
    const res = await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody({ stages: [] }));
    expect(res.status).toBe(400);
    const bad = await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody({ source: 'jira' }));
    expect(bad.status).toBe(400);
  });
});

// ── Patch ────────────────────────────────────────────────────────────────
describe('PATCH /web/factory/work-items/:id', () => {
  async function createItem(overrides: Record<string, unknown> = {}) {
    const res = await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody(overrides));
    return (await res.json()).workItem;
  }

  it('moves stages and appends history with the acting user', async () => {
    const item = await createItem();
    const res = await buildApp({ workosId: 'u2', organizationId: 'org1' }).request(
      `/web/factory/work-items/${item.id}`,
      {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ stages: ['execute'] }),
      },
    );
    const { workItem } = await res.json();
    expect(workItem.stages).toEqual(['execute']);
    expect(workItem.stageHistory).toHaveLength(2);
    expect(workItem.stageHistory[0]).toMatchObject({ stage: 'intake', by: 'u1' });
    expect(workItem.stageHistory[0].exitedAt).toBeTruthy();
    expect(workItem.stageHistory[1]).toMatchObject({ stage: 'execute', by: 'u2' });
  });

  it('keeps concurrent stages untouched when moving one of them', async () => {
    const item = await createItem({ stages: ['execute', 'review'] });
    const res = await json('PATCH', `/web/factory/work-items/${item.id}`, { stages: ['done'] });
    const { workItem } = await res.json();
    expect(workItem.stages).toEqual(['done']);
    const open = workItem.stageHistory.filter((e: any) => e.exitedAt === undefined);
    expect(open.map((e: any) => e.stage)).toEqual(['done']);
  });

  it('merges sessions and metadata instead of replacing', async () => {
    const item = await createItem({
      sessions: { work: { projectPath: '/sb/wt/a', branch: 'b-a', threadId: 't-a' } },
      metadata: { number: 42, labels: ['bug'] },
    });
    const res = await json('PATCH', `/web/factory/work-items/${item.id}`, {
      sessions: { review: { projectPath: '/sb/wt/r', branch: 'b-r', threadId: 't-r' } },
      metadata: { prNumber: 7 },
    });
    const { workItem } = await res.json();
    expect(Object.keys(workItem.sessions).sort()).toEqual(['review', 'work']);
    expect(workItem.metadata).toEqual({ number: 42, labels: ['bug'], prNumber: 7 });
  });

  it('serializes concurrent patches so neither session merge is dropped', async () => {
    const item = await createItem();
    // Two runs file their session refs on the same card at once (e.g. a work
    // run and a review run finishing kickoff together). Each merge reads the
    // current `sessions` and writes it back — without the row lock the last
    // write would silently drop the other role.
    const [workRes, reviewRes] = await Promise.all([
      json('PATCH', `/web/factory/work-items/${item.id}`, {
        sessions: { work: { projectPath: '/sb/wt/a', branch: 'b-a', threadId: 't-a' } },
      }),
      json('PATCH', `/web/factory/work-items/${item.id}`, {
        sessions: { review: { projectPath: '/sb/wt/r', branch: 'b-r', threadId: 't-r' } },
      }),
    ]);
    expect(workRes.status).toBe(200);
    expect(reviewRes.status).toBe(200);

    const list = await json('GET', `/web/factory/projects/${PROJECT_ID}/work-items`);
    const [workItem] = (await list.json()).workItems;
    expect(Object.keys(workItem.sessions).sort()).toEqual(['review', 'work']);
  });

  it('404s for items in another org', async () => {
    const item = await createItem();
    const res = await buildApp({ workosId: 'u9', organizationId: 'org2' }).request(
      `/web/factory/work-items/${item.id}`,
      {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ stages: ['done'] }),
      },
    );
    expect(res.status).toBe(404);
  });

  it('400s on an empty or invalid patch', async () => {
    const item = await createItem();
    expect((await json('PATCH', `/web/factory/work-items/${item.id}`, {})).status).toBe(400);
    expect((await json('PATCH', `/web/factory/work-items/${item.id}`, { title: '' })).status).toBe(400);
  });
});

// ── Delete ───────────────────────────────────────────────────────────────
describe('DELETE /web/factory/work-items/:id', () => {
  it('removes the item for the org', async () => {
    const created = await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody());
    const { workItem } = await created.json();
    const res = await json('DELETE', `/web/factory/work-items/${workItem.id}`);
    expect((await res.json()).ok).toBe(true);
    expect(tables['work_items']).toHaveLength(0);
  });

  it('404s for unknown or cross-org items', async () => {
    expect((await json('DELETE', `/web/factory/work-items/00000000-0000-4000-8000-000000000099`)).status).toBe(404);
  });
});

// ── Relations ────────────────────────────────────────────────────────────
describe('work item relationships', () => {
  async function create(overrides: Record<string, unknown> = {}) {
    const res = await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody(overrides));
    return { res, body: await res.json() };
  }

  it('relates a separate PR review item to an issue in the same org project', async () => {
    const { body: parent } = await create();
    const { res, body } = await create({
      source: 'github-pr',
      sourceKey: 'github-pr:7',
      title: 'Review the login fix',
      parentWorkItemId: parent.workItem.id,
    });

    expect(res.status).toBe(200);
    expect(body.workItem).toMatchObject({
      source: 'github-pr',
      parentWorkItemId: parent.workItem.id,
    });
    expect(tables['work_items']).toHaveLength(2);
  });

  it('preserves an existing relation when a repeated source-key upsert omits or nulls the parent field', async () => {
    const { body: parent } = await create({ sourceKey: 'github-issue:upsert-parent' });
    const { body: child } = await create({
      source: 'github-pr',
      sourceKey: 'github-pr:upsert-child',
      parentWorkItemId: parent.workItem.id,
    });

    const repeated = await create({
      source: 'github-pr',
      sourceKey: 'github-pr:upsert-child',
      title: 'Updated review title',
    });

    expect(repeated.res.status).toBe(200);
    expect(repeated.body.workItem).toMatchObject({
      id: child.workItem.id,
      parentWorkItemId: parent.workItem.id,
      title: 'Updated review title',
    });

    const repeatedWithNull = await create({
      source: 'github-pr',
      sourceKey: 'github-pr:upsert-child',
      parentWorkItemId: null,
      title: 'Updated again',
    });
    expect(repeatedWithNull.body.workItem).toMatchObject({
      id: child.workItem.id,
      parentWorkItemId: parent.workItem.id,
      title: 'Updated again',
    });
  });

  it('attaches a relation when a repeated source-key upsert supplies a parent', async () => {
    const { body: parent } = await create({ sourceKey: 'github-issue:late-parent' });
    const { body: existing } = await create({
      source: 'github-pr',
      sourceKey: 'github-pr:late-child',
      parentWorkItemId: null,
    });

    const related = await create({
      source: 'github-pr',
      sourceKey: 'github-pr:late-child',
      parentWorkItemId: parent.workItem.id,
    });

    expect(related.body.workItem).toMatchObject({
      id: existing.workItem.id,
      parentWorkItemId: parent.workItem.id,
    });
  });

  it('rejects missing, cross-project, and cross-org parents', async () => {
    const missing = await create({
      sourceKey: 'github-pr:missing',
      parentWorkItemId: '00000000-0000-4000-8000-000000000099',
    });
    expect(missing.res.status).toBe(400);

    const otherProjectId = '22222222-3333-4444-8555-666666666666';
    seedProject('org1', otherProjectId);
    const otherParentResponse = await json(
      'POST',
      `/web/factory/projects/${otherProjectId}/work-items`,
      createBody({ sourceKey: 'github-issue:other-project' }),
    );
    const otherParent = (await otherParentResponse.json()).workItem;
    const crossProject = await create({ sourceKey: 'github-pr:cross-project', parentWorkItemId: otherParent.id });
    expect(crossProject.res.status).toBe(400);

    const crossOrgId = '00000000-0000-4000-8000-000000000088';
    (tables['work_items'] ??= []).push({
      ...parentRow(crossOrgId),
      orgId: 'org2',
      githubProjectId: PROJECT_ID,
    });
    const crossOrg = await create({ sourceKey: 'github-pr:cross-org', parentWorkItemId: crossOrgId });
    expect(crossOrg.res.status).toBe(400);
  });

  it('rejects self-relations and cycles while allowing the relation to be cleared', async () => {
    const { body: first } = await create({ sourceKey: 'github-issue:first' });
    const { body: second } = await create({
      sourceKey: 'github-pr:second',
      parentWorkItemId: first.workItem.id,
    });

    const self = await json('PATCH', `/web/factory/work-items/${first.workItem.id}`, {
      parentWorkItemId: first.workItem.id,
    });
    expect(self.status).toBe(400);

    const cycle = await json('PATCH', `/web/factory/work-items/${first.workItem.id}`, {
      parentWorkItemId: second.workItem.id,
    });
    expect(cycle.status).toBe(400);

    const cleared = await json('PATCH', `/web/factory/work-items/${second.workItem.id}`, {
      parentWorkItemId: null,
    });
    expect(cleared.status).toBe(200);
    expect((await cleared.json()).workItem.parentWorkItemId).toBeNull();
  });

  it('serializes concurrent reciprocal relation updates so only one can commit', async () => {
    const { body: first } = await create({ sourceKey: 'github-issue:concurrent-first' });
    const { body: second } = await create({ sourceKey: 'github-issue:concurrent-second' });

    const results = await Promise.all([
      json('PATCH', `/web/factory/work-items/${first.workItem.id}`, {
        parentWorkItemId: second.workItem.id,
      }),
      json('PATCH', `/web/factory/work-items/${second.workItem.id}`, {
        parentWorkItemId: first.workItem.id,
      }),
    ]);

    expect(results.map(result => result.status).sort()).toEqual([200, 400]);
    expect(advisoryLockCalls).toBe(2);
    const firstRow = tables['work_items'].find(item => item.id === first.workItem.id);
    const secondRow = tables['work_items'].find(item => item.id === second.workItem.id);
    expect(firstRow?.parentWorkItemId === second.workItem.id && secondRow?.parentWorkItemId === first.workItem.id).toBe(
      false,
    );
  });

  it('keeps child and parent independent when either side is deleted', async () => {
    const { body: parent } = await create({ sourceKey: 'github-issue:parent' });
    const { body: child } = await create({
      source: 'github-pr',
      sourceKey: 'github-pr:child',
      parentWorkItemId: parent.workItem.id,
    });

    expect((await json('DELETE', `/web/factory/work-items/${parent.workItem.id}`)).status).toBe(200);
    const list = await json('GET', `/web/factory/projects/${PROJECT_ID}/work-items`);
    const items = (await list.json()).workItems;
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({ id: child.workItem.id, parentWorkItemId: null });
  });

  it('ships additive relation DDL, an ownership lookup index, and non-cascading parent deletion', () => {
    expect(FACTORY_MIGRATION_SQL).toContain('ADD COLUMN IF NOT EXISTS parent_work_item_id uuid');
    expect(FACTORY_MIGRATION_SQL).toContain('ON DELETE SET NULL');
    expect(FACTORY_MIGRATION_SQL).toContain('ON work_items (org_id, github_project_id, parent_work_item_id)');
  });
});

function parentRow(id: string) {
  const now = new Date();
  return {
    id,
    orgId: 'org1',
    createdBy: 'u1',
    githubProjectId: PROJECT_ID,
    source: 'github-issue',
    sourceKey: `github-issue:${id}`,
    parentWorkItemId: null,
    title: 'Parent',
    url: null,
    stages: ['intake'],
    stageHistory: [],
    sessions: {},
    metadata: {},
    createdAt: now,
    updatedAt: now,
  };
}

// ── Metrics ──────────────────────────────────────────────────────────────
describe('GET /web/factory/projects/:id/metrics', () => {
  it('401s without a user and 404s for projects outside the org', async () => {
    expect((await json('GET', `/web/factory/projects/${PROJECT_ID}/metrics`, undefined, null)).status).toBe(401);

    tables = {};
    seedProject('other-org');
    expect((await json('GET', `/web/factory/projects/${PROJECT_ID}/metrics`)).status).toBe(404);
  });

  it('clamps the days param to a supported window', async () => {
    const bodyFor = async (query: string) =>
      (await (await json('GET', `/web/factory/projects/${PROJECT_ID}/metrics${query}`)).json()).metrics;

    expect((await bodyFor('')).windowDays).toBe(30);
    expect((await bodyFor('?days=7')).windowDays).toBe(7);
    expect((await bodyFor('?days=90')).windowDays).toBe(90);
    expect((await bodyFor('?days=17')).windowDays).toBe(30);
    expect((await bodyFor('?days=evil')).windowDays).toBe(30);
  });

  it('aggregates the project board: throughput, WIP, transitions, and source mix', async () => {
    // One card completed today (intake → done), one still in intake.
    const created = await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody());
    const { workItem } = await created.json();
    await json('PATCH', `/web/factory/work-items/${workItem.id}`, { stages: ['done'] });
    await json(
      'POST',
      `/web/factory/projects/${PROJECT_ID}/work-items`,
      createBody({ source: 'manual', sourceKey: null, title: 'Manual card' }),
    );

    const res = await json('GET', `/web/factory/projects/${PROJECT_ID}/metrics?days=7`);
    expect(res.status).toBe(200);
    const { metrics } = await res.json();

    expect(metrics.windowDays).toBe(7);
    expect(metrics.throughput).toHaveLength(7);
    expect(metrics.throughput.reduce((sum: number, p: any) => sum + p.count, 0)).toBe(1);
    expect(metrics.cycleTime.samples).toBe(1);
    expect(Object.fromEntries(metrics.wip.map((w: any) => [w.stage, w.count]))).toEqual({ done: 1, intake: 1 });
    expect(metrics.wipTotal).toBe(1);
    expect(metrics.agingWip).toHaveLength(1);
    expect(metrics.agingWip[0]).toMatchObject({ title: 'Manual card', stage: 'intake' });
    // intake entered (x2) + done entered = 3 stage moves, all by the test user.
    expect(metrics.transitions).toEqual({ human: 3, total: 3 });
    expect(metrics.sourceMix).toEqual(
      expect.arrayContaining([
        { source: 'github-issue', count: 1 },
        { source: 'manual', count: 1 },
      ]),
    );
  });

  it('returns zeroed metrics for an empty board', async () => {
    const res = await json('GET', `/web/factory/projects/${PROJECT_ID}/metrics`);
    const { metrics } = await res.json();
    expect(metrics.throughput).toHaveLength(30);
    expect(metrics.cycleTime).toEqual({ medianMs: null, p90Ms: null, samples: 0 });
    expect(metrics.wip).toEqual([]);
    expect(metrics.agingWip).toEqual([]);
  });
});

// ── Audit events ─────────────────────────────────────────────────────────
describe('audit events', () => {
  async function createItem(overrides: Record<string, unknown> = {}) {
    const res = await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody(overrides));
    return (await res.json()).workItem;
  }

  it('records work_item.created on POST with actor, project, and target', async () => {
    const item = await createItem();
    expect(auditRecorded).toHaveLength(1);
    expect(auditRecorded[0]).toMatchObject({
      orgId: 'org1',
      actorId: 'u1',
      action: 'factory.work_item.created',
      githubProjectId: PROJECT_ID,
      targets: [{ type: 'work_item', id: item.id, name: 'Fix the login flow' }],
      metadata: { source: 'github-issue', sourceKey: 'github-issue:42', stages: ['intake'] },
    });
  });

  it('records updated (not created) when a POST reuses an existing sourceKey', async () => {
    const item = await createItem();
    auditRecorded = [];

    const session = { projectPath: '/sb/wt/issue-42', branch: 'factory/issue-42', threadId: 't-1' };
    await json(
      'POST',
      `/web/factory/projects/${PROJECT_ID}/work-items`,
      createBody({ stages: ['execute'], sessions: { work: session } }),
    );
    expect(auditRecorded.map(e => e.action)).toEqual([
      'factory.work_item.updated',
      'factory.work_item.stage_moved',
      'factory.run.started',
    ]);
    expect(auditRecorded[1]).toMatchObject({
      targets: [{ type: 'work_item', id: item.id, name: 'Fix the login flow' }],
      metadata: { from: ['intake'], to: ['execute'] },
    });
    expect(auditRecorded[2].metadata).toMatchObject({ role: 'work', branch: 'factory/issue-42' });
  });

  it('records updated + stage_moved with the server-diffed from/to on a stage PATCH', async () => {
    const item = await createItem();
    auditRecorded = [];

    await json('PATCH', `/web/factory/work-items/${item.id}`, { stages: ['execute'] });
    expect(auditRecorded.map(e => e.action)).toEqual(['factory.work_item.updated', 'factory.work_item.stage_moved']);
    expect(auditRecorded[0].metadata).toEqual({ fields: ['stages'] });
    expect(auditRecorded[1]).toMatchObject({
      githubProjectId: PROJECT_ID,
      targets: [{ type: 'work_item', id: item.id, name: 'Fix the login flow' }],
      metadata: { from: ['intake'], to: ['execute'] },
    });
  });

  it('records run.started when a PATCH introduces a new session role, but not on re-file', async () => {
    const item = await createItem();
    auditRecorded = [];

    const session = { projectPath: '/sb/wt/issue-42', branch: 'factory/issue-42', threadId: 't-1' };
    await json('PATCH', `/web/factory/work-items/${item.id}`, { sessions: { work: session } });
    expect(auditRecorded.map(e => e.action)).toEqual(['factory.work_item.updated', 'factory.run.started']);
    expect(auditRecorded[1].metadata).toEqual({
      role: 'work',
      branch: 'factory/issue-42',
      threadId: 't-1',
      projectPath: '/sb/wt/issue-42',
    });

    // Re-filing the same role is not a new run.
    auditRecorded = [];
    await json('PATCH', `/web/factory/work-items/${item.id}`, { sessions: { work: session } });
    expect(auditRecorded.map(e => e.action)).toEqual(['factory.work_item.updated']);
  });

  it('records only updated when the patch does not move stages', async () => {
    const item = await createItem();
    auditRecorded = [];

    await json('PATCH', `/web/factory/work-items/${item.id}`, { title: 'Renamed card' });
    expect(auditRecorded.map(e => e.action)).toEqual(['factory.work_item.updated']);
    expect(auditRecorded[0].metadata).toEqual({ fields: ['title'] });
  });

  it('records work_item.deleted on DELETE', async () => {
    const item = await createItem();
    auditRecorded = [];

    await json('DELETE', `/web/factory/work-items/${item.id}`);
    expect(auditRecorded).toHaveLength(1);
    expect(auditRecorded[0]).toMatchObject({
      action: 'factory.work_item.deleted',
      githubProjectId: PROJECT_ID,
      targets: [{ type: 'work_item', id: item.id, name: 'Fix the login flow' }],
    });
  });

  it('never blocks the mutation when the audit insert throws', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    auditFailure = new Error('audit db down');

    const created = await json('POST', `/web/factory/projects/${PROJECT_ID}/work-items`, createBody());
    expect(created.status).toBe(200);
    const { workItem } = await created.json();

    const patched = await json('PATCH', `/web/factory/work-items/${workItem.id}`, { stages: ['done'] });
    expect(patched.status).toBe(200);

    const deleted = await json('DELETE', `/web/factory/work-items/${workItem.id}`);
    expect(deleted.status).toBe(200);
    expect(tables['work_items']).toHaveLength(0);

    warn.mockRestore();
  });
});

// ── Validation units ─────────────────────────────────────────────────────
describe('parseCreateWorkItem', () => {
  it('accepts a minimal valid body and defaults sessions/metadata', () => {
    const input = parseCreateWorkItem({ source: 'manual', title: 'Card', stages: ['intake'] });
    expect(input).toEqual({
      source: 'manual',
      sourceKey: null,
      title: 'Card',
      url: null,
      stages: ['intake'],
      sessions: {},
      metadata: {},
    });
  });

  it('rejects bad stages, urls, and oversized metadata', () => {
    expect(parseCreateWorkItem(createBody({ stages: ['in take'] }))).toBeNull();
    expect(parseCreateWorkItem(createBody({ stages: ['a', 'a'] }))).toBeNull();
    expect(parseCreateWorkItem(createBody({ url: 'javascript:alert(1)' }))).toBeNull();
    expect(parseCreateWorkItem(createBody({ metadata: { blob: 'x'.repeat(20_000) } }))).toBeNull();
  });

  it('rejects malformed sessions', () => {
    expect(parseCreateWorkItem(createBody({ sessions: { work: { projectPath: '/p' } } }))).toBeNull();
    expect(
      parseCreateWorkItem(createBody({ sessions: { 'bad role!': { projectPath: '/p', branch: 'b', threadId: 't' } } })),
    ).toBeNull();
  });
});

describe('parseUpdateWorkItem', () => {
  it('rejects an empty patch and passes through valid fields', () => {
    expect(parseUpdateWorkItem({})).toBeNull();
    expect(parseUpdateWorkItem({ stages: ['done'] })).toEqual({ stages: ['done'] });
    expect(parseUpdateWorkItem({ url: null })).toEqual({ url: null });
  });
});
