import { randomUUID } from 'node:crypto';
import type { WorkflowsStorage } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';
import { createSampleSuspendedSnapshotWithThread } from '../../../../../_test-utils/src/domains/workflows/data';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle workflow run queries', () => {
  let store: OracleStore;
  let workflows: WorkflowsStorage;
  const workflowName = `thread-filter-${randomUUID()}`;
  const snapshots = [
    createSampleSuspendedSnapshotWithThread({ threadId: 'thread-a', layout: 'agentic-loop' }),
    createSampleSuspendedSnapshotWithThread({ threadId: 'thread-a', layout: 'durable' }),
    createSampleSuspendedSnapshotWithThread({ threadId: 'thread-b', layout: 'agentic-loop' }),
    createSampleSuspendedSnapshotWithThread({ threadId: 'thread-b', layout: 'durable' }),
  ];

  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-workflow-query-integration',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    await store.init();
    workflows = (await store.getStore('workflows'))!;
    for (const { snapshot, runId } of snapshots) {
      await workflows.persistWorkflowSnapshot({ workflowName, runId, snapshot });
    }
  });

  afterAll(async () => {
    for (const { runId } of snapshots) await workflows?.deleteWorkflowRunById({ workflowName, runId });
    await store?.close();
  });

  it('filters and counts both snapshot layouts by thread before pagination', async () => {
    const result = await workflows.listWorkflowRuns({ workflowName, threadId: 'thread-a', page: 0, perPage: 1 });
    expect(result.total).toBe(2);
    expect(result.runs).toHaveLength(1);
    expect(snapshots.slice(0, 2).map(snapshot => snapshot.runId)).toContain(result.runs[0]!.runId);
    expect(await workflows.listWorkflowRuns({ workflowName, threadId: 'unknown-thread' })).toEqual({
      runs: [],
      total: 0,
    });
  });

  it('returns only status and timestamp when a summary is requested', async () => {
    const result = await workflows.listWorkflowRuns({ workflowName, summary: true });
    expect(result.runs).toHaveLength(4);
    for (const run of result.runs) {
      expect(run.snapshot).toEqual({ status: 'suspended', timestamp: expect.any(Number) });
    }
  });
});
