import { randomUUID } from 'node:crypto';
import { Mastra } from '@mastra/core/mastra';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle concurrent time travel', () => {
  let storage: OracleStore;
  beforeAll(async () => {
    storage = new OracleStore({
      id: 'oracle-time-travel',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    await storage.init();
  });
  afterAll(async () => {
    await storage?.close();
  });

  it.each(['suspended', 'success'] as const)('claims a %s run for exactly one timeTravel caller', async status => {
    let block = false;
    let release!: () => void;
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    const execute = vi.fn(async () => {
      if (block) await gate;
      return {};
    });
    const work = createStep({ id: 'work', inputSchema: z.object({}), outputSchema: z.object({}), execute });
    const finish = createStep({
      id: 'finish',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ suspend }) => {
        if (status === 'suspended') await suspend({});
        return {};
      },
    });
    const workflow = createWorkflow({
      id: `oracle-time-travel-${randomUUID()}`,
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(work)
      .then(finish)
      .commit();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const run = await workflow.createRun();
    expect((await run.start({ inputData: {} })).status).toBe(status);
    const other = await workflow.createRun({ runId: run.runId });
    const workflows = (await storage.getStore('workflows'))!;
    const load = workflows.loadWorkflowSnapshot.bind(workflows);
    let loaded = 0;
    let releaseLoads!: () => void;
    const bothLoaded = new Promise<void>(resolve => {
      releaseLoads = resolve;
    });
    const loadSpy = vi.spyOn(workflows, 'loadWorkflowSnapshot').mockImplementation(async args => {
      const snapshot = await load(args);
      if (loaded < 2) {
        if (++loaded === 2) releaseLoads();
        await bothLoaded;
      }
      return snapshot;
    });
    block = true;
    execute.mockClear();
    let rejected = 0;
    const results = Promise.allSettled(
      [run.timeTravel({ step: 'work', inputData: {} }), other.timeTravel({ step: 'work', inputData: {} })].map(
        attempt =>
          attempt.catch(error => {
            rejected++;
            throw error;
          }),
      ),
    );
    try {
      await expect.poll(() => rejected, { timeout: 5000 }).toBe(1);
      release();
      const outcomes = await results;
      expect(outcomes.filter(outcome => outcome.status === 'fulfilled')).toHaveLength(1);
      expect(outcomes.find(outcome => outcome.status === 'rejected')?.reason.id).toBe(
        'WORKFLOW_TIME_TRAVEL_ALREADY_CLAIMED',
      );
      expect(execute).toHaveBeenCalledTimes(1);
      expect((await workflows.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }))?.status).toBe(
        status,
      );
    } finally {
      releaseLoads();
      release();
      await results;
      loadSpy.mockRestore();
      await workflows.deleteWorkflowRunById({ workflowName: workflow.id, runId: run.runId });
    }
  });
});
