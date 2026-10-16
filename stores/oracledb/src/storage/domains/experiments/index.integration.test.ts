import { randomUUID } from 'node:crypto';
import { TABLE_EXPERIMENTS, TABLE_EXPERIMENT_RESULTS } from '@mastra/core/storage';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';
import {
  createSampleExperiment,
  createSampleExperimentResult,
} from '../../../../../_test-utils/src/domains/experiments/data';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle experiment integration', () => {
  let store: OracleStore;
  const config = {
    user: process.env.ORACLE_DATABASE_USER,
    password: process.env.ORACLE_DATABASE_PASSWORD,
    connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    skipDefaultIndexes: true,
  };
  beforeAll(async () => {
    store = new OracleStore({ id: 'oracle-experiment-contract', ...config });
    await store.init();
  });
  beforeEach(async () => {
    await store.stores.experiments!.dangerouslyClearAll();
    await store.stores.datasets!.dangerouslyClearAll();
  });
  afterAll(async () => {
    await store?.stores.experiments!.dangerouslyClearAll();
    await store?.stores.datasets!.dangerouslyClearAll();
    await store?.close();
  });

  it('keeps one result identity across concurrent retries and preserves reviewer comments', async () => {
    const experiments = store.stores.experiments!;
    const other = new OracleStore({ id: 'oracle-experiment-retry', ...config, disableInit: true });
    try {
      const experiment = await experiments.createExperiment(createSampleExperiment());
      const input = { ...createSampleExperimentResult(), experimentId: experiment.id };
      const results = await Promise.all(
        Array.from({ length: 24 }, (_, i) =>
          (i % 2 ? experiments : other.stores.experiments!).upsertExperimentResult({ ...input, output: { writer: i } }),
        ),
      );
      expect(new Set(results.map(result => result.id)).size).toBe(1);
      const first = results[0]!;
      await experiments.updateExperimentResult({
        id: first.id,
        comment: 'Revisão 🌎 '.repeat(6000),
        status: 'reviewed',
      });
      const retry = await experiments.upsertExperimentResult({ ...input, output: false });
      expect(retry).toMatchObject({
        id: first.id,
        createdAt: first.createdAt,
        output: false,
        comment: 'Revisão 🌎 '.repeat(6000),
      });
      const secondAttempt = await experiments.upsertExperimentResult({ ...input, attempt: 1 });
      expect(secondAttempt.id).not.toBe(first.id);
      expect(
        (
          await experiments.listExperimentResults({
            experimentId: experiment.id,
            pagination: { page: 0, perPage: false },
          })
        ).results,
      ).toHaveLength(2);
    } finally {
      await other.close();
    }
  });

  it('preserves authored JSON keys, large values, primitives, empty text, and exact instants', async () => {
    const experiments = store.stores.experiments!;
    const payload = JSON.parse(
      '{"__proto__":{"literal":true},"constructor":"literal","empty":"","zero":0,"enabled":false,"large":1e308,"small":5e-324}',
    );
    payload.text = 'NUL\u0000 astral 🌎 '.repeat(5000);
    const experiment = await experiments.createExperiment(
      createSampleExperiment({ name: '', description: '', metadata: payload, provenance: payload }),
    );
    expect(await experiments.getExperimentById({ id: experiment.id })).toMatchObject({
      name: '',
      description: '',
      metadata: payload,
      provenance: payload,
    });
    for (const value of [null, false, 0, '', 'false', payload]) {
      const input = {
        ...createSampleExperimentResult(),
        experimentId: experiment.id,
        input: value,
        output: value,
        groundTruth: value,
        startedAt: new Date('2026-10-16T10:23:45.678Z'),
        completedAt: new Date('2026-10-16T10:23:46.789Z'),
      };
      const result = await experiments.addExperimentResult(input);
      expect(await experiments.getExperimentResultById({ id: result.id })).toMatchObject(input);
      expect(await experiments.updateExperimentResult({ id: result.id, comment: '' })).toMatchObject({ comment: '' });
    }
  });

  it('redacts all item versions and results without allowing late result writes to restore their payloads', async () => {
    const datasets = store.stores.datasets!;
    const experiments = store.stores.experiments!;
    const dataset = await datasets.createDataset({ name: 'purge experiment dataset' });
    const item = await datasets.addItem({ datasetId: dataset.id, input: { secret: 'first' } });
    await datasets.updateItem({ datasetId: dataset.id, id: item.id, input: { secret: 'second' } });
    const experiment = await experiments.createExperiment(
      createSampleExperiment({ datasetId: dataset.id, datasetVersion: 2 }),
    );
    const input = {
      ...createSampleExperimentResult(),
      experimentId: experiment.id,
      itemId: item.id,
      input: { secret: 'input' },
      output: { secret: 'output' },
      metadata: { secret: 'metadata' },
    };
    const result = await experiments.upsertExperimentResult(input);
    await experiments.updateExperimentResult({ id: result.id, comment: 'secret reviewer text', tags: ['secret'] });
    const other = new OracleStore({ id: 'oracle-experiment-purge-race', ...config, disableInit: true });
    try {
      await Promise.all([
        datasets.purgeItem({ datasetId: dataset.id, id: item.id }),
        other.stores.experiments!.upsertExperimentResult(input),
      ]);
      const retry = await other.stores.experiments!.upsertExperimentResult(input);
      expect(retry).toMatchObject({
        input: null,
        output: null,
        groundTruth: null,
        metadata: { __purged: true },
        tags: null,
        comment: null,
      });
      expect(
        await experiments.updateExperimentResult({ id: result.id, tags: ['restored'], comment: 'restored' }),
      ).toMatchObject({ tags: null, comment: null });
      for (const version of await datasets.getItemHistory(item.id))
        expect(version).toMatchObject({ input: null, metadata: { __purged: true } });
      const next = await experiments.addExperimentResult({ ...input, attempt: 1 });
      expect(next).toMatchObject({ input: null, output: null, metadata: { __purged: true } });
      await datasets.deleteDataset({ id: dataset.id });
      expect(await experiments.getExperimentById({ id: experiment.id })).toMatchObject({
        datasetId: null,
        datasetVersion: null,
      });
      expect(
        (await experiments.listExperimentResults({ experimentId: experiment.id, pagination: { page: 0, perPage: 10 } }))
          .results,
      ).toEqual([]);
    } finally {
      await other.close();
    }
  });

  it('prunes completed experiment families in bounded batches and respects cancellation', async () => {
    const experiments = store.stores.experiments!;
    const oldIds: string[] = [];
    for (let i = 0; i < 3; i++) {
      const experiment = await experiments.createExperiment(createSampleExperiment());
      oldIds.push(experiment.id);
      await experiments.addExperimentResult({ ...createSampleExperimentResult(), experimentId: experiment.id });
      await experiments.updateExperiment({
        id: experiment.id,
        completedAt: new Date(Date.now() - 40 * 86_400_000),
        status: 'completed',
      });
    }
    const recent = await experiments.createExperiment(createSampleExperiment());
    const signal = AbortSignal.abort();
    expect(await experiments.prune({ experiments: { maxAge: '30d', batchSize: 1 } }, { signal })).toEqual([
      { domain: 'experiments', table: TABLE_EXPERIMENT_RESULTS, deleted: 0, done: false },
      { domain: 'experiments', table: TABLE_EXPERIMENTS, deleted: 0, done: false },
    ]);
    const first = await experiments.prune({ experiments: { maxAge: '30d', batchSize: 1 } }, { maxBatches: 1 });
    expect(first.map(result => result.deleted)).toEqual([1, 1]);
    expect(first.every(result => !result.done)).toBe(true);
    const rest = await experiments.prune({ experiments: { maxAge: '30d', batchSize: 1 } });
    expect(rest.map(result => result.deleted)).toEqual([2, 2]);
    for (const id of oldIds) expect(await experiments.getExperimentById({ id })).toBeNull();
    expect(await experiments.getExperimentById({ id: recent.id })).not.toBeNull();
    await expect(
      experiments.upsertExperimentResult({ ...createSampleExperimentResult(), experimentId: oldIds[0]! }),
    ).rejects.toThrow('not found');
  });

  it('applies result tenancy filters to result rows even when the parent has a different scope', async () => {
    const experiments = store.stores.experiments!;
    const experiment = await experiments.createExperiment(createSampleExperiment({ organizationId: 'parent-org' }));
    const result = await experiments.addExperimentResult({
      ...createSampleExperimentResult(),
      experimentId: experiment.id,
      organizationId: 'result-org',
      projectId: randomUUID(),
    });
    await experiments.deleteExperimentResults({
      experimentId: experiment.id,
      filters: { organizationId: 'parent-org' },
    });
    expect(await experiments.getExperimentResultById({ id: result.id })).not.toBeNull();
    await experiments.deleteExperimentResults({
      experimentId: experiment.id,
      filters: { organizationId: 'result-org' },
    });
    expect(await experiments.getExperimentResultById({ id: result.id })).toBeNull();
    expect(await experiments.getExperimentById({ id: experiment.id })).not.toBeNull();
  });
});
