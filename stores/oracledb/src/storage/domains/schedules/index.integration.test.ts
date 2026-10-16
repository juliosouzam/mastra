import { randomUUID } from 'node:crypto';
import { TABLE_SCHEDULES } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';
import { createSampleSchedule, createSampleTrigger } from '../../../../../_test-utils/src/domains/schedules/data';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')(
  'Oracle schedule concurrency and retention',
  () => {
    const config = {
      id: 'oracle-schedule-concurrency',
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
      other = new OracleStore({ ...config, id: 'oracle-schedule-other', disableInit: true });
    });
    afterAll(async () => {
      await store?.close();
      await other?.close();
    });

    it('allows exactly one scheduler to claim a due fire across pools', async () => {
      const schedules = (await store.getStore('schedules'))!;
      const second = (await other.getStore('schedules'))!;
      const schedule = createSampleSchedule({ nextFireAt: Date.now() - 1000 });
      try {
        await schedules.createSchedule(schedule);
        const claims = await Promise.all(
          Array.from({ length: 16 }, (_, index) =>
            (index % 2 ? schedules : second).updateScheduleNextFire(
              schedule.id,
              schedule.nextFireAt,
              schedule.nextFireAt + 60_000,
              Date.now(),
              `run-${index}`,
              'completed',
            ),
          ),
        );
        expect(claims.filter(Boolean)).toHaveLength(1);
        expect(await schedules.getSchedule(schedule.id)).toMatchObject({
          status: 'completed',
          nextFireAt: schedule.nextFireAt + 60_000,
        });
      } finally {
        await schedules.deleteSchedule(schedule.id);
      }
    });

    it('prunes only old trigger history and preserves schedule definitions', async () => {
      const schedules = (await store.getStore('schedules'))!;
      const schedule = createSampleSchedule();
      const old = Date.now() - 40 * 24 * 60 * 60 * 1000;
      try {
        await schedules.createSchedule(schedule);
        await schedules.recordTrigger(createSampleTrigger({ scheduleId: schedule.id, actualFireAt: old }));
        const recent = createSampleTrigger({
          scheduleId: schedule.id,
          error: 'diagnostic '.repeat(5000),
          outcome: 'failed',
        });
        await schedules.recordTrigger(recent);
        await schedules.prune({ triggers: { maxAge: '30d', batchSize: 1 } });
        const history = await schedules.listTriggers(schedule.id);
        expect(history).toHaveLength(1);
        expect(history[0]).toMatchObject(recent);
        expect(await schedules.getSchedule(schedule.id)).not.toBeNull();
      } finally {
        await schedules.deleteSchedule(schedule.id);
      }
    });

    it('reads legacy heartbeat targets and can clear optional owner and timezone fields', async () => {
      const schedules = (await store.getStore('schedules'))!;
      const schedule = createSampleSchedule({ ownerType: 'agent', ownerId: randomUUID(), timezone: 'UTC' });
      try {
        await schedules.createSchedule(schedule);
        await store.db.update({
          tableName: TABLE_SCHEDULES,
          keys: { id: schedule.id },
          data: { target: { type: 'heartbeat', agentId: 'legacy-agent', prompt: 'Check status' } },
        });
        expect((await schedules.getSchedule(schedule.id))!.target).toEqual({
          type: 'agent',
          agentId: 'legacy-agent',
          prompt: 'Check status',
        });
        await schedules.updateSchedule(schedule.id, { ownerType: undefined, ownerId: undefined, timezone: undefined });
        expect(await schedules.getSchedule(schedule.id)).toMatchObject({
          ownerType: undefined,
          ownerId: undefined,
          timezone: undefined,
        });
        expect((await schedules.listSchedules({ ownerType: null, ownerId: null })).map(row => row.id)).toContain(
          schedule.id,
        );
      } finally {
        await schedules.deleteSchedule(schedule.id);
      }
    });
  },
);
