import { Pool, PoolClient } from 'pg';
import { InsertTask, TASK_STATES, createInsertTask, createPlans, getStartAfter } from './plans';
import { query, withTransaction } from './sql';
import { createTaskWorker } from './task';
import { defineTask } from '../../definitions';
import { Type } from '@sinclair/typebox';
import { directTask } from './with-pg';

export const maintanceQueue = '__maintaince__';

const cleanUpTask = defineTask({
  schema: Type.Object({
    flag: Type.Boolean(),
  }),
  task_name: 'cleanup',
  config: {
    retryLimit: 99,
    expireInSeconds: 30,
  },
});

const expireTask = defineTask({
  schema: Type.Object({
    flag: Type.Boolean(),
  }),
  task_name: 'exp',
  config: {
    retryLimit: 99,
    expireInSeconds: 30,
  },
});

export const createMaintainceWorker = (props: {
  schema: string;
  client: Pool;
  /**
   * Interval of the expire task (expiring event cursors and tasks).
   * @default 30 seconds
   */
  expireIntervalInSec?: number;
  /**
   * Interval of the clean up task.
   * @default 300 seconds
   */
  cleanUpIntervalInSec?: number;
}) => {
  const sqlPlans = createPlans(props.schema);
  const expireIntervalInSec = Math.max(props.expireIntervalInSec ?? 30, 1);
  const cleanUpIntervalInSec = Math.max(props.cleanUpIntervalInSec ?? 60 * 5, 1);

  function scheduleExpireCmd(flag: boolean, startAfterSec: number) {
    const expireTaskOut: InsertTask = createInsertTask(
      {
        ...expireTask.from(
          { flag },
          {
            startAfterSeconds: startAfterSec,
            // need to do this since the running task is not yet resolved
            singletonKey: `exp_${flag ? '1' : '0'}`,
          }
        ),
        queue: maintanceQueue,
      },
      directTask,
      60 * 60 * 24
    );

    const exprCmd = sqlPlans.createTasks([expireTaskOut]);

    return exprCmd;
  }

  function scheduleCleanCmd(flag: boolean, startAfterSec: number) {
    const cleanUpTaskOut: InsertTask = createInsertTask(
      {
        ...cleanUpTask.from(
          { flag },
          {
            startAfterSeconds: startAfterSec,
            // need to do this since the running task is not yet completed
            singletonKey: `cleanup_${flag ? '1' : '0'}`,
          }
        ),
        queue: maintanceQueue,
      },
      directTask,
      60 * 60 * 24 * 3
    );

    const scheduleCmd = sqlPlans.createTasks([cleanUpTaskOut]);

    return scheduleCmd;
  }

  const taskWorker = createTaskWorker({
    maxConcurrency: 20,
    poolInternvalInMs: 10000,
    refillThresholdPct: 0,
    async popTasks(amount) {
      const result = await query(props.client, sqlPlans.getAndStartTasks(maintanceQueue, amount));
      return result;
    },
    async resolveTask(task) {
      await query(props.client, sqlPlans.resolveTasks([task]));
    },
    async handler(task) {
      // expire
      if (task.meta_data.tn === expireTask.task_name) {
        return withTransaction(props.client, async (client) => {
          const hasMore = await expireTasksTx(client);
          await query(client, sqlPlans.expireCursorLocks());
          await query(client, scheduleExpireCmd(!(task.data as any).flag, hasMore ? 0 : expireIntervalInSec));
        });
      }

      // clean up
      if (task.meta_data.tn === cleanUpTask.task_name) {
        return withTransaction(props.client, async (client) => {
          await Promise.all([
            //
            query(client, sqlPlans.deleteOldEvents()),
            query(client, sqlPlans.purgeTasks()),
          ]);
          await query(client, scheduleCleanCmd(!(task.data as any).flag, cleanUpIntervalInSec));
        });
      }

      throw new Error(`handler for ${task.meta_data.tn} does not exist`);
    },
  });

  // should be used in transaction
  // this can be probably be optimized to move to a single SQL query to reduce data roundtrips
  async function expireTasksTx(client: PoolClient): Promise<boolean> {
    const limit = 300;
    // get events that are expired
    const tasks = await query(client, sqlPlans.get_expired_tasks(limit));

    if (tasks.length === 0) {
      return false;
    }

    // update
    await query(
      client,
      sqlPlans.resolveTasks(
        tasks.map((expTask) => {
          const newState =
            expTask.retrycount >= expTask.config.r_l
              ? //
                TASK_STATES.expired
              : TASK_STATES.retry;

          return {
            id: expTask.id,
            s: newState,
            saf: newState === TASK_STATES.retry ? getStartAfter(expTask.retrycount, expTask.config) : undefined,
            out: undefined,
          };
        })
      )
    );

    return tasks.length === limit;
  }

  return {
    ...taskWorker,
    async start() {
      await Promise.all([
        query(props.client, scheduleCleanCmd(true, 0)),
        query(props.client, scheduleExpireCmd(true, 0)),
      ]);

      taskWorker.start();
    },
  };
};
