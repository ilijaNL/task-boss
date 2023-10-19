import { Pool, PoolClient } from 'pg';
import { createBaseWorker } from '../../worker';
import { TASK_STATES, createPlans, getStartAfter } from './plans';
import { query, withTransaction } from './sql';

export const createMaintainceWorker = (props: {
  schema: string;
  client: Pool;
  expireInterval?: number;
  cleanupInterval?: number;
}) => {
  const plans = createPlans(props.schema);

  // should be used in transaction
  // this can be probably be optimized to move to a single SQL query
  async function expireTasksTrx(client: PoolClient): Promise<boolean> {
    const limit = 200;
    // get events that are expired
    const tasks = await query(client, plans.get_expired_tasks(limit));

    // update
    await query(
      client,
      plans.resolveTasks(
        tasks.map((expTask) => {
          const newState =
            expTask.retrycount >= expTask.config.r_l
              ? //
                TASK_STATES.expired
              : TASK_STATES.retry;

          return {
            id: expTask.id,
            s: newState,
            saf: newState === TASK_STATES.retry ? getStartAfter(expTask) : undefined,
            out: undefined,
          };
        })
      )
    );

    return tasks.length === limit;
  }

  const expireWorker = createBaseWorker(async () => withTransaction(props.client, expireTasksTrx), {
    loopInterval: props.expireInterval ?? 20000,
  });

  const cleanupWorker = createBaseWorker(
    async () => {
      await Promise.all([
        //
        query(props.client, plans.deleteOldEvents()),
        query(props.client, plans.purgeTasks()),
      ]);
    },
    { loopInterval: props.cleanupInterval ?? 120000 }
  );

  return {
    notify() {
      expireWorker.notify();
      cleanupWorker.notify();
    },
    async start() {
      expireWorker.start();
      cleanupWorker.start();
    },
    async stop() {
      await Promise.all([
        //
        expireWorker.stop(),
        cleanupWorker.stop(),
      ]);
    },
  };
};
