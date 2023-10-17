import { Pool, PoolClient } from 'pg';
import { createBaseWorker } from '../../worker';
import { SelectTask, TASK_STATES, createResolveTasksQueryFn, getStartAfter } from './plans';
import { createSql, query, withTransaction } from './sql';

const createPlans = (schema: string) => {
  const sql = createSql(schema);

  return {
    resolve_tasks: createResolveTasksQueryFn(sql),
    get_expired_tasks: (limit: number) => sql<SelectTask>`
      SELECT
        id,
        retryCount,
        state,
        data,
        meta_data,
        config,
        (EXTRACT(epoch FROM expireIn))::int as expire_in_seconds
      FROM {{schema}}.tasks
        WHERE state = ${TASK_STATES.active}
          AND (startedOn + expireIn) < now()
      ORDER BY startedOn ASC
      LIMIT ${limit}
      FOR UPDATE SKIP LOCKED
    `,
    purgeTasks: () => sql`
      DELETE FROM {{schema}}.tasks_completed
      WHERE "state" >= ${TASK_STATES.completed} 
        AND keepUntil < now()
    `,
    deleteOldEvents: () => sql`
      DELETE FROM {{schema}}.events WHERE "expire_at" < now()
    `,
  };
};

export const createMaintainceWorker = (props: {
  schema: string;
  client: Pool;
  expireInterval?: number;
  cleanupInterval?: number;
}) => {
  const plans = createPlans(props.schema);

  async function expireTasks(client: PoolClient): Promise<boolean> {
    const limit = 200;
    // get events that are expired
    const tasks = await query(client, plans.get_expired_tasks(limit));

    // update
    await query(
      client,
      plans.resolve_tasks(
        tasks.map((expTask) => {
          const newState = expTask.retrycount < expTask.config.r_l ? TASK_STATES.retry : TASK_STATES.expired;

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

  const expireWorker = createBaseWorker(async () => withTransaction(props.client, expireTasks), {
    loopInterval: props.expireInterval ?? 20000,
  });

  const cleanupWorker = createBaseWorker(
    async () => {
      await query(props.client, plans.deleteOldEvents());
      await query(props.client, plans.purgeTasks());
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
