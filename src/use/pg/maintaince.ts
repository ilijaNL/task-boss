import { createBaseWorker } from '../../worker';
import { TASK_STATES } from './plans';
import { PGClient, createSql, query } from './sql';

const createPlans = (schema: string) => {
  const sql = createSql(schema);

  return {
    expireTasks: () => sql`
      UPDATE {{schema}}.tasks
      SET state = CASE
          WHEN retryCount < retryLimit THEN ${TASK_STATES.retry}::smallint
          ELSE ${TASK_STATES.expired}::smallint
          END,
        completedOn = CASE
                      WHEN retryCount < retryLimit
                      THEN NULL
                      ELSE now()
                      END,
        startAfter = CASE
                      WHEN retryCount = retryLimit THEN startAfter
                      WHEN NOT retryBackoff THEN now() + retryDelay * interval '1'
                      ELSE now() +
                        (
                            retryDelay * 2 ^ LEAST(16, retryCount + 1) / 2
                            +
                            retryDelay * 2 ^ LEAST(16, retryCount + 1) / 2 * random()
                        )
                        * interval '1'
                      END
      WHERE state = ${TASK_STATES.active}
        AND (startedOn + expireIn) < now()
    `,
    purgeTasks: () => sql`
      DELETE FROM {{schema}}.tasks
      WHERE state >= ${TASK_STATES.completed} 
        AND keepUntil < now()
    `,
    deleteOldEvents: () => sql`
      DELETE FROM {{schema}}.events WHERE expire_at < now()
    `,
  };
};

export const createMaintainceWorker = (props: { schema: string; client: PGClient; intervalInMs?: number }) => {
  const plans = createPlans(props.schema);
  const worker = createBaseWorker(
    async () => {
      await query(props.client, plans.deleteOldEvents());
      await query(props.client, plans.purgeTasks());
      await query(props.client, plans.expireTasks());
    },
    { loopInterval: props.intervalInMs || 30000 }
  );

  return worker;
};
