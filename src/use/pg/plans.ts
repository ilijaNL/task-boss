import { TaskTrigger } from '../../definitions';
import { OutgoingTask } from '../../task-boss';
import { createSql } from './sql';

export type TaskName = string;

export const TASK_STATES = {
  created: 0,
  retry: 1,
  active: 2,
  completed: 3,
  expired: 4,
  cancelled: 5,
  failed: 6,
} as const;

export type TaskState = (typeof TASK_STATES)[keyof typeof TASK_STATES];

type SelectEvent = {
  /**
   * Bigint
   */
  id: string;
  event_name: string;
  event_data: any;
  position: string;
};

export type InsertEvent = {
  /**
   * Event name
   */
  e_n: string;
  /**
   * Data
   */
  d: Record<string, any>;
  /**
   * Retention in days.
   * Defaults to 30 days
   */
  rid?: number;
};

export type TaskDTO<T> = { tn: string; data: T; trace: TaskTrigger };

export type InsertTask<T = object> = {
  /**
   * Queue
   */
  q: string;
  /**
   * Data
   */
  d: TaskDTO<T>;
  /**
   * Task state
   */
  s?: TaskState;
  /**
   * Retry limit
   */
  r_l: number;
  /**
   * Retry delay
   */
  r_d: number;
  /**
   * Retry backoff
   */
  r_b: boolean;
  /**
   * Start after seconds
   */
  saf: number;
  /**
   * Expire in seconds
   */
  eis: number;
  /**
   * Keep until in seconds
   */
  kis: number;
  /**
   * Singleton key
   */
  skey: string | null;
};

export type SelectTask<T = object> = {
  /**
   * Bigint
   */
  id: string;
  retrycount: number;
  state: number;
  data: TaskDTO<T>;
  expire_in_seconds: number;
};

export type SQLPlans = ReturnType<typeof createPlans>;

export const createInsertTask = (task: OutgoingTask, trigger: TaskTrigger, keepInSeconds: number): InsertTask => ({
  d: {
    data: task.data,
    tn: task.task_name,
    trace: trigger,
  },
  q: task.queue,
  eis: task.config.expireInSeconds,
  kis: keepInSeconds,
  r_b: task.config.retryBackoff,
  r_d: task.config.retryDelay,
  r_l: task.config.retryLimit,
  saf: task.config.startAfterSeconds,
  skey: task.config.singletonKey,
});

export const createPlans = (schema: string, queue: string) => {
  const sql = createSql(schema);
  function createTasks(tasks: InsertTask[]) {
    return sql<{}>`SELECT {{schema}}.create_bus_tasks(${JSON.stringify(tasks)}::jsonb)`;
  }

  function createEvents(events: InsertEvent[]) {
    return sql`SELECT {{schema}}.create_bus_events(${JSON.stringify(events)}::jsonb)`;
  }

  function getLastEvent() {
    return sql<{ id: string; position: string }>`
      SELECT 
        id,
        pos as position
      FROM {{schema}}.events
      -- for index
      WHERE pos > 0
      ORDER BY pos DESC
      LIMIT 1`;
  }

  /**
   *  On new queue entry, insert a cursor
   */
  function ensureQueuePointer(position: number) {
    return sql`
      INSERT INTO {{schema}}.cursors (svc, l_p) 
      VALUES (${queue}, ${position}) 
      ON CONFLICT DO NOTHING`;
  }

  // combine into single query (CTE) to reduce round trips
  function createTasksAndSetCursor(tasks: InsertTask[], last_position: number) {
    return sql`
      with _cu as (
        UPDATE {{schema}}.cursors 
        SET l_p = ${last_position} 
        WHERE svc = ${queue}
      ) SELECT {{schema}}.create_bus_tasks(${JSON.stringify(tasks)}::jsonb)
    `;
  }

  // the pos > 0 is needed to have an index scan on events table
  function getCursorLockEvents(options: { limit: number }) {
    const events = sql<SelectEvent>`
      SELECT 
        id, 
        event_name, 
        event_data,
        pos as position
      FROM {{schema}}.events
      WHERE pos > 0 
      AND pos > (
        SELECT 
          l_p as cursor
        FROM {{schema}}.cursors 
        WHERE svc = ${queue}
        LIMIT 1
        FOR UPDATE
        SKIP LOCKED
      )
      ORDER BY pos ASC
      LIMIT ${options.limit}`;

    return events;
  }

  return {
    createTasks,
    getLastEvent,
    ensureQueuePointer: ensureQueuePointer,
    createTasksAndSetCursor,
    getCursorLockEvents,
    createEvents,
    getTasks: (props: { amount: number }) => sql<SelectTask>`
    with _tasks as (
      SELECT
        id
      FROM {{schema}}.tasks
      WHERE queue = ${queue}
        AND startAfter < now()
        AND state < ${TASK_STATES.active}
      ORDER BY createdOn ASC
      LIMIT ${props.amount}
      FOR UPDATE SKIP LOCKED
    ) UPDATE {{schema}}.tasks t 
      SET
        state = ${TASK_STATES.active}::smallint,
        startedOn = now(),
        retryCount = CASE WHEN state = ${TASK_STATES.retry} 
                      THEN retryCount + 1 
                      ELSE retryCount END
      FROM _tasks
      WHERE t.id = _tasks.id
      RETURNING t.id, t.retryCount, t.state, t.data, 
        (EXTRACT(epoch FROM expireIn))::int as expire_in_seconds
    `,
    resolveTasks: (tasks: Array<{ t: string; p: string; s: boolean }>) => sql`
    WITH _in as (
      SELECT 
        x.t as task_id,
        x.s as success,
        x.p as payload
      FROM json_to_recordset(${JSON.stringify(tasks)}) as x(
        t bigint,
        s boolean,
        p jsonb
      )
    ), _failed as (
      UPDATE {{schema}}.tasks t
      SET
        state = CASE
          WHEN retryCount < retryLimit THEN ${TASK_STATES.retry}::smallint ELSE ${TASK_STATES.failed}::smallint END,
        completedOn = CASE WHEN retryCount < retryLimit THEN NULL ELSE now() END,
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
                      END,
        output = _in.payload
      FROM _in
      WHERE t.id = _in.task_id
        AND _in.success = false
        AND t.state < ${TASK_STATES.completed}
    ) UPDATE {{schema}}.tasks t
      SET
        state = ${TASK_STATES.completed}::smallint,
        completedOn = now(),
        output = _in.payload
      FROM _in
      WHERE t.id = _in.task_id
        AND _in.success = true
        AND t.state = ${TASK_STATES.active}
    `,
  };
};
