import { TaskTrigger } from '../../definitions';
import { OutgoingTask } from '../../task-boss';
import { JsonObject, JsonValue } from '../../utils';
import { SchemaSQL, createSql } from './sql';

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
  event_data: JsonValue;
  position: string;
};

export function getStartAfter(task: SelectTask) {
  return task.config.r_b ? task.config.r_d * Math.pow(2, task.retrycount) : task.config.r_d;
}

export type InsertEvent = {
  /**
   * Event name
   */
  e_n: string;
  /**
   * Data
   */
  d: JsonValue;
  /**
   * Retention in days.
   * Defaults to 30 days
   */
  rid?: number;
};

export type MetaData = {
  tn: string;
  trace: TaskTrigger;
} & JsonObject;

export type InsertTask = {
  /**
   * Queue
   */
  q: string;
  /**
   * Task state
   */
  s?: TaskState;
  /**
   * Data
   */
  d: JsonValue;
  /**
   * Meta data
   */
  md: MetaData;

  cf: {
    /**
     * Retry limit
     */
    r_l: number;
    /**
     * Retry delay in seconds
     */
    r_d: number;
    /**
     * Retry backoff
     */
    r_b: boolean;
    /**
     * Keep in seconds after complete
     */
    ki_s: number;
  };
  /**
   * Singleton key
   */
  skey: string | null;
  /**
   * Start after seconds
   */
  saf: number;
  /**
   * Expire in seconds
   */
  eis: number;
};

export type SelectTask = {
  /**
   * Bigint
   */
  id: string;
  retrycount: number;
  state: TaskState;
  data: JsonValue;
  meta_data: MetaData;
  config: InsertTask['cf'];
  expire_in_seconds: number;
};

export type SQLPlans = ReturnType<typeof createPlans>;

export const createInsertTask = (task: OutgoingTask, trigger: TaskTrigger, keepInSeconds: number): InsertTask => ({
  d: task.data,
  md: {
    tn: task.task_name,
    trace: trigger,
  },
  q: task.queue,
  eis: task.config.expireInSeconds,
  cf: {
    r_b: task.config.retryBackoff,
    r_d: task.config.retryDelay,
    r_l: task.config.retryLimit,
    ki_s: keepInSeconds,
  },
  saf: task.config.startAfterSeconds,
  skey: task.config.singletonKey,
});

export type ResolvedTask = { id: string; s: TaskState; out: any; saf?: number };

export const createResolveTasksQueryFn = (sql: SchemaSQL) => (tasks: Array<ResolvedTask>) =>
  sql`
WITH _in as (
  SELECT 
    x.id as task_id,
    x.s as new_state,
    x.out as out,
    x.saf as saf
  FROM json_to_recordset(${JSON.stringify(tasks)}) as x(
    id bigint,
    s smallint,
    out jsonb,
    saf integer
  )
), compl as (
  DELETE FROM {{schema}}.tasks t
  WHERE t.id in (SELECT task_id FROM _in WHERE _in.new_state > ${TASK_STATES.active})
    AND t.state = ${TASK_STATES.active}
  RETURNING t.*
), _completed_tasks as (
  INSERT INTO {{schema}}.tasks_completed (
    id,
    queue,
    state,
    data,
    meta_data,
    config,
    output,
    retryCount,
    startedOn,
    createdOn,
    completedOn,
    keepUntil
  ) (SELECT 
    compl.id,
    compl.queue,
    _in.new_state,
    compl.data,
    compl.meta_data,
    compl.config,
    COALESCE(_in.out, compl.output),
    compl.retryCount,
    compl.startedOn,
    compl.createdOn,
    now(),
    (now() + (COALESCE((compl.config->>'ki_s')::integer, 60 * 60 * 24 * 7) * interval '1s'))
    FROM compl INNER JOIN _in ON _in.task_id = compl.id)
    RETURNING id
) UPDATE {{schema}}.tasks t
  SET
    state = _in.new_state,
    startAfter = (now() + (_in.saf * interval '1s')),
    output = _in.out
  FROM _in
  WHERE t.id = _in.task_id
    AND _in.new_state = ${TASK_STATES.retry}
    AND t.state = ${TASK_STATES.active}
`;

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
  function getCursorLockEvents(limit: number) {
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
      LIMIT ${limit}`;

    return events;
  }

  return {
    createTasks,
    getLastEvent,
    ensureQueuePointer: ensureQueuePointer,
    createTasksAndSetCursor,
    getCursorLockEvents,
    createEvents,
    getAndStartTasks: (amount: number) => sql<SelectTask>`
      SELECT
        id,
        retryCount,
        state,
        data,
        meta_data,
        config,
        expire_in_seconds
      FROM {{schema}}.get_tasks(${queue}, ${amount}::integer)
    `,
    resolveTasks: createResolveTasksQueryFn(sql),
  };
};
