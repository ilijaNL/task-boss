import { TaskTrace } from '../../definitions';
import { OutgoingTask } from '../../task-boss';
import { JsonValue } from '../../utils';
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

export function getStartAfter(retryCount: number, taskConfig: TaskConfig) {
  return taskConfig.r_b ? taskConfig.r_d * Math.pow(2, retryCount) : taskConfig.r_d;
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
  trace?: TaskTrace;
};

export const defaultKeepInSeconds = 7 * 24 * 60 * 60;
export const defaultEventRetentionInDays = 30;

export type TaskConfig = {
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

  cf: TaskConfig;
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
  config: TaskConfig;
  expire_in_seconds: number;
};

export type SQLPlans = ReturnType<typeof createPlans>;

export const createInsertTask = (task: OutgoingTask, keepInSeconds = defaultKeepInSeconds): InsertTask => ({
  d: task.data,
  md: {
    tn: task.task_name,
    trace: task.trace,
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

export const createInsertPlans = (sql: SchemaSQL) => {
  function createTasks(tasks: InsertTask[]) {
    return sql<{}>`SELECT {{schema}}.create_bus_tasks(${JSON.stringify(tasks)}::jsonb)`;
  }

  function createEvents(events: InsertEvent[]) {
    return sql`SELECT {{schema}}.create_bus_events(${JSON.stringify(events)}::jsonb)`;
  }

  return {
    createEvents,
    createTasks,
  };
};

export const createPlans = (schema: string) => {
  const sql = createSql(schema);
  const { createEvents, createTasks } = createInsertPlans(sql);

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
  function ensureQueuePointer(queue: string, position: number) {
    return sql`
      INSERT INTO {{schema}}.cursors (queue, "offset")
      VALUES (${queue}, ${position}) 
      ON CONFLICT DO NOTHING`;
  }

  // combine into single query (CTE) to reduce round trips
  function createTasksAndSetCursor(queue: string, tasks: InsertTask[], last_position: number) {
    return sql`
      with _cu as (
        UPDATE {{schema}}.cursors 
        SET "offset" = ${last_position},
            locked = false
        WHERE queue = ${queue}
      ) SELECT {{schema}}.create_bus_tasks(${JSON.stringify(tasks)}::jsonb)
    `;
  }

  function unlockCursor(queue: string) {
    return sql`
      UPDATE {{schema}}.cursors 
      SET locked = false,
          expire_lock_at = null
      WHERE queue = ${queue}
    `;
  }

  // the pos > 0 is needed to have an index scan on events table
  function getCursorLockEvents(queue: string, limit: number, expireLockInSec: number) {
    const events = sql<SelectEvent>`
      with _cursor as (
        SELECT 
          id,
          "offset"
        FROM {{schema}}.cursors 
        WHERE queue = ${queue} AND locked = false
        LIMIT 1
        FOR UPDATE
        SKIP LOCKED
      ), lock as (
        UPDATE {{schema}}.cursors
        SET 
          locked = true,
          expire_lock_at = (now() + (${expireLockInSec} * interval '1s'))::timestamptz
        FROM _cursor
        WHERE cursors.id = _cursor.id
      )
      SELECT 
        events.id as id, 
        event_name, 
        event_data,
        pos as position
      FROM {{schema}}.events, _cursor
      WHERE pos > 0 AND pos > _cursor.offset
      ORDER BY pos ASC
      LIMIT ${limit}`;

    return events;
  }

  return {
    createEvents,
    createTasks,
    getLastEvent,
    ensureQueuePointer: ensureQueuePointer,
    createTasksAndSetCursor,
    unlockCursor,
    getCursorLockEvents,
    expireCursorLocks: () => sql`
      UPDATE {{schema}}.cursors
      SET locked = false
      WHERE locked = true AND expire_lock_at < now()
    `,
    getAndStartTasks: (queue: string, amount: number) => sql<SelectTask>`
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
    resolveTasks: (tasks: Array<ResolvedTask>) => sql`SELECT {{schema}}.resolve_tasks(${JSON.stringify(tasks)}::jsonb)`,
    get_expired_tasks: (limit: number) => sql<{
      id: string;
      retrycount: number;
      state: TaskState;
      config: TaskConfig;
    }>`
      SELECT
        id,
        retryCount,
        state,
        config
      FROM {{schema}}.tasks
        WHERE state = ${TASK_STATES.active}
          AND (startedOn + expireIn) < now()
      ORDER BY startedOn ASC
      LIMIT ${limit}
      FOR UPDATE SKIP LOCKED
    `,
    purgeTasks: () => sql`
      DELETE FROM {{schema}}.tasks_completed
      WHERE keepUntil < now()
    `,
    deleteOldEvents: () => sql`
      DELETE FROM {{schema}}.events WHERE expire_at < now()
    `,
  };
};
