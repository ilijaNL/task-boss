import { Pool, PoolConfig } from 'pg';
import { BaseClient, OutgoingTask, TaskBoss, createTaskFactory } from '../../task-boss';
import { TEvent, Task } from '../../definitions';
import { QueryCommand, createSql, query, withTransaction } from './sql';
import { migrate } from './migrations';
import {
  InsertTask,
  createPlans,
  createInsertTask,
  ResolvedTask,
  createInsertPlans,
  defaultKeepInSeconds,
  defaultEventRetentionInDays,
} from './plans';
import { createMaintainceWorker, maintanceQueue } from './maintaince';
import { createBaseWorker } from '../../worker';
import { createTaskWorker } from './task';
import { DeferredPromise, JsonValue, debounce } from '../../utils';
import { createBatcher } from 'node-batcher';

function _createTaskToCommandFactory(
  schema: string,
  taskFactory: (task: Task<JsonValue>) => OutgoingTask,
  opts: {
    keepInSeconds: number;
  }
) {
  const plans = createInsertPlans(createSql(schema));

  return function toCommands(...task: Task[]): QueryCommand<{}> {
    return plans.createTasks(task.map((t) => createInsertTask(taskFactory(t), directTask, opts.keepInSeconds)));
  };
}

export function createTaskToCommandFactory(
  schema: string,
  queue: string,
  opts: {
    keepInSeconds?: number;
  }
) {
  return _createTaskToCommandFactory(schema, createTaskFactory(queue), {
    keepInSeconds: opts.keepInSeconds ?? defaultKeepInSeconds,
  });
}

export function createEventToCommandFactory(
  schema: string,
  opts: {
    retentionInDays: number;
  }
) {
  const plans = createInsertPlans(createSql(schema));
  return function toCommands(...events: TEvent<string>[]): QueryCommand<{}> {
    return plans.createEvents(events.map((e) => ({ d: e.data, e_n: e.event_name, rid: opts.retentionInDays })));
  };
}

export type WorkerConfig = {
  /**
   * Amount of concurrent tasks that can be executed, default 25
   */
  concurrency: number;
  /**
   * Interval of pooling the database for new work, default 1500
   */
  intervalInMs: number;
  /**
   * Refill threshold factor, value between 0 - 1, default 0.33.
   * Automatically refills the task workers queue when activeItems / concurrency < refillPct.
   */
  refillFactor: number;
};

export interface PGTaskBoss extends BaseClient {
  /**
   * Start the workers of pg-task-boss
   */
  start: () => Promise<void>;
  /**
   * Gracefully stops all the workers of task-boss.
   */
  stop: () => Promise<void>;
  getPublishCommand: (...events: TEvent<string, any>[]) => QueryCommand<{}>;
  getSendCommand: (...tasks: Task[]) => QueryCommand<unknown>;
}

export const directTask = Object.freeze({
  type: 'direct',
});

export const withPG = (
  taskBoss: TaskBoss,
  opts: {
    /**
     * Postgres database pool or existing pool
     */
    db: Pool | PoolConfig;
    /**
     * Task worker configuration
     */
    worker?: Partial<WorkerConfig>;
    /**
     * Postgres database schema to use.
     * Note: changing schemas will result in event/task loss
     */
    schema: string;
    /**
     * Event retention in days.
     * Default 30;
     */
    retention_in_days?: number;
    /**
     * How long task is hold in the tasks table before it is archieved. Default 7 * 24 * 60 * 60 (7 days)
     */
    keepInSeconds?: number;
  }
): PGTaskBoss => {
  if (taskBoss.queue === maintanceQueue) {
    throw new Error('cannot use a reserved queue name: ' + taskBoss.queue);
  }

  const { schema, db, worker, retention_in_days, keepInSeconds = defaultKeepInSeconds } = opts;

  const workerConfig = Object.assign<WorkerConfig, Partial<WorkerConfig>>(
    {
      concurrency: 25,
      intervalInMs: 1500,
      refillFactor: 0.33,
    },
    worker ?? {}
  );

  const sqlPlans = createPlans(schema);

  const state = {
    started: false,
    stopped: false,
  };

  let pool: Pool;

  let cleanupDB = async () => {
    // noop
  };

  if ('query' in db) {
    pool = db;
  } else {
    // create connection pool
    pool = new Pool({
      max: 3,
      ...db,
    });
    cleanupDB = async () => {
      await pool.end();
    };
  }

  const resolveTaskBatcher = createBatcher<ResolvedTask>({
    async onFlush(batch) {
      await query(pool, sqlPlans.resolveTasks(batch.map((i) => i.data)));
    },
    // dont make to big since payload can be big
    maxSize: 75,
    // keep it low latency
    maxTimeInMs: 30,
  });

  const tWorker = createTaskWorker({
    maxConcurrency: workerConfig.concurrency,
    poolInternvalInMs: workerConfig.intervalInMs,
    refillThresholdPct: workerConfig.refillFactor,
    async popTasks(amount) {
      return query(pool, sqlPlans.getAndStartTasks(taskBoss.queue, amount));
    },
    async resolveTask(task) {
      await resolveTaskBatcher.add(task);
    },
    async handler({ data, meta_data, expire_in_seconds, retrycount, id }): Promise<any> {
      const future: DeferredPromise = new DeferredPromise();

      taskBoss
        // todo convert TaskHandlerCtx to class
        .handle(data, {
          expire_in_seconds: expire_in_seconds,
          id: id,
          retried: retrycount,
          task_name: meta_data.tn,
          trigger: meta_data.trace,
          fail(data) {
            future.reject(data);
          },
          resolve(data) {
            future.resolve(data);
          },
        })
        .then((data) => {
          future.resolve(data);
        })
        .catch((e) => {
          future.reject(e);
        });

      // clean up
      return future.promise;
    },
  });

  // Worker which is responsible for processing incoming events and creating tasks
  const fanoutWorker = createBaseWorker(
    async () => {
      const fetchSize = 200;
      const events = await query(pool, sqlPlans.getCursorLockEvents(taskBoss.queue, fetchSize, 30));
      if (events.length === 0) {
        await query(pool, sqlPlans.unlockCursor(taskBoss.queue));
        return false;
      }

      const newCursor = +events[events.length - 1]!.position;

      const insertTasks = events.reduce((agg, event) => {
        const tasks = taskBoss.toTasks(event);

        agg.push(
          ...tasks.map<InsertTask>((task) =>
            createInsertTask(
              task,
              {
                type: 'event',
                e: {
                  id: event.id,
                  name: event.event_name,
                },
              },
              keepInSeconds
            )
          )
        );

        return agg;
      }, [] as Array<InsertTask>);

      await query(pool, sqlPlans.createTasksAndSetCursor(taskBoss.queue, insertTasks, newCursor));
      return events.length === fetchSize;
    },
    {
      loopInterval: 1500,
    }
  );

  const maintainceWorker = createMaintainceWorker({ client: pool, schema: schema });

  const notifyFanout = debounce(() => fanoutWorker.notify(), { ms: 75, maxMs: 300 });
  const notifyWorker = debounce(() => tWorker.notify(), { ms: 75, maxMs: 150 });

  const eventsToCommandFactory = createEventToCommandFactory(schema, {
    retentionInDays: retention_in_days ?? defaultEventRetentionInDays,
  });

  const taskToCommandFactory = _createTaskToCommandFactory(schema, taskBoss.getTask, {
    keepInSeconds: defaultKeepInSeconds,
  });

  function shouldNotifyTaskWorker(t: Task<JsonValue>) {
    return (t.queue === taskBoss.queue || !t.queue) && !t.config.startAfterSeconds;
  }

  return {
    getState: taskBoss.getState,
    getPublishCommand: eventsToCommandFactory,
    getSendCommand: taskToCommandFactory,
    async send(...tasks) {
      await query(pool, taskToCommandFactory(...tasks));

      // check if instance is affected by the new tasks
      // if queue is not specified, it means we will create task for this instance
      const hasEffectToCurrentWorker = tasks.some(shouldNotifyTaskWorker);

      if (hasEffectToCurrentWorker) {
        notifyWorker();
      }
    },
    async publish(...events) {
      await query(pool, eventsToCommandFactory(...events));
      const taskBossEvents = taskBoss.getState().events;
      // check if instance is affected by the published events
      const hasEffectToCurrentWorker = events.some((e) => taskBossEvents.some((ee) => ee.event_name === e.event_name));
      if (hasEffectToCurrentWorker) {
        notifyFanout();
      }
    },
    async start() {
      if (state.started) {
        return;
      }

      state.started = true;
      state.stopped = false;

      await migrate(pool, schema);

      const lastCursor = (await query(pool, sqlPlans.getLastEvent()))[0];
      await query(pool, sqlPlans.ensureQueuePointer(taskBoss.queue, +(lastCursor?.position ?? 0)));
      await maintainceWorker.start();

      tWorker.start();
      fanoutWorker.start();
    },
    stop: async () => {
      if (state.started === false || state.stopped === true) {
        return;
      }

      state.stopped = true;

      await Promise.all([
        fanoutWorker.stop(),
        maintainceWorker.stop(),
        tWorker.stop(),
        resolveTaskBatcher.waitForAll(),
      ]);

      await cleanupDB();
      // only allow to start when fully stopped
      state.started = false;
    },
  };
};

export default withPG;
