import { Pool } from 'pg';
import tap from 'tap';
import { InsertTask, SelectTask, TASK_STATES, createInsertTask, createPlans } from '../../../src/use/pg/plans';
import { createMaintainceWorker } from '../../../src/use/pg/maintaince';
import { migrate } from '../../../src/use/pg/migrations';
import { cleanupSchema } from './helpers';
import { PGClient, query } from '../../../src/use/pg/sql';
import { createTaskWorker } from '../../../src/use/pg/task';
import createTaskBoss from '../../../src/task-boss';
import EventEmitter, { once } from 'events';
import { resolveWithinSeconds } from '../../../src/utils';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

function resolvesInTime<T>(p1: Promise<T>, ms: number) {
  return Promise.race([p1, new Promise((_, reject) => setTimeout(reject, ms))]);
}

tap.test('task worker', async (t) => {
  t.jobs = 5;

  t.test('happy path', async (t) => {
    t.plan(3);
    const ee = new EventEmitter();

    const insertTask: InsertTask = createInsertTask(
      {
        task_name: 'happy-task',
        queue: 'test',
        data: {},
        config: {
          expireInSeconds: 10,
          retryBackoff: false,
          retryLimit: 1,
          retryDelay: 1,
          singletonKey: null,
          startAfterSeconds: 0,
        },
      },
      { type: 'direct' },
      120
    );

    const worker = createTaskWorker({
      async popTasks(amount) {
        t.equal(amount, 20);
        return [
          {
            config: insertTask.cf,
            data: insertTask.d,
            expire_in_seconds: insertTask.eis,
            id: '222',
            meta_data: insertTask.md,
            retrycount: 0,
            state: TASK_STATES.active,
          },
        ];
      },
      async resolveTask(task) {
        t.equal(task.id, '222');
        t.same(task.out, {
          works: insertTask.md.tn,
        });
        ee.emit('resolved');
      },
      async handler(event) {
        return {
          works: event.meta_data.tn,
        };
      },
      maxConcurrency: 20,
      poolInternvalInMs: 100,
      refillThresholdPct: 0.33,
    });

    worker.start();
    t.teardown(() => worker.stop());

    await once(ee, 'resolved');
  });

  t.test('stop fetching when maxConcurrency is reached', async (t) => {
    const ee = new EventEmitter();

    const insertTask: InsertTask = createInsertTask(
      {
        task_name: 'happy-task',
        queue: 'test',
        data: {},
        config: {
          expireInSeconds: 10,
          retryBackoff: false,
          retryLimit: 1,
          retryDelay: 1,
          singletonKey: null,
          startAfterSeconds: 0,
        },
      },
      { type: 'direct' },
      120
    );

    let handlerCalls = 0;
    let queryCalls = 0;
    const amountOfTasks = 50;

    const worker = createTaskWorker({
      async popTasks(amount) {
        queryCalls += 1;
        return new Array<SelectTask>(amountOfTasks)
          .fill({
            config: insertTask.cf,
            data: insertTask.d,
            expire_in_seconds: insertTask.eis,
            id: '',
            meta_data: insertTask.md,
            retrycount: 0,
            state: TASK_STATES.active,
          })
          .map((r, idx) => ({ ...r, id: `${idx}` }));
      },
      async resolveTask(task) {},
      async handler(event) {
        handlerCalls += 1;
        await new Promise((resolve) => setTimeout(resolve, 80));
        if (handlerCalls === amountOfTasks) {
          ee.emit('completed');
        }
      },
      maxConcurrency: amountOfTasks,
      poolInternvalInMs: 20,
      refillThresholdPct: 0.33,
    });

    worker.start();
    t.teardown(() => worker.stop());

    await once(ee, 'completed');
    // wait for last item to be resolved
    await worker.stop();

    t.equal(queryCalls, 1);
    t.equal(handlerCalls, amountOfTasks);
  });

  t.test('refills', async (t) => {
    let handlerCalls = 0;
    let popCalls = 0;
    const amountOfTasks = 100;
    const ee = new EventEmitter();
    const tasks: SelectTask[] = new Array<SelectTask>(amountOfTasks)
      .fill({
        state: 0,
        retrycount: 0,
        id: '0',
        meta_data: {
          tn: 't',
          trace: { type: 'direct' },
        },
        config: {
          ki_s: 21,
          r_b: false,
          r_d: 1,
          r_l: 3,
        },
        data: {},
        expire_in_seconds: 10,
      })
      .map((r, idx) => ({ ...r, id: `${idx}` }));

    const worker = createTaskWorker({
      async resolveTask(task) {},
      async popTasks(amount) {
        popCalls = popCalls + 1;
        const result = tasks.splice(0, amount);
        if (tasks.length === 0) {
          ee.emit('drained');
        }

        return result;
      },
      async handler(event) {
        handlerCalls += 1;
        // resolves between 10-30ms
        const delay = 10 + Math.random() * 20;
        await new Promise((resolve) => setTimeout(resolve, delay));
        return {
          works: event.meta_data.tn,
        };
      },
      maxConcurrency: 10,
      refillThresholdPct: 0.33,
      poolInternvalInMs: 100,
    });

    worker.start();

    // should be faster than 1000 because of refilling
    await t.resolves(resolvesInTime(once(ee, 'drained'), 600));

    // wait for last item to be resolved
    await worker.stop();

    // make some assumptions such that we dont fetch to much, aka threshold
    t.ok(popCalls > 10);
    t.ok(popCalls < 20);

    t.equal(handlerCalls, amountOfTasks);
  });

  t.test('retries', async (t) => {
    t.plan(2);
    const ee = new EventEmitter();

    const insertTask: InsertTask = createInsertTask(
      {
        task_name: 'happy-task',
        queue: 'test',
        data: {},
        config: {
          expireInSeconds: 10,
          retryBackoff: false,
          retryLimit: 1,
          retryDelay: 1,
          singletonKey: null,
          startAfterSeconds: 0,
        },
      },
      { type: 'direct' },
      120
    );

    const remoteTask: SelectTask = {
      config: insertTask.cf,
      data: insertTask.d,
      expire_in_seconds: insertTask.eis,
      id: '222',
      meta_data: insertTask.md,
      retrycount: 0,
      state: TASK_STATES.created,
    };

    const worker = createTaskWorker({
      async popTasks(amount) {
        if (remoteTask.state > TASK_STATES.retry) {
          return [];
        }
        remoteTask.state = TASK_STATES.active;
        return [remoteTask];
      },
      async resolveTask(task) {
        t.equal(task.out.message, "Cannot read properties of undefined (reading 'run')");
        remoteTask.retrycount += 1;
        remoteTask.state = task.s;

        if (task.s === TASK_STATES.failed) {
          ee.emit('failed');
        }
      },
      async handler(event) {
        const item = {} as any;
        // throw with stack trace
        item.balbala.run();
      },
      maxConcurrency: 10,
      poolInternvalInMs: 100,
      refillThresholdPct: 0.33,
    });

    worker.start();
    t.teardown(() => worker.stop());

    await once(ee, 'failed');
  });

  t.test('exponential backoff', async (t) => {
    const ee = new EventEmitter();
    const insertTask: InsertTask = createInsertTask(
      {
        task_name: 'taskA',
        data: {},
        queue: 'queue',
        config: {
          expireInSeconds: 1,
          retryBackoff: true,
          retryLimit: 8,
          retryDelay: 2,
          singletonKey: null,
          startAfterSeconds: 0,
        },
      },
      { type: 'direct' },
      10
    );

    const remoteTask: SelectTask = {
      config: insertTask.cf,
      data: insertTask.d,
      expire_in_seconds: insertTask.eis,
      id: '222',
      meta_data: insertTask.md,
      retrycount: 0,
      state: TASK_STATES.created,
    };

    const worker = createTaskWorker({
      async popTasks() {
        if (remoteTask.state > TASK_STATES.retry) {
          return [];
        }
        remoteTask.state = TASK_STATES.active;
        return [remoteTask];
      },
      async resolveTask(task) {
        if (task.s === TASK_STATES.failed) {
          ee.emit('failed');
          remoteTask.state = task.s;
          return;
        }

        remoteTask.state = task.s;
        remoteTask.retrycount += 1;
        t.equal(task.saf, Math.pow(remoteTask.config.r_d, remoteTask.retrycount));
      },
      async handler() {
        throw new Error('fail');
      },
      maxConcurrency: 10,
      poolInternvalInMs: 20,
      refillThresholdPct: 0.33,
    });

    worker.start();
    t.teardown(() => worker.stop());
    await once(ee, 'failed');
  });

  t.test('expires', async (t) => {
    const ee = new EventEmitter();
    const insertTask: InsertTask = createInsertTask(
      {
        task_name: 'taskA',
        data: {},
        queue: 'queue',
        config: {
          expireInSeconds: 1,
          retryBackoff: false,
          retryLimit: 0,
          retryDelay: 1,
          singletonKey: null,
          startAfterSeconds: 0,
        },
      },
      { type: 'direct' },
      10
    );

    t.plan(3);

    const remoteTask: SelectTask = {
      config: insertTask.cf,
      data: insertTask.d,
      expire_in_seconds: insertTask.eis,
      id: '222',
      meta_data: insertTask.md,
      retrycount: 0,
      state: TASK_STATES.created,
    };

    const worker = createTaskWorker({
      async popTasks() {
        if (remoteTask.state > TASK_STATES.retry) {
          return [];
        }
        remoteTask.state = TASK_STATES.active;
        return [remoteTask];
      },
      async resolveTask(task) {
        remoteTask.state = task.s;
        remoteTask.retrycount += 1;
        t.equal(task.out.message, 'handler execution exceeded 1000ms');
        t.equal(task.s, TASK_STATES.failed);
        if (task.s === TASK_STATES.failed) {
          ee.emit('failed');
        }
      },
      async handler({ expire_in_seconds }) {
        await resolveWithinSeconds(new Promise((resolve) => setTimeout(resolve, 1500)), expire_in_seconds);
      },
      maxConcurrency: 10,
      poolInternvalInMs: 100,
      refillThresholdPct: 0.33,
    });

    worker.start();
    t.teardown(() => worker.stop());

    await once(ee, 'failed');

    t.equal(remoteTask.state, TASK_STATES.failed);
  });

  tap.test('maintaince worker', async (t) => {
    t.jobs = 1;

    const queue = 'maintaince_q';
    const schema = 'maintaince_schema';
    const plans = createPlans(schema, queue);

    const tboss = createTaskBoss(queue);

    const sqlPool = new Pool({
      connectionString: connectionString,
    });

    await migrate(sqlPool, schema);

    t.teardown(async () => {
      await cleanupSchema(sqlPool, schema);
      await sqlPool.end();
    });

    t.test('expires', async (t) => {
      const worker = createMaintainceWorker({
        client: sqlPool,
        schema,
      });

      worker.start();
      t.teardown(() => worker.stop());

      const outgoingTask = tboss.getTask({
        task_name: 'expired-task-123',
        config: {
          expireInSeconds: 1,
          retryBackoff: false,
          retryLimit: 0,
          retryDelay: 1,
        },
        data: {},
      });

      const insertTask = createInsertTask(outgoingTask, { type: 'direct' }, 120);

      await query(sqlPool, plans.createTasks([insertTask]));

      // mark the task as started
      await query(sqlPool, plans.getAndStartTasks(100));

      await new Promise((resolve) => setTimeout(resolve, 1500));
      worker.notify();
      await new Promise((resolve) => setTimeout(resolve, 100));

      const result = await sqlPool
        .query(
          `SELECT * FROM ${schema}.tasks_completed WHERE queue = '${queue}' AND meta_data->>'tn' = 'expired-task-123' LIMIT 1`
        )
        .then((r) => r.rows[0]);

      t.equal(result.state, TASK_STATES.expired);
    });

    t.test('expires with retry', async (t) => {
      const worker = createMaintainceWorker({
        client: sqlPool,
        schema,
        expireInterval: 100,
      });

      worker.start();
      t.teardown(() => worker.stop());

      const outgoingTask = tboss.getTask({
        task_name: 'expired-task-123-retry',
        config: {
          expireInSeconds: 1,
          retryBackoff: false,
          retryLimit: 1,
          retryDelay: 1,
        },
        data: {},
      });

      const insertTask = createInsertTask(outgoingTask, { type: 'direct' }, 120);

      await query(sqlPool, plans.createTasks([insertTask]));

      // mark the task as started
      await query(sqlPool, plans.getAndStartTasks(100));

      await new Promise((resolve) => setTimeout(resolve, 1200));
      await new Promise((resolve) => setTimeout(resolve, 100));

      {
        const r1 = await sqlPool
          .query(
            `SELECT * FROM ${schema}.tasks_completed WHERE queue = '${queue}' AND meta_data->>'tn' = 'expired-task-123-retry' LIMIT 1`
          )
          .then((r) => r.rows);

        t.equal(r1.length, 0);

        const r2 = await sqlPool
          .query(
            `SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND meta_data->>'tn' = 'expired-task-123-retry' LIMIT 1`
          )
          .then((r) => r.rows[0]);

        t.equal(r2.state, TASK_STATES.retry);
        t.equal(r2.retrycount, 0);
      }

      await new Promise((resolve) => setTimeout(resolve, 1200));

      // mark the task as started
      const res = await query(sqlPool, plans.getAndStartTasks(100));
      t.equal(res.length, 1);

      await new Promise((resolve) => setTimeout(resolve, 1200));

      {
        const result = await sqlPool
          .query(
            `SELECT * FROM ${schema}.tasks_completed WHERE queue = '${queue}' AND meta_data->>'tn' = 'expired-task-123-retry' LIMIT 1`
          )
          .then((r) => r.rows[0]);

        t.equal(result.state, TASK_STATES.expired);
        t.equal(result.retrycount, 1);
      }
    });

    t.test('purges tasks', async (t) => {
      const worker = createMaintainceWorker({
        client: sqlPool,
        schema,
      });

      worker.start();
      t.teardown(() => worker.stop());

      const plans = createPlans(schema, queue);
      const outgoingTask = tboss.getTask({
        task_name: 'purges-task-worker',
        config: {
          retryBackoff: false,
          retryLimit: 0,
          retryDelay: 1,
        },
        data: {},
      });

      const insertTask = createInsertTask(outgoingTask, { type: 'direct' }, 0);

      t.equal(insertTask.cf.ki_s, 0);
      t.equal(insertTask.saf, 0);

      await query(sqlPool, plans.createTasks([insertTask]));
      // mark the task as started
      const tasks = await query(sqlPool, plans.getAndStartTasks(100));

      const runnignTasks = await sqlPool.query(
        `SELECT * FROM ${schema}.tasks WHERE  queue = '${queue}' AND meta_data->>'tn' = '${outgoingTask.task_name}'`
      );

      t.equal(runnignTasks.rowCount, 1);

      // complete tasks
      await query(
        sqlPool,
        plans.resolveTasks(
          tasks.map((t) => ({
            id: t.id,
            s: TASK_STATES.completed,
            out: undefined,
          }))
        )
      );
      {
        const taskComlpeted = await sqlPool.query(
          `SELECT * FROM ${schema}.tasks_completed WHERE queue = '${queue}' AND meta_data->>'tn' = '${outgoingTask.task_name}'`
        );

        t.equal(taskComlpeted.rowCount, 1);
      }

      // triggers manual cleanup
      worker.notify();

      await new Promise((resolve) => setTimeout(resolve, 100));

      {
        const taskComlpeted = await sqlPool.query(
          `SELECT * FROM ${schema}.tasks_completed WHERE queue = '${queue}' AND meta_data->>'tn' = '${outgoingTask.task_name}'`
        );

        t.equal(taskComlpeted.rowCount, 0);
      }
    });

    t.test('event retention', async (t) => {
      const worker = createMaintainceWorker({
        client: sqlPool,
        schema,
        cleanupInterval: 200,
      });

      const plans = createPlans(schema, queue);

      await query(
        sqlPool,
        plans.createEvents([
          { d: {}, e_n: 'event_retention_123', rid: -1 },
          { d: { exists: true }, e_n: 'event_retention_123' },
          { d: { exists: true }, e_n: 'event_retention_123', rid: 3 },
        ])
      );

      worker.start();
      t.teardown(() => worker.stop());

      await new Promise((resolve) => setTimeout(resolve, 500));
      const r2 = await sqlPool.query(
        `SELECT id, expire_at - now()::date as days FROM ${schema}.events WHERE event_name = 'event_retention_123' ORDER BY days desc`
      );
      t.equal(r2.rowCount, 2);
      // default retention
      t.equal(r2.rows[0].days, 30);
      t.equal(r2.rows[1].days, 3);
    });
  });
});
