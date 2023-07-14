import { Pool } from 'pg';
import tap from 'tap';
import { InsertTask, SelectTask, TASK_STATES, createInserTask, createPlans } from '../../../src/use/pg/plans';
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

  const schema = 'taskworker';

  const sqlPool = new Pool({
    connectionString: connectionString,
  });

  await migrate(sqlPool, schema);

  t.teardown(async () => {
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  t.test('happy path', async (t) => {
    const queue = 'happy';
    const plans = createPlans(schema, queue);

    const worker = createTaskWorker({
      client: sqlPool,
      async handler(event) {
        return {
          works: event.data.tn,
        };
      },
      maxConcurrency: 10,
      poolInternvalInMs: 100,
      refillThresholdPct: 0.33,
      plans: plans,
    });

    worker.start();
    t.teardown(() => worker.stop());

    const insertTask: InsertTask = createInserTask(
      {
        task_name: 'happy-task',
        queue: queue,
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

    await query(sqlPool, plans.createTasks([insertTask]));
    await new Promise((resolve) => setTimeout(resolve, 1000));

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${insertTask.d.tn}' LIMIT 1`)
      .then((r) => r.rows[0]);

    t.equal(result.output.works, 'happy-task');
    t.equal(result.state, TASK_STATES.completed);
  });

  t.test('skip fetching when maxConcurrency is reached', async (t) => {
    let handlerCalls = 0;
    let queryCalls = 0;
    const amountOfTasks = 50;
    const tasks: SelectTask[] = new Array<SelectTask>(amountOfTasks)
      .fill({
        state: 0,
        retrycount: 0,
        id: '0',
        data: {} as any,
        expire_in_seconds: 10,
      })
      .map((r, idx) => ({ ...r, id: `${idx}` }));
    const ee = new EventEmitter();

    const mockedPool: PGClient = {
      async query(props) {
        // this is to fetch tasks
        if (props.text.includes('FOR UPDATE SKIP LOCKED')) {
          queryCalls += 1;
          // $3 is amount of rows requested
          const rows = tasks.splice(0, props.values[2]);

          return {
            rowCount: rows.length,
            rows: rows as any,
          };
        }

        return {
          rowCount: 0,
          rows: [],
        };
      },
    };

    const worker = createTaskWorker({
      plans: createPlans(schema, 'doesnotmatter'),
      client: mockedPool,
      async handler(event) {
        handlerCalls += 1;

        const delay = 80;
        await new Promise((resolve) => setTimeout(resolve, delay));
        if (handlerCalls === amountOfTasks) {
          ee.emit('completed');
        }
        return {
          works: event.data.tn,
        };
      },
      // fetch all at once
      maxConcurrency: amountOfTasks,
      poolInternvalInMs: 20,
      refillThresholdPct: 0.33,
    });

    worker.start();

    await once(ee, 'completed');

    // wait for last item to be resolved
    await worker.stop();

    t.equal(queryCalls, 1);
    t.equal(handlerCalls, amountOfTasks);
  });

  t.test('refills', async (t) => {
    const queue = 'refills';
    let handlerCalls = 0;
    let queryCalls = 0;
    const ee = new EventEmitter();
    const plans = createPlans(schema, queue);
    const tasks: SelectTask[] = new Array<SelectTask>(100)
      .fill({
        state: 0,
        retrycount: 0,
        id: '0',
        data: {} as any,
        expire_in_seconds: 10,
      })
      .map((r, idx) => ({ ...r, id: `${idx}` }));

    const mockedPool: PGClient = {
      async query(props) {
        // this is to fetch tasks
        if (props.text.includes('FOR UPDATE SKIP LOCKED')) {
          queryCalls += 1;
          // $3 is amount of rows
          const rows = tasks.splice(0, props.values[2]);

          if (tasks.length === 0) {
            ee.emit('drained');
          }

          return {
            rowCount: rows.length,
            rows: rows as any,
          };
        }

        return {
          rowCount: 0,
          rows: [],
        };
      },
    };
    const worker = createTaskWorker({
      client: mockedPool,
      plans: plans,
      async handler(event) {
        handlerCalls += 1;
        // resolves between 10-30ms
        const delay = 10 + Math.random() * 20;
        await new Promise((resolve) => setTimeout(resolve, delay));
        return {
          works: event.data.tn,
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
    t.ok(queryCalls > 10);
    t.ok(queryCalls < 20);

    t.equal(handlerCalls, 100);
  });

  t.test('retries', async (t) => {
    const queue = 'retries';
    let called = 0;

    const plans = createPlans(schema, queue);

    const worker = createTaskWorker({
      client: sqlPool,
      async handler(event) {
        called += 1;
        const item = {} as any;
        // throw with stack trace
        item.balbala.run();
      },
      maxConcurrency: 10,
      poolInternvalInMs: 200,
      refillThresholdPct: 0.33,
      plans: plans,
    });

    worker.start();
    t.teardown(() => worker.stop());

    const insertTask: InsertTask = createInserTask(
      {
        task_name: 'task',
        data: {},
        queue: queue,
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
      10
    );

    t.equal(insertTask.r_l, 1);

    await query(sqlPool, plans.createTasks([insertTask]));

    await new Promise((resolve) => setTimeout(resolve, 2000));

    t.equal(called, 2);

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${insertTask.d.tn}' LIMIT 1`)
      .then((r) => r.rows[0]);

    t.equal(result.state, TASK_STATES.failed);
    t.equal(result.output.message, "Cannot read properties of undefined (reading 'run')");
  });

  t.test('expires', async (t) => {
    const queue = 'expires';
    let called = 0;
    const plans = createPlans(schema, queue);

    const worker = createTaskWorker({
      client: sqlPool,
      async handler({ expire_in_seconds }) {
        called += 1;
        await resolveWithinSeconds(new Promise((resolve) => setTimeout(resolve, 1500)), expire_in_seconds);
      },
      maxConcurrency: 10,
      poolInternvalInMs: 100,
      refillThresholdPct: 0.33,
      plans: plans,
    });

    worker.start();
    t.teardown(() => worker.stop());

    const taskName = 'expired-task';

    const insertTask: InsertTask = createInserTask(
      {
        task_name: taskName,
        data: {},
        queue: queue,
        config: {
          expireInSeconds: 1,
          retryBackoff: false,
          retryLimit: 1,
          retryDelay: 1,
          singletonKey: null,
          startAfterSeconds: 0,
        },
      },
      { type: 'direct' },
      10
    );

    await query(sqlPool, plans.createTasks([insertTask]));

    await new Promise((resolve) => setTimeout(resolve, 4000));

    t.equal(called, 2);

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${taskName}' LIMIT 1`)
      .then((r) => r.rows[0]);

    t.equal(result.state, TASK_STATES.failed);
    t.equal(result.output.message, 'handler execution exceeded 1000ms');
  });
});

tap.test('maintaince worker', async (t) => {
  t.jobs = 5;

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
      intervalInMs: 200,
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

    const insertTask = createInserTask(outgoingTask, { type: 'direct' }, 120);

    await query(sqlPool, plans.createTasks([insertTask]));

    // mark the task as started
    await query(sqlPool, plans.getTasks({ amount: 100 }));

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = 'expired-task-123' LIMIT 1`)
      .then((r) => r.rows[0]);

    t.equal(result.state, TASK_STATES.expired);
  });

  t.test('purges tasks', async (t) => {
    const worker = createMaintainceWorker({
      client: sqlPool,
      schema,
      intervalInMs: 200,
    });

    const plans = createPlans(schema, queue);
    const outgoingTask = tboss.getTask({
      task_name: 'expired-task-worker',
      config: {
        expireInSeconds: 1,
        retryBackoff: false,
        retryLimit: 0,
        retryDelay: 1,
      },
      data: {},
    });

    const insertTask = createInserTask(outgoingTask, { type: 'direct' }, 0);

    t.equal(insertTask.kis, 0);
    t.equal(insertTask.saf, 0);

    await query(sqlPool, plans.createTasks([insertTask]));
    // mark the task as started
    await query(sqlPool, plans.getTasks({ amount: 100 }));
    // complete
    const result = await sqlPool
      .query(
        `UPDATE ${schema}.tasks SET state = ${TASK_STATES.completed} WHERE queue = '${queue}' AND data->>'tn' = 'expired-task-worker' RETURNING *`
      )
      .then((r) => r.rows[0]);

    t.ok(result.id);

    worker.start();
    t.teardown(() => worker.stop());

    await new Promise((resolve) => setTimeout(resolve, 1000));

    const r = await sqlPool.query(`SELECT * FROM ${schema}.tasks WHERE id = ${result.id}`);
    t.equal(r.rowCount, 0);
  });

  t.test('event retention', async (t) => {
    const worker = createMaintainceWorker({
      client: sqlPool,
      schema,
      intervalInMs: 200,
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
      `SELECT id, expire_at - now()::date as days, event_data FROM ${schema}.events WHERE event_name = 'event_retention_123' ORDER BY days desc`
    );
    t.equal(r2.rowCount, 2);
    t.equal(r2.rows[0].event_data.exists, true);
    // default retention
    t.equal(r2.rows[0].days, 30);
    t.equal(r2.rows[1].days, 3);
  });
});
