import { Pool } from 'pg';
import t from 'tap';
import { TASK_STATES, createInsertTask, createPlans } from '../../../src/use/pg/plans';
import { createMaintainceWorker, maintanceQueue } from '../../../src/use/pg/maintaince';
import { migrate } from '../../../src/use/pg/migrations';
import { cleanupSchema, createRandomSchema } from './helpers';
import { query } from '../../../src/use/pg/sql';
import { defaultTaskConfig } from '../../../src/definitions';
import createTaskBoss from '../../../src';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

t.jobs = 2;

t.test('recreates maintaince tasks', async (t) => {
  const schema = createRandomSchema();
  const plans = createPlans(schema);

  const sqlPool = new Pool({
    connectionString: connectionString,
  });

  await migrate(sqlPool, schema);

  const mWorker = createMaintainceWorker({
    client: sqlPool,
    schema,
    expireIntervalInSec: 1,
    cleanUpIntervalInSec: 1,
  });

  await mWorker.start();

  t.teardown(async () => {
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  const tasks = await sqlPool
    .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${maintanceQueue}'`)
    .then((r) => r.rows);

  t.equal(tasks.length >= 2 && tasks.length <= 4, true);

  await mWorker.stop();

  t.equal(tasks.length >= 2 && tasks.length <= 4, true);

  await mWorker.start();
  t.equal(tasks.length >= 2 && tasks.length <= 4, true);
  await mWorker.stop();
});

t.test('maintaince worker', async (t) => {
  t.jobs = 1;

  const queue = 'maintaince_q';
  const schema = 'maintaince_schema';
  const plans = createPlans(schema);
  const tboss = createTaskBoss(queue);

  const sqlPool = new Pool({
    connectionString: connectionString,
  });

  await migrate(sqlPool, schema);

  const mWorker = createMaintainceWorker({
    client: sqlPool,
    schema,
    expireIntervalInSec: 1,
    cleanUpIntervalInSec: 1,
  });

  await mWorker.start();
  t.teardown(() => mWorker.stop());

  t.teardown(async () => {
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  t.test('expires', async (t) => {
    const outgoingTask = tboss.toOutTask({
      task_name: 'expired-task-123',
      config: {
        ...defaultTaskConfig,
        expireInSeconds: 0,
        retryBackoff: false,
        retryLimit: 0,
        retryDelay: 1,
      },
      data: {},
    });

    const insertTask = createInsertTask(outgoingTask, 120);

    await query(sqlPool, plans.createTasks([insertTask]));

    // mark the task as started
    await query(sqlPool, plans.getAndStartTasks(queue, 100));

    // wait to expire
    await new Promise((resolve) => setTimeout(resolve, 1300));
    // wait for the expire task
    await new Promise((resolve) => setTimeout(resolve, 1200));

    // notify internal maintance worker that there is work
    mWorker.notify();

    await new Promise((resolve) => setTimeout(resolve, 200));

    const result = await sqlPool
      .query(
        `SELECT * FROM ${schema}.tasks_completed WHERE queue = '${queue}' AND meta_data->>'tn' = 'expired-task-123' LIMIT 1`
      )
      .then((r) => r.rows[0]);

    t.equal(result.state, TASK_STATES.expired);
  });

  t.test('expires with retry', async (t) => {
    const outgoingTask = tboss.toOutTask({
      task_name: 'expired-task-123-retry',
      config: {
        ...defaultTaskConfig,
        expireInSeconds: 1,
        retryBackoff: false,
        retryLimit: 1,
        retryDelay: 0,
      },
      data: {},
    });

    const insertTask = createInsertTask(outgoingTask, 120);

    await query(sqlPool, plans.createTasks([insertTask]));

    // mark the task as started
    await query(sqlPool, plans.getAndStartTasks(queue, 100));

    // wait to expire
    await new Promise((resolve) => setTimeout(resolve, 1300));

    // wait for the maintaince task
    await new Promise((resolve) => setTimeout(resolve, 1200));

    // notify internal maintance worker that there is work
    mWorker.notify();

    await new Promise((resolve) => setTimeout(resolve, 50));

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

    // mark the task as started
    const res = await query(sqlPool, plans.getAndStartTasks(queue, 100));
    t.equal(res.length, 1);

    // wait to expire
    await new Promise((resolve) => setTimeout(resolve, 1300));

    // wait for the maintaince task
    await new Promise((resolve) => setTimeout(resolve, 1200));

    // notify internal maintance worker that there is work
    mWorker.notify();

    await new Promise((resolve) => setTimeout(resolve, 100));

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
    const outgoingTask = tboss.toOutTask({
      task_name: 'purges-task-worker',
      config: {
        ...defaultTaskConfig,
        retryBackoff: false,
        retryLimit: 0,
        retryDelay: 1,
      },
      data: {},
    });

    const insertTask = createInsertTask(outgoingTask, 0);

    t.equal(insertTask.cf.ki_s, 0);
    t.equal(insertTask.saf, 0);

    await query(sqlPool, plans.createTasks([insertTask]));
    // mark the task as started
    const tasks = await query(sqlPool, plans.getAndStartTasks(queue, 100));

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

    await new Promise((resolve) => setTimeout(resolve, 1200));

    // triggers manual cleanup
    mWorker.notify();

    await new Promise((resolve) => setTimeout(resolve, 100));

    {
      const taskComlpeted = await sqlPool.query(
        `SELECT * FROM ${schema}.tasks_completed WHERE queue = '${queue}' AND meta_data->>'tn' = '${outgoingTask.task_name}'`
      );

      t.equal(taskComlpeted.rowCount, 0);
    }
  });

  t.test('event retention', async (t) => {
    await query(
      sqlPool,
      plans.createEvents([
        { d: {}, e_n: 'event_retention_123', rid: -1 },
        { d: { exists: true }, e_n: 'event_retention_123' },
        { d: { exists: true }, e_n: 'event_retention_123', rid: 3 },
      ])
    );

    await new Promise((resolve) => setTimeout(resolve, 1200));

    mWorker.notify();

    await new Promise((resolve) => setTimeout(resolve, 100));

    const r2 = await sqlPool.query(
      `SELECT id, expire_at - now()::date as days FROM ${schema}.events WHERE event_name = 'event_retention_123' ORDER BY days desc`
    );
    t.equal(r2.rowCount, 2);
    // default retention
    t.equal(r2.rows[0].days, 30);
    t.equal(r2.rows[1].days, 3);
  });

  t.test('cursor lock release', async (t) => {
    const queue = 'cursorlockqueue';
    await query(sqlPool, plans.ensureQueuePointer(queue, 0));
    await query(
      sqlPool,
      plans.createEvents([
        { d: { exists: true }, e_n: 'event_retention_123' },
        { d: { exists: true }, e_n: 'event_retention_123' },
      ])
    );

    const cursor = await sqlPool
      .query(`SELECT id, locked, queue, "offset" FROM ${schema}.cursors WHERE queue = '${queue}'`)
      .then((r) => r.rows[0]);

    const offsetBeforeFail = cursor.offset;
    t.equal(cursor.locked, false);

    // lock queue
    await query(sqlPool, plans.getCursorLockEvents(queue, 100, 0));

    {
      const cursor = await sqlPool
        .query(`SELECT id, locked, queue, "offset" FROM ${schema}.cursors WHERE queue = '${queue}'`)
        .then((r) => r.rows[0]);

      t.equal(cursor.locked, true);
    }

    // wait for maintaince task to kick in
    await new Promise((resolve) => setTimeout(resolve, 1200));
    mWorker.notify();
    await new Promise((resolve) => setTimeout(resolve, 50));

    {
      const cursor = await sqlPool
        .query(`SELECT id, locked, queue, "offset" FROM ${schema}.cursors WHERE queue = '${queue}'`)
        .then((r) => r.rows[0]);

      t.equal(cursor.offset, offsetBeforeFail);
      t.equal(cursor.locked, false);
    }
  });
});
