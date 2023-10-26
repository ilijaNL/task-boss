import { Pool } from 'pg';
import t from 'tap';
import { createInsertTask, createPlans } from '../../../src/use/pg/plans';
import { cleanupSchema, createRandomSchema } from './helpers';
import { migrate } from '../../../src/use/pg/migrate';
import { createMigrationStore } from '../../../src/use/pg/migrations';
import { query } from '../../../src/use/pg/sql';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

t.test('postgres plans', async (t) => {
  const schema = createRandomSchema();

  const sqlPool = new Pool({
    connectionString: connectionString,
    max: 12,
    min: 12,
  });

  const queue = 'concurrency';

  const plans = createPlans(schema);

  const migrations = createMigrationStore(schema);

  await migrate(sqlPool, schema, migrations);

  t.teardown(async () => {
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  t.jobs = 1;

  // test for high concurrency
  t.test('pop concurrency', async (t) => {
    // should be a order of fetchsize
    const amountOfTasks = 200;
    const fetchSize = 10;

    await query(
      sqlPool,
      plans.createTasks(
        new Array(amountOfTasks).fill(null).map(() =>
          createInsertTask(
            {
              task_name: 'happy-task',
              queue: queue,
              data: {},
              config: {
                expireInSeconds: 1,
                retryBackoff: false,
                retryLimit: 1,
                retryDelay: 1,
                singletonKey: null,
                startAfterSeconds: 0,
              },
            },
            120
          )
        )
      )
    );

    // concurrent pop tasks from the queue
    const tasks = await Promise.all(
      new Array(amountOfTasks / fetchSize).fill(0).map(() => query(sqlPool, plans.getAndStartTasks(queue, fetchSize)))
    );

    // // check for duplicates
    const taskIds = new Set();
    tasks.flat().forEach((task) => {
      if (taskIds.has(task.id)) {
        t.fail(`duplicate task detected with ${task.id}`);
      }
      taskIds.add(task.id);
    });

    t.equal(taskIds.size, amountOfTasks);
  });

  t.test('concurrency fetching events', async (t) => {
    const queue = 'concurrent_events';
    await query(sqlPool, plans.ensureQueuePointer(queue, 0));
    await query(
      sqlPool,
      plans.createEvents([
        { d: { exists: true }, e_n: 'event_retention_123' },
        { d: { exists: true }, e_n: 'event_retention_123' },
      ])
    );

    const events = await Promise.all(
      new Array(10).fill(0).map(() => query(sqlPool, plans.getCursorLockEvents(queue, 100, 0)))
    );

    const b = await query(sqlPool, plans.getCursorLockEvents(queue, 100, 0));

    events.push(b);

    t.equal(events.flat().length, 2);
  });
});
