import { Pool } from 'pg';
import { withPG } from '../../../src/use/pg';
import { cleanupSchema, createRandomSchema } from './helpers';
import t from 'tap';
import { createTaskBoss } from '../../../src';
import { Task, defineTask } from '../../../src/definitions';
import { Type } from '@sinclair/typebox';
import { createBatcher } from 'node-batcher';
import { EventEmitter, once } from 'stream';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

t.test('performance', async (t) => {
  const schema = createRandomSchema();
  const sqlPool = new Pool({
    connectionString: connectionString,
    max: 15,
  });

  const task = defineTask({
    schema: Type.Number(),
    task_name: 'taskA',
    config: {},
  });

  const client = createTaskBoss('queueA');
  const pg = withPG(client, {
    db: sqlPool,
    schema: schema,
  });

  const taskCount = 20000;

  const nodeBatcher = createBatcher<Task<number>>({
    maxSize: 200,
    maxTimeInMs: 10,
    onFlush: async (batch) => {
      await pg.send(...batch.map((i) => i.data));
    },
  });

  // this will trigger migrations
  await pg.start();
  await pg.stop();

  t.teardown(async () => {
    await nodeBatcher.waitForAll();
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  t.beforeEach(async () => {
    await Promise.all(
      new Array(taskCount)
        .fill(null)
        .map((_, index) => task.from(index))
        .map((task) => nodeBatcher.add(task))
    );
  });

  t.test('should be able to fetch and complete tasks', async (t) => {
    t.setTimeout(5000);

    const ee = new EventEmitter();
    let tasksProcessed = 0;

    const client = createTaskBoss('queueA');

    client.registerTask(task, {
      async handler() {
        tasksProcessed++;
        if (tasksProcessed === taskCount) {
          ee.emit('ready');
        }
      },
    });

    const pg = withPG(client, {
      db: sqlPool,
      schema: schema,
      worker: {
        concurrency: 2000,
        intervalInMs: 1500,
        refillFactor: 0.5,
      },
    });

    await pg.start();
    const started = Date.now();
    t.teardown(() => pg.stop());

    await once(ee, 'ready');

    t.equal(tasksProcessed, taskCount);
    console.log('ready in: ', Date.now() - started);
  });
});
