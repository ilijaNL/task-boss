import { Pool } from 'pg';
import { PGTaskBoss, withPG } from '../../../src/use/pg';
import { cleanupSchema, createRandomSchema } from './helpers';
import tap from 'tap';
import { createTaskBoss } from '../../../src';
import EventEmitter, { once } from 'node:events';
import { defineEvent, defineTask } from '../../../src/definitions';
import { Type } from '@sinclair/typebox';
import { resolveWithinSeconds } from '../../../src/utils';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

tap.test('concurrency start', async (tap) => {
  const schema = createRandomSchema();
  const sqlPool = new Pool({
    connectionString: connectionString,
    max: 4,
  });

  // create some tasks
  const taskDef = defineTask({
    schema: Type.Object({}),
    task_name: 'task1',
  });

  const ee = new EventEmitter();
  let called = 0;

  const taskBoss = createTaskBoss('smoke_test');

  taskBoss.registerTask(taskDef, {
    async handler() {
      called += 1;

      if (called === 2) {
        ee.emit('complete');
      }
      return 'ok';
    },
  });

  const clients = [
    withPG(taskBoss, { db: sqlPool, schema: schema }),
    withPG(taskBoss, { db: sqlPool, schema: schema }),
    withPG(taskBoss, { db: sqlPool, schema: schema }),
  ] as const;

  // apply mgirations
  await clients[0].start();
  // apply mgirations
  await clients[0].stop();

  await clients[0].send(taskDef.from({}), taskDef.from({}));

  tap.teardown(async () => {
    await Promise.all(clients.map((c) => c.stop()));
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  const prm = once(ee, 'complete');
  await Promise.all(clients.map((c) => c.start()));
  await prm;
});

tap.test('with-pg', async (tap) => {
  tap.jobs = 5;
  const schema = createRandomSchema();

  const sqlPool = new Pool({
    connectionString: connectionString,
    max: 5,
  });

  tap.teardown(async () => {
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  tap.test('smoke test', async ({ pass }) => {
    const pgTasks = withPG(createTaskBoss('smoke_test'), { db: sqlPool, schema: schema });

    await pgTasks.start();
    await pgTasks.stop();
    pass('passes');
  });

  tap.test('emit tasks', async ({ teardown, equal, same }) => {
    const ee = new EventEmitter();
    const queue = 'emit_tasks_result';
    const tb = createTaskBoss(queue);

    const task_name = 'emit_task';
    const taskDef = defineTask({
      task_name: task_name,
      schema: Type.Object({ works: Type.String() }),
    });
    let handled = 0;

    tb.registerTask(taskDef, {
      handler: async (input, { trigger }) => {
        handled += 1;
        equal(input.works, 'abcd');
        equal(trigger.type, 'direct');
        if (handled > 1) {
          ee.emit('handled');
        }
        return {
          success: 'with result',
        };
      },
    });

    const pgTasks = withPG(tb, { db: sqlPool, schema: schema });

    await pgTasks.start();

    teardown(() => pgTasks.stop());

    const waitProm = once(ee, 'handled');

    await pgTasks.send(taskDef.from({ works: 'abcd' }), taskDef.from({ works: 'abcd' }));
    await waitProm;

    await new Promise((resolve) => setTimeout(resolve, 300));

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${task_name}'`)
      .then((r) => r.rows[0]!);

    same(result.output, {
      success: 'with result',
    });
  });

  tap.test('emit tasks with resolve', async ({ teardown, equal, same }) => {
    const ee = new EventEmitter();
    const queue = 'emit_tasks_result_resolve';
    const tb = createTaskBoss(queue);

    const task_name = 'emit_task_resolve';
    const taskDef = defineTask({
      task_name: task_name,
      schema: Type.Object({ works: Type.String() }),
    });
    let handled = 0;

    tb.registerTask(taskDef, {
      handler: async (input, { trigger, resolve }) => {
        handled += 1;
        equal(input.works, 'abcd');
        equal(trigger.type, 'direct');
        if (handled > 1) {
          ee.emit('handled');
        }

        resolve({
          success: 'with resolve result',
        });

        return {
          success: 'with result',
        };
      },
    });

    const pgTasks = withPG(tb, { db: sqlPool, schema: schema });

    await pgTasks.start();

    teardown(() => pgTasks.stop());

    const waitProm = once(ee, 'handled');

    await pgTasks.send(taskDef.from({ works: 'abcd' }), taskDef.from({ works: 'abcd' }));
    await waitProm;

    await new Promise((resolve) => setTimeout(resolve, 300));

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${task_name}'`)
      .then((r) => r.rows[0]!);

    same(result.output, {
      success: 'with resolve result',
    });
  });

  tap.test('stores error as result on task handler throw', async ({ teardown, equal, ok }) => {
    const ee = new EventEmitter();
    const queue = 'emit_tasks_error';
    const tb = createTaskBoss(queue);

    const task_name = 'emit_task';
    const taskDef = defineTask({
      task_name: task_name,
      schema: Type.Object({ works: Type.String() }),
    });

    tb.registerTask(taskDef, {
      handler: async () => {
        ee.emit('handled');
        throw new Error('expected-error');
      },
    });

    const pgTasks = withPG(tb, { db: sqlPool, schema: schema });

    await pgTasks.start();

    teardown(() => pgTasks.stop());

    const waitProm = once(ee, 'handled');

    await pgTasks.send(taskDef.from({ works: 'abcd' }));
    await waitProm;

    await new Promise((resolve) => setTimeout(resolve, 300));

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${task_name}'`)
      .then((r) => r.rows[0]!);
    equal(result.output.message, 'expected-error');

    ok(!!result.output.stack);
  });

  tap.test('fail manually', async ({ teardown, same }) => {
    const ee = new EventEmitter();
    const queue = 'emit_tasks_error_fail';
    const tb = createTaskBoss(queue);

    const task_name = 'emit_task_fail';
    const taskDef = defineTask({
      task_name: task_name,
      schema: Type.Object({ works: Type.String() }),
    });

    tb.registerTask(taskDef, {
      handler: async (_input, { fail }) => {
        ee.emit('handled');
        fail({ custom_payload: 123 });

        throw new Error('bla');
      },
    });

    const pgTasks = withPG(tb, { db: sqlPool, schema: schema });

    await pgTasks.start();

    teardown(() => pgTasks.stop());

    const waitProm = once(ee, 'handled');

    await pgTasks.send(taskDef.from({ works: 'abcd' }));
    await waitProm;

    // this ensures that the batch is flushed
    await new Promise((resolve) => setTimeout(resolve, 300));

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${task_name}'`)
      .then((r) => r.rows[0]!);

    same(result.output, { custom_payload: 123 });
  });

  tap.test('emit event', async (t) => {
    const ee = new EventEmitter();
    const tb = createTaskBoss('emit_event_queue');
    // const bus = createTBus('emit_event_queue', { db: sqlPool, schema: schema });

    const event = defineEvent({
      event_name: 'test_event',
      schema: Type.Object({
        text: Type.String(),
      }),
    });

    const event2 = defineEvent({
      event_name: 'awdawd',
      schema: Type.Object({
        rrr: Type.String(),
      }),
    });

    tb.on(event, {
      task_name: 'task1',
      handler: async (input, { trigger }) => {
        t.equal(input.text, 'text222');
        t.equal(trigger.type, 'event');
        ee.emit('handled1');
      },
    });

    tb.on(event, {
      task_name: 'task_2',
      handler: async (input, { trigger }) => {
        t.equal(input.text, 'text222');
        t.equal(trigger.type, 'event');
        ee.emit('handled2');
      },
    });

    tb.on(event2, {
      task_name: 'task_3',
      handler: async (input, { trigger }) => {
        t.equal(input.rrr, 'event2');
        t.equal(trigger.type, 'event');
        ee.emit('handled3');
      },
    });

    const pgTasks = withPG(tb, { db: sqlPool, schema: schema });

    await pgTasks.start();

    t.teardown(() => pgTasks.stop());

    await t.resolves(
      resolveWithinSeconds(
        Promise.all([
          pgTasks.publish(event.from({ text: 'text222' }), event2.from({ rrr: 'event2' })),
          once(ee, 'handled1'),
          once(ee, 'handled2'),
          once(ee, 'handled3'),
        ]),
        3
      )
    );
  });

  tap.test('singleton task', async ({ teardown, equal }) => {
    const queue = `singleton_task`;
    const tb = createTaskBoss(queue);
    const taskName = 'singleton_task';

    const taskDef = defineTask({
      task_name: taskName,
      schema: Type.Object({ works: Type.String() }),
      config: {
        expireInSeconds: 5,
        retryBackoff: true,
        retryDelay: 45,
        retryLimit: 4,
        startAfterSeconds: 45,
      },
    });

    tb.registerTask(taskDef, {
      handler: async () => {},
    });

    const pgTb = withPG(tb, { db: sqlPool, schema: schema });

    await pgTb.start();

    teardown(() => pgTb.stop());

    await pgTb.send(
      taskDef.from({ works: 'abcd' }, { singletonKey: 'single' }),
      taskDef.from({ works: 'abcd' }, { singletonKey: 'single' })
    );

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${taskName}'`)
      .then((r) => r.rows);

    equal(result.length, 1);
  });

  tap.test('event handler singleton from payload', async ({ teardown, equal }) => {
    const queue = `singleton_queue_payload`;
    const schema = createRandomSchema();
    // const bus = createTBus(queue, { db: sqlPool, schema: schema, worker: { intervalInMs: 200 } });

    const event = defineEvent({
      event_name: 'event_handler_singleton_payload',
      schema: Type.Object({
        c: Type.Number(),
      }),
    });

    const task_name = 'event_singleton_task';

    const tb = createTaskBoss(queue);

    tb.on(event, {
      task_name: task_name,
      handler: async () => {
        await new Promise((resolve) => setTimeout(resolve, 200));
      },
      config: ({ c }) => {
        return {
          singletonKey: 'event_singleton_task_' + c,
        };
      },
    });

    const pgTb = withPG(tb, { db: sqlPool, schema: schema, worker: { intervalInMs: 200 } });

    await pgTb.start();

    teardown(async () => {
      await pgTb.stop();
      await cleanupSchema(sqlPool, schema);
    });

    const cursor = await sqlPool
      .query(`SELECT * FROM ${schema}.cursors WHERE svc = '${queue}'`)
      .then((r) => +r.rows[0].l_p);

    await pgTb.publish(event.from({ c: 91 }), event.from({ c: 93 }), event.from({ c: 91 }));
    await pgTb.publish(event.from({ c: 91 }));
    await pgTb.publish(event.from({ c: 93 }));

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${task_name}'`)
      .then((r) => r.rows);

    equal(result.length, 2);

    // this means that all events are processed by the service
    equal(
      await sqlPool.query(`SELECT * FROM ${schema}.cursors WHERE svc = '${queue}'`).then((r) => +r.rows[0].l_p),
      cursor + 5
    );
  });

  tap.test('when registering new service, add last event as cursor', async ({ equal, teardown }) => {
    const schema = createRandomSchema();
    const event = defineEvent({
      event_name: 'test_event',
      schema: Type.Object({}),
    });

    const tb1 = withPG(createTaskBoss('serviceA'), { db: { connectionString }, schema: schema });
    const tb2 = withPG(createTaskBoss('serviceB'), { db: { connectionString }, schema: schema });

    await tb1.start();
    await tb1.publish(event.from({}), event.from({}));
    await tb2.start();

    teardown(async () => {
      await tb1.stop();
      await tb2.stop();
      await cleanupSchema(sqlPool, schema);
    });

    const result = await sqlPool.query<{ l_p: string }>(
      `SELECT l_p FROM ${schema}.cursors WHERE svc = 'serviceB' LIMIT 1`
    );

    equal(result.rows[0]?.l_p, '2');
  });

  tap.test('cursor', async ({ teardown, equal }) => {
    const ee = new EventEmitter();
    const schema = createRandomSchema();
    const queue = 'cursorservice';
    const tb = createTaskBoss(queue);
    const pgTB = withPG(tb, { db: { connectionString }, schema: schema });

    const event = defineEvent({
      event_name: 'test_event',
      schema: Type.Object({
        text: Type.String(),
      }),
    });

    let count = 0;

    tb.on(event, {
      task_name: 'task_1',
      handler: async (input) => {
        count++;
        equal(input.text, count.toString());

        ee.emit('task_1');
      },
    });

    await pgTB.start();

    teardown(async () => {
      await pgTB.stop();
      await cleanupSchema(sqlPool, schema);
    });

    const p1 = once(ee, 'task_1');
    await pgTB.publish(event.from({ text: '1' }));

    await p1;

    const p2 = once(ee, 'task_1');
    await pgTB.publish(event.from({ text: '2' }));

    await p2;
  });
});
