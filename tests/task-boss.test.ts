import { Type } from '@sinclair/typebox';
import tap from 'tap';
import createTaskBoss from '../src/task-boss';
import stringify from 'safe-stable-stringify';
import { createTaskClient, defineEvent, defineTask } from '../src/definitions';

tap.test('task-boss', async (tap) => {
  tap.jobs = 5;

  tap.test('create task for queues', async ({ equal }) => {
    const defaultQueue = 'queueA';
    const taskBoss = createTaskBoss('queueA', {});

    const sameQueue = taskBoss.getTask({ data: {}, task_name: 'task_a', config: {} });
    equal(sameQueue.queue, defaultQueue);
    const diffQueue = taskBoss.getTask({ data: {}, task_name: 'task_b', config: {}, queue: 'custom_queue' });
    equal(diffQueue.queue, 'custom_queue');
  });

  tap.test('throws when task-boss receives unknown event', async (t) => {
    const taskBoss = createTaskBoss('queueA', {});

    await t.rejects(() =>
      taskBoss.handle({ expire_in_seconds: 12, input: {}, task_name: 'test', trigger: { type: 'direct' } })
    );
  });

  tap.test('throws when handler takes to long', async (t) => {
    const taskBoss = createTaskBoss('queueA', {});

    const event = defineEvent({
      event_name: 'test',
      schema: Type.Object({}),
    });

    taskBoss.on(event, {
      task_name: 'test',
      handler: async () => {
        await new Promise((resolve) => setTimeout(resolve, 200));
      },
    });

    await t.rejects(() =>
      taskBoss.handle({ expire_in_seconds: 0.1, input: {}, task_name: 'test', trigger: { type: 'direct' } })
    );
  });

  tap.test('resovles handler', async (t) => {
    const taskBoss = createTaskBoss('queueA', {});

    const event = defineEvent({
      event_name: 'test',
      schema: Type.Object({}),
    });

    const handlerResult = {
      success: true,
    };

    taskBoss.on(event, {
      task_name: 'test',
      handler: async () => {
        await new Promise((resolve) => setTimeout(resolve, 100));
        return handlerResult;
      },
    });

    const eventData = event.from({});

    const result = await taskBoss.handle({
      expire_in_seconds: 10,
      input: eventData.data,
      task_name: 'test',
      trigger: { type: 'direct' },
    });

    t.equal(result, handlerResult);
  });

  tap.test('throws when task with different queue is registered', async (t) => {
    const bus = createTaskBoss('svc');

    const validTaskDef = defineTask({
      task_name: 'task_abc',
      queue: 'svc',
      schema: Type.Object({ works: Type.String() }),
    });

    const diffQueueTask = defineTask({
      task_name: 'task_abc',
      queue: 'diff',
      schema: Type.Object({ works: Type.String() }),
    });

    t.doesNotThrow(() => bus.registerTask(validTaskDef, { handler: async () => {} }));
    t.throws(() => bus.registerTask(diffQueueTask, { handler: async () => {} }));
  });

  tap.test('throws when same task is registered', async ({ throws }) => {
    const bus = createTaskBoss('svc');

    const taskA = defineTask({
      task_name: 'task_abc',
      schema: Type.Object({ works: Type.String() }),
    });

    bus.registerTask(taskA, { handler: async () => {} });

    throws(() => bus.registerTask(taskA, { handler: async () => {} }));
  });

  tap.test('throws when same eventHandler is registered', async ({ throws }) => {
    const taskBoss = createTaskBoss('qa');

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

    taskBoss.on(event, {
      task_name: 'task_2',
      handler: async (t) => {},
      config(input) {
        //d
        return {};
      },
    });

    throws(() =>
      taskBoss.on(event2, {
        task_name: 'task_2',
        handler: async (t) => {},
        config(input) {
          //
          return {};
        },
      })
    );
  });

  tap.test('task options', async ({ equal }) => {
    const queue = `task_options`;
    const taskBoss = createTaskBoss(queue);

    const taskDef = defineTask({
      task_name: 'options_task',
      schema: Type.Object({ works: Type.String() }),
      config: {
        expireInSeconds: 5,
        retryBackoff: true,
        retryDelay: 45,
        retryLimit: 4,
        startAfterSeconds: 45,
      },
    });

    taskBoss.registerTask(taskDef, {
      handler: async () => {},
    });

    const outgoingTask = taskBoss.getTask(taskDef.from({ works: 'abc' }));

    equal(outgoingTask.config.retryLimit, 4);
    equal(outgoingTask.config.retryDelay, 45);
    equal(outgoingTask.config.retryBackoff, true);
    equal(outgoingTask.config.singletonKey, null);
    equal(outgoingTask.config.startAfterSeconds, 45);
  });

  tap.test('task options merge', async ({ equal }) => {
    const queue = `task_options`;
    const taskBoss = createTaskBoss(queue, {
      handlerConfig: {
        retryDelay: 212,
        singletonKey: 'bla',
      },
    });

    const taskDef = defineTask({
      task_name: 'options_task',
      schema: Type.Object({ works: Type.String() }),
      config: {
        expireInSeconds: 5,
        startAfterSeconds: 45,
      },
    });

    taskBoss.registerTask(taskDef, {
      handler: async () => {},
    });

    const outgoingTask = taskBoss.getTask(
      taskDef.from(
        { works: 'abc' },
        {
          singletonKey: 'override',
          retryLimit: 4,
          retryBackoff: true,
        }
      )
    );

    equal(outgoingTask.config.retryLimit, 4);
    equal(outgoingTask.config.retryDelay, 212);
    equal(outgoingTask.config.expireInSeconds, 5);
    equal(outgoingTask.config.retryBackoff, true);
    equal(outgoingTask.config.singletonKey, 'override');
    equal(outgoingTask.config.startAfterSeconds, 45);
  });

  tap.test('on event options', async ({ equal }) => {
    const queue = 'handler-options';
    const taskboss = createTaskBoss(queue, {
      handlerConfig: {
        expireInSeconds: 33,
        retryBackoff: false,
      },
    });

    const event = defineEvent({
      event_name: 'test',
      schema: Type.Object({}),
    });

    const config = {
      expireInSeconds: 3,
      retryBackoff: true,
      retryDelay: 33,
      retryLimit: 5,
      startAfterSeconds: 1,
    };

    taskboss.on(event, {
      task_name: 'handler_task_config',
      config,
      handler: async () => {},
    });

    const eventData = event.from({});

    const tasks = taskboss.toTasks({
      event_data: eventData.data,
      event_name: eventData.event_name,
      id: '123',
    });

    equal(tasks.length, 1);
    const task = tasks[0]!;

    equal(task.config.retryLimit, config.retryLimit);
    equal(task.config.retryDelay, config.retryDelay);
    equal(task.config.retryBackoff, config.retryBackoff);
    equal(task.config.startAfterSeconds, config.startAfterSeconds);
  });

  tap.test('event handler config from payload', async ({ equal }) => {
    const queue = 'handler-options';
    const taskboss = createTaskBoss(queue);

    const event = defineEvent({
      event_name: 'event_handler_p',
      schema: Type.Object({
        c: Type.Number(),
      }),
    });

    taskboss.on(event, {
      task_name: 'handler_task_config',
      config: (data) => ({
        retryDelay: data.c + 2,
        singletonKey: 'singleton',
      }),
      handler: async () => {},
    });

    const eventData = event.from({ c: 91 });

    const tasks = taskboss.toTasks({
      event_data: eventData.data,
      event_name: eventData.event_name,
      id: '123',
    });

    equal(tasks.length, 1);
    const task = tasks[0]!;

    equal(task.config.retryDelay, 91 + 2);
    equal(task.config.singletonKey, 'singleton');
  });
});

tap.test('taskclient', async (t) => {
  const bus = createTaskBoss('queueA');

  const client = createTaskClient('queueA')
    .defineTask({
      name: 'test',
      schema: Type.Object({ n: Type.Number({ minimum: 2 }) }),
      config: {
        retryDelay: 20,
      },
    })
    .defineTask({
      name: 'abc',
      schema: Type.Object({ n: Type.String() }),
      config: {
        retryDelay: 10,
      },
    });

  t.throws(() => client.defs.test.from({ n: 1 }));

  t.same(client.defs.test.from({ n: 2 }), {
    queue: 'queueA',
    task_name: 'test',
    data: {
      n: 2,
    },
    config: {
      retryDelay: 20,
    },
  });
  t.same(client.defs.abc.from({ n: 'abc' }), {
    queue: 'queueA',
    task_name: 'abc',
    data: {
      n: 'abc',
    },
    config: {
      retryDelay: 10,
    },
  });

  bus.registerTaskClient(client, {
    async abc({ input }) {
      return {};
    },
    async test() {
      return {};
    },
  });

  t.equal(bus.getState().tasks.length, 2);
  t.matchSnapshot(stringify(bus.getState(), null, 2));
});

tap.test('taskclient throws with different queue', async (t) => {
  const bus = createTaskBoss('queueA');

  const client = createTaskClient('queueB').defineTask({
    name: 'test',
    schema: Type.Object({ n: Type.Number({ minimum: 2 }) }),
    config: {
      retryDelay: 20,
    },
  });

  t.throws(() =>
    bus.registerTaskClient(client, {
      async test() {
        return {};
      },
    })
  );
});

tap.test('getState', async (t) => {
  const tboss = createTaskBoss('doesnot-matter');

  const taskA = defineTask({
    task_name: 'task_a',
    schema: Type.Object({ works: Type.String() }),
  });

  tboss.registerTask(taskA, {
    handler: async () => {},
  });

  const taskB = defineTask({
    task_name: 'task_b',
    schema: Type.Object({ works: Type.String() }),
  });

  tboss.registerTask(taskB, {
    handler: async () => {},
  });

  const eventA = defineEvent({
    event_name: 'eventA',
    schema: Type.Object({
      c: Type.Number(),
    }),
  });

  const eventB = defineEvent({
    event_name: 'eventB',
    schema: Type.Object({
      d: Type.Number(),
    }),
  });

  tboss.on(eventA, {
    task_name: 'task_event_a',
    handler: async () => {},
  });

  tboss.on(eventB, {
    task_name: 'task_event_b',
    handler: async () => {},
  });

  t.matchSnapshot(stringify(tboss.getState(), null, 2));
  // 2 taskhandlers and 2 event handlers
  t.equal(tboss.getState().tasks.length, 4);
});
