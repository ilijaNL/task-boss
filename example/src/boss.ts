import { defineEvent, defineTask, createTaskBoss, Type, createTaskClient } from 'task-boss';

const queue = 'test-queue';

const taskBoss = createTaskBoss(queue, {
  handlerConfig: {
    expireInSeconds: 10,
    retryLimit: 1,
  },
});

const task = defineTask({
  schema: Type.Object({
    t: Type.String(),
  }),
  task_name: 'test-task',
  queue: queue,
});

const event = defineEvent({
  event_name: 'test-event',
  schema: Type.Object({
    duration: Type.Number(),
    event_d: Type.String(),
  }),
});

const taskClient = createTaskClient(queue)
  .defineTask({
    name: 't1',
    schema: Type.Object({
      a: Type.String(),
    }),
  })
  .defineTask({
    name: 't2',
    schema: Type.Object({
      b: Type.String(),
    }),
  });

taskBoss
  .registerTask(task, {
    handler: async (d, meta) => {
      console.log('task', meta);
      return {
        success: true,
      };
    },
  })
  .registerTaskClient(taskClient, {
    async t1({ a }, meta) {
      console.log('t1', meta);
      return a;
    },
    async t2({ b }, meta) {
      console.log('t2', meta);
      return b;
    },
  })
  .on(event, {
    task_name: 'on_event',
    handler: async ({ event_d }, meta) => {
      console.log('onEvent', meta);
      return {
        payload: event_d,
      };
    },
    config(input) {
      return {
        expireInSeconds: input.duration,
      };
    },
  });

export { taskBoss, taskClient, task, event };
