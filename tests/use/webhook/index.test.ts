import tap from 'tap';
import createTaskBoss, { IncomingEvent } from '../../../src';
import withHandler, { IncomingRemoteEvent, IncomingRemoteTask, RemoteTask } from '../../../src/use/webhook';
import { TEvent, defineEvent, defineTask } from '../../../src';
import { Type } from '@sinclair/typebox';
import { Request } from '@whatwg-node/fetch';
import { getHMAC } from '../../../src/use/webhook/crypto';

function createEventRequest(data: IncomingRemoteEvent) {
  return new Request('http://test.localhost', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
    },
    body: Buffer.from(
      JSON.stringify({
        e: true,
        b: data,
      })
    ),
  });
}

function createTaskRequest(data: IncomingRemoteTask) {
  return new Request('http://test.localhost', {
    method: 'POST',
    headers: {
      'x-remote-secret': '123',
      'content-type': 'application/json',
    },
    body: Buffer.from(
      JSON.stringify({
        t: true,
        b: data,
      })
    ),
  });
}

tap.test('happy path', async (t) => {
  const tb = createTaskBoss('smoke_test');

  const withH = withHandler(tb, {
    sign_secret: null,
    service: {
      async submitEvents(events) {},
      async submitTasks(tasks) {},
    },
  });

  const ie: IncomingEvent = {
    event_data: {},
    event_name: 'test',
    id: '123',
  };

  const req = new Request('http://test.localhost', {
    method: 'POST',
    headers: {
      'x-remote-secret': '123',
      'content-type': 'application/json',
    },
    body: Buffer.from(
      JSON.stringify({
        e: true,
        b: ie,
      })
    ),
  });

  t.resolves(withH.handle(req));
});

tap.test('happy path with signature', async (t) => {
  const tb = createTaskBoss('smoke_test');
  const sign_secret = 'signature-secret';

  const withH = withHandler(tb, {
    sign_secret: sign_secret,
    service: {
      async submitEvents(events) {},
      async submitTasks(tasks) {},
    },
  });

  const ie: IncomingEvent = {
    event_data: {},
    event_name: 'test',
    id: '123',
  };

  const payload = JSON.stringify({
    e: true,
    b: ie,
  });

  // no signature
  {
    const req1 = new Request('http://test.localhost', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: Buffer.from(payload),
    });

    const res1 = await withH.handle(req1);
    t.equal(res1.status, 403);
    t.equal(await res1.text(), 'forbidden: missing x-body-signature');
  }

  // correct signature
  {
    const req2 = new Request('http://test.localhost', {
      method: 'POST',
      headers: {
        'x-body-signature': await getHMAC(payload, sign_secret, 'SHA-256'),
        'content-type': 'application/json',
      },
      body: Buffer.from(payload),
    });

    const res2 = await withH.handle(req2);

    t.equal(res2.status, 200);
    t.equal(await res2.json(), null);
  }

  // wrong signature
  {
    const req = new Request('http://test.localhost', {
      method: 'POST',
      headers: {
        'x-body-signature': await getHMAC(payload, 'abc', 'SHA-256'),
        'content-type': 'application/json',
      },
      body: Buffer.from(payload),
    });

    const res = await withH.handle(req);
    t.equal(res.status, 403);
    t.equal(await res.text(), 'forbidden: invalid signature');
  }
});

tap.test('publish event', async (t) => {
  t.plan(2);
  const tb = createTaskBoss('www');
  const event: TEvent = {
    data: '123',
    event_name: 'name',
  };
  const htb = withHandler(tb, {
    sign_secret: null,
    service: {
      async submitEvents(events) {
        t.equal(events[0]!.d, event.data);
        t.equal(events[0]!.e, event.event_name);
      },
      async submitTasks() {},
    },
  });

  await htb.publish(event);
});

tap.test('throws wrong payload', async (t) => {
  const tb = createTaskBoss('smoke_test');

  const withH = withHandler(tb, {
    sign_secret: null,
    service: {
      async submitEvents() {},
      async submitTasks() {},
    },
  });

  const ie: IncomingEvent = {
    event_data: {},
    event_name: 'test',
    id: '123',
  };

  const re2 = new Request('http://test.localhost', {
    method: 'POST',
    headers: {
      'x-remote-secret': '123',
      'content-type': 'application/json',
    },
    body: Buffer.from(
      JSON.stringify({
        b: ie,
      })
    ),
  });

  const res2 = await withH.handle(re2);
  t.equal(res2.status, 400);
  t.equal(res2.headers.get('content-type'), 'application/json');
  t.equal((await res2.json()).message, 'unknown body');
});

tap.test('on new task', async (tap) => {
  const queue = 'task_queue';
  const tb = createTaskBoss(queue);

  const task_name = 'emit_task';

  const taskDef = defineTask({
    task_name: task_name,
    schema: Type.Object({ works: Type.String() }),
  });

  const task = taskDef.from({ works: '12312312' });
  const handlerResponse = {
    success: 'balba',
  };

  tb.registerTask(taskDef, {
    handler: async (r) => {
      tap.equal(r.works, '12312312');
      return handlerResponse;
    },
  });

  const handlerBoss = withHandler(tb, {
    sign_secret: null,
    service: {
      async submitTasks(tasks) {
        tap.fail('should not call');
        //
      },
      async submitEvents(events) {
        tap.fail('should not call');
        //
      },
    },
  });

  const taskReq = createTaskRequest({
    es: 10,
    d: task.data,
    id: '123',
    r: 0,
    tn: task.task_name,
    tr: { type: 'direct' },
  });

  const res = await handlerBoss.handle(taskReq);

  tap.same(await res.json(), handlerResponse);
});

tap.test('on new task resolve', async (tap) => {
  const queue = 'task_queue';
  const tb = createTaskBoss(queue);

  const task_name = 'emit_task';

  const taskDef = defineTask({
    task_name: task_name,
    schema: Type.Object({ works: Type.String() }),
  });

  const task = taskDef.from({ works: '12312312' });

  tb.registerTask(taskDef, {
    handler: async (r, { resolve }) => {
      tap.equal(r.works, '12312312');

      resolve({ resolves: true });

      return {
        success: 'balba',
      };
    },
  });

  const handlerBoss = withHandler(tb, {
    sign_secret: null,
    service: {
      async submitTasks(tasks) {
        tap.fail('should not call');
        //
      },
      async submitEvents(events) {
        tap.fail('should not call');
        //
      },
    },
  });

  const taskReq = createTaskRequest({
    es: 10,
    d: task.data,
    id: '123',
    r: 0,
    tn: task.task_name,
    tr: { type: 'direct' },
  });

  const res = await handlerBoss.handle(taskReq);

  tap.same(await res.json(), { resolves: true });
});

tap.test('on new task throws', async (tap) => {
  const queue = 'task_queue';
  const tb = createTaskBoss(queue);

  const task_name = 'emit_task';

  const taskDef = defineTask({
    task_name: task_name,
    schema: Type.Object({ works: Type.String() }),
  });

  const task = taskDef.from({ works: '12312312' });

  tb.registerTask(taskDef, {
    handler: async (r) => {
      tap.equal(r.works, '12312312');
      throw new Error('failed');
    },
  });

  const handlerBoss = withHandler(tb, {
    sign_secret: null,
    service: {
      async submitTasks(tasks) {
        tap.fail('should not call');
      },
      async submitEvents(events) {
        tap.fail('should not call');
      },
    },
  });

  const taskReq = createTaskRequest({
    es: 10,
    d: task.data,
    id: '123',
    r: 0,
    tn: task.task_name,
    tr: { type: 'direct' },
  });

  const res = await handlerBoss.handle(taskReq);

  tap.same((await res.json()).message, 'failed');
});

tap.test('on new task fails', async (tap) => {
  const queue = 'task_queue';
  const tb = createTaskBoss(queue);

  const task_name = 'emit_task';

  const taskDef = defineTask({
    task_name: task_name,
    schema: Type.Object({ works: Type.String() }),
  });

  const task = taskDef.from({ works: '12312312' });

  tb.registerTask(taskDef, {
    handler: async (r, { fail }) => {
      tap.equal(r.works, '12312312');

      fail({ failed: true });

      throw new Error('failed');
    },
  });

  const handlerBoss = withHandler(tb, {
    sign_secret: null,
    service: {
      async submitTasks(tasks) {
        tap.fail('should not call');
        //
      },
      async submitEvents(events) {
        tap.fail('should not call');
        //
      },
    },
  });

  const taskReq = createTaskRequest({
    es: 10,
    d: task.data,
    id: '123',
    r: 0,
    tn: task.task_name,
    tr: { type: 'direct' },
  });

  const res = await handlerBoss.handle(taskReq);

  tap.same(await res.json(), { failed: true });
});

tap.test('submit tasks', async (tap) => {
  const queue = 'emit_queue';
  const tb = createTaskBoss(queue);

  const task_name = 'emit_task';

  const taskDef = defineTask({
    task_name: task_name,
    schema: Type.Object({ works: Type.String() }),
  });
  let sent = 0;

  const instance = withHandler(tb, {
    sign_secret: null,
    service: {
      async submitEvents() {},
      async submitTasks(tasks) {
        tasks.forEach((t) => {
          tap.equal(t.q, queue);
          sent += 1;
        });
      },
    },
  });

  await instance.send(taskDef.from({ works: 'abcd' }), taskDef.from({ works: 'abcd' }));

  tap.equal(sent, 2);
});

tap.test('submit tasks on event', async (tap) => {
  const tb = createTaskBoss('emit_event_queue');

  const event1 = defineEvent({
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

  tb.on(event1, {
    task_name: 'task1',
    handler: async (input, { trigger }) => {
      tap.equal(input.text, 'text222');
      tap.equal(trigger.type, 'event');
    },
  });

  tb.on(event1, {
    task_name: 'task_2',
    handler: async (input, { trigger }) => {
      tap.equal(input.text, 'text222');
      tap.equal(trigger.type, 'event');
    },
  });

  tb.on(event2, {
    task_name: 'task_3',
    handler: async (input, { trigger }) => {
      tap.equal(input.rrr, 'event2');
      tap.equal(trigger.type, 'event');
    },
  });

  const submittedTasks: RemoteTask<any>[] = [];

  const pgTasks = withHandler(tb, {
    sign_secret: null,
    service: {
      async submitEvents() {},
      async submitTasks(tasks) {
        submittedTasks.push(...tasks);
      },
    },
  });

  const ie1: IncomingRemoteEvent = {
    d: event1.from({ text: 'abc ' }).data,
    n: event1.from({ text: 'abc ' }).event_name,
    id: '123',
  };

  const ie2: IncomingRemoteEvent = {
    d: event2.from({ rrr: '123' }).data,
    n: event2.from({ rrr: '123' }).event_name,
    id: '5a4wdawdawd',
  };

  await Promise.all([
    //
    pgTasks.handle(createEventRequest(ie1)),
    pgTasks.handle(createEventRequest(ie2)),
  ]);

  tap.equal(submittedTasks.length, 3);
  tap.matchSnapshot(submittedTasks);
});
