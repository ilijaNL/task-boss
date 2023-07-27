import { TEvent, Task, TaskTrigger } from '../../definitions';
import { BaseClient, OutgoingTask, TaskBoss } from '../../task-boss';
import { DeferredPromise, mapCompletionDataArg } from '../../utils';
import { Response } from '@whatwg-node/fetch';

export type TaskDTO<T> = { tn: string; data: T; trace: TaskTrigger };

/**
 * Event that is send to the webhook service
 */

export type RemoteEvent = {
  /**
   * Event name
   */
  e: string;
  /**
   * Event data
   */
  d: any;
};

/**
 * Task that is send to the webhook service
 */
export type RemoteTask<T = object> = {
  /**
   * Queue
   */
  q: string;
  /**
   * Data
   */
  d: TaskDTO<T>;
  /**
   * Retry limit
   */
  r_l: number;
  /**
   * Retry delay
   */
  r_d: number;
  /**
   * Retry backoff
   */
  r_b: boolean;
  /**
   * Start after seconds
   */
  saf: number;
  /**
   * Expire in seconds
   */
  eis: number;
  /**
   * Singleton key
   */
  skey: string | null;
};

export const createRemoteTask = (task: OutgoingTask, trigger: TaskTrigger): RemoteTask => ({
  d: {
    data: task.data,
    tn: task.task_name,
    trace: trigger,
  },
  q: task.queue,
  eis: task.config.expireInSeconds,
  r_b: task.config.retryBackoff,
  r_d: task.config.retryDelay,
  r_l: task.config.retryLimit,
  saf: task.config.startAfterSeconds,
  skey: task.config.singletonKey,
});

export interface WebhookService {
  submitEvents(events: RemoteEvent[]): Promise<void>;
  submitTasks(tasks: RemoteTask<any>[]): Promise<void>;
}

const withWebhook = (taskBoss: TaskBoss, remote: WebhookService) => {
  return {
    /* istanbul ignore next */
    getState() {
      return taskBoss.getState();
    },
    publish(...events: TEvent[]) {
      return remote.submitEvents(events.map((e) => ({ d: e.data, e: e.event_name })));
    },
    async send(...tasks: Task[]) {
      const outgoing = tasks.map((t) => taskBoss.getTask(t));
      return remote.submitTasks(outgoing.map((t) => createRemoteTask(t, { type: 'direct' })));
    },
    async onEvent(event: IncomingRemoteEvent) {
      const tasks = taskBoss.toTasks({ event_data: event.d, event_name: event.n, id: event.id });

      if (tasks.length === 0) {
        return;
      }

      const remoteTasks = tasks.map((t) => createRemoteTask(t, { type: 'event', e: { id: event.id, name: event.n } }));

      return remote.submitTasks(remoteTasks);
    },
    async onTask(task: IncomingRemoteTask): Promise<any> {
      const future = new DeferredPromise();

      taskBoss
        .handle(task.d, {
          expire_in_seconds: task.es,
          id: task.id,
          retried: task.r,
          task_name: task.tn,
          trigger: task.tr,
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

      return future.promise;
    },
  };
};

export interface HandlerClient extends BaseClient {
  handle: (req: Request) => Promise<Response>;
}

export type IncomingRemoteEvent = {
  id: string;
  /**
   * Event name
   */
  n: string;
  /**
   * Event data
   */
  d: any;
};

export type IncomingRemoteTask = {
  id: string;
  /**
   * Task data
   */
  d: any;
  /**
   * Expire in seconds
   */
  es: number;
  /**
   * Retried
   */
  r: number;
  /**
   * Task name
   */
  tn: string;
  /**
   * Task trigger
   */
  tr: TaskTrigger;
};

export const withHandler = (taskBoss: TaskBoss, service: WebhookService): HandlerClient => {
  const client = withWebhook(taskBoss, service);

  /**
   * Body is a json, which should contain {t: true, b: IncomingRemoteTask for task or { e: true, b: IncomingRemoteEvent } for event.
   * also should contain x-remote-secret to validate that the request is coming from valid resource
   * TODO: improve the security of this
   */
  async function handle(req: Request) {
    const header = req.headers.get('x-remote-secret');

    if (!header) {
      return new Response('forbidden', {
        status: 403,
      });
    }

    // TODO: we need to validate that it comes from valid source

    try {
      // parse
      const parsed = await req.json();

      let res;
      if (parsed.t === true) {
        // catch the error and format it to some serializiable data

        res = await client.onTask(parsed.b);
      } else if (parsed.e === true) {
        await client.onEvent(parsed.b);
      } else {
        throw new Error('unknown body');
      }

      return new Response(res ? JSON.stringify(mapCompletionDataArg(res)) : 'ok', {
        headers: {
          'content-type': 'application/json',
        },
        status: 200,
      });
    } catch (err) {
      return new Response(JSON.stringify(mapCompletionDataArg(err)), {
        headers: {
          'content-type': 'application/json',
        },
        status: 400,
      });
    }
  }

  return {
    ...client,
    handle: handle,
  };
};

export default withHandler;
