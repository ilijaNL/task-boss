import { TEvent, Task, TaskTrace } from '../../definitions';
import { BaseClient, OutgoingTask, TaskBoss } from '../../task-boss';
import { DeferredPromise, JsonValue, mapCompletionDataArg } from '../../utils';
import { Response } from '@whatwg-node/fetch';
import { generateRandomSecretKey, getHMAC, webTimingSafeEqual } from './crypto';

export type TaskDTO<T> = { tn: string; data: T; trace?: TaskTrace };

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
export type RemoteTask<T = JsonValue> = {
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

export const createRemoteTask = (task: OutgoingTask): RemoteTask => ({
  d: {
    data: task.data,
    tn: task.task_name,
    trace: task.trace,
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
      const outgoing = tasks.map((t) => taskBoss.toOutTask(t));
      return remote.submitTasks(outgoing.map((t) => createRemoteTask(t)));
    },
    onEvents(events: IncomingRemoteEvent[]) {
      const inEvents = events.map((event) => ({ event_data: event.d, event_name: event.n, id: event.id }));
      const tasks = taskBoss.eventsToTasks(inEvents);

      if (tasks.length === 0) {
        return;
      }

      const remoteTasks = tasks.map(createRemoteTask);

      return remote.submitTasks(remoteTasks);
    },
    async onTask(task: IncomingRemoteTask): Promise<any> {
      const future = new DeferredPromise();

      taskBoss
        .handleTask(task.d, {
          expire_in_seconds: task.es,
          id: task.id,
          retried: task.r,
          task_name: task.tn,
          trace: task.tr,
          fail(data) {
            future.reject(data);
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
   * Task trace
   */
  tr?: TaskTrace;
};

export const withHandler = (
  taskBoss: TaskBoss,
  options: {
    service: WebhookService;
    sign_secret: string | null;
  }
): HandlerClient => {
  const client = withWebhook(taskBoss, options.service);

  /**
   * Body is a json, which should contain {t: true, b: IncomingRemoteTask for task or { e: true, b: IncomingRemoteEvent } for event.
   */
  async function handle(req: Request) {
    try {
      let body;

      if (options.sign_secret === null) {
        body = await req.text();
      }

      if (options.sign_secret !== null) {
        const expectedSignature = req.headers.get('x-body-signature');

        if (!expectedSignature) {
          return new Response('forbidden: missing x-body-signature', {
            status: 403,
          });
        }

        body = await req.text();

        // calculate the signature from the body
        const [randomSecretForTimingAttack, bodySignature] = await Promise.all([
          generateRandomSecretKey(),
          getHMAC(body, options.sign_secret, 'SHA-256'),
        ]);
        const verified = await webTimingSafeEqual(randomSecretForTimingAttack, expectedSignature, bodySignature);

        if (!verified) {
          return new Response('forbidden: invalid signature', {
            status: 403,
          });
        }
      }

      const parsed = JSON.parse(body as string);

      let res;
      if (parsed.t === true) {
        // catch the error and format it to some serializiable data

        res = await client.onTask(parsed.b);
      } else if (parsed.e === true) {
        await client.onEvents(parsed.b);
      } else {
        throw new Error('unknown body');
      }

      return new Response(JSON.stringify(mapCompletionDataArg(res)), {
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
