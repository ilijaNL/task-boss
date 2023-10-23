import { Static, TSchema } from '@sinclair/typebox';
import {
  EventDefinition,
  EventHandler,
  EventSpec,
  TEvent,
  Task,
  TaskConfig,
  TaskDefinition,
  TaskHandler,
  createEventHandler,
  TaskClient,
  defaultTaskConfig,
} from './definitions';
import { JsonValue, resolveWithinSeconds } from './utils';

export type TaskState = {
  task_name: string;
  /**
   * This task will be created when this event happens
   */
  on_event?: string;
  schema: TSchema;
  config: Partial<TaskConfig>;
};

export type IncomingEvent = {
  id: string;
  event_name: string;
  event_data: JsonValue;
};

export type TaskBossState = {
  queue: string;
  events: Array<EventSpec<string, TSchema>>;
  tasks: Array<TaskState>;
};

export type OutgoingTask = Task<JsonValue> & {
  config: TaskConfig;
  queue: string;
};

export type TaskClientImpl<R extends Record<string, TaskDefinition<TSchema>>> = {
  [K in keyof R]: TaskHandler<Static<R[K]['schema']>>;
};

/**
 * Base interface which is used to create a specific client
 */
export interface BaseClient {
  getState: () => TaskBossState;
  send: (...tasks: Task[]) => Promise<void>;
  publish: (...events: TEvent<string, any>[]) => Promise<void>;
}

export type TaskBoss = {
  /**
   * Queue that is used for this taskboss instance
   */
  queue: string;
  /** Get serializble state of the taskboss */
  getState: () => TaskBossState;
  /**
   * Convert a task to outgoing task (which can be submitted)
   */
  getTask: (task: Task) => OutgoingTask;
  /**
   * Get tasks from events.
   * This performs no validation against the task schema.
   */
  toTasks: (event: IncomingEvent) => OutgoingTask[];
  /**
   * Execute the handler for an incoming task
   */
  handle: TaskHandler<unknown>;
  /**
   * Register task handler
   */
  registerTask: <T extends TSchema>(
    taskDef: TaskDefinition<T>,
    props: {
      handler: TaskHandler<Static<T>>;
      overrideConfig?: Partial<TaskConfig>;
    }
  ) => TaskBoss;
  on: <T extends TSchema>(
    eventDef: EventDefinition<any, T>,
    props: {
      /**
       * Task name. Should be for this bus instance
       */
      task_name: string;
      handler: TaskHandler<Static<T>>;
      /**
       * Event handler configuration. Can be static or a function
       */
      config?: Partial<TaskConfig> | ((input: Static<T>) => Partial<TaskConfig>);
    }
  ) => TaskBoss;
  registerTaskClient: <R extends Record<string, TaskDefinition<any>>>(
    client: TaskClient<R>,
    impl: TaskClientImpl<R>
  ) => TaskBoss;
};

export type TaskOverrideConfig = Partial<Omit<TaskConfig, 'singletonKey' | 'startAfterSeconds'>>;

type TaskName = string & { __type?: string };

export const createTaskFactory = (queue: string) => {
  return function toTask(task: Task): OutgoingTask {
    const config: OutgoingTask['config'] = task.config;
    return {
      config,
      data: task.data,
      task_name: task.task_name,
      queue: task.queue ?? queue,
    };
  };
};

export const createTaskBoss = (queue: string): TaskBoss => {
  const eventHandlers: Array<EventHandler<string, any>> = [];
  const taskHandlersMap = new Map<TaskName, TaskState & { handler: TaskHandler<any> }>();

  const taskFactory = createTaskFactory(queue);

  return {
    get queue() {
      return queue;
    },
    getTask(task) {
      return taskFactory(task);
    },
    toTasks(event) {
      const handlers = eventHandlers.filter((eh) => eh.def.event_name === event.event_name);
      return handlers.map<OutgoingTask>((h) => {
        const config: OutgoingTask['config'] = {
          ...defaultTaskConfig,
          ...(typeof h.config === 'function' ? h.config(event.event_data) : h.config),
        };

        return {
          data: event.event_data,
          config: config,
          task_name: h.task_name,
          queue: queue,
        };
      });
    },
    async handle(input, meta_data) {
      const taskHandler = taskHandlersMap.get(meta_data.task_name);

      // log
      if (!taskHandler) {
        console.error('task handler ' + meta_data.task_name + 'not registered for queue ' + queue);
        throw new Error('task handler ' + meta_data.task_name + 'not registered for queue ' + queue);
      }

      return await resolveWithinSeconds(taskHandler.handler(input as any, meta_data), meta_data.expire_in_seconds);
    },
    registerTaskClient<R extends Record<string, TaskDefinition<TSchema>>>(
      client: TaskClient<R>,
      impl: TaskClientImpl<R>
    ) {
      (Object.keys(client) as Array<keyof R>).forEach((key) => {
        this.registerTask(client[key], {
          handler: impl[key],
        });
      });

      return this;
    },
    registerTask(taskDef, props) {
      if (taskHandlersMap.has(taskDef.task_name)) {
        throw new Error(`task ${taskDef.task_name} already registered`);
      }

      if (taskDef.queue && taskDef.queue !== queue) {
        throw new Error(
          `task ${taskDef.task_name} belongs to a different queue. Expected ${queue}, got ${taskDef.queue}`
        );
      }

      taskHandlersMap.set(taskDef.task_name, {
        config: props.overrideConfig ?? taskDef.config ?? defaultTaskConfig,
        handler: props.handler,
        schema: taskDef.schema,
        task_name: taskDef.task_name,
      });

      return this;
    },
    on(eventDef, props) {
      if (eventHandlers.some((h) => h.task_name === props.task_name)) {
        throw new Error(`task ${props.task_name} already registered`);
      }

      taskHandlersMap.set(props.task_name, {
        handler: props.handler,
        schema: eventDef.schema,
        task_name: props.task_name,
        on_event: eventDef.event_name,
        config: typeof props.config === 'function' ? {} : props.config ?? defaultTaskConfig,
      });

      eventHandlers.push(
        createEventHandler({
          eventDef: eventDef,
          handler: props.handler,
          task_name: props.task_name,
          config: props.config,
        })
      );

      return this;
    },
    getState() {
      const events: TaskBossState['events'] = eventHandlers.map((eh) => ({
        event_name: eh.def.event_name,
        schema: eh.def.schema,
      }));

      const tasks: TaskBossState['tasks'] = Array.from(taskHandlersMap.values()).map((eh) => ({
        config: eh.config,
        task_name: eh.task_name,
        on_event: eh.on_event,
        schema: eh.schema,
      }));

      return {
        events,
        queue: queue,
        tasks: tasks,
      };
    },
  };
};

export default createTaskBoss;
