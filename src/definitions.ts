import { Static, TSchema } from '@sinclair/typebox';
import { JsonValue, Simplify } from './utils';
import { Value } from '@sinclair/typebox/value';

export interface TEvent<Name = string, Data = JsonValue> {
  event_name: Name;
  data: Data;
}

export type TaskTrigger =
  | {
      /**
       * Directly scheduled task
       */
      type: 'direct';
    }
  | {
      type: 'event';
      /**
       * Triggered by event
       */
      e: {
        /**
         * Event id
         */
        id: string;
        /**
         * Event name
         */
        name: string;
      };
    };

export interface EventSpec<Name extends string, Schema extends TSchema> {
  /**
   * Event name.
   * It is wisely to prefix with servicename abbr
   */
  event_name: Name;
  /**
   * Typebox schema of the payload
   */
  schema: Schema;
}

export interface EventDefinition<Name extends string, Schema extends TSchema> {
  event_name: Name;
  schema: Schema;
  /**
   * Creates an event from a input. This input is validated against the schema.
   */
  from: (input: Static<Schema>) => TEvent<Name, Static<Schema>>;
}

/**
 * Define an integration event.  Task name should be unique when registering
 */
export const defineEvent = <TName extends string, T extends TSchema>(
  spec: EventSpec<TName, T>
): EventDefinition<TName, T> => {
  const { event_name, schema } = spec;

  function from(input: Static<T>): TEvent<TName, Static<T>> {
    const errors = [...Value.Errors(spec.schema, input)];
    if (errors.length > 0) {
      throw new Error(`invalid input for event ${event_name}: ${errors.map((e) => e.message).join(' \n')}`);
    }
    return {
      data: input,
      event_name: event_name,
    };
  }

  return {
    event_name,
    schema,
    from,
  };
};

export interface EventHandler<TName extends string, T extends TSchema> {
  task_name: string;
  def: EventDefinition<TName, T>;
  handler: TaskHandler<Static<T>>;
  config: Partial<TaskConfig> | ((input: Static<T>) => Partial<TaskConfig>);
}

export interface DefineTaskProps<T extends TSchema> {
  /**
   * Task name
   */
  task_name: string;
  /**
   * Queue this task belongs to.
   * If not specified, the queue will be set to instances's queue when task is send.
   */
  queue?: string;
  /**
   * Task payload schema
   */
  schema: T;
  /**
   * Default task configuration
   */
  config?: Partial<TaskConfig>;
}

export interface TaskDefinition<T extends TSchema> extends DefineTaskProps<T> {
  /**
   * Creates a tasks from an input. The input is validated against the schema.
   */
  from: (input: Static<T>, config?: Partial<TaskConfig>) => Task<Static<T>>;
}

export interface Task<Data = JsonValue> {
  task_name: string;
  queue?: string;
  data: Data;
  config: Partial<TaskConfig>;
}

// export type IncomingTask<Input> = { task_name: string; input: Input; trigger: TaskTrigger; expire_in_seconds: number };
export type TaskHandlerCtx<Res> = {
  id: string;
  expire_in_seconds: number;
  task_name: string;
  trigger: TaskTrigger;
  retried: number;
  fail: (reason?: any) => void;
  resolve: (data: Res) => void;
};

export interface TaskHandler<Input = JsonValue, Res = any> {
  (data: Input, ctx: TaskHandlerCtx<Res>): Promise<Res>;
}

export const defaultTaskConfig: TaskConfig = {
  retryBackoff: false,
  retryDelay: 5,
  retryLimit: 3,
  startAfterSeconds: 0,
  expireInSeconds: 60 * 5, // 5 minutes
  singletonKey: null,
};

export type TaskConfig = {
  /**
   * Amount of times the task is retried, default 3
   */
  retryLimit: number;
  /**
   * Delay between retries of failed tasks, in seconds. Default 5
   */
  retryDelay: number;
  /**
   * Expentional retrybackoff, default false
   */
  retryBackoff: boolean;
  /**
   * Start after n seconds, default 0
   */
  startAfterSeconds: number;
  /**
   * How many seconds a task may be in active state before it is failed because of expiration. Default 60 * 5 (5minutes)
   */
  expireInSeconds: number;
  /**
   * A singleton key which can be used to have an unique scheduled/active task in a queue.
   */
  singletonKey: string | null;
};

export const defineTask = <T extends TSchema>(props: DefineTaskProps<T>): TaskDefinition<T> => {
  const _from: TaskDefinition<T>['from'] = function from(input, config) {
    return {
      queue: props.queue,
      task_name: props.task_name,
      data: input,
      config: { ...props.config, ...config },
    };
  };

  const from: TaskDefinition<T>['from'] = function from(input, config) {
    const errors = [...Value.Errors(props.schema, input)];
    if (errors.length > 0) {
      throw new Error(`invalid input for task ${props.task_name}: ${errors.map((e) => e.message).join(' \n')}`);
    }

    return _from(input, config);
  };

  return {
    schema: props.schema,
    task_name: props.task_name,
    queue: props.queue,
    from,
    // specifiy some defaults
    config: props.config ?? {},
  };
};

/**
 * Create an event handler from an event definition.
 */
export const createEventHandler = <TName extends string, T extends TSchema>(props: {
  task_name: string;
  /**
   * Event definitions
   */
  eventDef: EventDefinition<TName, T>;
  /**
   * Event handler
   */
  handler: TaskHandler<Static<T>>;
  /**
   * Event handler configuration. Can be static or a function
   */
  config?: Partial<TaskConfig> | ((input: Static<T>) => Partial<TaskConfig>);
}): EventHandler<TName, T> => {
  return {
    task_name: props.task_name,
    def: props.eventDef,
    handler: props.handler,
    config: typeof props.config === 'function' ? props.config : { ...props.config },
  };
};

export type TaskClient<T extends Record<string, TaskDefinition<any>>> = {
  [K in keyof T]: T[K];
};

export type TaskBuilder<D extends Record<string, TaskDefinition<any>> = {}> = {
  defineTask<T extends TSchema, N extends string>(props: {
    name: N;
    schema: T;
    /**
     * Default task configuration. Can be (partially) override when creating the task
     */
    config?: Partial<TaskConfig>;
  }): TaskBuilder<Simplify<D & { [n in N]: TaskDefinition<T> }>>;
  compile: () => TaskClient<D>;
};

/**
 * A task client that holds multiple task definitions.
 * Can be exported as a lightweight object and used by other services.
 *
 * A task client should be registered by a single service tbus.
 * @example
 * const client = createTaskBuilder('queueA')
 *   .defineTask({
 *     name: 'test',
 *     schema: Type.Object({ n: Type.Number({ minimum: 2 }) }),
 *     config: {
 *       retryDelay: 20,
 *     },
 *   })
 *   .defineTask({
 *     name: 'abc',
 *     schema: Type.Object({ n: Type.String() }),
 *     config: {
 *       keepInSeconds: 8,
 *       retryDelay: 10,
 *     },
 *   })
 *   .compile();
 *
 * tb.registerTaskClient(client, {
 *  async abc({ input }) {
 *     return {};
 *   },
 *   async test() {
 *     return {};
 *   },
 * });
 */
export const createTaskBuilder = <D extends Record<string, TaskDefinition<any>> = {}>(
  queue: string
): TaskBuilder<D> => {
  const definitions: D = {} as any;

  return {
    defineTask<T extends TSchema, N extends string>(props: {
      name: N;
      schema: T;
      /**
       * Default task configuration. Can be (partially) override when creating the task
       */
      config?: Partial<TaskConfig>;
    }) {
      (definitions as any)[props.name] = defineTask({
        schema: props.schema,
        task_name: props.name,
        config: props.config,
        queue: queue,
      });

      return this as TaskBuilder<D & { [n in N]: TaskDefinition<T> }>;
    },
    compile() {
      return Object.freeze(definitions) as TaskClient<D>;
    },
  };
};
