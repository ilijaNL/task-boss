import { Static, TSchema } from '@sinclair/typebox';
import { ValidateFunction } from 'ajv';
import { createValidateFn } from './schema';
import { Simplify } from './utils';

export interface TEvent<Name = string, Data = {}> {
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
        /**
         * Event position
         */
        p: string;
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
  validate: ValidateFunction<Static<Schema, []>>;
  from: (input: Static<Schema>) => TEvent<Name, Static<Schema>>;
}

/**
 * Define an integration event.  Task name should be unique for a pg-tbus instance
 */
export const defineEvent = <TName extends string, T extends TSchema>(
  spec: EventSpec<TName, T>
): EventDefinition<TName, T> => {
  const { event_name, schema } = spec;
  const validate = createValidateFn(schema);

  function from(input: Static<T>, opts?: Partial<{ retention_in_days: number }>): TEvent<TName, Static<T>> {
    const validateFn = validate;
    if (!validateFn(input)) {
      throw new Error(`invalid input for event ${event_name}: ${validateFn.errors?.map((e) => e.message).join(' \n')}`);
    }
    return {
      data: input,
      event_name: event_name,
    };
  }

  return {
    event_name,
    schema,
    validate,
    from,
  };
};

export interface EventHandler<TName extends string, T extends TSchema> {
  task_name: string;
  def: EventDefinition<TName, T>;
  handler: Handler<Static<T>>;
  config: Partial<TaskConfig> | ((input: Static<T>) => Partial<TaskConfig>);
}

export interface DefineTaskProps<T extends TSchema> {
  /**
   * Task name
   */
  task_name: string;
  /**
   * Queue this task belongs to.
   * If not specified, the queue will be set to service when task is send from a tbus instance.
   */
  queue?: string;
  /**
   * Task payload schema
   */
  schema: T;
  /**
   * Default task configuration. Can be (partially) override when creating the task
   */
  config?: Partial<TaskConfig>;
}

export interface TaskDefinition<T extends TSchema> extends DefineTaskProps<T> {
  validate: ValidateFunction<Static<T, []>>;
  from: (input: Static<T>, config?: Partial<TaskConfig>) => Task<Static<T>>;
}

// export interface TaskHandler<T extends TSchema> extends TaskDefinition<T> {
//   handler: Handler<Static<T>>;
//   config: Partial<TaskConfig>;
// }

export interface Task<Data = {}> {
  task_name: string;
  queue?: string;
  data: Data;
  config: Partial<TaskConfig>;
}

export interface Handler<Input> {
  (props: { task_name: string; input: Input; trigger: TaskTrigger }): Promise<any>;
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
  const validateFn = createValidateFn(props.schema);

  const from: TaskDefinition<T>['from'] = function from(input, config) {
    if (!validateFn(input)) {
      throw new Error(
        `invalid input for task ${props.task_name}: ${validateFn.errors?.map((e) => e.message).join(' \n')}`
      );
    }

    return {
      queue: props.queue,
      task_name: props.task_name,
      data: input,
      config: { ...props.config, ...config },
    };
  };

  return {
    schema: props.schema,
    task_name: props.task_name,
    queue: props.queue,
    validate: validateFn,
    from,
    // specifiy some defaults
    config: props.config ?? {},
  };
};

// export const createTaskHandler = <T extends TSchema>(props: {
//   taskDef: TaskDefinition<T>;
//   handler: Handler<Static<T>>;
// }): TaskHandler<T> => {
//   return {
//     config: props.taskDef.config ?? {},
//     handler: props.handler,
//     from: props.taskDef.from,
//     schema: props.taskDef.schema,
//     task_name: props.taskDef.task_name,
//     validate: props.taskDef.validate,
//     queue: props.taskDef.queue,
//   };
// };

/**
 * Create an event handler from an event definition. Task name should be unique for a pg-tbus instance
 */
export const createEventHandler = <TName extends string, T extends TSchema>(props: {
  /**
   * Task name. Should be unique per pg-tbus instance
   */
  task_name: string;
  /**
   * Event definitions
   */
  eventDef: EventDefinition<TName, T>;
  /**
   * Event handler
   */
  handler: Handler<Static<T>>;
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

export type TaskClient<D = {}> = {
  defineTask<T extends TSchema, N extends string>(props: {
    name: N;
    schema: T;
    /**
     * Default task configuration. Can be (partially) override when creating the task
     */
    config?: Partial<TaskConfig>;
  }): TaskClient<Simplify<D & { [n in N]: TaskDefinition<T> }>>;
  defs: Readonly<D>;
};

/**
 * A task client that holds multiple task definitions.
 * Can be exported as a lightweight object and used by other services.
 *
 * A task client should be registered by a single service tbus.
 * @example
 * const client = createTaskClient('queueA')
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
 *   });
 *
 * bus.registerTaskClient(client, {
 *  async abc({ input }) {
 *     return {};
 *   },
 *   async test() {
 *     return {};
 *   },
 * });
 */
export const createTaskClient = <D>(queue: string): TaskClient<D> => {
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

      return this as TaskClient<D & { [n in N]: TaskDefinition<T> }>;
    },
    get defs() {
      return Object.freeze(definitions);
    },
  };
};
