import { createBaseWorker } from '../../worker';
import { PGClient, query } from './sql';
import { createBatcher } from 'node-batcher';
import { ResolvedTask, SQLPlans, SelectTask, TASK_STATES, TaskState } from './plans';
import { mapCompletionDataArg } from '../../utils';

export function getStartAfter(task: SelectTask, newState: TaskState) {
  const startAfter =
    newState === TASK_STATES.retry
      ? task.config.r_b
        ? task.config.r_d * Math.pow(2, task.retrycount)
        : task.config.r_d
      : undefined;

  return startAfter;
}

export const createTaskWorker = (props: {
  client: PGClient;
  plans: SQLPlans;
  handler: (event: SelectTask) => Promise<any>;
  maxConcurrency: number;
  poolInternvalInMs: number;
  refillThresholdPct: number;
}) => {
  const activeTasks = new Map<string, Promise<any>>();
  const { maxConcurrency, client, handler, poolInternvalInMs, refillThresholdPct, plans } = props;
  // used to determine if we can refetch early
  let hasMoreTasks = false;

  const resolveTaskBatcher = createBatcher<ResolvedTask>({
    async onFlush(batch) {
      const q = plans.resolveTasks(batch.map(({ data: i }) => i));
      await query(client, q);
    },
    // dont make to big since payload can be big
    maxSize: 100,
    // keep it low latency
    maxTimeInMs: 50,
  });

  function resolveTask(task: SelectTask, err: any, result?: any) {
    // calculate new state
    // calculate new state
    const newState =
      err === null
        ? TASK_STATES.completed
        : task.retrycount >= task.config.r_l
        ? TASK_STATES.failed
        : TASK_STATES.retry;
    const startAfter = getStartAfter(task, newState);

    // if this throws, something went really wrong
    resolveTaskBatcher.add({ out: mapCompletionDataArg(err ?? result), s: newState, id: task.id, saf: startAfter });

    activeTasks.delete(task.id);

    // if some treshhold is reached, we can refetch
    const threshHoldPct = refillThresholdPct;
    if (hasMoreTasks && activeTasks.size / maxConcurrency < threshHoldPct) {
      taskWorker.notify();
    }
  }

  const taskWorker = createBaseWorker(
    async () => {
      if (activeTasks.size >= maxConcurrency) {
        return;
      }

      const requestedAmount = maxConcurrency - activeTasks.size;
      const tasks = await query(client, plans.getAndStartTasks({ amount: requestedAmount }));

      // high chance that there are more tasks when requested amount is same as fetched
      hasMoreTasks = tasks.length === requestedAmount;

      if (tasks.length === 0) {
        return;
      }

      tasks.forEach((task) => {
        const taskPromise = handler(task)
          .then((result) => {
            resolveTask(task, null, result);
          })
          .catch((err) => {
            resolveTask(task, err);
          });

        activeTasks.set(task.id, taskPromise);
      });
    },
    { loopInterval: poolInternvalInMs }
  );

  return {
    ...taskWorker,
    async stop() {
      await taskWorker.stop();
      await Promise.all(Array.from(activeTasks.values()));
      await resolveTaskBatcher.waitForAll();
    },
  };
};
