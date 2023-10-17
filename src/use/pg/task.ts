import { createBaseWorker } from '../../worker';
import { ResolvedTask, SelectTask, TASK_STATES, getStartAfter } from './plans';
import { mapCompletionDataArg } from '../../utils';

function failTask(task: SelectTask) {
  const newState = task.retrycount >= task.config.r_l ? TASK_STATES.failed : TASK_STATES.retry;
  const startAfter = newState === TASK_STATES.retry ? getStartAfter(task) : undefined;

  return {
    state: newState,
    startAfter,
  };
}

export const createTaskWorker = (props: {
  resolveTask: (task: ResolvedTask) => Promise<void>;
  popTasks: (amount: number) => Promise<SelectTask[]>;
  handler: (event: SelectTask) => Promise<any>;
  maxConcurrency: number;
  poolInternvalInMs: number;
  refillThresholdPct: number;
}) => {
  const activeTasks = new Map<string, Promise<any>>();
  const { maxConcurrency, handler, poolInternvalInMs, refillThresholdPct, popTasks } = props;
  // used to determine if we can refetch early
  let hasMoreTasks = false;

  async function resolveTask(task: SelectTask, err: any, result?: any) {
    const t = err === null ? { state: TASK_STATES.completed, startAfter: undefined } : failTask(task);

    await props.resolveTask({ out: mapCompletionDataArg(err ?? result), s: t.state, id: task.id, saf: t.startAfter });

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
      const tasks = await popTasks(requestedAmount);

      // high chance that there are more tasks when requested amount is same as fetched
      hasMoreTasks = tasks.length === requestedAmount;

      if (tasks.length === 0) {
        return;
      }

      tasks.forEach((task) => {
        const taskPromise = handler(task)
          .then((result) => {
            return resolveTask(task, null, result);
          })
          .catch((err) => {
            return resolveTask(task, err);
          });

        activeTasks.set(task.id, taskPromise);
      });
    },
    { loopInterval: poolInternvalInMs }
  );

  return {
    ...taskWorker,
    get activeTasks() {
      return activeTasks.size;
    },
    async stop() {
      await taskWorker.stop();
      await Promise.all(Array.from(activeTasks.values()));
    },
  };
};
