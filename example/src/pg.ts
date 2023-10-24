import { Pool } from 'pg';
import { withPG } from 'task-boss/lib/use/pg';
import { event, task, taskBoss, taskClient } from './boss';
import closeWithGrace from 'close-with-grace';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

async function main() {
  const sqlPool = new Pool({
    connectionString: connectionString,
    max: 5,
  });

  const boss = withPG(taskBoss, {
    db: sqlPool,
    schema: 'taskboss_example',
    retention_in_days: 1,
    worker: {
      intervalInMs: 150,
      concurrency: 100,
    },
  });

  await boss.start();

  const intervals: NodeJS.Timer[] = [];

  // emit event interval
  intervals.push(
    setInterval(() => {
      boss.publish(event.from({ duration: 3, event_d: 'event data' }));
    }, 10)
  );

  // emit task interval
  intervals.push(
    setInterval(() => {
      boss.send(task.from({ t: 'task' }));
    }, 50)
  );

  // emit task using taskclient
  intervals.push(
    setInterval(() => {
      boss.send(taskClient.t1.from({ a: 'aa' }));
      boss.send(taskClient.t2.from({ b: 'bb' }));
    }, 50)
  );

  const closeListeners = closeWithGrace({ delay: 3000 }, async ({ err }: any) => {
    if (err) {
      console.error(err);
    }

    intervals.forEach((i) => clearInterval(i));

    await boss.stop();

    console.log('task-boss stopped');

    closeListeners.uninstall();
  });
}

main();
