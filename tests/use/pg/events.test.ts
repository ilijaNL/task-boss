import { Pool } from 'pg';
import { withPG } from '../../../src/use/pg';
import { cleanupSchema, createRandomSchema } from './helpers';
import t from 'tap';
import { createTaskBoss } from '../../../src';
import { defineEvent } from '../../../src/definitions';
import { Type } from '@sinclair/typebox';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

t.test('produces events in order', async (t) => {
  const schema = createRandomSchema();
  const sqlPool = new Pool({
    connectionString: connectionString,
    max: 10,
  });

  const client = createTaskBoss('queueA');
  const pg = withPG(client, {
    db: sqlPool,
    schema: schema,
  });

  await pg.start();

  t.teardown(() => pg.stop());

  t.teardown(async () => {
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  const eventCount = 200;

  const eventDef = defineEvent({
    event_name: 'test_e',
    schema: Type.Number(),
  });

  const events = new Array(eventCount).fill(null).map((_, idx) => eventDef.from(idx + 1));

  while (events.length) {
    await pg.publish(...events.splice(0, Math.floor(Math.random() * 5 + 5)));
  }

  const remoteEvents = await sqlPool.query(
    `SELECT events.id as id, event_data as d, pos as position FROM ${schema}.events ORDER BY pos ASC`
  );

  t.equal(remoteEvents.rowCount, eventCount);

  for (let i = 0; i < remoteEvents.rows.length; ++i) {
    t.equal(+remoteEvents.rows[i].position, remoteEvents.rows[i].d);
  }
});
