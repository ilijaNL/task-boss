import { Pool } from 'pg';
import tap from 'tap';
import { cleanupSchema, createRandomSchema } from './helpers';
import { createMigrationPlans, migrate, migrationTable } from '../../../src/use/pg/migrate';
import { createMigrationStore } from '../../../src/use/pg/migrations';
import { query } from '../../../src/use/pg/sql';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

tap.test('happy path', async ({ teardown, equal }) => {
  const schema = createRandomSchema();
  const pool = new Pool({
    connectionString: connectionString,
    max: 3,
  });

  const migrations = createMigrationStore(schema);

  const plans = createMigrationPlans(schema);

  await migrate(pool, schema, migrations);

  const hasMigrations = await query(pool, plans.tableExists(migrationTable));

  equal(hasMigrations[0]!.exists, true);

  teardown(async () => {
    await cleanupSchema(pool, schema);
    await pool.end();
  });
});

tap.test('concurrently migrate', async ({ teardown }) => {
  const schema = createRandomSchema();

  const p0 = new Pool({
    connectionString: connectionString,
    max: 3,
  });
  const p1 = new Pool({
    connectionString: connectionString,
    max: 3,
  });

  const migrations = createMigrationStore(schema);

  await Promise.all([
    migrate(p0, schema, migrations),
    migrate(p1, schema, migrations),
    migrate(p0, schema, migrations),
  ]);

  teardown(async () => {
    await cleanupSchema(p0, schema);
    await p0.end();
    await p1.end();
  });
});

tap.test('applies new migration', async ({ teardown, equal }) => {
  const schema = createRandomSchema();

  const pool = new Pool({
    connectionString: connectionString,
    max: 2,
  });

  const migrations = createMigrationStore(schema);

  await migrate(pool, schema, migrations);

  teardown(async () => {
    await cleanupSchema(pool, schema);
    await pool.end();
  });

  const allMigrations = await query(pool, createMigrationPlans(schema).getMigrations());

  const lastId = allMigrations[allMigrations.length - 1]!.id;

  const newMigration = `SELECT * FROM ${schema}.${migrationTable}`;
  const migrationStore = createMigrationStore(schema);

  await migrate(pool, schema, [...migrationStore, newMigration]);

  const newMigrations = await query(pool, createMigrationPlans(schema).getMigrations());

  equal(allMigrations.length + 1, newMigrations.length);

  const lastAppliedMig = newMigrations[newMigrations.length - 1]!;

  equal(lastAppliedMig.name, `${lastId + 1}_m`);
  equal(lastAppliedMig.id, lastId + 1);
});

tap.test('throws when migration is changed', async ({ teardown, rejects }) => {
  const schema = createRandomSchema();

  const pool = new Pool({
    connectionString: connectionString,
    max: 2,
  });

  const migrations = createMigrationStore(schema);

  await migrate(pool, schema, migrations);

  const migs = createMigrationStore(schema);
  migs[0] = migs[0] + `\n--- schema: ${schema}`;

  rejects(migrate(pool, schema, migs));

  teardown(async () => {
    await cleanupSchema(pool, schema);
    await pool.end();
  });
});
