import crypto from 'crypto';
import { createSql, query, withTransaction } from './sql';
import { Pool } from 'pg';

const migrationTable = 'tb_migrations';

export const createMigrationStore = (schema: string) => [
  `
    CREATE SCHEMA IF NOT EXISTS ${schema};

    CREATE TABLE IF NOT EXISTS ${schema}."${migrationTable}" (
      id integer PRIMARY KEY,
      name varchar(100) UNIQUE NOT NULL,
      -- sha1 hex encoded hash of the file name and contents, to ensure it hasn't been altered since applying the migration
      hash varchar(40) NOT NULL,
      "created_at" timestamptz NOT NULL DEFAULT now()
    );
  `,
  `
    CREATE TABLE ${schema}."cursors" (
      "id" uuid NOT NULL DEFAULT gen_random_uuid(),
      "svc" text not null,
      "l_p" bigint not null default 0,
      "created_at" timestamptz NOT NULL DEFAULT now(), 
      PRIMARY KEY ("id"),
      UNIQUE ("svc")
    );
    
    CREATE TABLE ${schema}."events" (
      "id" uuid NOT NULL DEFAULT gen_random_uuid(),
      "event_name" text NOT NULL,
      "event_data" jsonb NOT NULL,
      "pos" bigint not null default 0,
      "created_at" timestamptz NOT NULL DEFAULT now(), 
      PRIMARY KEY ("id") 
    );
    
    CREATE INDEX idx_events_pos ON ${schema}."events" (pos) WHERE pos > 0;
    
    CREATE SEQUENCE ${schema}.event_order as bigint start 1;
    
    CREATE FUNCTION ${schema}.proc_set_position()
        RETURNS TRIGGER
        LANGUAGE plpgsql
    AS
    $$
    BEGIN
        PERFORM pg_advisory_xact_lock(1723683380);
        update ${schema}."events" set pos = NEXTVAL('${schema}.event_order') where id = new.id;
        RETURN NULL;
    END;
    $$;
    
    CREATE CONSTRAINT TRIGGER set_commit_order
        AFTER INSERT ON ${schema}."events"
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
    EXECUTE PROCEDURE ${schema}.proc_set_position();
  `,
  `
    CREATE TABLE ${schema}.tasks (
      id uuid primary key not null default gen_random_uuid(),
      queue text not null,
      data jsonb,
      state smallint not null default(0),
      retryLimit integer not null default(0),
      retryCount integer not null default(0),
      retryDelay integer not null default(0),
      retryBackoff boolean not null default false,
      startAfter timestamp with time zone not null default now(),
      startedOn timestamp with time zone,
      expireIn interval not null default interval '2 minutes',
      createdOn timestamp with time zone not null default now(),
      completedOn timestamp with time zone,
      keepUntil timestamp with time zone NOT NULL default now() + interval '14 days',
      output jsonb
    ) -- https://www.cybertec-postgresql.com/en/what-is-fillfactor-and-how-does-it-affect-postgresql-performance/
    WITH (fillfactor=90);

    CREATE INDEX idx_get_tasks ON ${schema}."tasks" ("queue", startAfter) WHERE state < 2;
    CREATE INDEX idx_expire_tasks ON ${schema}."tasks" ("state") WHERE state = 2;
    CREATE INDEX idx_purge_tasks ON ${schema}."tasks" (keepUntil) WHERE state >= 3;
  `,
  `
    ALTER TABLE  ${schema}."tasks" ADD COLUMN "singleton_key" text default null;
    -- 0: create, 1: retry, 2: active, 3 >= all completed/failed
    CREATE UNIQUE INDEX idx_unique_queue_task ON ${schema}."tasks" ("queue", "singleton_key") WHERE state < 3;
  `,
  `
    ALTER TABLE ${schema}.tasks DROP COLUMN "id";
    ALTER TABLE ${schema}.events DROP COLUMN "id";
    
    ALTER TABLE ${schema}.events ADD COLUMN id BIGSERIAL PRIMARY KEY;
    ALTER TABLE ${schema}.tasks ADD COLUMN id BIGSERIAL PRIMARY KEY;
    
    CREATE OR REPLACE FUNCTION ${schema}.create_bus_tasks(tasks jsonb)
      RETURNS SETOF ${schema}.tasks
      AS $$
    BEGIN
      INSERT INTO ${schema}.tasks (
        "queue",
        "data",
        "state",
        retryLimit,
        retryDelay,
        retryBackoff,
        singleton_key,
        startAfter,
        expireIn,
        keepUntil
      )
      SELECT
        "q" as "queue",
        "d" as "data",
        COALESCE("s", 0) as "state",
        "r_l" as "retryLimit",
        "r_d" as "retryDelay",
        "r_b" as "retryBackoff",
        "skey" as singleton_key,
        (now() + ("saf" * interval '1s'))::timestamptz as startAfter,
        "eis" * interval '1s' as expireIn,
        (now() + ("saf" * interval '1s') + ("kis" * interval '1s'))::timestamptz as keepUntil
      FROM jsonb_to_recordset(tasks) as x(
        "q" text,
        "d" jsonb,
        "s" smallint,
        "r_l" integer,
        "r_d" integer,
        "r_b" boolean,
        "saf" integer,
        "eis" integer,
        "kis" integer,
        "skey" text
      )
      ON CONFLICT DO NOTHING;
      RETURN;
    END;
    $$ LANGUAGE 'plpgsql';  
  `,
  `
    ALTER TABLE ${schema}.events ADD COLUMN expire_at date not null default now() + interval '30 days';

    CREATE INDEX idx_events_expire_at ON ${schema}."events" (expire_at);
    
    CREATE OR REPLACE FUNCTION ${schema}.create_bus_events(events jsonb)
      RETURNS SETOF ${schema}.events
      AS $$
    BEGIN
      INSERT INTO ${schema}.events (
        event_name,
        event_data,
        expire_at
      ) 
      SELECT
        e_n,
        d as event_data,
        (now()::date + COALESCE("rid", 30) * interval '1 day') as expire_at
      FROM jsonb_to_recordset(events) as x(
        e_n text,
        d jsonb,
        rid int
      );
      RETURN;
    END;
    $$ LANGUAGE 'plpgsql';
  `,
];

const hashString = (s: string) => crypto.createHash('sha1').update(s, 'utf8').digest('hex');

type Migration = {
  id: number;
  name: string;
  hash: string;
  sql: string;
};

const loadMigrations = (items: string[]): Array<Migration> => {
  return items.map((sql, idx) => ({
    hash: hashString(sql),
    id: idx,
    name: `${idx}_m`,
    sql: sql,
  }));
};

function filterMigrations(migrations: Array<Migration>, appliedMigrations: Set<number>) {
  const notAppliedMigration = (migration: Migration) => !appliedMigrations.has(migration.id);

  return migrations.filter(notAppliedMigration);
}

function validateMigrationHashes(
  migrations: Array<Migration>,
  appliedMigrations: Array<{
    id: number;
    name: string;
    hash: string;
  }>
) {
  const invalidHash = (migration: Migration) => {
    const appliedMigration = appliedMigrations.find((m) => m.id === migration.id);
    return !!appliedMigration && appliedMigration.hash !== migration.hash;
  };

  // Assert migration hashes are still same
  const invalidHashes = migrations.filter(invalidHash);
  if (invalidHashes.length > 0) {
    // Someone has altered one or more migrations which has already run - gasp!
    const invalidIdx = invalidHashes.map(({ id }) => id.toString());
    throw new Error(`Hashes don't match for migrations id's '${invalidIdx.join(',')}'.
This means that the createMigrationStore items have changed since it was applied. You only allow to append new migrations`);
  }
}

export const createMigrationPlans = (schema: string) => {
  const sql = createSql(schema);

  function getMigrations() {
    return sql<{ id: number; name: string; hash: string }>`
      SELECT * FROM {{schema}}.tb_migrations ORDER BY id
    `;
  }

  function insertMigration(migration: { id: number; hash: string; name: string }) {
    return sql`
      INSERT INTO 
        {{schema}}.tb_migrations (id, name, hash) 
      VALUES (${migration.id}, ${migration.name}, ${migration.hash})
    `;
  }

  function tableExists(table: string) {
    return sql<{ exists: boolean }>`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE  table_schema = '{{schema}}'
        AND    table_name   = ${table}
      );  
    `;
  }

  return {
    tableExists,
    getMigrations,
    insertMigration,
  };
};

export async function migrate(pool: Pool, schema: string, migrations?: string[]) {
  const allMigrations = migrations ? loadMigrations(migrations) : loadMigrations(createMigrationStore(schema));
  let toApply = [...allMigrations];
  // check if table exists
  const plans = createMigrationPlans(schema);

  await withTransaction(pool, async (client) => {
    // acquire lock
    await client.query(`
      SELECT pg_advisory_xact_lock( ('x' || md5(current_database() || '.tb.${schema}'))::bit(64)::bigint )
    `);

    const rows = await query(client, plans.tableExists(migrationTable));
    const migTableExists = rows[0]?.exists;

    // fetch latest migration
    if (migTableExists) {
      const appliedMigrations = await query(client, plans.getMigrations());
      validateMigrationHashes(allMigrations, appliedMigrations);
      toApply = filterMigrations(allMigrations, new Set(appliedMigrations.map((m) => m.id)));
    }

    for (const migration of toApply) {
      await client.query(migration.sql);
      await query(client, plans.insertMigration(migration));
    }
  });
}
