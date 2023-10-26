import { migrationTable } from './migrate';
import { TASK_STATES } from './plans';

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
      "queue" text not null,
      "offset" bigint not null default 0,
      "locked" boolean not null default false,
      "expire_lock_at" timestamptz,
      "created_at" timestamptz NOT NULL DEFAULT now(), 
      PRIMARY KEY ("id"),
      UNIQUE ("queue")
    );

    CREATE INDEX ON ${schema}."cursors" (expire_lock_at) WHERE locked = true;
    
    CREATE TABLE ${schema}."events" (
      "id" BIGSERIAL PRIMARY KEY,
      "event_name" text NOT NULL,
      "event_data" jsonb NOT NULL,
      "pos" bigint not null default 0,
      "created_at" timestamptz NOT NULL DEFAULT now(),
      "expire_at" date not null default now() + interval '30 days'
    );

    CREATE INDEX ON ${schema}."events" (expire_at);
    CREATE INDEX ON ${schema}."events" (pos) WHERE pos > 0;
    
    CREATE SEQUENCE ${schema}.event_order as bigint start 1;
    
    -- to imrpove performance, dont update seperately
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
      id BIGSERIAL PRIMARY KEY,
      queue text not null,
      state smallint not null default(0),
      data jsonb not null,
      meta_data jsonb,
      config jsonb not null,
      retryCount smallint not null default 0,
      startedOn timestamp with time zone,
      createdOn timestamp with time zone not null default now(),
      startAfter timestamp with time zone not null default now(),
      expireIn interval not null default interval '2 minutes',
      "singleton_key" text default null,
      output jsonb
    ) -- https://www.cybertec-postgresql.com/en/what-is-fillfactor-and-how-does-it-affect-postgresql-performance/
    WITH (fillfactor=90);

    -- get tasks
    CREATE INDEX ON ${schema}."tasks" ("queue", startAfter) WHERE state < ${TASK_STATES.active};

    -- used for expiring tasks
    CREATE INDEX ON ${schema}."tasks" ("state") WHERE state = ${TASK_STATES.active};

    -- singleton task, which holds for scheduled, active, and retry state
    CREATE UNIQUE INDEX ON ${schema}."tasks" ("queue", "singleton_key") WHERE state < ${TASK_STATES.expired};

    CREATE TABLE ${schema}.tasks_completed (
      -- coming from the task table
      id bigint PRIMARY KEY,
      queue text not null,
      state smallint not null,
      data jsonb,
      meta_data jsonb,
      config jsonb,
      output jsonb,
      retryCount smallint not null default 0,
      startedOn timestamp with time zone,
      createdOn timestamp with time zone not null default now(),
      completedOn timestamp with time zone,
      keepUntil timestamp with time zone NOT NULL default now() + interval '14 days'
    );

    CREATE INDEX ON ${schema}."tasks_completed" (keepUntil);
  `,
  `
    CREATE OR REPLACE FUNCTION ${schema}.get_tasks(target_q text, amount integer)
      RETURNS TABLE (id bigint, retryCount smallint, state smallint, data jsonb, meta_data jsonb, config jsonb, expire_in_seconds integer) 
      AS $$
      BEGIN
        RETURN QUERY
        with _tasks as (
          SELECT
            _t.id as id
          FROM ${schema}.tasks _t
          WHERE _t.queue = target_q
            AND _t.startAfter < now()
            AND _t.state < ${TASK_STATES.active}
          ORDER BY _t.createdOn ASC
          LIMIT amount
          FOR UPDATE SKIP LOCKED
        ) UPDATE ${schema}.tasks t 
          SET
            state = ${TASK_STATES.active},
            startedOn = now(),
            retryCount = CASE WHEN t.state = ${TASK_STATES.retry}
                          THEN t.retryCount + 1 
                          ELSE t.retryCount END
          FROM _tasks
          WHERE t.id = _tasks.id
          RETURNING t.id, t.retryCount, t.state, t.data, t.meta_data, t.config, (EXTRACT(epoch FROM expireIn))::int as expire_in_seconds;
      END
    $$ LANGUAGE 'plpgsql';

    CREATE OR REPLACE FUNCTION ${schema}.create_bus_tasks(tasks jsonb)
      RETURNS void AS $$
      BEGIN
        INSERT INTO ${schema}.tasks (
          "queue",
          "state",
          "data",
          "meta_data",
          "config",
          "singleton_key",
          startAfter,
          expireIn
        )
        SELECT
          "q" as "queue",
          COALESCE("s", ${TASK_STATES.created}) as "state",
          "d" as "data",
          "md" as meta_data,
          "cf" as config,
          "skey" as singleton_key,
          (now() + ("saf" * interval '1s'))::timestamptz as startAfter,
          "eis" * interval '1s' as expireIn
        FROM jsonb_to_recordset(tasks) as x(
          "q" text,
          "s" smallint,
          "d" jsonb,
          "md" jsonb,
          "cf" jsonb,
          "skey" text,
          "saf" integer,
          "eis" integer
        )
        ON CONFLICT DO NOTHING;
        RETURN;
      END
    $$ LANGUAGE 'plpgsql' VOLATILE;  

    CREATE OR REPLACE FUNCTION ${schema}.create_bus_events(events jsonb)
    RETURNS void
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
    $$ LANGUAGE 'plpgsql' VOLATILE;

    CREATE OR REPLACE FUNCTION ${schema}.resolve_tasks(tasks jsonb)
      RETURNS void
      AS $$
    BEGIN
      WITH _in as (
        SELECT 
          x.id as task_id,
          x.s as new_state,
          x.out as out,
          x.saf as saf
        FROM jsonb_to_recordset(tasks) as x(
          id bigint,
          s smallint,
          out jsonb,
          saf integer
        )
      ), compl as (
        DELETE FROM ${schema}.tasks t
        WHERE t.id in (SELECT task_id FROM _in WHERE _in.new_state > ${TASK_STATES.active})
          AND t.state = ${TASK_STATES.active}
        RETURNING t.*
      ), _completed_tasks as (
        INSERT INTO ${schema}.tasks_completed (
          id,
          queue,
          state,
          data,
          meta_data,
          config,
          output,
          retryCount,
          startedOn,
          createdOn,
          completedOn,
          keepUntil
        ) (SELECT 
          compl.id,
          compl.queue,
          _in.new_state,
          compl.data,
          compl.meta_data,
          compl.config,
          COALESCE(_in.out, compl.output),
          compl.retryCount,
          compl.startedOn,
          compl.createdOn,
          now(),
          (now() + (COALESCE((compl.config->>'ki_s')::integer, 60 * 60 * 24 * 7) * interval '1s'))
          FROM compl INNER JOIN _in ON _in.task_id = compl.id)
          RETURNING id
      ) UPDATE ${schema}.tasks t
        SET
          state = _in.new_state,
          startAfter = (now() + (_in.saf * interval '1s')),
          output = _in.out
        FROM _in
        WHERE t.id = _in.task_id
          AND _in.new_state = ${TASK_STATES.retry}
          AND t.state = ${TASK_STATES.active};

      RETURN;
    END;
    $$ LANGUAGE 'plpgsql' VOLATILE;
  `,
];
