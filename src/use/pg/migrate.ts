import crypto from 'node:crypto';
import { createSql, query, withTransaction } from './sql';
import { Pool } from 'pg';

/**
 * Changing this requires to change the sql plans for migrations (See below)
 */
export const migrationTable = '_tb_migrations';

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
      SELECT * FROM {{schema}}._tb_migrations ORDER BY id
    `;
  }

  function insertMigration(migration: { id: number; hash: string; name: string }) {
    return sql`
      INSERT INTO 
        {{schema}}._tb_migrations (id, name, hash) 
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

export async function migrate(pool: Pool, schema: string, migrations: string[]) {
  const allMigrations = loadMigrations(migrations);
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
