import { Pool, PoolClient } from 'pg';

export interface QueryResultRow {
  [column: string]: any;
}

const schemaRE = new RegExp('{{schema}}', 'g');

export type PGClient = {
  query: <T = any>(props: {
    text: string;
    values: any[];
    name?: string;
  }) => Promise<{
    rows: T[];
    rowCount: number;
  }>;
};

export type QueryCommand<Result> = {
  text: string;
  values: unknown[];
  // used to keep the type definition and is always undefined
  __result?: Result;
};

/**
 * Helper function to convert string literal to parameterized postgres query
 */
export function createSql(schema: string) {
  const cache = new WeakMap<ReadonlyArray<string>, string>();

  return function sql<Result extends QueryResultRow>(
    sqlFragments: ReadonlyArray<string>,
    ...parameters: unknown[]
  ): QueryCommand<Result> {
    let text: string;
    if (cache.has(sqlFragments)) {
      text = cache.get(sqlFragments)!;
    } else {
      const reduced: string = sqlFragments.reduce((prev, curr, i) => prev + '$' + i + curr);
      text = reduced.replace(schemaRE, schema);
      cache.set(sqlFragments, text);
    }

    const result = {
      text: text,
      values: parameters,
    };

    return result;
  };
}

export async function query<Result extends QueryResultRow>(client: PGClient, command: QueryCommand<Result>) {
  return client
    .query<Result>({
      text: command.text,
      values: command.values,
    })
    .then((d) => d.rows);
}

export async function withTransaction<T>(pool: Pool, handler: (client: PoolClient) => Promise<T>) {
  let client: PoolClient | null = await pool.connect();
  let result: T;
  try {
    await client.query('BEGIN');
    result = await handler(client);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
    client = null;
  }

  return result;
}
