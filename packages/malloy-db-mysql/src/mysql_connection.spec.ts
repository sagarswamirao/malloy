/*
 * Copyright Contributors to the Malloy project
 * SPDX-License-Identifier: MIT
 */

import {MySQLConnection, MySQLExecutor} from '.';
import {createTestRuntime, mkTestModel} from '@malloydata/malloy/test';
import '@malloydata/malloy/test/matchers';
import crypto from 'crypto';

const config = MySQLExecutor.getConnectionOptionsFromEnv();
const hasCredentials = !!config.user;

const describeMySQL = hasCredentials ? describe : describe.skip;

describeMySQL('db:MySQL', () => {
  const connection = new MySQLConnection('mysql', config, {});

  afterAll(async () => {
    await connection.close();
  });

  it('runs a SQL query', async () => {
    const res = await connection.runSQL('SELECT 1 as t');
    expect(res.rows[0]['t']).toBe(1);
  });

  it('fetches schema for SQL block', async () => {
    const res = await connection.fetchSchemaForSQLStruct(
      {
        selectStr: 'SELECT 1 as one',
        connection: 'mysql',
      },
      {}
    );
    expect(res.structDef?.fields[0].name).toBe('one');
  });

  it('maps integer types correctly', async () => {
    const res = await connection.fetchSchemaForSQLStruct(
      {
        selectStr: `
          SELECT
            CAST(1 AS SIGNED) as signed_int,
            CAST(2 AS UNSIGNED) as unsigned_int
        `,
        connection: 'mysql',
      },
      {}
    );
    expect(res.structDef?.fields[0]).toEqual({
      name: 'signed_int',
      type: 'number',
      numberType: 'bigint',
    });
  });

  it('fetches schema for tables whose names contain dashes', async () => {
    // fetchSchemaForTables expects canonical SQL (post-translator), so
    // we pass the backtick-quoted form directly.
    await connection.runRawSQL('DROP TABLE IF EXISTS `arrests-latest`');
    await connection.runRawSQL(
      'CREATE TABLE `arrests-latest` (id INT, name VARCHAR(50))'
    );
    try {
      const res = await connection.fetchSchemaForTables(
        {dashed: '`arrests-latest`'},
        {}
      );
      expect(res.errors).toEqual({});
      const fields = res.schemas['dashed']?.fields ?? [];
      expect(fields.map(f => f.name).sort()).toEqual(['id', 'name']);
    } finally {
      await connection.runRawSQL('DROP TABLE `arrests-latest`');
    }
  });

  // The unit spec (packages/malloy/src/dialect/mysql/mysql_types.spec.ts)
  // pins what each reported spelling means. This pins that these are the
  // spellings a live server reports -- including the ones MySQL rewrites on
  // the way in, which is why the declared and reported columns differ here.
  //
  // Declared columns are the point, so this is the one case a SELECT cannot
  // stand in for. The table is TEMPORARY, which scopes it to this connection
  // and leaves parallel workers alone.
  it('reads the declared types a live server reports', async () => {
    await connection.runRawSQL('DROP TEMPORARY TABLE IF EXISTS type_survey');
    await connection.runRawSQL(`CREATE TEMPORARY TABLE type_survey (
      c_tinyint    TINYINT,
      c_boolean    BOOLEAN,
      c_tinyint_u  TINYINT UNSIGNED,
      c_int_u      INT UNSIGNED,
      c_bigint_u   BIGINT UNSIGNED,
      c_serial     SERIAL,
      c_float      FLOAT,
      c_real       REAL,
      c_dec_scaled DECIMAL(10,2),
      c_dec_small  DECIMAL(15,0),
      c_dec_big    DECIMAL(16,0),
      c_longtext   LONGTEXT,
      c_mediumtext MEDIUMTEXT,
      c_tinytext   TINYTEXT,
      c_varbinary  VARBINARY(20),
      c_enum       ENUM('a','b'),
      c_year       YEAR
    )`);
    try {
      const res = await connection.fetchSchemaForTables({t: 'type_survey'}, {});
      expect(res.errors).toEqual({});
      const byName = Object.fromEntries(
        (res.schemas['t']?.fields ?? []).map(f => [f.name, f])
      );
      const num = (numberType: string) => ({type: 'number', numberType});
      expect(byName).toMatchObject({
        // TINYINT is not a boolean: BOOLEAN is only a spelling of TINYINT(1),
        // the (1) is a display width, and the column accepts 42.
        'c_tinyint': num('integer'),
        'c_boolean': num('integer'),
        // `unsigned` never changes the Malloy type.
        'c_tinyint_u': num('integer'),
        'c_int_u': num('integer'),
        'c_bigint_u': num('bigint'),
        'c_serial': num('bigint'),
        // REAL is reported as double; FLOAT is its own spelling.
        'c_float': num('float'),
        'c_real': num('float'),
        // Scale decides float vs exact; precision decides whether an exact
        // value survives a JS double.
        'c_dec_scaled': num('float'),
        'c_dec_small': num('integer'),
        'c_dec_big': num('bigint'),
        'c_longtext': {type: 'string'},
        'c_mediumtext': {type: 'string'},
        'c_tinytext': {type: 'string'},
        // Deliberately opaque.
        'c_varbinary': {type: 'sql native', rawType: 'varbinary'},
        'c_enum': {type: 'sql native', rawType: 'enum'},
        'c_year': {type: 'sql native', rawType: 'year'},
      });
    } finally {
      await connection.runRawSQL('DROP TEMPORARY TABLE type_survey');
    }
  });
});

/**
 * Tests for reading numeric values through Malloy queries
 */
describeMySQL('numeric value reading', () => {
  const connection = new MySQLConnection('mysql_numeric_tests', config, {});
  const runtime = createTestRuntime(connection);
  const testModel = mkTestModel(runtime, {});

  afterAll(async () => {
    await connection.close();
  });

  const half = BigInt('9007199254740993'); // 2^53 + 1

  describe('integer types', () => {
    // MySQL infers int for values <= 2^31-1, bigint for larger
    it('reads int correctly', async () => {
      await expect('run: mysql.sql("SELECT 2147483647 as d")').toMatchResult(
        testModel,
        {d: 2147483647}
      );
    });

    it('reads bigint correctly', async () => {
      await expect('run: mysql.sql("SELECT 2147483648 as d")').toMatchResult(
        testModel,
        {d: 2147483648}
      );
    });

    it('preserves precision for literal integers > 2^53', async () => {
      const largeInt = BigInt('9007199254740993'); // 2^53 + 1
      await expect(`
        run: mysql.sql("select 1") -> { select: d is ${largeInt} }
      `).toMatchResult(testModel, {d: largeInt});
    });

    // MySQL returns SUM() of an integer column as a DECIMAL, so a sum is not
    // covered by supportBigNumbers the way the column itself is.
    it('preserves precision when summing above 2^53', async () => {
      await expect(`
        run: mysql.sql("""
          SELECT CAST(${half} AS SIGNED) as v
          UNION ALL SELECT CAST(${half} AS SIGNED)
        """) -> { aggregate: s is v.sum() }
      `).toMatchResult(testModel, {s: half * BigInt(2)});
    });

    // A sum across a join_many is computed symmetrically, in a fixed-point
    // domain wide enough to hold the join key's hash -- DECIMAL(55,10). Its
    // result therefore carries a ten-place fraction that BigInt() rejects
    // unless the driver strips it.
    it('preserves precision when summing above 2^53 across a join', async () => {
      await expect(`
        run: mysql.sql("""
          SELECT 1 as id, CAST(${half} AS SIGNED) as v
          UNION ALL SELECT 2, CAST(${half} AS SIGNED)
        """) extend {
          join_many: kid is mysql.sql("""
            SELECT 1 as parent_id
            UNION ALL SELECT 1
            UNION ALL SELECT 1
            UNION ALL SELECT 2
            UNION ALL SELECT 2
          """) on id = kid.parent_id
        } -> {
          aggregate: s is v.sum()
          // Referencing the join is what keeps it from being elided, which is
          // what makes the sum symmetric.
          aggregate: kids is kid.count()
        }
      `).toMatchResult(testModel, {s: half * BigInt(2), kids: 5});
    });
  });

  describe('float types', () => {
    it.each(['FLOAT', 'DOUBLE', 'DECIMAL(10,2)'])(
      'reads %s correctly',
      async sqlType => {
        await expect(
          `run: mysql.sql("SELECT CAST(10.5 AS ${sqlType}) as f")`
        ).toMatchResult(testModel, {f: 10.5});
      }
    );
  });
});

describeMySQL('session lifecycle', () => {
  const admin = new MySQLConnection('mysql_lifecycle_admin', config);
  // The users created below need a database to be granted on and to connect
  // to; CI names the fixture database in MYSQL_DATABASE.
  const database = config.database ?? 'malloytest';
  const users: string[] = [];
  const connections: MySQLConnection[] = [];

  const track = (connection: MySQLConnection) => {
    connections.push(connection);
    return connection;
  };

  function numberIn(
    row: {[column: string]: unknown} | undefined,
    column: string,
    sql: string
  ): number {
    const value = row?.[column];
    if (typeof value !== 'number') {
      throw new Error(
        `Expected a numeric ${column}, got ${JSON.stringify(row)} from: ${sql}`
      );
    }
    return value;
  }

  async function adminCount(sql: string): Promise<number> {
    const {rows} = await admin.runRawSQL(sql);
    return numberIn(rows[0], 'n', sql);
  }

  async function connectionId(connection: MySQLConnection): Promise<number> {
    const sql = 'SELECT CONNECTION_ID() AS id';
    const {rows} = await connection.runSQL(sql);
    return numberIn(rows[0], 'id', sql);
  }

  // The server takes an ended session off the processlist asynchronously,
  // so poll for the count to reach zero rather than reading it once.
  async function settledCount(sql: string, timeoutMs = 5000): Promise<number> {
    const deadline = Date.now() + timeoutMs;
    let n = await adminCount(sql);
    while (n > 0 && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 50));
      n = await adminCount(sql);
    }
    return n;
  }

  const sessionsWithId = (id: number) =>
    `SELECT COUNT(*) AS n FROM performance_schema.processlist WHERE ID = ${id}`;
  const sessionsOf = (user: string) =>
    `SELECT COUNT(*) AS n FROM performance_schema.processlist WHERE USER = '${user}'`;

  // A user of its own lets the server's views attribute sessions, temporary
  // tables and statement totals to one MySQLConnection, whatever else is
  // running against the same server.
  async function connectAsNewUser(privileges: string) {
    const user = `malloy_test_${crypto.randomBytes(4).toString('hex')}`;
    const password = crypto.randomBytes(16).toString('hex');
    await admin.runRawSQL(
      `CREATE USER '${user}'@'%' IDENTIFIED BY '${password}'`
    );
    users.push(user);
    await admin.runRawSQL(
      `GRANT ${privileges} ON \`${database}\`.* TO '${user}'@'%'`
    );
    const connection = track(
      new MySQLConnection('mysql', {...config, user, password, database})
    );
    return {connection, user};
  }

  async function killSessionsOf(user: string) {
    const list = `SELECT ID AS id FROM performance_schema.processlist WHERE USER = '${user}'`;
    const {rows} = await admin.runRawSQL(list);
    for (const row of rows) {
      try {
        await admin.runRawSQL(`KILL ${numberIn(row, 'id', list)}`);
      } catch (e) {
        // The session can end on its own between the listing and the KILL.
        if (!(e instanceof Error && e.message.includes('Unknown thread id'))) {
          throw e;
        }
      }
    }
  }

  afterAll(async () => {
    await Promise.all(connections.map(c => c.close()));
    for (const user of users) {
      await killSessionsOf(user);
      await admin.runRawSQL(`DROP USER IF EXISTS '${user}'@'%'`);
    }
    await admin.close();
  });

  it('answers a query after the server kills its session', async () => {
    const connection = track(new MySQLConnection('mysql', config));
    const killed = await connectionId(connection);
    await admin.runRawSQL(`KILL ${killed}`);
    // KILL returns before the session has ended. Once it has, the next query
    // cannot be answered by it.
    expect(await settledCount(sessionsWithId(killed))).toBe(0);
    expect(await connectionId(connection)).not.toBe(killed);
  });

  it('answers a query after the server closes its idle session', async () => {
    const connection = track(
      new MySQLConnection('mysql', {
        ...config,
        setupSQL: 'SET SESSION wait_timeout = 1',
      })
    );
    const closed = await connectionId(connection);
    // The deadline covers the second of idleness the server waits out first.
    expect(await settledCount(sessionsWithId(closed), 10000)).toBe(0);
    expect(await connectionId(connection)).not.toBe(closed);
  });

  it('leaves no session open after close() when first used concurrently', async () => {
    const {connection, user} = await connectAsNewUser('SELECT');
    await Promise.all(
      Array.from({length: 5}, () => connection.runSQL('SELECT 1 AS one'))
    );
    await connection.close();
    expect(await settledCount(sessionsOf(user))).toBe(0);
  });

  describe('fetchSelectSchema', () => {
    const digits = Array.from({length: 10}, (_, d) => `SELECT ${d} AS d`).join(
      ' UNION ALL '
    );
    const thousandRows = `SELECT a.d * 100 + b.d * 10 + c.d AS n FROM (${digits}) a CROSS JOIN (${digits}) b CROSS JOIN (${digits}) c`;

    it("writes none of the query's rows to read its schema", async () => {
      const {connection, user} = await connectAsNewUser(
        'SELECT, CREATE TEMPORARY TABLES'
      );
      const schema = await connection.fetchSelectSchema({
        connection: 'mysql',
        selectStr: thousandRows,
      });
      expect(schema.fields.map(f => f.name)).toEqual(['n']);
      // performance_schema totals each user's statements, rows stored into a
      // temporary table included, across every session the user opened.
      const totals = `SELECT CAST(SUM(COUNT_STAR) AS SIGNED) AS statements, CAST(SUM(SUM_ROWS_AFFECTED) AS SIGNED) AS rows_written FROM performance_schema.events_statements_summary_by_user_by_event_name WHERE USER = '${user}'`;
      const {rows} = await admin.runRawSQL(totals);
      // A zero for rows written is a measurement only if the user's
      // statements were recorded at all.
      expect(numberIn(rows[0], 'statements', totals)).toBeGreaterThan(0);
      expect(numberIn(rows[0], 'rows_written', totals)).toBe(0);
    });

    it('leaves no temporary table behind on its session', async () => {
      const {connection, user} = await connectAsNewUser(
        'SELECT, CREATE TEMPORARY TABLES'
      );
      const temporaryTables = `SELECT COUNT(*) AS n FROM information_schema.INNODB_TEMP_TABLE_INFO t JOIN information_schema.INNODB_SESSION_TEMP_TABLESPACES s ON t.SPACE = s.SPACE JOIN performance_schema.processlist p ON p.ID = s.ID WHERE p.USER = '${user}'`;
      // A table the test made itself shows the count can see this user's
      // temporary tables, so an unchanged count is not an empty view.
      await connection.runSQL('CREATE TEMPORARY TABLE witness (x INT)');
      expect(await adminCount(temporaryTables)).toBe(1);
      await connection.fetchSelectSchema({
        connection: 'mysql',
        selectStr: thousandRows,
      });
      expect(await adminCount(temporaryTables)).toBe(1);
    });

    it("reads a query's schema as a user who may only SELECT", async () => {
      const {connection} = await connectAsNewUser('SELECT');
      const selectStr = 'SELECT 1 AS one';
      expect((await connection.runSQL(selectStr)).rows).toEqual([{one: 1}]);
      const schema = await connection.fetchSelectSchema({
        connection: 'mysql',
        selectStr,
      });
      expect(schema.fields).toMatchObject([{name: 'one', type: 'number'}]);
    });
  });
});
