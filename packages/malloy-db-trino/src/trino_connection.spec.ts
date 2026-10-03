/*
 * Copyright Contributors to the Malloy project
 * SPDX-License-Identifier: MIT
 */

import type {
  AtomicTypeDef,
  FieldDef,
  MalloyQueryData,
  SQLSourceDef,
  StructDef,
} from '@malloydata/malloy';
import {TrinoDialect} from '@malloydata/malloy';
import {PrestoConnection, TrinoConnection, TrinoExecutor} from '.';
import {TrinoPrestoConnection} from './trino_connection';
import type {BaseRunner} from './trino_connection';

// array(varchar) is array
const ARRAY_SCHEMA = 'array(integer)';

// row(...) is inline
const INLINE_SCHEMA = 'row(a double, b integer, c varchar(60))';

// array(row(....)) is nested
const NESTED_SCHEMA = 'array(row(a double, b integer, c varchar(60)))';

// array(row(..., array(row(....)))) is deeply nested
const DEEP_SCHEMA =
  'array(row(a double, b array(row(c integer, d varchar(60)))))';

const intType: AtomicTypeDef = {type: 'number', numberType: 'integer'};
const doubleType: AtomicTypeDef = {type: 'number', numberType: 'float'};
const stringType: AtomicTypeDef = {type: 'string'};
const recordSchema: FieldDef[] = [
  {name: 'a', ...doubleType},
  {name: 'b', ...intType},
  {name: 'c', ...stringType},
];

describe('Trino connection', () => {
  let connection: TrinoConnection;

  beforeAll(() => {
    connection = new TrinoConnection(
      'trino',
      {},
      TrinoExecutor.getConnectionOptionsFromEnv('trino')
    );
  });

  afterAll(() => {
    connection.close();
  });

  describe('schema parser', () => {
    it('parses arrays', () => {
      expect(connection.malloyTypeFromTrinoType(ARRAY_SCHEMA)).toEqual({
        type: 'array',
        elementTypeDef: intType,
      });
    });

    it('parses inline', () => {
      expect(connection.malloyTypeFromTrinoType(INLINE_SCHEMA)).toEqual({
        'type': 'record',
        'fields': recordSchema,
      });
    });

    it('parses nested', () => {
      expect(connection.malloyTypeFromTrinoType(NESTED_SCHEMA)).toEqual({
        'type': 'array',
        'elementTypeDef': {type: 'record_element'},
        'fields': recordSchema,
      });
    });

    it('parses a simple type', () => {
      expect(connection.malloyTypeFromTrinoType('varchar(60)')).toEqual(
        stringType
      );
    });

    it('parses a decimal integer type', () => {
      expect(connection.malloyTypeFromTrinoType('decimal(10)')).toEqual({
        type: 'number',
        numberType: 'integer',
      });
    });

    it('parses a decimal float type', () => {
      expect(connection.malloyTypeFromTrinoType('decimal(10,10)')).toEqual({
        type: 'number',
        numberType: 'float',
      });
    });

    it('parses row with timestamp(3)', () => {
      expect(
        connection.malloyTypeFromTrinoType('row(la_time timestamp(3))')
      ).toEqual({
        type: 'record',
        fields: [{name: 'la_time', type: 'timestamp'}],
      });
    });

    it('parses timestamp with time zone', () => {
      expect(
        connection.malloyTypeFromTrinoType('timestamp(3) with time zone)')
      ).toEqual({type: 'timestamptz'});
    });

    it('parses deep nesting', () => {
      expect(connection.malloyTypeFromTrinoType(DEEP_SCHEMA)).toEqual({
        'type': 'array',
        'elementTypeDef': {type: 'record_element'},
        'fields': [
          {'name': 'a', ...doubleType},
          {
            'name': 'b',
            'type': 'array',
            'elementTypeDef': {type: 'record_element'},
            'join': 'many',
            'fields': [
              {'name': 'c', ...intType},
              {'name': 'd', ...stringType},
            ],
          },
        ],
      });
    });

    describe('integer type mappings', () => {
      it('maps integer to integer', () => {
        expect(connection.malloyTypeFromTrinoType('integer')).toEqual({
          type: 'number',
          numberType: 'integer',
        });
      });

      it('maps smallint to integer', () => {
        expect(connection.malloyTypeFromTrinoType('smallint')).toEqual({
          type: 'number',
          numberType: 'integer',
        });
      });

      it('maps tinyint to integer', () => {
        expect(connection.malloyTypeFromTrinoType('tinyint')).toEqual({
          type: 'number',
          numberType: 'integer',
        });
      });

      it('maps bigint to bigint', () => {
        expect(connection.malloyTypeFromTrinoType('bigint')).toEqual({
          type: 'number',
          numberType: 'bigint',
        });
      });
    });
  });

  // Without TRINO_SERVER the connection falls back to localhost:8080, which in
  // the presto CI job (it runs this file too) is a Presto server.
  const describeWithTrino = TrinoExecutor.getConnectionOptionsFromEnv('trino')
    ? describe
    : describe.skip;

  describeWithTrino('a query that fails after rows have been returned', () => {
    // Trino stops producing output once about 64MB of it (the default 32MB
    // task output buffer plus the 32MB exchange buffer) is waiting for the
    // client. The rows come out in x order with 1000 bytes of padding each,
    // so the division by zero at x = 128000 cannot run until the client has
    // been sent pages holding tens of thousands of the rows before it.
    const failsAtRow128000 = `
      SELECT
        x,
        1 / (x - 128000) AS divided,
        rpad(CAST(x AS varchar), 1000, '.') AS padding
      FROM UNNEST(sequence(0, 999)) AS a(i)
      CROSS JOIN UNNEST(sequence(i * 1000, i * 1000 + 999)) AS b(x)`;

    // Reduces a resolved result to its row count, so that a failure message
    // does not print every row.
    async function outcome(query: Promise<MalloyQueryData>): Promise<string> {
      try {
        const result = await query;
        return `resolved with ${result.rows.length} rows`;
      } catch (error) {
        return error instanceof Error ? error.message : String(error);
      }
    }

    it('rejects with the query error', async () => {
      expect(await outcome(connection.runSQL(failsAtRow128000))).toMatch(
        /Division by zero/
      );
    });

    it('rejects with the query error when it fails before the rowLimit', async () => {
      expect(
        await outcome(connection.runSQL(failsAtRow128000, {rowLimit: 200000}))
      ).toMatch(/Division by zero/);
    });
  });
});

class SchemaConnection extends TrinoPrestoConnection {
  protected async fillStructDefForSqlBlockSchema(
    sql: string,
    structDef: StructDef
  ): Promise<void> {
    await this.loadSchemaForSqlBlock(sql, structDef, `query ${sql}`);
  }
}

describe.each(['trino', 'presto'])('%s schema discovery', dialect => {
  it('retries an empty DESCRIBE and caches only the successful schema', async () => {
    const runSQL = jest
      .fn<ReturnType<BaseRunner['runSQL']>, [string]>()
      .mockResolvedValueOnce({rows: [], columns: []})
      .mockResolvedValue({rows: [['state', 'varchar', '', '']], columns: []});
    const connection = new SchemaConnection(dialect, {runSQL});
    const tables = {states: 'malloytest.state_facts'};

    const failed = await connection.fetchSchemaForTables(tables, {});
    expect(failed).toEqual({
      schemas: {},
      errors: {
        states:
          'Could not fetch schema for table malloytest.state_facts: DESCRIBE returned no columns',
      },
    });

    const recovered = await connection.fetchSchemaForTables(tables, {});
    expect(recovered.errors).toEqual({});
    expect(recovered.schemas['states'].fields).toEqual([
      {name: 'state', type: 'string'},
    ]);
    expect(await connection.fetchSchemaForTables(tables, {})).toEqual(
      recovered
    );
    expect(runSQL).toHaveBeenCalledTimes(2);
    expect(runSQL).toHaveBeenCalledWith('DESCRIBE malloytest.state_facts', {});
  });
});

describe('setupSQL failure', () => {
  it('runs setup again on the next query after setup fails', async () => {
    const runSQL = jest
      .fn<ReturnType<BaseRunner['runSQL']>, [string]>()
      .mockResolvedValueOnce({
        rows: [],
        columns: [],
        error: 'Schema does not exist: memory.later',
      })
      .mockResolvedValueOnce({rows: [], columns: []})
      .mockResolvedValue({
        rows: [[1]],
        columns: [{name: 'v', type: 'integer'}],
      });
    const connection = new SchemaConnection(
      'trino',
      {runSQL},
      undefined,
      'USE memory.later'
    );

    await expect(connection.runSQL('SELECT 1 AS v')).rejects.toThrow(
      'Schema does not exist: memory.later'
    );
    const retried = await connection.runSQL('SELECT 1 AS v').then(
      result => result.rows,
      (error: unknown) => error
    );
    expect(runSQL.mock.calls.map(([sql]) => sql)).toEqual([
      'USE memory.later',
      'USE memory.later',
      'SELECT 1 AS v',
    ]);
    expect(retried).toEqual([{v: 1}]);
  });
});

describe('Presto EXPLAIN schema', () => {
  function fieldsFromPlan(plan: string): FieldDef[] {
    const structDef: SQLSourceDef = {
      type: 'sql_select',
      name: 'explained',
      connection: 'presto',
      selectStr: 'SELECT 1 AS a',
      dialect: 'presto',
      fields: [],
    };
    PrestoConnection.schemaFromExplain(
      {rows: [{'Query Plan': plan}], totalRows: 1},
      structDef,
      new TrinoDialect()
    );
    return structDef.fields;
  }

  it('reads a plan with a PlanNodeId group (presto >= 0.284)', () => {
    expect(
      fieldsFromPlan(
        '- Output[PlanNodeId 5][a, b] => [expr:integer, expr_1:varchar]\n' +
          '        Estimates: {rows: 1 (5B)}\n'
      )
    ).toEqual([
      {name: 'a', ...intType},
      {name: 'b', ...stringType},
    ]);
  });

  it('reads a plan without a PlanNodeId group (presto < 0.284)', () => {
    expect(
      fieldsFromPlan('- Output[a, b] => [expr:integer, expr_1:varchar]\n')
    ).toEqual([
      {name: 'a', ...intType},
      {name: 'b', ...stringType},
    ]);
  });

  it('reads a field named PlanNodeId', () => {
    expect(
      fieldsFromPlan(
        '- Output[PlanNodeId 5][PlanNodeId, b] => [expr:integer, expr_1:varchar]\n'
      )
    ).toEqual([
      {name: 'PlanNodeId', ...intType},
      {name: 'b', ...stringType},
    ]);
    expect(fieldsFromPlan('- Output[PlanNodeId] => [expr:integer]\n')).toEqual([
      {name: 'PlanNodeId', ...intType},
    ]);
  });

  it('reads the plan line as reported in issue #3042', () => {
    expect(fieldsFromPlan('Output[a]=>[expr:integer]')).toEqual([
      {name: 'a', ...intType},
    ]);
  });
});
