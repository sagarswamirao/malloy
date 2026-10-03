/*
 * Copyright Contributors to the Malloy project
 * SPDX-License-Identifier: MIT
 */

import {MySQLConnection, MySQLExecutor} from '.';
import crypto from 'crypto';

const config = MySQLExecutor.getConnectionOptionsFromEnv();
const describeMySQL = config.user ? describe : describe.skip;

describeMySQL('setupSQL', () => {
  const uid = crypto.randomBytes(4).toString('hex');
  const connections: MySQLConnection[] = [];

  function makeConn(name: string, setupSQL: string): MySQLConnection {
    const conn = new MySQLConnection(name, {...config, setupSQL});
    connections.push(conn);
    return conn;
  }

  afterAll(async () => {
    await Promise.all(connections.map(c => c.close()));
  });

  it('runs a single setup statement', async () => {
    const varName = `@setup_single_${uid}`;
    const conn = makeConn('mysql', `SET ${varName} = 42`);
    const result = await conn.runSQL(`SELECT ${varName} AS v`);
    expect(result.rows[0]['v']).toBe(42);
  });

  it('runs multiple semicolon-newline-separated statements', async () => {
    const varA = `@setup_a_${uid}`;
    const varB = `@setup_b_${uid}`;
    const conn = makeConn(
      'mysql',
      [`SET ${varA} = 10`, `SET ${varB} = 20`].join(';\n')
    );
    const result = await conn.runSQL(`SELECT ${varA} + ${varB} AS v`);
    expect(result.rows[0]['v']).toBe(30);
  });

  it('handles multi-line statements', async () => {
    const varName = `@setup_ml_${uid}`;
    const conn = makeConn('mysql', `SET\n  ${varName} = 99`);
    const result = await conn.runSQL(`SELECT ${varName} AS v`);
    expect(result.rows[0]['v']).toBe(99);
  });

  // The middle statement fails on every attempt, so no session can ever be
  // fully set up, and every query has to fail, not just the first.
  it('rejects every query while a setup statement keeps failing', async () => {
    const before = `@setup_before_${uid}`;
    const after = `@setup_after_${uid}`;
    const missing = `setup_missing_${uid}`;
    const conn = makeConn(
      'mysql',
      [
        `SET ${before} = 'applied'`,
        `SELECT * FROM ${missing}`,
        `SET ${after} = 'applied'`,
      ].join(';\n')
    );
    const probe = `SELECT ${before} AS before_value, ${after} AS after_value`;
    await expect(conn.runSQL(probe)).rejects.toThrow(missing);
    await expect(conn.runSQL(probe)).rejects.toThrow(missing);
  });
});
