/*
 * Copyright Contributors to the Malloy project
 * SPDX-License-Identifier: MIT
 */

import {once} from 'events';
import {Readable} from 'stream';
import {createPool} from 'generic-pool';
import type {Options as PoolOptions, Pool} from 'generic-pool';
import type {QueryRecord} from '@malloydata/malloy';
import {SnowflakeConnection} from './snowflake_connection';

// snowflake-sdk is replaced by a warehouse that lives in this file. The
// executor still builds its pool through the SDK's createPool with its own
// pool options, so the pool here is a real generic-pool, the library the SDK
// builds its pool with, sized the way the executor asks for: one connection
// by default.
jest.mock('snowflake-sdk', () => ({
  configure: () => {},
  createPool: (_connOptions: unknown, poolOptions: PoolOptions) =>
    mockWarehouse.createPool(poolOptions),
}));

const TOTAL_ROWS = 1000;
const DEADLINE_MS = 5000;
const STREAMED_QUERY = 'SELECT i FROM a_large_result';
const NEXT_QUERY = 'SELECT 1';

/**
 * An object-mode Readable read in flowing mode, as the stream from
 * RowStatement.streamRows() is. It produces one row per macrotask, the way
 * result chunks arrive off the network: a source that pushed every row
 * synchronously would be drained before the consumer saw its first row, and a
 * source that was stopped would be indistinguishable from one that ran out.
 */
class RowSource extends Readable {
  rowsProduced = 0;

  constructor(private readonly total: number) {
    super({objectMode: true});
  }

  override _read(): void {
    setImmediate(() => {
      if (this.destroyed) return;
      if (this.rowsProduced < this.total) {
        this.push({I: this.rowsProduced++});
      } else {
        this.push(null);
      }
    });
  }
}

interface FakeStatement {
  cancel(): void;
  streamRows(): Readable;
}

interface ExecuteOptions {
  sqlText: string;
  complete: (
    error: Error | undefined,
    statement: FakeStatement,
    rows?: QueryRecord[]
  ) => void;
}

interface FakeConnection {
  execute(options: ExecuteOptions): FakeStatement;
}

/**
 * The warehouse behind the stubbed SDK. Every statement completes a macrotask
 * after it is executed. A streamed statement's rows come from `result`, and
 * each statement records how many rows `result` had produced when it was
 * executed, which is how a test tells whether a query ran before or after an
 * earlier stream finished.
 */
class FakeWarehouse {
  readonly result = new RowSource(TOTAL_ROWS);
  private readonly executed: {sqlText: string; rowsStreamedBefore: number}[] =
    [];
  private pool: Pool<FakeConnection> | undefined;

  createPool(options: PoolOptions): Pool<FakeConnection> {
    this.pool = createPool<FakeConnection>(
      {
        create: async () => ({execute: statement => this.execute(statement)}),
        destroy: async () => {},
        validate: async () => true,
      },
      options
    );
    return this.pool;
  }

  execute(options: ExecuteOptions): FakeStatement {
    this.executed.push({
      sqlText: options.sqlText,
      rowsStreamedBefore: this.result.rowsProduced,
    });
    // By the time rows stream, the statement has completed in the warehouse,
    // so there is nothing for cancel() to stop; only the download remains.
    const statement: FakeStatement = {
      cancel: () => {},
      streamRows: () => this.result,
    };
    setImmediate(() => options.complete(undefined, statement, []));
    return statement;
  }

  connectionsBorrowed(): number {
    if (this.pool === undefined) {
      throw new Error('SnowflakeExecutor never created its pool');
    }
    return this.pool.borrowed;
  }

  rowsStreamedBefore(sqlText: string): number {
    const statement = this.executed.find(s => s.sqlText === sqlText);
    if (statement === undefined) {
      throw new Error(`${sqlText} was never executed`);
    }
    return statement.rowsStreamedBefore;
  }
}

let mockWarehouse: FakeWarehouse;

/**
 * Bounds a wait on the code under test, so that a connection that is never
 * returned, or a stream that never closes, fails the test with a message
 * rather than hanging until the Jest timeout.
 */
async function withinDeadline<T>(work: Promise<T>, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const expired = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(
      () =>
        reject(
          new Error(
            `Timed out after ${DEADLINE_MS}ms waiting for ${what}; the row stream had produced ${mockWarehouse.result.rowsProduced} of ${TOTAL_ROWS} rows`
          )
        ),
      DEADLINE_MS
    );
  });
  try {
    return await Promise.race([work, expired]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Waits for the row stream to close, which it does either when it is
 * destroyed or after it has produced every row. Waiting for that event,
 * rather than checking right after the consumer stops, gives a stream that
 * was not stopped the time to produce the rest of its rows.
 */
async function resultClosed(): Promise<void> {
  const result = mockWarehouse.result;
  if (result.closed) return;
  await withinDeadline(once(result, 'close'), 'the row stream to close');
}

/** Reads the first two rows of STREAMED_QUERY and breaks out of the loop. */
async function readTwoRowsAndBreak(
  connection: SnowflakeConnection
): Promise<QueryRecord[]> {
  const seen: QueryRecord[] = [];
  for await (const row of connection.runSQLStream(STREAMED_QUERY)) {
    seen.push(row);
    if (seen.length === 2) break;
  }
  return seen;
}

describe('SnowflakeConnection.runSQLStream (hermetic, stubbed SDK)', () => {
  let connection: SnowflakeConnection;

  beforeEach(() => {
    mockWarehouse = new FakeWarehouse();
    // Default pool options: the executor's own pool of one connection.
    connection = new SnowflakeConnection('hermetic', {
      connOptions: {account: 'hermetic', username: 'hermetic'},
    });
  });

  it('returns the pooled connection when the consumer breaks out of for await', async () => {
    const seen = await readTwoRowsAndBreak(connection);
    // Read as soon as the loop has exited: by then the consumer has stopped,
    // and nothing it does afterwards can return the connection.
    const connectionsBorrowedAfterStop = mockWarehouse.connectionsBorrowed();
    const rowsAtStop = mockWarehouse.result.rowsProduced;
    expect(seen).toEqual([{I: 0}, {I: 1}]);

    await resultClosed();
    expect({
      connectionsBorrowedAfterStop,
      rowsProducedAfterStop: mockWarehouse.result.rowsProduced - rowsAtStop,
    }).toEqual({connectionsBorrowedAfterStop: 0, rowsProducedAfterStop: 0});
    await connection.close();
  });

  it('runs the next query on the connection without waiting for the rest of an abandoned stream', async () => {
    await readTwoRowsAndBreak(connection);
    const rowsAtStop = mockWarehouse.result.rowsProduced;

    // The pool holds one connection, so the next query reaches the warehouse
    // only once the stream has returned it. How many more rows the abandoned
    // stream had produced by then is how long the next query waited for it.
    await withinDeadline(connection.runSQL(NEXT_QUERY), `${NEXT_QUERY} to run`);
    expect({
      rowsStreamedBeforeNextQueryRan:
        mockWarehouse.rowsStreamedBefore(NEXT_QUERY) - rowsAtStop,
    }).toEqual({rowsStreamedBeforeNextQueryRan: 0});
    await connection.close();
  });
});
