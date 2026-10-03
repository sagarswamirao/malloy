/*
 * Copyright Contributors to the Malloy project
 * SPDX-License-Identifier: MIT
 */

import {once} from 'events';
import {Readable} from 'stream';
import {toAsyncGenerator} from './connection_utils';

interface Row {
  i: number;
}

const TOTAL_ROWS = 1000;
const CLOSE_DEADLINE_MS = 5000;

/**
 * An object-mode Readable read in flowing mode, as the Snowflake and BigQuery
 * result streams are. It produces one row per macrotask, the way rows arrive
 * off the network: a source that pushed every row synchronously would be
 * drained before the consumer saw its first row, and a source that was stopped
 * would be indistinguishable from one that ran out.
 */
class RowSource extends Readable {
  rowsProduced = 0;

  constructor(
    private readonly total: number,
    private readonly failure?: Error
  ) {
    super({objectMode: true});
  }

  override _read(): void {
    setImmediate(() => {
      if (this.destroyed) return;
      if (this.rowsProduced < this.total) {
        this.push({i: this.rowsProduced++});
      } else if (this.failure) {
        this.destroy(this.failure);
      } else {
        this.push(null);
      }
    });
  }
}

/**
 * Attaches to the source the way the Snowflake and BigQuery executors do, and
 * returns the source's teardown so the generator can stop the source once its
 * consumer stops reading.
 */
function streamFrom(source: RowSource) {
  return (
    onError: (error: Error) => void,
    onData: (row: Row) => void,
    onEnd: () => void
  ) => {
    source.on('error', onError).on('data', onData).on('end', onEnd);
    return () => {
      source.destroy();
    };
  };
}

/**
 * Waits for the source to close, which it does either when it is destroyed or
 * after it has produced every row. Waiting for that event, rather than checking
 * right after the consumer stops, gives a source that was not stopped the time
 * to produce the rest of its rows.
 */
async function sourceClosed(source: RowSource): Promise<void> {
  if (source.closed) return;
  const deadline = AbortSignal.timeout(CLOSE_DEADLINE_MS);
  try {
    await once(source, 'close', {signal: deadline});
  } catch (error) {
    if (!deadline.aborted) throw error;
    throw new Error(
      `Source still open ${CLOSE_DEADLINE_MS}ms after the consumer stopped, having produced ${source.rowsProduced} of ${TOTAL_ROWS} rows`
    );
  }
}

async function expectStoppedAt(source: RowSource, rowsAtStop: number) {
  await sourceClosed(source);
  expect({
    sourceReachedEnd: source.readableEnded,
    rowsProducedAfterStop: source.rowsProduced - rowsAtStop,
  }).toEqual({sourceReachedEnd: false, rowsProducedAfterStop: 0});
}

describe('toAsyncGenerator', () => {
  it('yields every row in order and finishes when the source ends', async () => {
    const source = new RowSource(TOTAL_ROWS);
    const seen: number[] = [];
    for await (const row of toAsyncGenerator<Row>(streamFrom(source))) {
      seen.push(row.i);
    }
    expect(seen).toEqual(Array.from({length: TOTAL_ROWS}, (_, i) => i));
  });

  it('throws the source error after yielding the rows before it', async () => {
    const source = new RowSource(3, new Error('connection reset'));
    const seen: number[] = [];
    const consume = async () => {
      for await (const row of toAsyncGenerator<Row>(streamFrom(source))) {
        seen.push(row.i);
      }
    };
    await expect(consume()).rejects.toThrow('connection reset');
    expect(seen).toEqual([0, 1, 2]);
  });

  it('tears down the source when the consumer breaks out of for await', async () => {
    const source = new RowSource(TOTAL_ROWS);
    const seen: number[] = [];
    for await (const row of toAsyncGenerator<Row>(streamFrom(source))) {
      seen.push(row.i);
      if (seen.length === 2) break;
    }
    const rowsAtStop = source.rowsProduced;
    expect(seen).toEqual([0, 1]);
    await expectStoppedAt(source, rowsAtStop);
  });

  it('tears down the source when the consumer calls return() on the iterator', async () => {
    const source = new RowSource(TOTAL_ROWS);
    const rows = toAsyncGenerator<Row>(streamFrom(source));
    expect(await rows.next()).toEqual({done: false, value: {i: 0}});
    if (rows.return === undefined) {
      throw new Error('toAsyncGenerator returned an iterator with no return()');
    }
    expect(await rows.return()).toEqual({done: true, value: undefined});
    await expectStoppedAt(source, source.rowsProduced);
  });

  it('tears down the source when the for await body throws', async () => {
    const source = new RowSource(TOTAL_ROWS);
    const consume = async () => {
      for await (const row of toAsyncGenerator<Row>(streamFrom(source))) {
        if (row.i === 1) throw new Error('consumer gave up');
      }
    };
    await expect(consume()).rejects.toThrow('consumer gave up');
    await expectStoppedAt(source, source.rowsProduced);
  });

  it('tears down the source when the consumer calls throw() on the iterator', async () => {
    const source = new RowSource(TOTAL_ROWS);
    const rows = toAsyncGenerator<Row>(streamFrom(source));
    expect(await rows.next()).toEqual({done: false, value: {i: 0}});
    if (rows.throw === undefined) {
      throw new Error('toAsyncGenerator returned an iterator with no throw()');
    }
    await expect(rows.throw(new Error('consumer gave up'))).rejects.toThrow(
      'consumer gave up'
    );
    await expectStoppedAt(source, source.rowsProduced);
  });
});
