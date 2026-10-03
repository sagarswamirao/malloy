/*
 * Copyright Contributors to the Malloy project
 * SPDX-License-Identifier: MIT
 */

import {once} from 'events';
import type {BigQueryOptions} from '@google-cloud/bigquery';
import {paginator, ResourceStream} from '@google-cloud/paginator';
import type {ParsedArguments} from '@google-cloud/paginator';
import {BigQueryConnection} from './bigquery_connection';

// The callback overload yields (err, rows, nextQuery, apiResponse).
type Cb = (
  err: unknown,
  rows: unknown,
  nextQuery: unknown,
  apiResponse: unknown
) => void;
type Step = (cb: Cb) => void;

const stillRunning: Step = cb =>
  cb(new Error('The query did not complete before 120000ms'), null, null, {
    jobComplete: false,
  });
const complete =
  (rows: unknown[] = [{n: 1}]): Step =>
  cb =>
    cb(null, rows, null, {jobComplete: true, totalRows: String(rows.length)});

// A BigQueryConnection with its BigQuery SDK and timeout config stubbed, so
// runSQL drives the real createBigQueryJob -> getQueryResultsUntilComplete seam
// (deadline wiring, and that a jobComplete:false response never surfaces as
// data) without a live warehouse.
function hermeticConnection(steps: Step[], timeoutMs?: string) {
  const conn = new BigQueryConnection('hermetic');
  let i = 0;
  const getQueryResults = jest.fn((_options: unknown, cb: Cb) => {
    const step = steps[Math.min(i, steps.length - 1)];
    i++;
    step(cb);
  });
  const job = {getQueryResults, cancel: jest.fn()};
  const createQueryJob = jest.fn(async (_options: unknown) => [job]);
  (conn as unknown as {bigQuery: unknown}).bigQuery = {createQueryJob};
  (conn as unknown as {config: {timeoutMs?: string}}).config = {timeoutMs};
  return {conn, getQueryResults, createQueryJob};
}

describe('BigQueryConnection.runSQL (hermetic, stubbed job)', () => {
  it('treats config.timeoutMs "0" as unset: uses the default jobTimeoutMs', async () => {
    // 0 falls back to the default (like blank or non-numeric); it means neither
    // "wait 0ms" nor "wait forever". (Poll-through is covered separately, so
    // this stays a single completed poll to avoid the real inter-poll wait.)
    const {conn, createQueryJob} = hermeticConnection(
      [complete([{n: 5}])],
      '0'
    );
    const data = await conn.runSQL('SELECT 1');
    expect(data.rows).toEqual([{n: 5}]);
    expect(createQueryJob.mock.calls[0][0]).toMatchObject({
      jobTimeoutMs: 600000,
    });
  });

  it('passes a positive timeoutMs through to the job as jobTimeoutMs', async () => {
    const {conn, createQueryJob} = hermeticConnection(
      [complete([{n: 7}])],
      '300000'
    );
    await conn.runSQL('SELECT 1');
    expect(createQueryJob.mock.calls[0][0]).toMatchObject({
      jobTimeoutMs: 300000,
    });
  });

  it('treats a whitespace-only timeoutMs as the default', async () => {
    // Number('   ') is 0, so a naive parse could go wrong here; it must fall
    // back to the default (TIMEOUT_MS, 600000ms) like any other blank value.
    const {conn, createQueryJob} = hermeticConnection(
      [complete([{n: 1}])],
      '   '
    );
    await conn.runSQL('SELECT 1');
    expect(createQueryJob.mock.calls[0][0]).toMatchObject({
      jobTimeoutMs: 600000,
    });
  });

  it('treats a negative timeoutMs as the default', async () => {
    // Number('-5') is -5, which is truthy, so a bare `|| TIMEOUT_MS` would let
    // it through as jobTimeoutMs: -5 and blow the deadline on the first poll.
    // A non-positive value must fall back to the default.
    const {conn, createQueryJob} = hermeticConnection(
      [complete([{n: 1}])],
      '-5'
    );
    await conn.runSQL('SELECT 1');
    expect(createQueryJob.mock.calls[0][0]).toMatchObject({
      jobTimeoutMs: 600000,
    });
  });

  it('polls a still-running response rather than returning it as empty data', async () => {
    const {conn, getQueryResults} = hermeticConnection([
      stillRunning,
      complete([{n: 1}, {n: 2}]),
    ]);
    const data = await conn.runSQL('SELECT 1');
    expect(data.rows).toEqual([{n: 1}, {n: 2}]);
    expect(data.totalRows).toBe(2);
    expect(getQueryResults).toHaveBeenCalledTimes(2);
  });
});

describe('BigQueryConnection authClient', () => {
  // A stand-in for a google-auth AuthClient. Nothing in Malloy inspects one —
  // that is what `opaque` means — so the double only has to be identifiable.
  const authClient = {
    getAccessToken: async () => ({token: 'from-the-host'}),
  } as unknown as BigQueryOptions['authClient'];

  it('hands a host-supplied auth client to the SDK', async () => {
    // The SDK wraps what it is given in a GoogleAuth rather than keeping it as
    // a field, so the claim worth pinning is behavioral: the auth this
    // connection will actually query with is the host's client, not ambient
    // credentials. `getClient()` returns the supplied one without any IO.
    const conn = new BigQueryConnection({
      name: 'bq',
      projectId: 'test-project',
      authClient,
    });
    const sdk = (
      conn as unknown as {
        bigQuery: {authClient: {getClient: () => Promise<unknown>}};
      }
    ).bigQuery;
    await expect(sdk.authClient.getClient()).resolves.toBe(authClient);
  });

  it('separates the digests of two identities on one project', () => {
    // Same project, same SQL, different impersonated service accounts. If the
    // digests matched, the BuildIDs would match, and one tenant would be
    // served rows persisted for the other.
    const forTenant = (tenant: string) =>
      new BigQueryConnection({
        name: 'bq',
        projectId: 'shared-project',
        authClient,
        rawConfigData: {is: 'bigquery', authClient: {tenantAuth: tenant}},
      }).getDigest();

    expect(forTenant('acme')).not.toBe(forTenant('globex'));
    expect(forTenant('acme')).toBe(forTenant('acme'));
  });

  it('leaves the digest alone when no auth client is named', () => {
    // Connections that don't use one keep the digests they have today, so
    // their persisted tables survive this change.
    const conn = new BigQueryConnection({
      name: 'bq',
      projectId: 'shared-project',
    });
    expect(conn.getDigest()).toBe(
      new BigQueryConnection({
        name: 'bq',
        projectId: 'shared-project',
        rawConfigData: {is: 'bigquery'},
      }).getDigest()
    );
  });
});

const PAGES = 10;
const ROWS_PER_PAGE = 100;
const CLOSE_DEADLINE_MS = 5000;

interface PagedRow {
  i: number;
}

// The paginator's request function answers through a callback taking
// (err, rows, nextQuery); a null nextQuery marks the last page.
type PageCallback = (
  err: Error | null,
  rows: PagedRow[],
  nextQuery: {pageToken: number} | null
) => void;

/**
 * A query result BigQuery hands back a page at a time. createQueryStream stays
 * the SDK's own, and so does the paginator's ResourceStream it returns; only
 * the request the stream makes for each page is answered here. In production
 * that request is BigQuery.queryAsStream_, which fetches the page with
 * getQueryResults. Each page arrives a macrotask after it is requested, the
 * way a page arrives off the network.
 */
class PagedResult {
  pagesRequested = 0;
  private stream: ResourceStream<PagedRow> | undefined;

  open(parsedArguments: ParsedArguments): ResourceStream<PagedRow> {
    this.stream = new ResourceStream<PagedRow>(parsedArguments, this.servePage);
    return this.stream;
  }

  private readonly servePage = (_query: unknown, callback: PageCallback) => {
    const page = this.pagesRequested++;
    setImmediate(() => {
      const rows = Array.from({length: ROWS_PER_PAGE}, (_, k) => ({
        i: page * ROWS_PER_PAGE + k,
      }));
      callback(null, rows, page + 1 < PAGES ? {pageToken: page + 1} : null);
    });
  };

  /**
   * Waits for the query stream to close, which it does either when it is
   * torn down or after it has read the last page. Waiting for that event,
   * rather than checking right after the consumer stops, gives a stream that
   * was not stopped the time to request the rest of the pages.
   */
  async closed(): Promise<void> {
    const stream = this.stream;
    if (stream === undefined) {
      throw new Error('runSQLStream never opened a query stream');
    }
    if (stream.closed) return;
    const deadline = AbortSignal.timeout(CLOSE_DEADLINE_MS);
    try {
      await once(stream, 'close', {signal: deadline});
    } catch (error) {
      if (!deadline.aborted) throw error;
      throw new Error(
        `Query stream still open ${CLOSE_DEADLINE_MS}ms after the consumer stopped, having requested ${this.pagesRequested} of ${PAGES} pages`
      );
    }
  }
}

describe('BigQueryConnection.runSQLStream (hermetic, stubbed pages)', () => {
  afterEach(() => jest.restoreAllMocks());

  function pagedResult(): PagedResult {
    const result = new PagedResult();
    jest
      .spyOn(paginator, 'runAsStream_')
      .mockImplementation(parsedArguments => result.open(parsedArguments));
    return result;
  }

  it('stops paging through the result when the consumer breaks out of for await', async () => {
    const result = pagedResult();
    const conn = new BigQueryConnection('hermetic');
    const seen: unknown[] = [];
    for await (const row of conn.runSQLStream('SELECT i FROM a_large_result')) {
      seen.push(row);
      if (seen.length === 2) break;
    }
    const pagesAtStop = result.pagesRequested;
    expect(seen).toEqual([{i: 0}, {i: 1}]);

    await result.closed();
    // pagesAtStop already counts the second page: the stream asks for the next
    // page before the consumer has seen a row of the current one, so that
    // request was in flight before the consumer stopped. A request made after
    // the stop is paging for a consumer that has gone.
    expect({
      pagesRequestedAfterStop: result.pagesRequested - pagesAtStop,
    }).toEqual({pagesRequestedAfterStop: 0});
  });
});
