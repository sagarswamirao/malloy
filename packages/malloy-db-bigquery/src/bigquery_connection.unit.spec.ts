/*
 * Copyright Contributors to the Malloy project
 * SPDX-License-Identifier: MIT
 */

import type {
  BigQueryOptions,
  CancelResponse,
  JobResponse,
} from '@google-cloud/bigquery';
import {BigQuery, Job} from '@google-cloud/bigquery';
import {ApiError} from '@google-cloud/common';
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

describe('BigQueryConnection.runSQL when the caller aborts', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('rejects with the abort error and absorbs a failed job cancel instead of leaving it unhandled', async () => {
    // A real SDK Job with the two calls this path makes stubbed, so no request
    // leaves the process.
    const job = new Job(new BigQuery({projectId: 'test-project'}), 'job-1');
    // A poll that never answers, so the abort lands while the job is running.
    const pollInFlight = new Promise<void>(resolve => {
      jest.spyOn(job, 'getQueryResults').mockImplementation(() => resolve());
    });
    // The jobs.cancel request itself fails, as it does on a dropped
    // connection or a 5xx from the API.
    const cancel = jest
      .spyOn(job, 'cancel')
      .mockImplementation((): Promise<CancelResponse> => {
        const error = new ApiError('Bad Gateway');
        error.code = 502;
        return Promise.reject(error);
      });
    // Stubbed on the prototype: the connection builds its own SDK client and
    // keeps it private.
    const createQueryJob = async (): Promise<JobResponse> => [job, {}];
    jest
      .spyOn(BigQuery.prototype, 'createQueryJob')
      .mockImplementation(createQueryJob);

    const conn = new BigQueryConnection({
      name: 'bq',
      projectId: 'test-project',
    });
    const controller = new AbortController();
    const run = conn.runSQL('SELECT 1', {abortSignal: controller.signal});
    await pollInFlight;
    controller.abort();

    await expect(run).rejects.toThrow(
      'BigQuery getQueryResults was aborted before the query completed.'
    );
    expect(cancel).toHaveBeenCalledTimes(1);
    // Node reports a rejection that nothing handled only after the microtask
    // queue drains, and Jest fails whichever test is running when it does.
    // One turn of the event loop makes that report land inside this test
    // rather than after the file has finished.
    await new Promise(resolve => setImmediate(resolve));
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
