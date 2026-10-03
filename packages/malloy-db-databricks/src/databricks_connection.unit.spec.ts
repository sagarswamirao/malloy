/*
 * Copyright Contributors to the Malloy project
 * SPDX-License-Identifier: MIT
 */

// A stubbed driver, so the session-setup tests drive the connection's real
// connect-and-setup path and can tell which session ran each statement. The
// timeout tests pre-set a session and never construct a client, so the stub
// is invisible to them.
interface MockSession {
  // Statements that completed on this session, in completion order.
  completed: string[];
}

interface MockClient {
  closed: boolean;
}

const mockDriver: {
  clients: MockClient[];
  sessions: MockSession[];
  // Every statement submitted to any session, in submission order.
  submitted: string[];
  connect: () => Promise<void>;
  openSession: () => Promise<void>;
  // Resolves to the statement's rows; a rejection is the server failing it.
  runStatement: (sql: string) => Promise<unknown[]>;
} = {
  clients: [],
  sessions: [],
  submitted: [],
  connect: async () => {},
  openSession: async () => {},
  runStatement: async () => [],
};

function mockNewClient() {
  const client: MockClient = {closed: false};
  mockDriver.clients.push(client);
  return {
    connect: async () => {
      await mockDriver.connect();
    },
    openSession: async () => {
      await mockDriver.openSession();
      const session: MockSession = {completed: []};
      mockDriver.sessions.push(session);
      return {
        executeStatement: async (sql: string) => {
          mockDriver.submitted.push(sql);
          const rows = await mockDriver.runStatement(sql);
          session.completed.push(sql);
          return {
            fetchAll: async () => rows,
            cancel: async () => {},
            close: async () => {},
          };
        },
        close: async () => {},
      };
    },
    close: async () => {
      client.closed = true;
    },
  };
}

jest.mock('@databricks/sql', () => ({
  LogLevel: {error: 'error'},
  DBSQLLogger: jest.fn(),
  DBSQLClient: jest.fn(() => mockNewClient()),
}));

import {DatabricksConnection} from './databricks_connection';

// A never-settling fetch, used to force the timeout/abort path.
const never = () => new Promise<unknown[]>(() => {});

// Flush enough microtasks that runRawSQL reaches its Promise.race (so the
// timeout timer is registered) before the test advances fake time.
const flush = async () => {
  for (let i = 0; i < 5; i++) {
    await Promise.resolve();
  }
};

// A DatabricksConnection with a stubbed session/operation so runSQL drives the
// real executeRaw timeout+cancel path without a live warehouse. A pre-set
// session short-circuits ensureConnected, so doConnect never runs.
function hermeticConnection(opts: {
  timeoutMs?: number;
  fetchAll: () => Promise<unknown[]>;
}) {
  const cancel = jest.fn(async () => ({}));
  const close = jest.fn(async () => {});
  const operation = {fetchAll: opts.fetchAll, cancel, close};
  const executeStatement = jest.fn(async () => operation);
  const conn = new DatabricksConnection('hermetic', {
    host: '',
    path: '',
    timeoutMs: opts.timeoutMs,
  });
  (conn as unknown as {session: unknown}).session = {executeStatement};
  return {conn, cancel, close, executeStatement};
}

describe('DatabricksConnection timeout + abort', () => {
  beforeEach(() => jest.useFakeTimers());
  afterEach(() => jest.useRealTimers());

  it('cancels the operation and throws when the query exceeds timeoutMs', async () => {
    const {conn, cancel} = hermeticConnection({
      timeoutMs: 1000,
      fetchAll: never,
    });
    const promise = conn.runRawSQL('SELECT 1');
    promise.catch(() => {});
    await flush();
    expect(cancel).not.toHaveBeenCalled();
    jest.advanceTimersByTime(1000);
    await expect(promise).rejects.toThrow(
      /did not complete within the configured timeout of 1000ms/
    );
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it('times out even if closing the operation hangs', async () => {
    // close() over an unresponsive connection can hang; the cleanup must not be
    // awaited on the failure path, or it would swallow the timeout.
    const {conn, cancel, close} = hermeticConnection({
      timeoutMs: 1000,
      fetchAll: never,
    });
    close.mockImplementation(() => new Promise<void>(() => {})); // never resolves
    const promise = conn.runRawSQL('SELECT 1');
    promise.catch(() => {});
    await flush();
    jest.advanceTimersByTime(1000);
    await expect(promise).rejects.toThrow(
      /did not complete within the configured timeout of 1000ms/
    );
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it('does not cancel a query that completes within the timeout', async () => {
    const {conn, cancel} = hermeticConnection({
      timeoutMs: 1000,
      fetchAll: async () => [{n: 1}],
    });
    const data = await conn.runRawSQL('SELECT 1');
    expect(data.rows).toEqual([{n: 1}]);
    expect(data.totalRows).toBe(1);
    expect(cancel).not.toHaveBeenCalled();
  });

  it('cancels the operation when the abort signal fires', async () => {
    const ac = new AbortController();
    const {conn, cancel} = hermeticConnection({
      timeoutMs: 600_000,
      fetchAll: never,
    });
    const promise = conn.runSQL('SELECT 1', {abortSignal: ac.signal});
    promise.catch(() => {});
    await flush();
    ac.abort();
    await expect(promise).rejects.toThrow(/aborted/);
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it('throws without executing when the signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const {conn, executeStatement, cancel} = hermeticConnection({
      timeoutMs: 600_000,
      fetchAll: never,
    });
    await expect(
      conn.runSQL('SELECT 1', {abortSignal: ac.signal})
    ).rejects.toThrow(/aborted/);
    expect(executeStatement).not.toHaveBeenCalled();
    expect(cancel).not.toHaveBeenCalled();
  });
});

const SETUP_CONFIG = {
  host: 'h',
  path: '/sql/1.0/warehouses/w',
  token: 't',
  defaultCatalog: 'malloy_catalog',
  defaultSchema: 'malloy_schema',
  setupSQL: "SET TIME ZONE 'Asia/Tokyo'",
};

// What a session must have completed, in this order, before it can run a
// query for SETUP_CONFIG.
const SETUP = [
  "SET TIME ZONE 'UTC'",
  'USE CATALOG malloy_catalog',
  'USE SCHEMA malloy_schema',
  "SET TIME ZONE 'Asia/Tokyo'",
];

const FIRST = 'SELECT 1 AS first';
const SECOND = 'SELECT 2 AS second';

const FAILURE_POINTS: Array<'connect' | 'openSession'> = [
  'connect',
  'openSession',
];

// The SETUP.length statements that completed on the session that ran `sql`,
// immediately before it. Only that window counts: a connection may retry
// setup on the same session, which leaves a failed attempt's statements
// further back.
function statementsBefore(sql: string): string[] {
  const session = mockDriver.sessions.find(s => s.completed.includes(sql));
  if (session === undefined) {
    throw new Error(
      `No stub session completed ${JSON.stringify(sql)}; submitted: ${JSON.stringify(mockDriver.submitted)}`
    );
  }
  const at = session.completed.indexOf(sql);
  return session.completed.slice(Math.max(0, at - SETUP.length), at);
}

// A promise the test settles by hand, to hold a stubbed statement in flight.
function gate(): {opened: Promise<void>; open: () => void} {
  let open = () => {};
  const opened = new Promise<void>(resolve => {
    open = resolve;
  });
  return {opened, open};
}

// Every stubbed driver call settles on the microtask queue, and the whole
// microtask queue drains before the next macrotask, so when this resolves a
// query that is not waiting for setup has already been submitted.
function afterPendingMicrotasks(): Promise<void> {
  return new Promise(resolve => setImmediate(resolve));
}

describe('DatabricksConnection session setup', () => {
  beforeEach(() => {
    mockDriver.clients = [];
    mockDriver.sessions = [];
    mockDriver.submitted = [];
    mockDriver.connect = async () => {};
    mockDriver.openSession = async () => {};
    mockDriver.runStatement = async () => [];
  });

  it.each(SETUP)(
    'runs setup again after %s fails, so the next query runs on a fully set-up session',
    async failing => {
      let failed = false;
      mockDriver.runStatement = async sql => {
        if (sql === failing && !failed) {
          failed = true;
          throw new Error(`stub rejection of ${failing}`);
        }
        return [];
      };
      const conn = new DatabricksConnection('setup', SETUP_CONFIG);
      await expect(conn.runSQL(FIRST)).rejects.toThrow(
        `stub rejection of ${failing}`
      );
      await expect(conn.runSQL(SECOND)).resolves.toEqual({
        rows: [],
        totalRows: 0,
      });
      expect(statementsBefore(SECOND)).toEqual(SETUP);
    }
  );

  it('fails every query while USE SCHEMA fails, instead of running one on the half-configured session', async () => {
    mockDriver.runStatement = async sql => {
      if (sql === 'USE SCHEMA malloy_schema') {
        throw new Error(
          '[SCHEMA_NOT_FOUND] The schema `malloy_schema` cannot be found.'
        );
      }
      return [];
    };
    const conn = new DatabricksConnection('setup', SETUP_CONFIG);
    await expect(conn.runSQL(FIRST)).rejects.toThrow('SCHEMA_NOT_FOUND');
    await expect(conn.runSQL(SECOND)).rejects.toThrow('SCHEMA_NOT_FOUND');
    expect(mockDriver.submitted).not.toContain(SECOND);
  });

  it.each(FAILURE_POINTS)(
    'connects again after %s fails, instead of returning the cached error to the next query',
    async step => {
      let failed = false;
      mockDriver[step] = async () => {
        if (!failed) {
          failed = true;
          throw new Error(`stub rejection of ${step}`);
        }
      };
      const conn = new DatabricksConnection('setup', SETUP_CONFIG);
      await expect(conn.runSQL(FIRST)).rejects.toThrow(
        `stub rejection of ${step}`
      );
      await expect(conn.runSQL(SECOND)).resolves.toEqual({
        rows: [],
        totalRows: 0,
      });
      expect(statementsBefore(SECOND)).toEqual(SETUP);
      // The client from the failed attempt is not left open beside the one
      // now in use.
      expect(mockDriver.clients.filter(c => !c.closed)).toHaveLength(1);
    }
  );

  it('holds a query issued during setup until setup has finished', async () => {
    const setupStarted = gate();
    const releaseSetup = gate();
    mockDriver.runStatement = async sql => {
      if (sql === SETUP[0]) {
        setupStarted.open();
        await releaseSetup.opened;
      }
      return [];
    };
    const conn = new DatabricksConnection('setup', SETUP_CONFIG);
    const first = conn.runSQL(FIRST);
    await setupStarted.opened;
    const second = conn.runSQL(SECOND);
    await afterPendingMicrotasks();
    const submittedWhileSetupHeld = [...mockDriver.submitted];
    releaseSetup.open();
    await Promise.all([first, second]);
    expect(submittedWhileSetupHeld).toEqual([SETUP[0]]);
    expect(mockDriver.submitted.slice(0, SETUP.length)).toEqual(SETUP);
  });
});
