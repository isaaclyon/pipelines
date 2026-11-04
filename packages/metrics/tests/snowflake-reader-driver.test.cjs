const { test } = require('node:test');
const assert = require('node:assert/strict');
const { SnowflakeDriver } = require('@cubejs-backend/snowflake-driver');
const { SnowflakeReaderDriver, DISABLE_SECONDARY_ROLES } = require('../snowflake-reader-driver.cjs');

/**
 * Fake Snowflake session. New sessions enable all secondary roles, as a writer
 * user with DEFAULT_SECONDARY_ROLES = ('ALL') does, so the writer's grants leak
 * into reads until a statement disables them.
 * @param {{ failOn?: string }} [options]
 */
function fakeSnowflake(options = {}) {
  /** @type {Array<{ statements: string[], destroyed: boolean, up: boolean, secondary: string }>} */
  const sessions = [];
  /** @param {SnowflakeDriver} driver */
  const install = (driver) => Reflect.set(driver, 'createConnection', async () => {
    /** @type {(typeof sessions)[number]} */
    const session = { statements: [], destroyed: false, up: false, secondary: 'ALL' };
    sessions.push(session);
    return {
      /** @param {(error?: Error) => void} callback */
      connect(callback) { session.up = true; callback(); },
      isUp: () => session.up,
      /** @param {(error?: Error) => void} callback */
      destroy(callback) { session.destroyed = true; session.up = false; callback(); },
      /** @param {{ sqlText: string, complete: (error?: Error, statement?: unknown, rows?: unknown[]) => void }} request */
      execute({ sqlText, complete }) {
        session.statements.push(sqlText);
        if (sqlText === options.failOn) { complete(new Error('statement rejected')); return; }
        if (sqlText === DISABLE_SECONDARY_ROLES) session.secondary = 'NONE';
        complete(undefined, { getColumns: () => [] }, sqlText.startsWith('SELECT')
          ? [{ VISIBLE: session.secondary === 'ALL' ? 'writer-only customers' : 'reader marts' }] : []);
      },
    };
  });
  return { sessions, install };
}

test('the stock driver leaves default secondary roles active (the gap)', async () => {
  const fake = fakeSnowflake();
  const driver = new SnowflakeDriver({ dataSource: 'default' });
  fake.install(driver);
  const rows = await driver.query('SELECT visible', []);
  assert.deepEqual(rows, [{ VISIBLE: 'writer-only customers' }]);
  assert.ok(!fake.sessions[0]?.statements.includes(DISABLE_SECONDARY_ROLES));
});

test('every initialized session disables secondary roles before application queries', async () => {
  const fake = fakeSnowflake();
  const driver = new SnowflakeReaderDriver({ dataSource: 'default' });
  fake.install(driver);
  const rows = await driver.query('SELECT visible', []);
  assert.deepEqual(rows, [{ VISIBLE: 'reader marts' }]);
  const statements = fake.sessions[0]?.statements ?? [];
  const application = statements.indexOf('SELECT visible');
  assert.ok(statements.indexOf(DISABLE_SECONDARY_ROLES) >= 0 && statements.indexOf(DISABLE_SECONDARY_ROLES) < application);
  assert.ok(statements.slice(0, statements.indexOf(DISABLE_SECONDARY_ROLES)).every((s) => s.startsWith('ALTER SESSION')));
});

test('a failed disable blocks the query, closes the session, and a later query retries', async () => {
  const fake = fakeSnowflake({ failOn: DISABLE_SECONDARY_ROLES });
  const driver = new SnowflakeReaderDriver({ dataSource: 'default' });
  fake.install(driver);
  await assert.rejects(driver.query('SELECT visible', []), /statement rejected/);
  assert.equal(fake.sessions.length, 1);
  assert.ok(fake.sessions[0]?.destroyed);
  assert.ok(!fake.sessions[0]?.statements.includes('SELECT visible'));
  await assert.rejects(driver.query('SELECT visible', []), /statement rejected/);
  assert.equal(fake.sessions.length, 2);
  assert.ok(fake.sessions.every((s) => s.destroyed && !s.statements.includes('SELECT visible')));
});

test('a reconnection disables secondary roles again', async () => {
  const fake = fakeSnowflake();
  const driver = new SnowflakeReaderDriver({ dataSource: 'default' });
  fake.install(driver);
  await driver.query('SELECT visible', []);
  const first = fake.sessions[0];
  assert.ok(first);
  first.up = false;
  const rows = await driver.query('SELECT visible', []);
  assert.equal(fake.sessions.length, 2);
  assert.ok(fake.sessions[1]?.statements.includes(DISABLE_SECONDARY_ROLES));
  assert.deepEqual(rows, [{ VISIBLE: 'reader marts' }]);
});

test('testConnection uses a reader session and closes it, including on failure', async () => {
  const ok = fakeSnowflake();
  const driver = new SnowflakeReaderDriver({ dataSource: 'default' });
  ok.install(driver);
  await driver.testConnection();
  assert.ok(ok.sessions[0]?.statements.includes(DISABLE_SECONDARY_ROLES));
  assert.ok(ok.sessions[0]?.destroyed);

  const bad = fakeSnowflake({ failOn: DISABLE_SECONDARY_ROLES });
  const failing = new SnowflakeReaderDriver({ dataSource: 'default' });
  bad.install(failing);
  await assert.rejects(failing.testConnection(), /statement rejected/);
  assert.ok(bad.sessions[0]?.destroyed);
});
