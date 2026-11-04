const { test } = require('node:test');
const assert = require('node:assert/strict');

// The caller provisions disposable development fixtures and a dedicated reader
// SERVICE user with its own key, then removes them in a finally block.
// Ordinary checks never open a network connection.
test('development dedicated reader cannot assume writer roles or read restricted data', {
  skip: process.env.CUBE_READER_ACCEPTANCE !== 'development',
  timeout: 60000,
}, async () => {
  assert.equal(process.env.ANALYTICS_ENVIRONMENT, 'development');
  assert.ok(process.env.CUBEJS_DB_NAME);
  assert.equal(process.env.SHOPIFY_CUSTOMERS_ENABLED, 'true');
  const schema = process.env.CUBE_READER_FIXTURE_SCHEMA || '';
  const user = process.env.CUBEJS_DB_USER || '';
  const role = process.env.ANALYTICS_MARTS_READER_ROLE || '';
  const writer = process.env.ANALYTICS_MARTS_WRITER_ROLE || '';
  assert.match(schema, /^CUBE_READER_[A-F0-9]+_MARTS$/);
  assert.match(user, /^CUBE_READER_[A-F0-9]+_USER$/);
  assert.match(role, /^CUBE_READER_[A-F0-9]+_ROLE$/);
  assert.match(writer, /^CUBE_READER_[A-F0-9]+_WRITER$/);
  const config = require('../cube.js');
  assert.equal(typeof config.driverFactory, 'function');
  const driver = await config.driverFactory?.({ dataSource: 'default' });
  assert.ok(driver);
  try {
    const identity = await driver.query('SELECT CURRENT_USER() AS READER_USER, CURRENT_ROLE() AS READER_ROLE', []);
    assert.deepEqual(identity, [{ READER_USER: user, READER_ROLE: role }]);
    assert.deepEqual(await driver.query(`SELECT COUNT(*) AS CUSTOMER_COUNT FROM ${schema}.dim_customers`, []),
      [{ CUSTOMER_COUNT: '2' }]);
    for (const relation of ['dim_customer_details', 'dim_order_contacts', 'raw_customers']) {
      await assert.rejects(driver.query(`SELECT * FROM ${schema}.${relation}`, []));
    }
    await assert.rejects(driver.query(`USE ROLE ${writer}`, []));
    // A separate credential must stay safe even if the client enables all roles.
    await driver.query('USE SECONDARY ROLES ALL', []);
    await assert.rejects(driver.query(`SELECT * FROM ${schema}.dim_customer_details`, []));
    await assert.rejects(driver.query(`SELECT * FROM ${schema}.dim_order_contacts`, []));
    await assert.rejects(driver.query(`CREATE TABLE ${schema}.reader_write_attempt (id INTEGER)`, []));
    assert.deepEqual(await driver.query(`SELECT COUNT(*) AS CUSTOMER_COUNT FROM ${schema}.dim_customers`, []),
      [{ CUSTOMER_COUNT: '2' }]);
  } finally {
    await driver.release();
  }
});
