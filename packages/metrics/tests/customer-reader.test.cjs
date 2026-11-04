const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');

/** @type {Array<[string, string, string, boolean]>} */
const cases = [
  ['', 'loader', 'loader', false],
  ['loader', 'loader', 'loader', false],
  ['SYSADMIN', 'SYSADMIN', 'loader', false],
  ['analytics_reader', 'other_reader', 'loader', false],
  ['analytics_reader', 'analytics_reader', 'loader', true],
];

for (const [reader, effective, writer, succeeds] of cases) {
  test(`customer access requires a separate restricted reader: ${reader}/${effective}`, () => {
    const result = spawnSync(process.execPath, ['-e', "require('./cube.js')"], {
      timeout: 10000,
      env: {
        ...process.env,
        ANALYTICS_ENVIRONMENT: 'development',
        CUBEJS_DB_TYPE: 'snowflake',
        SHOPIFY_CUSTOMERS_ENABLED: 'true',
        ANALYTICS_MARTS_READER_ROLE: reader,
        ANALYTICS_MARTS_WRITER_ROLE: writer,
        CUBEJS_DB_SNOWFLAKE_ROLE: effective,
        CUBEJS_DB_USER: 'cube_reader',
        ANALYTICS_MARTS_WRITER_USER: 'loader',
        ANALYTICS_MARTS_WRITER_KEY_HOST_PATH: '/secrets/writer.pem',
        ANALYTICS_MARTS_READER_KEY_HOST_PATH: '/secrets/reader.pem',
        CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH: '/run/secrets/snowflake_reader_private_key',
      },
    });
    assert.equal(result.status === 0, succeeds, result.stderr.toString());
  });
}

test('enabled customers use the secondary-role restricted driver through Cube configuration', () => {
  const result = spawnSync(process.execPath, ['-e', `
    const assert = require('node:assert/strict');
    const { SnowflakeReaderDriver } = require('./snowflake-reader-driver.cjs');
    const config = require('./cube.js');
    assert.equal(typeof config.driverFactory, 'function');
    assert.ok(config.driverFactory({ dataSource: 'default' }) instanceof SnowflakeReaderDriver);
  `], {
    timeout: 10000,
    env: {
      ...process.env,
      ANALYTICS_ENVIRONMENT: 'development',
      CUBEJS_DB_TYPE: 'snowflake',
      SHOPIFY_CUSTOMERS_ENABLED: 'true',
      ANALYTICS_MARTS_READER_ROLE: 'analytics_reader',
      ANALYTICS_MARTS_WRITER_ROLE: 'loader',
      CUBEJS_DB_SNOWFLAKE_ROLE: 'analytics_reader',
      CUBEJS_DB_USER: 'cube_reader',
      ANALYTICS_MARTS_WRITER_USER: 'loader',
      ANALYTICS_MARTS_WRITER_KEY_HOST_PATH: '/secrets/writer.pem',
      ANALYTICS_MARTS_READER_KEY_HOST_PATH: '/secrets/reader.pem',
      CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH: '/run/secrets/snowflake_reader_private_key',
    },
  });
  assert.equal(result.status, 0, result.stderr.toString());
});

/** @type {Array<[string, Record<string, string>]>} */
const identityCases = [
  ['missing reader user', { CUBEJS_DB_USER: '' }],
  ['shared user', { CUBEJS_DB_USER: 'LOADER' }],
  ['missing writer user', { ANALYTICS_MARTS_WRITER_USER: '' }],
  ['shared key', { ANALYTICS_MARTS_READER_KEY_HOST_PATH: '/secrets/writer.pem' }],
  ['missing reader key', { ANALYTICS_MARTS_READER_KEY_HOST_PATH: '' }],
  ['writer key mount', { CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH: '/run/secrets/snowflake_private_key' }],
];
for (const [label, overrides] of identityCases) {
  test(`customer access rejects ${label}`, () => {
    const result = spawnSync(process.execPath, ['-e', "require('./cube.js')"], {
      timeout: 10000,
      env: {
        ...process.env,
        ANALYTICS_ENVIRONMENT: 'development', CUBEJS_DB_TYPE: 'snowflake',
        SHOPIFY_CUSTOMERS_ENABLED: 'true',
        ANALYTICS_MARTS_READER_ROLE: 'analytics_reader',
        ANALYTICS_MARTS_WRITER_ROLE: 'loader',
        CUBEJS_DB_SNOWFLAKE_ROLE: 'analytics_reader',
        CUBEJS_DB_USER: 'cube_reader', ANALYTICS_MARTS_WRITER_USER: 'loader',
        ANALYTICS_MARTS_WRITER_KEY_HOST_PATH: '/secrets/writer.pem',
        ANALYTICS_MARTS_READER_KEY_HOST_PATH: '/secrets/reader.pem',
        CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH: '/run/secrets/snowflake_reader_private_key',
        ...overrides,
      },
    });
    assert.notEqual(result.status, 0, label);
    assert.match(result.stderr.toString(), /separate Snowflake reader identity and key/);
  });
}

test('order-only Snowflake rejects an incomplete reader migration', () => {
  const result = spawnSync(process.execPath, ['-e', "require('./cube.js')"], {
    timeout: 10000,
    env: {
      ...process.env,
      ANALYTICS_ENVIRONMENT: 'development', CUBEJS_DB_TYPE: 'snowflake',
      SHOPIFY_CUSTOMERS_ENABLED: 'false',
      ANALYTICS_MARTS_READER_ROLE: '',
      ANALYTICS_MARTS_WRITER_ROLE: 'loader',
      CUBEJS_DB_SNOWFLAKE_ROLE: 'loader',
      CUBEJS_DB_USER: 'cube_reader', ANALYTICS_MARTS_WRITER_USER: 'loader',
      ANALYTICS_MARTS_WRITER_KEY_HOST_PATH: '/secrets/writer.pem',
      ANALYTICS_MARTS_READER_KEY_HOST_PATH: '/secrets/reader.pem',
      CUBEJS_DB_SNOWFLAKE_PRIVATE_KEY_PATH: '/run/secrets/snowflake_reader_private_key',
    },
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr.toString(), /separate restricted Snowflake reader role/);
});
