const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const { mkdtempSync, writeFileSync } = require('node:fs');
const { tmpdir } = require('node:os');
const { join } = require('node:path');
const { reportingTimeZone, queryRewrite } = require('../reporting-timezone.cjs');

const CLIENT_CONFIG = join(__dirname, 'client.yaml');

/** @type {Array<[string | undefined, string, boolean]>} */
const cases = [
  ['local', 'duckdb', false],
  ['ci', 'duckdb', false],
  ['local', 'snowflake', true],
  ['ci', 'snowflake', true],
  ['development', 'snowflake', true],
  ['production', 'snowflake', true],
  ['production', 'duckdb', false],
  ['unexpected', 'snowflake', false],
  [undefined, 'duckdb', false],
  ['toString', 'duckdb', false],
];

/** @param {Record<string, string | undefined>} env */
function startCube(env) {
  return spawnSync(process.execPath, ['-e', "require('./cube.js')"], { env: { ...process.env, ...env } });
}

for (const [environment, backend, succeeds] of cases) {
  test(`configuration ${environment}/${backend}`, () => {
    const result = startCube({
      ANALYTICS_ENVIRONMENT: environment, CUBEJS_DB_TYPE: backend, ANALYTICS_CLIENT_CONFIG: CLIENT_CONFIG,
    });
    assert.equal(result.status === 0, succeeds, result.stderr.toString());
  });
}

test('startup fails without a readable client configuration', () => {
  const missing = join(mkdtempSync(join(tmpdir(), 'metrics-')), 'client.yaml');
  const result = startCube({ ANALYTICS_ENVIRONMENT: 'local', CUBEJS_DB_TYPE: 'snowflake', ANALYTICS_CLIENT_CONFIG: missing });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr.toString(), new RegExp(`${missing}: client configuration is unreadable`));
});

test('the reporting time zone comes from client.yaml', () => {
  assert.equal(reportingTimeZone({ ANALYTICS_CLIENT_CONFIG: CLIENT_CONFIG }), 'America/New_York');
});

test('queries use the reporting time zone, replacing a client timezone', async () => {
  const rewrite = queryRewrite('America/New_York');
  const rewritten = await rewrite({ measures: ['placeholder.count'], timezone: 'UTC' }, { securityContext: {}, requestId: 'test' });
  assert.equal(rewritten.timezone, 'America/New_York');
});

test('client.yaml is read from /etc/analytics/client.yaml by default', () => {
  assert.throws(() => reportingTimeZone({}), /^Error: \/etc\/analytics\/client\.yaml: client configuration is unreadable/);
  assert.throws(() => reportingTimeZone({ ANALYTICS_CLIENT_CONFIG: ' ' }), /^Error: \/etc\/analytics\/client\.yaml:/);
});

test('ANALYTICS_CLIENT_CONFIG must be an absolute path', () => {
  assert.throws(() => reportingTimeZone({ ANALYTICS_CLIENT_CONFIG: 'client.yaml' }), /ANALYTICS_CLIENT_CONFIG must be an absolute path/);
});

/** @type {Array<[string, string, RegExp]>} */
const invalid = [
  ['unparseable YAML', 'version: 1\nreporting_time_zone: [America/New_York\n', /file is not valid YAML/],
  ['not a mapping', '- version: 1\n', /file must be a mapping/],
  ['missing version', 'reporting_time_zone: America/New_York\n', /version must be 1/],
  ['another version', 'version: 2\nreporting_time_zone: America/New_York\n', /version must be 1/],
  ['version as text', 'version: "1"\nreporting_time_zone: America/New_York\n', /version must be 1/],
  ['missing time zone', 'version: 1\n', /reporting_time_zone must be nonempty text/],
  ['blank time zone', 'version: 1\nreporting_time_zone: " "\n', /reporting_time_zone must be nonempty text/],
  ['time zone as a number', 'version: 1\nreporting_time_zone: 5\n', /reporting_time_zone must be nonempty text/],
  ['unknown time zone', 'version: 1\nreporting_time_zone: Mars/Olympus_Mons\n', /reporting_time_zone must be an IANA time zone/],
];

for (const [name, content, message] of invalid) {
  test(`client.yaml with ${name} fails, naming the file`, () => {
    const path = join(mkdtempSync(join(tmpdir(), 'metrics-')), 'client.yaml');
    writeFileSync(path, content);
    assert.throws(() => reportingTimeZone({ ANALYTICS_CLIENT_CONFIG: path }), error => {
      assert.ok(error instanceof Error);
      assert.ok(error.message.startsWith(`${path}: `), error.message);
      assert.match(error.message, message);
      return true;
    });
  });
}
