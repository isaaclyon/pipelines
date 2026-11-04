const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');

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

for (const [environment, backend, succeeds] of cases) {
  test(`configuration ${environment}/${backend}`, () => {
    const result = spawnSync(process.execPath, ['-e', "require('./cube.js')"], {
      env: { ...process.env, ANALYTICS_ENVIRONMENT: environment, CUBEJS_DB_TYPE: backend },
    });
    assert.equal(result.status === 0, succeeds, result.stderr.toString());
  });
}

test('queries use the reporting time zone, replacing a client timezone', async () => {
  const { REPORTING_TIMEZONE, queryRewrite } = require('../reporting-timezone.cjs');
  const rewritten = await queryRewrite({ measures: ['placeholder.count'], timezone: 'UTC' }, { securityContext: {}, requestId: 'test' });
  assert.equal(rewritten.timezone, REPORTING_TIMEZONE);
});
