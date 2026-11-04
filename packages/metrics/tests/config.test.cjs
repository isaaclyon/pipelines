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

test('ordinary service startup rejects governed consumers on Snowflake in every environment', () => {
  for (const environment of ['local', 'ci', 'development', 'production']) {
    for (const flag of ['true', 'invalid']) {
      const result = spawnSync(process.execPath, ['-e', "require('./cube.js')"], {
        env: { ...process.env, ANALYTICS_ENVIRONMENT: environment, CUBEJS_DB_TYPE: 'snowflake',
          GOVERNED_ORDER_METRICS_ENABLED: flag },
      });
      assert.notEqual(result.status, 0, `${environment}/${flag} must fail before startup`);
    }
  }
});

for (const [environment, backend, succeeds] of cases) {
  test(`configuration ${environment}/${backend}`, () => {
    const result = spawnSync(process.execPath, ['-e', "require('./cube.js')"], {
      env: { ...process.env, ANALYTICS_ENVIRONMENT: environment, CUBEJS_DB_TYPE: backend },
    });
    assert.equal(result.status === 0, succeeds, result.stderr.toString());
  });
}
