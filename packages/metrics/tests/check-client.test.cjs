const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const { mkdirSync, mkdtempSync, writeFileSync } = require('node:fs');
const { tmpdir } = require('node:os');
const { dirname, join } = require('node:path');
const { checkClient } = require('../check-client.cjs');

const SCRIPT = join(__dirname, '..', 'check-client.cjs');
const TOOLKIT = join(__dirname, '..', 'model');
const ENV = { ANALYTICS_CLIENT_CONFIG: join(__dirname, 'client.yaml') };

/** @param {Record<string, string>} files @returns {string} */
function folder(files) {
  const root = mkdtempSync(join(tmpdir(), 'metrics-check-'));
  for (const [name, content] of Object.entries(files)) {
    mkdirSync(dirname(join(root, name)), { recursive: true });
    writeFileSync(join(root, name), content);
  }
  return root;
}

/** @param {string} name */
const cubeYaml = name => `cubes:
  - name: ${name}
    sql_table: marts.orders
    description: Test cube
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
        meta: {pii: false}
        description: Test identifier
`;

test('a client model compiles beside the toolkit model', { timeout: 60000 }, async () => {
  const client = folder({ 'client/orders.yml': cubeYaml('client_orders') });
  const summary = await checkClient(ENV, TOOLKIT, client);
  assert.match(summary, /^2 cubes and views compiled from 2 model files \(1 from the client\)/);
  assert.match(summary, /reporting time zone America\/New_York$/);
});

test('a client cube that repeats a toolkit cube name fails, naming both files', { timeout: 60000 }, async () => {
  const client = folder({ 'extra.yml': cubeYaml('placeholder') });
  await assert.rejects(checkClient(ENV, TOOLKIT, client), error => {
    assert.ok(error instanceof Error);
    assert.match(error.message, /cube placeholder is defined more than once/);
    assert.ok(error.message.includes(join(client, 'extra.yml')), error.message);
    return true;
  });
});

test('a client model that does not compile fails', { timeout: 60000 }, async () => {
  const client = folder({ 'broken.yml': 'cubes:\n  - name: broken\n    sql_table: t\n    measures:\n      - name: n\n        type: no_such_type\n' });
  await assert.rejects(checkClient(ENV, TOOLKIT, client));
});

test('an invalid client.yaml fails before the model compiles, naming the file', async () => {
  const config = join(folder({ 'client.yaml': 'version: 1\nreporting_time_zone: Mars/Olympus_Mons\n' }), 'client.yaml');
  await assert.rejects(checkClient({ ANALYTICS_CLIENT_CONFIG: config }, TOOLKIT, folder({})), error => {
    assert.ok(error instanceof Error);
    assert.ok(error.message.startsWith(`${config}: reporting_time_zone`), error.message);
    return true;
  });
});

test('the command exits 0 when the checks pass and 1 with the reason when one fails', { timeout: 60000 }, () => {
  const passed = spawnSync(process.execPath, [SCRIPT], { env: { ...process.env, ...ENV }, encoding: 'utf8' });
  assert.equal(passed.status, 0, passed.stderr);
  assert.match(passed.stdout, /^Cube client check passed: .*reporting time zone America\/New_York/);
  const missing = join(folder({}), 'client.yaml');
  const failed = spawnSync(process.execPath, [SCRIPT], { env: { ...process.env, ANALYTICS_CLIENT_CONFIG: missing }, encoding: 'utf8' });
  assert.equal(failed.status, 1);
  assert.match(failed.stderr, /^Cube client check failed: .*client configuration is unreadable/);
});
