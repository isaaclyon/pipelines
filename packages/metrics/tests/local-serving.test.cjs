const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdtemp, rm, symlink, writeFile } = require('node:fs/promises');
const { join } = require('node:path');
const { tmpdir } = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');
const { publish, SCHEMA } = require('./orders-fixture.cjs');
const { openLocalMarts } = require('../local-serving.cjs');
const GENERATION = '00000000-0000-4000-8000-000000000001';

test('local startup validates a closed fixture and denies writes', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'local-startup-'));
  const path = join(directory, "analytical'copy.duckdb");
  try {
    await publish(path);
    const driver = await openLocalMarts(path, SCHEMA, GENERATION);
    try {
      assert.deepEqual(await driver.query(`SELECT COUNT(*) AS n FROM ${SCHEMA}.fct_orders`), [{ n: '4' }]);
      await assert.rejects(driver.query(`DELETE FROM ${SCHEMA}.fct_orders`), /read.only/i);
    } finally { await driver.release(); }
    await assert.rejects(openLocalMarts(path, SCHEMA, 'wrong-generation'), /publication/);
    await assert.rejects(openLocalMarts(path, 'missing_marts', GENERATION), /does not exist|not found/i);
    await assert.rejects(openLocalMarts(join(directory, 'missing.duckdb'), SCHEMA, GENERATION));
    await symlink(path, join(directory, 'link.duckdb'));
    await assert.rejects(openLocalMarts(join(directory, 'link.duckdb'), SCHEMA, GENERATION), /symlink/);
    const writer = new DuckDBDriver({ databasePath: path });
    try { await writer.query(`DELETE FROM ${SCHEMA}._publication`); }
    finally { await writer.release(); }
    await assert.rejects(openLocalMarts(path, SCHEMA, GENERATION), /publication/);
    const broken = join(directory, 'broken.duckdb');
    await writeFile(broken, 'not a DuckDB file');
    await assert.rejects(openLocalMarts(broken, SCHEMA, GENERATION));
    const missingRelation = join(directory, 'missing-relation.duckdb');
    await publish(missingRelation);
    const incomplete = new DuckDBDriver({ databasePath: missingRelation });
    try { await incomplete.query(`DROP TABLE ${SCHEMA}.fct_orders`); }
    finally { await incomplete.release(); }
    await assert.rejects(openLocalMarts(missingRelation, SCHEMA, GENERATION), /does not exist|not found/i);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('demo and lifecycle entry points reject weak auth before accessing a file', async () => {
  for (const entry of ['tests/local-demo.cjs', 'local-server.cjs']) {
    await assert.rejects(promisify(execFile)(process.execPath, [entry, 'serve'], {
      timeout: 10000, env: { ...process.env, ANALYTICS_ENVIRONMENT: 'local', CUBEJS_API_SECRET: 'short' },
    }), /Supply a fresh private/);
  }
  await assert.rejects(promisify(execFile)(process.execPath, ['local-server.cjs', 'serve'], {
    timeout: 10000, env: { ...process.env, ANALYTICS_ENVIRONMENT: 'development' },
  }), /Explicit local environment required/);
});
