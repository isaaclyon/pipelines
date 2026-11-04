const assert = require('node:assert/strict');
const { realpath, stat } = require('node:fs/promises');
const { isAbsolute } = require('node:path');
const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');

/** Open a validated local analytical file with the unchanged vendor driver.
 * @param {string} path
 * @param {string} schema
 * @param {string} publicationId Expected generation from the completed export.
 */
async function openLocalMarts(path, schema, publicationId) {
  assert.ok(isAbsolute(path), 'Local analytical path must be absolute');
  assert.equal(await realpath(path), path, 'Local analytical path must not contain a symlink');
  assert.ok((await stat(path)).isFile(), 'Local analytical path must be a file');
  assert.match(schema, /^[a-z][a-z0-9_]*_marts$/);
  assert.ok(publicationId.trim(), 'Expected publication must not be empty');
  const driver = new DuckDBDriver({ databasePath: ':memory:',
    initSql: `ATTACH '${path.replaceAll("'", "''")}' AS analytics_local (READ_ONLY); USE analytics_local;` });
  try {
    // initSql failures are swallowed upstream. Do not return a driver until
    // its exact attachment and real relations are verified, never SELECT 1.
    assert.deepEqual(await driver.query("SELECT database_name, path, readonly FROM duckdb_databases() WHERE database_name = 'analytics_local'"),
      [{ database_name: 'analytics_local', path, readonly: true }], 'Invalid local attachment');
    assert.deepEqual(await driver.query('SELECT current_database() AS database_name, current_schema() AS schema_name'),
      [{ database_name: 'analytics_local', schema_name: 'main' }], 'Invalid local default catalog');
    assert.deepEqual(await driver.query(`SELECT publication_id FROM analytics_local.${schema}._publication WHERE published_at IS NOT NULL`),
      [{ publication_id: publicationId }], 'Invalid local publication');
    assert.deepEqual(await driver.query(`SELECT COUNT(*) AS n FROM analytics_local.${schema}._publication`),
      [{ n: '1' }], 'Invalid local publication row count');
    await driver.query(`SELECT publication_id FROM analytics_local.${schema}.fct_orders LIMIT 0`);
    return driver;
  } catch (error) {
    await driver.release();
    throw error;
  }
}

module.exports = { openLocalMarts };
