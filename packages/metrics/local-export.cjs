const assert = require('node:assert/strict');
const { mkdtemp, chmod, rm, realpath } = require('node:fs/promises');
const { join, isAbsolute } = require('node:path');
const { randomUUID } = require('node:crypto');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');
const { compileExposures } = require('./exposures.cjs');

/** Exact member/filter/join-key dependencies of the current local, customers-off model. */
async function requiredLocalColumns() {
  /** @type {Record<string, Set<string>>} */
  const columns = {};
  for (const exposure of await compileExposures(false, undefined, true)) {
    for (const relation of exposure.marts) columns[relation] ??= new Set();
    for (const field of exposure.fields) for (const column of field.columns) {
      const [relation = '', name = ''] = column.split('.');
      columns[relation]?.add(name);
    }
  }
  // Both base SQL predicates read is_test; this is not a public line dimension.
  columns.fct_orders?.add('is_test');
  columns.fct_order_lines?.add('is_test');
  return Object.fromEntries(Object.entries(columns).map(([relation, names]) => [relation, [...names].sort()]));
}

/** Export only approved analytical dependencies to a NEW file; never replace a live file.
 * Source privacy declarations must describe this input, not be inferred from names.
 * @param {string} source @param {string} directory @param {string} schema
 * @param {Record<string, Record<string, boolean>>} privacy
 */
async function exportLocalMarts(source, directory, schema, privacy) {
  assert.match(schema, /^[a-z][a-z0-9_]*_marts$/);
  assert.ok(isAbsolute(source) && isAbsolute(directory));
  assert.equal(await realpath(source), source, 'Source must not contain a symlink');
  assert.equal(await realpath(directory), directory, 'Destination must not contain a symlink');
  const columns = await requiredLocalColumns();
  for (const [relation, names] of Object.entries(columns)) for (const name of names) {
    // publication_id is generated, never copied from the input.
    if (name !== 'publication_id') assert.equal(privacy[relation]?.[name], false,
      `${relation}.${name}: missing or restricted source privacy declaration`);
  }
  const output = await mkdtemp(join(directory, 'serving-'));
  const receipt = { path: join(output, 'marts.duckdb'), publicationId: randomUUID() };
  try {
    // Process exit, not just vendor release(), closes every export handle.
    await promisify(execFile)(process.execPath, [__filename,
      JSON.stringify({ source, schema, columns, ...receipt })], { timeout: 30000 });
    await chmod(receipt.path, 0o400);
    return receipt;
  } catch (error) {
    await rm(output, { recursive: true, force: true });
    throw error;
  }
}

/** @param {{source: string, schema: string, columns: Record<string, string[]>, path: string, publicationId: string}} input */
async function exportFile(input) {
  const { source, schema, columns, path, publicationId } = input;
  const driver = new DuckDBDriver({ databasePath: path });
  try {
    await driver.query(`ATTACH '${source.replaceAll("'", "''")}' AS source_file (READ_ONLY)`);
    await driver.query(`CREATE SCHEMA ${schema}`);
    await driver.query('BEGIN TRANSACTION');
    for (const [relation, names] of Object.entries(columns)) {
      assert.match(relation, /^[a-z][a-z0-9_]*$/);
      names.forEach(name => assert.match(name, /^[a-z][a-z0-9_]*$/));
      const selection = names.filter(name => name !== 'publication_id').map(name => `"${name}"`);
      selection.push(`'${publicationId}'::VARCHAR AS publication_id`);
      await driver.query(`CREATE TABLE ${schema}.${relation} AS SELECT ${selection.join(', ')} FROM source_file.${schema}.${relation}`);
    }
    await driver.query(`CREATE TABLE ${schema}._publication AS SELECT '${publicationId}'::VARCHAR AS publication_id, CURRENT_TIMESTAMP AS published_at`);
    await driver.query('COMMIT');
    await driver.query('CHECKPOINT');
  } finally { await driver.release(); }
}

/** Refuse extra schemas, tables and columns, and missing model dependencies.
 * @param {import('@cubejs-backend/duckdb-driver').DuckDBDriver} driver
 * @param {string} schema
 */
async function checkAnalyticalFile(driver, schema) {
  const columns = await requiredLocalColumns();
  const expected = Object.entries(columns).flatMap(([relation, names]) =>
    [...new Set([...names, 'publication_id'])].map(name => `${schema}.${relation}.${name}`));
  expected.push(`${schema}._publication.publication_id`, `${schema}._publication.published_at`);
  const rows = /** @type {Array<{table_schema: string, table_name: string, column_name: string}>} */ (
    await driver.query("SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE table_catalog = 'analytics_local'"));
  assert.deepEqual(rows.map(row => `${row.table_schema}.${row.table_name}.${row.column_name}`).sort(),
    expected.sort(), 'File must contain exactly the approved analytical columns');
}

module.exports = { exportLocalMarts, requiredLocalColumns, checkAnalyticalFile };
if (require.main === module) {
  exportFile(JSON.parse(process.argv[2] ?? '{}')).catch(error => {
    console.error(error instanceof Error ? error.message : 'Export failed');
    process.exitCode = 1;
  });
}
