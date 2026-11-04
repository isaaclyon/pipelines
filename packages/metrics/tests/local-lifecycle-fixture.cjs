// Synthetic-only input generator for scripts/test-local-cube.sh.
const { writeFile } = require('node:fs/promises');
const { join } = require('node:path');
const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');
const { publish } = require('./orders-fixture.cjs');
const { requiredLocalColumns } = require('../local-export.cjs');
async function main() {
  const directory = process.argv[2] ?? '';
  const path = join(directory, 'source.duckdb');
  await publish(path);
  const driver = new DuckDBDriver({ databasePath: path });
  try {
    await driver.query("CREATE SCHEMA raw; CREATE TABLE raw.private_data AS SELECT 'synthetic-private' AS email");
    await driver.query("ALTER TABLE ci_marts.fct_orders ADD COLUMN private_email VARCHAR DEFAULT 'synthetic-private'");
  } finally { await driver.release(); }
  // These declarations apply ONLY to authored deterministic fixture columns.
  const privacy = Object.fromEntries(Object.entries(await requiredLocalColumns()).map(([relation, names]) =>
    [relation, Object.fromEntries(names.map(name => [name, false]))]));
  await writeFile(join(directory, 'privacy.json'), JSON.stringify(privacy));
  console.log('Synthetic source and privacy fixture created; process exit closes all source handles.');
}
main().catch(error => { console.error(error); process.exit(1); });
