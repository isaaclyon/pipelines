const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdtemp, rm, chmod } = require('node:fs/promises');
const { join } = require('node:path');
const { tmpdir } = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { randomBytes } = require('node:crypto');
const { openLocalMarts } = require('../local-serving.cjs');
const { exportLocalMarts, requiredLocalColumns, checkAnalyticalFile } = require('../local-export.cjs');

test('local export contains only required analytical columns and refuses unknown privacy', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'local-export-'));
  try {
    const source = join(directory, 'working.duckdb');
    await promisify(execFile)(process.execPath, ['-e',
      "require('./tests/orders-fixture.cjs').publish(process.argv[1]).catch(e => { console.error(e); process.exit(1); })", source], { timeout: 10000 });
    const columns = await requiredLocalColumns();
    // Authored synthetic fixture provenance. Real inputs need their own evidence.
    const privacy = Object.fromEntries(Object.entries(columns).map(([relation, names]) =>
      [relation, Object.fromEntries(names.map(name => [name, false]))]));
    const receipt = await exportLocalMarts(source, directory, 'ci_marts', privacy);
    const driver = await openLocalMarts(receipt.path, 'ci_marts', receipt.publicationId);
    try {
      await checkAnalyticalFile(driver, 'ci_marts');
      const rows = /** @type {Array<{table_name: string, column_name: string}>} */ (
        await driver.query("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'ci_marts'"));
      for (const row of rows) {
        if (row.table_name === '_publication') continue;
        assert.ok(row.column_name === 'publication_id' || columns[String(row.table_name)]?.includes(String(row.column_name)));
      }
      assert.deepEqual(await driver.query('SELECT count(*) AS n FROM ci_marts.fct_orders'), [{ n: '4' }]);
    } finally { await driver.release(); }
    // A polluted file must fail startup even when its marker and facts are valid.
    const polluted = await exportLocalMarts(source, directory, 'ci_marts', privacy);
    await chmod(polluted.path, 0o600);
    await promisify(execFile)(process.execPath, ['-e',
      "const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver'); const d = new DuckDBDriver({databasePath:process.argv[1]}); d.query('ALTER TABLE ci_marts.fct_orders ADD COLUMN private_email VARCHAR').then(()=>d.release()).catch(e=>{console.error(e);process.exit(1);})", polluted.path], { timeout: 10000 });
    await assert.rejects(promisify(execFile)(process.execPath,
      ['local-server.cjs', 'serve', polluted.path, 'ci_marts', polluted.publicationId], {
        timeout: 10000, env: { ...process.env, ANALYTICS_ENVIRONMENT: 'local',
          CUBEJS_API_SECRET: randomBytes(32).toString('hex') },
      }), /exactly the approved analytical columns/);
    assert.ok(privacy.fct_orders);
    delete privacy.fct_orders.net_revenue_shop_amount;
    await assert.rejects(exportLocalMarts(source, directory, 'ci_marts', privacy), /privacy/);
    privacy.fct_orders.net_revenue_shop_amount = true;
    await assert.rejects(exportLocalMarts(source, directory, 'ci_marts', privacy), /privacy/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});
