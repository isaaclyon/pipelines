// Disposable deterministic demonstration, never a real-data serving command.
const assert = require('node:assert/strict');
const { createHmac } = require('node:crypto');
const { setTimeout: delay } = require('node:timers/promises');
const { openLocalMarts } = require('../local-serving.cjs');
const { publish, SCHEMA } = require('./orders-fixture.cjs');
const { queryRewrite } = require('../reporting-timezone.cjs');
const FILE = '/fixture/marts.duckdb';

async function main() {
  const command = process.argv[2];
  if (command === 'prepare') {
    // CREATE SCHEMA fails if this file already holds a fixture. Never overwrite.
    await publish(FILE);
    console.log('Created synthetic orders/lines fixture; no source data accessed.');
    return;
  }
  const secret = process.env.CUBEJS_API_SECRET ?? '';
  assert.ok(secret.length >= 32 && !/fixture|test|example/i.test(secret), 'Supply a fresh private demo API secret');
  if (command === 'query') {
    const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
    const payload = Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 60 })).toString('base64url');
    const signature = createHmac('sha256', secret).update(`${header}.${payload}`).digest('base64url');
    const authorization = `${header}.${payload}.${signature}`;
    /** @param {string[]} measures */
    async function query(measures) {
      const deadline = Date.now() + 30000;
      while (Date.now() < deadline) {
        const response = await fetch('http://127.0.0.1:4000/cubejs-api/v1/load?query=' +
          encodeURIComponent(JSON.stringify({ measures })), { headers: { authorization }, signal: AbortSignal.timeout(5000) });
        const result = await response.json();
        if (result.error === 'Continue wait') { await delay(100); continue; }
        assert.equal(response.status, 200, JSON.stringify(result));
        assert.ok(!result.error, JSON.stringify(result));
        return result.data[0];
      }
      throw new Error('Demo query timed out');
    }
    const orders = await query(['orders.net_sales_usd', 'orders.aov_usd']);
    const lines = await query(['order_lines.asp_usd']);
    assert.equal(Number(orders['orders.net_sales_usd']), 70);
    assert.equal(Number(orders['orders.aov_usd']), 48);
    assert.equal(Number(lines['order_lines.asp_usd']), 9.5);
    console.log(JSON.stringify({ data: 'DETERMINISTIC FIXTURE — NOT REAL SALES',
      net_sales_usd: 70, aov_usd: 48, asp_usd: 9.5, expected_values_match: true }, null, 2));
    return;
  }
  assert.equal(command, 'serve', 'Use prepare, serve, or query');
  Object.assign(process.env, { ANALYTICS_ENVIRONMENT: 'local', CUBEJS_DB_TYPE: 'duckdb',
    ANALYTICS_MARTS_SCHEMA: SCHEMA, GOVERNED_ORDER_METRICS_ENABLED: 'true',
    SHOPIFY_CUSTOMERS_ENABLED: 'false', ANALYTICS_CUSTOMERS_MARTS_SCHEMA: '',
    CUBEJS_CACHE_AND_QUEUE_DRIVER: 'memory', CUBEJS_TELEMETRY: 'false' });
  const driver = await openLocalMarts(FILE, SCHEMA, '00000000-0000-4000-8000-000000000001');
  try {
    const { CubejsServer } = require('@cubejs-backend/server');
    const server = new CubejsServer({ dbType: 'duckdb', apiSecret: secret,
      schemaPath: 'model', devServer: false, scheduledRefreshTimer: false,
      driverFactory: () => driver, queryRewrite });
    await server.listen();
    console.log('Synthetic local Cube ready on port 4000; automatic stop after 180 seconds.');
    const stop = async () => { await server.shutdown('SIGTERM'); process.exit(0); };
    process.once('SIGTERM', () => { void stop(); });
    process.once('SIGINT', () => { void stop(); });
    setTimeout(() => { void stop(); }, 180000);
  } catch (error) {
    await driver.release();
    throw error;
  }
}
main().catch(error => { console.error(error); process.exit(1); });
