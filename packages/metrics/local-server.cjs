const assert = require('node:assert/strict');
const { createHmac } = require('node:crypto');
const { setTimeout: delay } = require('node:timers/promises');
const { openLocalMarts } = require('./local-serving.cjs');
const { queryRewrite } = require('./reporting-timezone.cjs');
const { checkAnalyticalFile } = require('./local-export.cjs');

function privateSecret() {
  const secret = process.env.CUBEJS_API_SECRET ?? '';
  assert.ok(secret.length >= 32 && !/fixture|test|example/i.test(secret), 'Supply a fresh private local API secret');
  return secret;
}

/** @param {string} path @param {string} schema @param {string} publicationId */
async function serve(path, schema, publicationId) {
  assert.equal(process.env.ANALYTICS_ENVIRONMENT, 'local', 'Explicit local environment required');
  const secret = privateSecret();
  Object.assign(process.env, { CUBEJS_DB_TYPE: 'duckdb', ANALYTICS_MARTS_SCHEMA: schema,
    GOVERNED_ORDER_METRICS_ENABLED: 'true', SHOPIFY_CUSTOMERS_ENABLED: 'false',
    ANALYTICS_CUSTOMERS_MARTS_SCHEMA: '', CUBEJS_CACHE_AND_QUEUE_DRIVER: 'memory', CUBEJS_TELEMETRY: 'false' });
  const driver = await openLocalMarts(path, schema, publicationId);
  try {
    await checkAnalyticalFile(driver, schema);
    const { CubejsServer } = require('@cubejs-backend/server');
    const server = new CubejsServer({ dbType: 'duckdb', apiSecret: secret, schemaPath: 'model',
      devServer: false, scheduledRefreshTimer: false, driverFactory: () => driver, queryRewrite });
    await server.listen();
    console.log('Local Cube ready; automatic stop after 180 seconds.');
    const stop = async () => { await server.shutdown('SIGTERM'); process.exit(0); };
    process.once('SIGTERM', () => { void stop(); });
    process.once('SIGINT', () => { void stop(); });
    setTimeout(() => { void stop(); }, 180000);
  } catch (error) {
    await driver.release();
    throw error;
  }
}

async function query() {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const payload = Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 60 })).toString('base64url');
  const signature = createHmac('sha256', privateSecret()).update(`${header}.${payload}`).digest('base64url');
  const authorization = `${header}.${payload}.${signature}`;
  /** @param {string[]} measures */
  async function load(measures) {
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
    throw new Error('Local query timed out');
  }
  return { ...await load(['orders.net_sales_usd', 'orders.aov_usd']), ...await load(['order_lines.asp_usd']) };
}

module.exports = { serve, query, privateSecret };
if (require.main === module) {
  const command = process.argv[2];
  const operation = command === 'serve'
    ? serve(process.argv[3] ?? '', process.argv[4] ?? '', process.argv[5] ?? '')
    : command === 'query' ? query().then(result => console.log(JSON.stringify(result)))
      : Promise.reject(new Error('Use serve PATH SCHEMA PUBLICATION_ID, or query'));
  operation.catch(error => { console.error(error); process.exit(1); });
}
