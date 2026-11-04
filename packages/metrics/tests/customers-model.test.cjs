const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdtemp, rm, writeFile } = require('node:fs/promises');
const { tmpdir } = require('node:os');
const { join, relative } = require('node:path');
const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');
const { queryRewrite } = require('../reporting-timezone.cjs');
const { assertDocumented } = require('./documentation.cjs');
const { copyModel } = require('./model-directory.cjs');

const SCHEMA = 'ci_marts';
const PUBLIC = [
  'shopify_shop_id', 'shopify_customer_id', 'created_at', 'updated_at', 'locale', 'account_state',
  'is_email_verified', 'product_subscriber_status', 'is_data_sale_opted_out', 'is_tax_exempt',
  'shopify_order_count', 'shopify_lifetime_spent_amount', 'shopify_lifetime_spent_currency_code',
  'shopify_last_order_id', 'shopify_predicted_spend_tier', 'shopify_rfm_group',
  'default_address_country_code', 'default_address_country_name', 'default_address_province_code',
  'default_address_province_name', 'default_address_time_zone', 'default_address_city', 'default_address_postal_code',
];
// Values that must never appear in any response or generated SQL.
const SECRETS = ['Secret Street 1', 'ada@example.test', '+15550100', 'secret-note', 'secret-tag', 'Lovelace', '47.61'];

/** Published snapshot: dim_customers holds restricted columns the model must not read. */
async function publish(/** @type {string} */ path) {
  const writer = new DuckDBDriver({ databasePath: path });
  try {
    await writer.query(`CREATE SCHEMA ${SCHEMA}`);
    await writer.query(`CREATE TABLE ${SCHEMA}.dim_customers AS
      SELECT * FROM (VALUES
        ('shop-1', '1', '2026-01-01 00:00:00'::timestamp, '2026-02-01 00:00:00'::timestamp, 'en', 'ENABLED', true, 'NONE', false, false,
         2::bigint, '25.50'::decimal(38, 9), 'USD', '11', 'HIGH', 'LOYAL', 'US', 'United States', 'WA', 'Washington', 'America/Los_Angeles',
         'Springfield', 'Ada', 'Lovelace', 'ada@example.test', '+15550100', 'secret-note', 'secret-tag', 'Secret Street 1', '98101', 47.61),
        ('shop-1', '2', '2026-01-02 00:00:00'::timestamp, '2026-02-02 00:00:00'::timestamp, 'fr', 'DISABLED', false, 'NONE', true, true,
         0::bigint, '0'::decimal(38, 9), 'USD', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
        ('shop-2', '1', '2026-01-03 00:00:00'::timestamp, '2026-02-03 00:00:00'::timestamp, 'de', 'ENABLED', true, 'NONE', false, false,
         1::bigint, '5'::decimal(38, 9), 'EUR', '21', NULL, NULL, 'DE', 'Germany', NULL, NULL, 'Europe/Berlin',
         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
      ) t(shopify_shop_id, shopify_customer_id, created_at, updated_at, locale, account_state, is_email_verified,
        product_subscriber_status, is_data_sale_opted_out, is_tax_exempt, shopify_order_count,
        shopify_lifetime_spent_amount, shopify_lifetime_spent_currency_code, shopify_last_order_id,
        shopify_predicted_spend_tier, shopify_rfm_group, default_address_country_code, default_address_country_name,
        default_address_province_code, default_address_province_name, default_address_time_zone,
        default_address_city, first_name, last_name, email_address, phone_number, customer_note, tags, address_line_1,
        default_address_postal_code, latitude)`);
    // Order 1 and 2 belong to shop-1 customer 1; order 3 to shop-2 customer 1; order 4 to no customer row.
    await writer.query(`CREATE TABLE ${SCHEMA}.fct_orders AS
      SELECT shop AS shopify_shop_id, id AS shopify_order_id, '#' || id AS order_name, customer AS shopify_customer_id,
        '2026-07-01 12:00:00'::timestamp AS ordered_at, 'processed_at' AS order_date_source,
        '2026-07-01 12:00:00'::timestamp AS processed_at, NULL::timestamp AS cancelled_at, false AS is_cancelled,
        false AS is_test, NULL::varchar AS cancel_reason, 'web' AS source_name, 'PAID' AS financial_status,
        'FULFILLED' AS fulfillment_status, 'USD' AS presentment_currency_code, 1::bigint AS line_count,
        1::bigint AS gross_ordered_quantity,
        ${['total_price', 'current_total_price', 'subtotal_price', 'total_discounts', 'total_tax', 'total_shipping_price', 'total_refunded', 'original_line_value']
    .map(n => `cast(1 as decimal(38, 9)) as ${n}_shop_amount`).join(', ')}
      FROM (VALUES ('shop-1', '1', '1'), ('shop-1', '2', '1'), ('shop-2', '3', '1'), ('shop-1', '4', '99')) t(shop, id, customer)`);
    // dim_order_contacts is restricted when customers are enabled, so the default
    // reader's snapshot holds approved geography without direct contact details.
    await writer.query(`CREATE TABLE ${SCHEMA}.dim_order_geography AS
      SELECT shopify_shop_id, shopify_order_id, 'WA' AS shipping_address_province_code,
        'US' AS shipping_address_country_code, 'OR' AS billing_address_province_code, 'US' AS billing_address_country_code,
        'Probe Ship City' AS shipping_address_city, '84601-ship' AS shipping_address_zip,
        'Probe Bill City' AS billing_address_city, '98101-bill' AS billing_address_zip
      FROM ${SCHEMA}.fct_orders`);
    await writer.query(`CREATE TABLE ${SCHEMA}._publication AS
      SELECT '00000000-0000-4000-8000-000000000001' AS publication_id, current_timestamp AS published_at`);
    await writer.query('CHECKPOINT');
  } finally { await writer.release(); }
}

for (const independent of [false, true]) {
const testName = `customers view ${independent ? 'independent' : 'legacy'}`;
test(testName, { timeout: 60000 }, async () => {
  if (process.env.CUSTOMERS_CUBE_CHILD !== 'true') {
    const { execFile } = require('node:child_process');
    const { promisify } = require('node:util');
    /** @type {NodeJS.ProcessEnv} */
    const childEnvironment = { ...process.env, CUSTOMERS_CUBE_CHILD: 'true' };
    delete childEnvironment.NODE_TEST_CONTEXT;
    const child = await promisify(execFile)(process.execPath, ['--test-force-exit', '--test', `--test-name-pattern=^${testName}$`, __filename], {
      env: childEnvironment, timeout: 55000,
    }).catch((/** @type {Error & { stdout?: string }} */ error) => {
      throw new Error(`${error.message}\n${error.stdout || ''}`, { cause: error });
    });
    assert.match(child.stdout, /# pass 1/);
    return;
  }
  const directory = await mkdtemp(join(tmpdir(), 'cube-customers-'));
  const path = join(directory, 'marts.duckdb');
  const model = join(directory, 'model');
  const customerPath = join(directory, 'customers.duckdb');
  const { createHmac } = require('node:crypto');
  const { setTimeout: delay } = require('node:timers/promises');
  Object.assign(process.env, {
    CUBEJS_CACHE_AND_QUEUE_DRIVER: 'memory', CUBEJS_TELEMETRY: 'false', PORT: '0',
    CUBEJS_DB_TYPE: 'snowflake', ANALYTICS_MARTS_SCHEMA: SCHEMA,
    SHOPIFY_CUSTOMERS_ENABLED: 'true',
    ANALYTICS_CUSTOMERS_MARTS_SCHEMA: independent ? 'customers_marts' : '',
  });
  const { CubejsServer } = require('@cubejs-backend/server');
  const server = new CubejsServer({
    dbType: 'duckdb', apiSecret: 'offline-customers-test-secret',
    schemaPath: relative(process.cwd(), model), devServer: false, scheduledRefreshTimer: false,
    driverFactory: (context) => new DuckDBDriver({ databasePath: context?.dataSource === 'customers' ? customerPath : path }), queryRewrite,
  });
  try {
    await publish(path);
    if (independent) {
      await publish(customerPath);
      const customersWriter = new DuckDBDriver({ databasePath: customerPath });
      const ordersWriter = new DuckDBDriver({ databasePath: path });
      try {
        await customersWriter.query('CREATE SCHEMA customers_marts');
        await customersWriter.query(`CREATE TABLE customers_marts.dim_customers AS SELECT * FROM ${SCHEMA}.dim_customers`);
        await customersWriter.query(`CREATE TABLE customers_marts._publication AS SELECT * FROM ${SCHEMA}._publication`);
        await customersWriter.query(`DROP SCHEMA ${SCHEMA} CASCADE`);
        await ordersWriter.query(`DROP TABLE ${SCHEMA}.dim_customers`);
      } finally { await customersWriter.release(); await ordersWriter.release(); }
    }
    await copyModel(directory);
    // Test-only view that makes the hidden orders-to-customers join reachable.
    if (!independent) await writeFile(join(model, 'join_probe.js'), `view('join_probe', { cubes: [
      { joinPath: shopify_orders, includes: ['count', 'shopify_shop_id', 'shopify_customer_id'] },
      { joinPath: shopify_orders.shopify_customers, includes: ['locale'], prefix: true },
    ] });`);
    const listening = await server.listen();
    const address = listening.server.address();
    assert.ok(address && typeof address !== 'string');
    const port = address.port;
    const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
    const payload = Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 120 })).toString('base64url');
    const signature = createHmac('sha256', 'offline-customers-test-secret').update(`${header}.${payload}`).digest('base64url');
    /** @param {string} endpoint @param {object} [query] @returns {Promise<{ status: number, body: Record<string, any> }>} */
    async function call(endpoint, query) {
      const search = query ? `?query=${encodeURIComponent(JSON.stringify(query))}` : '';
      const deadline = Date.now() + 20000;
      while (Date.now() < deadline) {
        const response = await fetch(`http://127.0.0.1:${port}/cubejs-api/v1/${endpoint}${search}`, {
          headers: { Authorization: `${header}.${payload}.${signature}` }, signal: AbortSignal.timeout(10000),
        });
        const body = /** @type {Record<string, any>} */ (await response.json());
        if (body.error !== 'Continue wait') return { status: response.status, body };
        await delay(100);
      }
      return assert.fail(`${endpoint} did not answer`);
    }
    /** @param {string} endpoint @param {object} [query] */
    async function get(endpoint, query) {
      const { status, body } = await call(endpoint, query);
      assert.equal(status, 200, JSON.stringify(body));
      return body;
    }

    const meta = await get('meta');
    const metaText = JSON.stringify(meta);
    const customers = meta.cubes.find((/** @type {{ name: string }} */ c) => c.name === 'customers');
    assert.ok(customers, 'customers view is public');
    assertDocumented(meta, ['customers', 'orders']);
    assert.deepEqual(meta.cubes.map((/** @type {{ name: string }} */ c) => c.name).sort(), ['customers', 'data_availability', ...(!independent ? ['join_probe'] : []), 'orders', 'revenue_coverage']);
    // This predecessor fixture has no AOV columns or line relation. Existing
    // order measures must still execute while new consumers are unavailable.
    assert.deepEqual((await get('load', { measures: ['orders.count', 'orders.subtotal_price_usd'] })).data,
      [{ 'orders.count': '4', 'orders.subtotal_price_usd': '4' }]);
    for (const measure of ['orders.aov_usd', 'orders.net_sales_usd', 'order_lines.asp_usd']) {
      assert.notEqual((await call('load', { measures: [measure] })).status, 200);
    }
    assert.deepEqual(customers.dimensions.map((/** @type {{ name: string }} */ d) => d.name).sort(),
      PUBLIC.map(n => `customers.${n}`).sort(), 'dimension allowlist is exact');
    assert.deepEqual(customers.measures.map((/** @type {{ name: string }} */ m) => m.name), ['customers.count'],
      'no money or lifetime measures');
    assert.deepEqual(customers.segments, []);
    for (const restricted of ['first_name', 'last_name', 'email_address', 'phone', 'customer_note', 'tags', 'metafield', 'address_line', 'latitude', 'longitude', 'legacy']) {
      assert.ok(!JSON.stringify(customers).includes(restricted), `${restricted} is not in customers metadata`);
    }

    // Allowlisted data answers; the customer without orders and the same ID in two shops stay separate.
    const rows = (await get('load', {
      dimensions: ['customers.shopify_shop_id', 'customers.shopify_customer_id', 'customers.default_address_province_name',
        'customers.default_address_city', 'customers.default_address_postal_code', 'customers.shopify_lifetime_spent_amount', 'customers.shopify_lifetime_spent_currency_code'],
      order: { 'customers.shopify_shop_id': 'asc', 'customers.shopify_customer_id': 'asc' },
    })).data;
    assert.equal(rows.length, 3);
    assert.deepEqual(rows[0], {
      'customers.shopify_shop_id': 'shop-1', 'customers.shopify_customer_id': '1',
      'customers.default_address_province_name': 'Washington',
      'customers.default_address_city': 'Springfield', 'customers.default_address_postal_code': '98101',
      'customers.shopify_lifetime_spent_amount': '25.5', 'customers.shopify_lifetime_spent_currency_code': 'USD',
    });
    assert.equal(rows[1]['customers.default_address_city'], null);
    assert.equal(rows[1]['customers.default_address_postal_code'], null);
    assert.deepEqual((await get('load', { measures: ['customers.count'] })).data, [{ 'customers.count': '3' }]);

    // The generated SQL reads only allowlisted columns.
    const sql = (await get('sql', { dimensions: PUBLIC.map(n => `customers.${n}`), measures: ['customers.count'] })).sql.sql[0];
    for (const column of ['first_name', 'last_name', 'email_address', 'phone_number', 'customer_note', 'tags', 'address_line_1', 'latitude']) {
      assert.doesNotMatch(sql, new RegExp(column), `${column} is not read`);
    }

    // Guessed restricted members fail without echoing data, on the view, the hidden cube and join paths.
    const guesses = ['first_name', 'last_name', 'display_name', 'email_address', 'phone_number', 'customer_note', 'tags', 'metafields',
      'address_line_1', 'latitude', 'longitude', 'is_email_format_valid'];
    /** @type {string[]} */ const members = [];
    for (const cubeName of ['customers', 'shopify_customers', 'orders', 'join_probe', 'shopify_order_geography', 'shopify_order_contacts']) {
      for (const guess of guesses) members.push(`${cubeName}.${guess}`);
    }
    for (const member of members) {
      for (const query of [{ dimensions: [member] }, { measures: [member] }, { dimensions: ['customers.locale'], filters: [{ member, operator: 'equals', values: ['x'] }] }]) {
        const { status, body } = await call('load', query);
        assert.notEqual(status, 200, member);
        assert.equal(body.data, undefined, member);
        for (const secret of SECRETS) assert.ok(!JSON.stringify(body).includes(secret), `${member} leaks ${secret}`);
      }
    }
    // All approved order geography stays available with customers enabled, and the
    // default reader snapshot has no order-contact table to select PII from.
    const geographySql = (await get('sql', { dimensions: ['orders.shipping_province_code', 'orders.billing_country_code'] })).sql.sql[0];
    assert.match(geographySql, /dim_order_geography/);
    assert.doesNotMatch(geographySql, /dim_order_contacts|_zip|_city/);
    assert.deepEqual((await get('load', { dimensions: ['orders.shipping_province_code'] })).data
      .map((/** @type {Record<string, string | null>} */ r) => r['orders.shipping_province_code']).filter(Boolean), ['WA']);
    const reader = new DuckDBDriver({ databasePath: path });
    try {
      const expected = {
        'orders.shipping_city': 'Probe Ship City', 'orders.shipping_postal_code': '84601-ship',
        'orders.billing_city': 'Probe Bill City', 'orders.billing_postal_code': '98101-bill',
      };
      const geography = Object.keys(expected);
      assert.deepEqual((await get('load', { dimensions: geography })).data, [expected]);
      const sql = (await get('sql', { dimensions: geography })).sql.sql[0];
      assert.match(sql, /dim_order_geography/);
      assert.doesNotMatch(sql, /dim_order_contacts|address1|email|phone/);
      assert.deepEqual(await reader.query(`SELECT shipping_address_city, shipping_address_zip FROM ${SCHEMA}.dim_order_geography LIMIT 1`),
        [{ shipping_address_city: 'Probe Ship City', shipping_address_zip: '84601-ship' }]);
      await assert.rejects(reader.query(`SELECT order_email FROM ${SCHEMA}.dim_order_contacts`), /dim_order_contacts|Catalog Error/i);
    } finally { await reader.release(); }
    // Hidden cube members are denied even when they exist: the allowlisted dimension is not public either.
    assert.notEqual((await call('load', { dimensions: ['shopify_customers.locale'] })).status, 200);
    assert.notEqual((await call('load', { measures: ['shopify_customers.count'] })).status, 200);
    for (const secret of SECRETS) assert.ok(!metaText.includes(secret));
    if (!independent) {
    assert.ok(!JSON.stringify(meta.cubes.find((/** @type {{ name: string }} */ c) => c.name === 'join_probe')).includes('shopify_customers_first'));

    // Orders join customers on shop and customer; one order row stays one row.
    const joined = await get('load', {
      measures: ['join_probe.count'], dimensions: ['join_probe.shopify_shop_id', 'join_probe.shopify_customer_id', 'join_probe.shopify_customers_locale'],
      order: { 'join_probe.shopify_shop_id': 'asc', 'join_probe.shopify_customer_id': 'asc' },
    }).catch(() => null);
    assert.ok(joined, 'join probe answers');
    const total = (await get('load', { measures: ['join_probe.count'] })).data;
    assert.deepEqual(total, [{ 'join_probe.count': '4' }], 'join does not multiply orders');
    assert.deepEqual(joined.data.map((/** @type {Record<string, string>} */ r) =>
      [r['join_probe.shopify_shop_id'], r['join_probe.shopify_customer_id'], r['join_probe.shopify_customers_locale'], r['join_probe.count']]), [
      ['shop-1', '1', 'en', '2'], ['shop-1', '99', null, '1'], ['shop-2', '1', 'de', '1'],
    ], 'shop and customer both match');
    }
  } finally { await server.shutdown('SIGTERM'); await rm(directory, { recursive: true, force: true }); }
});

}
