const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdtemp, rm } = require('node:fs/promises');
const { tmpdir } = require('node:os');
const { join, relative } = require('node:path');
const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');
const { queryRewrite } = require('../reporting-timezone.cjs');
const { publishedMarts } = require('../publication.cjs');
const { assertDocumented } = require('./documentation.cjs');
const { copyModel } = require('./model-directory.cjs');

const SCHEMA = 'ci_marts';

test('published marts resolve in Snowflake and reject unsafe names', () => {
  const marts = publishedMarts({ CUBEJS_DB_TYPE: 'snowflake', ANALYTICS_MARTS_SCHEMA: SCHEMA });
  assert.equal(marts.table('fct_orders'), 'ci_marts.fct_orders');
  assert.equal(marts.refreshKey.sql(), 'SELECT publication_id FROM ci_marts._publication');
  assert.throws(() => publishedMarts({ CUBEJS_DB_TYPE: 'snowflake' }), /schema/);
  assert.throws(() => publishedMarts({ CUBEJS_DB_TYPE: 'duckdb', ANALYTICS_MARTS_SCHEMA: SCHEMA }), /backend/);
  assert.throws(() => marts.table('fct_orders; drop'), /mart name/);
});

const { publish } = require('./orders-fixture.cjs');

test('the orders view joins contacts only when needed and reports Pacific days', { timeout: 60000 }, async () => {
  if (process.env.ORDERS_CUBE_CHILD !== 'true') {
    const { execFile } = require('node:child_process');
    const { promisify } = require('node:util');
    // Cube retains background timers after shutdown; bound a dedicated process
    // as tests/publication.test.cjs does.
    /** @type {NodeJS.ProcessEnv} */
    const childEnvironment = { ...process.env, ORDERS_CUBE_CHILD: 'true' };
    delete childEnvironment.NODE_TEST_CONTEXT;
    const child = await promisify(execFile)(process.execPath, ['--test-force-exit', '--test', '--test-name-pattern=orders view', __filename], {
      env: childEnvironment, timeout: 55000,
    }).catch((/** @type {Error & { stdout?: string }} */ error) => {
      throw new Error(`${error.message}\n${error.stdout || ''}`, { cause: error });
    });
    assert.match(child.stdout, /# pass 1/);
    return;
  }
  const directory = await mkdtemp(join(tmpdir(), 'cube-orders-'));
  const path = join(directory, 'marts.duckdb');
  const model = join(directory, 'model');
  const { createHmac } = require('node:crypto');
  const { setTimeout: delay } = require('node:timers/promises');
  Object.assign(process.env, {
    CUBEJS_CACHE_AND_QUEUE_DRIVER: 'memory', CUBEJS_TELEMETRY: 'false', PORT: '0',
    CUBEJS_DB_TYPE: 'duckdb', ANALYTICS_ENVIRONMENT: 'local',
    GOVERNED_ORDER_METRICS_ENABLED: 'true', ANALYTICS_MARTS_SCHEMA: SCHEMA,
  });
  const { CubejsServer } = require('@cubejs-backend/server');
  const fixtureDriver = new DuckDBDriver({ databasePath: path });
  const server = new CubejsServer({
    dbType: 'duckdb', apiSecret: 'offline-orders-test-secret',
    schemaPath: relative(process.cwd(), model), devServer: false, scheduledRefreshTimer: false,
    driverFactory: () => fixtureDriver, queryRewrite,
  });
  try {
    await publish(path);
    await copyModel(directory);
    const listening = await server.listen();
    const address = listening.server.address();
    assert.ok(address && typeof address !== 'string');
    const port = address.port;
    const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
    const payload = Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 120 })).toString('base64url');
    const signature = createHmac('sha256', 'offline-orders-test-secret').update(`${header}.${payload}`).digest('base64url');
    /** @param {string} endpoint @param {object} [query] */
    async function get(endpoint, query) {
      const search = query ? `?query=${encodeURIComponent(JSON.stringify(query))}` : '';
      const deadline = Date.now() + 20000;
      while (Date.now() < deadline) {
        const response = await fetch(`http://127.0.0.1:${port}/cubejs-api/v1/${endpoint}${search}`, {
          headers: { Authorization: `${header}.${payload}.${signature}` }, signal: AbortSignal.timeout(10000),
        });
        const body = /** @type {Record<string, any>} */ (await response.json());
        if (body.error !== 'Continue wait') {
          assert.equal(response.status, 200, JSON.stringify(body));
          return body;
        }
        await delay(100);
      }
      return assert.fail(`${endpoint} did not answer`);
    }

    const meta = await get('meta');
    assert.deepEqual(meta.cubes.map((/** @type {{ name: string }} */ c) => c.name).sort(), ['data_availability', 'order_lines', 'orders', 'revenue_coverage'],
      'only views are public');
    assertDocumented(meta, ['data_availability', 'order_lines', 'orders', 'revenue_coverage']);

    const availability = { dimensions: ['data_availability.publication_id', 'data_availability.shopify_shop_id',
      'data_availability.environment', 'data_availability.is_loop_snapshot_complete'] };
    assert.deepEqual((await get('load', availability)).data, [{
      'data_availability.publication_id': '00000000-0000-4000-8000-000000000001',
      'data_availability.shopify_shop_id': 'shop-1', 'data_availability.environment': 'ci',
      'data_availability.is_loop_snapshot_complete': false,
    }]);
    assert.doesNotMatch((await get('sql', availability)).sql.sql[0], /\bfct_orders\b/);
    // Raw time dimensions retain UTC source instants. They are not Pacific
    // calendar-day buckets, despite the reporting-timezone query rewrite.
    assert.deepEqual((await get('load', { dimensions: [
      'data_availability.shopify_coverage_started_at',
      'data_availability.shopify_coverage_ended_at',
      'data_availability.loop_snapshot_ended_at',
    ] })).data, [{
      'data_availability.shopify_coverage_started_at': '2026-06-01T00:00:00.000',
      'data_availability.shopify_coverage_ended_at': '2026-07-02T07:00:00.000',
      'data_availability.loop_snapshot_ended_at': null,
    }]);

    const dimensions = meta.cubes.find((/** @type {{ name: string }} */ c) => c.name === 'orders').dimensions.map((/** @type {{ name: string }} */ d) => d.name);
    for (const name of ['shopify_shop_id', 'shopify_order_id', 'shopify_customer_id']) {
      assert.ok(dimensions.includes(`orders.${name}`), `${name} is a public source identity`);
    }
    for (const name of ['shop_id', 'order_id', 'customer_id']) {
      assert.ok(!dimensions.includes(`orders.${name}`), `${name} does not imply an analytical identity`);
    }
    assert.deepEqual((await get('load', {
      dimensions: ['orders.shopify_shop_id', 'orders.shopify_order_id', 'orders.shipping_province_code'],
      order: { 'orders.shopify_order_id': 'asc' },
    })).data, [
      { 'orders.shopify_shop_id': 'shop-1', 'orders.shopify_order_id': '1', 'orders.shipping_province_code': 'UT' },
      { 'orders.shopify_shop_id': 'shop-1', 'orders.shopify_order_id': '2', 'orders.shipping_province_code': 'UT' },
      { 'orders.shopify_shop_id': 'shop-1', 'orders.shopify_order_id': '3', 'orders.shipping_province_code': 'CA' },
    ]);

    const totals = { measures: ['orders.count', 'orders.gross_ordered_quantity', 'orders.total_price_usd'] };
    const sql = (await get('sql', totals)).sql.sql[0];
    assert.match(sql, /fct_orders/);
    assert.doesNotMatch(sql, /dim_order_contacts/, 'order totals skip the geography join');
    assert.deepEqual((await get('load', totals)).data, [{
      'orders.count': '3', 'orders.gross_ordered_quantity': '8', 'orders.total_price_usd': '30.338333333',
    }]);

    // The generation must be read in the metric SQL, not attached by a later request.
    const withPublication = { ...totals, dimensions: ['orders.publication_id'] };
    assert.match((await get('sql', withPublication)).sql.sql[0], /publication_id/);
    assert.deepEqual((await get('load', withPublication)).data, [{
      'orders.publication_id': '00000000-0000-4000-8000-000000000001',
      'orders.count': '3', 'orders.gross_ordered_quantity': '8', 'orders.total_price_usd': '30.338333333',
    }]);

    const byProvince = { ...totals, dimensions: ['orders.shipping_province_code'], order: { 'orders.shipping_province_code': 'asc' } };
    const provinceSql = (await get('sql', byProvince)).sql.sql[0];
    assert.match(provinceSql, /dim_order_contacts/, 'legacy deployments keep reading the existing snapshot');
    assert.doesNotMatch(provinceSql, /_zip|_city|address1|email/, 'only country and province columns are read');
    assert.deepEqual((await get('load', byProvince)).data, [
      { 'orders.shipping_province_code': 'CA', 'orders.count': '1', 'orders.gross_ordered_quantity': '4', 'orders.total_price_usd': '20' },
      { 'orders.shipping_province_code': 'UT', 'orders.count': '2', 'orders.gross_ordered_quantity': '4', 'orders.total_price_usd': '10.338333333' },
    ]);

    // Approved public geography stays available in both customer flag states.
    const legacy = ['shipping_city', 'billing_city', 'shipping_postal_code', 'billing_postal_code'];
    for (const member of legacy) assert.ok(dimensions.includes(`orders.${member}`));
    assert.deepEqual((await get('load', { dimensions: legacy.map(name => `orders.${name}`) })).data, [{
      'orders.shipping_city': 'Probe Ship City', 'orders.billing_city': 'Probe Bill City',
      'orders.shipping_postal_code': '84601-ship', 'orders.billing_postal_code': '98101-bill',
    }]);

    // Unknown channel evidence stays null rather than counting as not D2C.
    for (const name of ['is_d2c', 'is_commercial', 'is_bulk', 'is_gifted', 'has_classification_evidence']) {
      assert.ok(dimensions.includes(`orders.${name}`), `${name} is a public flag`);
    }
    assert.deepEqual((await get('load', {
      measures: ['orders.count'], dimensions: ['orders.is_d2c', 'orders.is_bulk', 'orders.has_classification_evidence'],
      order: { 'orders.is_d2c': 'asc' },
    })).data, [
      { 'orders.is_d2c': false, 'orders.is_bulk': true, 'orders.has_classification_evidence': true, 'orders.count': '1' },
      { 'orders.is_d2c': true, 'orders.is_bulk': false, 'orders.has_classification_evidence': true, 'orders.count': '1' },
      { 'orders.is_d2c': null, 'orders.is_bulk': null, 'orders.has_classification_evidence': false, 'orders.count': '1' },
    ]);

    // Reporting buckets are not source geography; draft markers are explicit.
    assert.deepEqual((await get('load', {
      measures: ['orders.count'], dimensions: ['orders.reporting_market', 'orders.is_draft_order'],
      order: { 'orders.reporting_market': 'asc' },
    })).data, [
      { 'orders.reporting_market': 'CA', 'orders.is_draft_order': true, 'orders.count': '1' },
      { 'orders.reporting_market': 'US', 'orders.is_draft_order': false, 'orders.count': '2' },
    ]);
    assert.deepEqual((await get('load', {
      measures: ['orders.count'], filters: [
        { member: 'orders.reporting_market', operator: 'equals', values: ['US'] },
        { member: 'orders.is_draft_order', operator: 'equals', values: ['false'] },
        { member: 'orders.is_d2c', operator: 'equals', values: ['true'] },
      ],
    })).data, [{ 'orders.count': '1' }]);

    // Every governed component uses the same eligible population. The test
    // order is eligible in the fixture but still excluded by the orders view.
    const revenue = (await get('load', { measures: [
      'orders.gross_sales_usd', 'orders.discounts_usd', 'orders.merchandise_refunds_usd',
      'orders.shipping_revenue_usd', 'orders.shipping_refunds_usd', 'orders.net_revenue_usd',
    ] })).data;
    assert.deepEqual(revenue, [{
      'orders.gross_sales_usd': '100', 'orders.discounts_usd': '-10',
      'orders.merchandise_refunds_usd': '-20', 'orders.shipping_revenue_usd': '8',
      'orders.shipping_refunds_usd': '-2', 'orders.net_revenue_usd': '76',
    }]);
    const lineMeta = meta.cubes.find((/** @type {{ name: string }} */ c) => c.name === 'order_lines');
    assert.ok(!lineMeta.measures.some((/** @type {{ name: string }} */ m) => /aov/.test(m.name)),
      'whole-order AOV is not a line reporting measure');
    // Line ASP divides sums (38 / 4), not the average of line prices.
    // Negative activity and a full amount-only refund retain their governed units.
    const lineTotals = { measures: ['order_lines.asp_usd', 'order_lines.net_sales_usd',
      'order_lines.net_ordered_quantity', 'order_lines.asp_net_ordered_quantity',
      'order_lines.unresolved_revenue_count', 'order_lines.unresolved_quantity_count'] };
    const lineResult = (await get('load', lineTotals)).data[0];
    assert.equal(Number(lineResult['order_lines.asp_usd']).toFixed(6), '9.500000');
    assert.equal(lineResult['order_lines.net_sales_usd'], '28');
    assert.equal(lineResult['order_lines.net_ordered_quantity'], '6');
    assert.equal(lineResult['order_lines.asp_net_ordered_quantity'], '4');
    assert.equal(lineResult['order_lines.unresolved_revenue_count'], '1');
    assert.equal(lineResult['order_lines.unresolved_quantity_count'], '1');
    assert.deepEqual((await get('load', { measures: ['order_lines.asp_usd'], filters: [
      { member: 'order_lines.shopify_order_id', operator: 'equals', values: ['3'] },
    ] })).data, [{ 'order_lines.asp_usd': null }]);
    assert.deepEqual((await get('load', { measures: ['order_lines.net_ordered_quantity'], filters: [
      { member: 'order_lines.is_d2c', operator: 'equals', values: ['true'] },
      { member: 'order_lines.is_draft_order', operator: 'equals', values: ['false'] },
      { member: 'order_lines.reporting_market', operator: 'equals', values: ['US'] },
    ] })).data, [{ 'order_lines.net_ordered_quantity': '4' }]);
    for (const [line, asp] of [['2', '0'], ['3', null], ['4', '6']]) {
      assert.deepEqual((await get('load', { measures: ['order_lines.asp_usd'], filters: [
        { member: 'order_lines.shopify_order_id', operator: 'equals', values: ['2'] },
        { member: 'order_lines.shopify_line_item_id', operator: 'equals', values: [line] },
      ] })).data, [{ 'order_lines.asp_usd': asp }]);
    }
    assert.deepEqual((await get('load', { measures: ['order_lines.eligible_asp_count',
      'order_lines.excluded_asp_count', 'order_lines.unresolved_asp_count'] })).data, [{
      'order_lines.eligible_asp_count': '5', 'order_lines.excluded_asp_count': '2',
      'order_lines.unresolved_asp_count': '2',
    }]);
    assert.doesNotMatch((await get('sql', { measures: ['orders.aov_usd'] })).sql.sql[0], /fct_order_lines/,
      'whole-order AOV never expands into line rows');
    assert.doesNotMatch((await get('sql', lineTotals)).sql.sql[0], /JOIN/i,
      'line-only totals do not join orders');

    // AOV uses its own order population, not revenue eligibility or line count.
    assert.deepEqual((await get('load', { measures: [
      'orders.net_sales_usd', 'orders.aov_usd', 'orders.aov_order_count',
    ] })).data, [{
      'orders.net_sales_usd': '70', 'orders.aov_usd': '48', 'orders.aov_order_count': '2',
    }]);
    assert.deepEqual((await get('load', { measures: ['orders.aov_usd'], filters: [
      { member: 'orders.shopify_order_id', operator: 'equals', values: ['3'] },
    ] })).data, [{ 'orders.aov_usd': null }], 'unknown eligibility cannot supply a zero denominator');

    // Coverage deliberately retains the test order and distinguishes unknown.
    assert.deepEqual((await get('load', { measures: [
      'revenue_coverage.count', 'revenue_coverage.eligible_count',
      'revenue_coverage.excluded_count', 'revenue_coverage.unresolved_count',
    ] })).data, [{
      'revenue_coverage.count': '4', 'revenue_coverage.eligible_count': '2',
      'revenue_coverage.excluded_count': '1', 'revenue_coverage.unresolved_count': '1',
    }]);

    assert.deepEqual((await get('load', { measures: [
      'revenue_coverage.aov_eligible_count', 'revenue_coverage.aov_excluded_count',
      'revenue_coverage.aov_unresolved_count',
    ] })).data, [{
      'revenue_coverage.aov_eligible_count': '2', 'revenue_coverage.aov_excluded_count': '1',
      'revenue_coverage.aov_unresolved_count': '1',
    }]);

    // Days are Pacific even when a client asks for UTC: order 2 is 30 June.
    const daily = { measures: ['orders.count'], timeDimensions: [{ dimension: 'orders.order_date', granularity: 'day' }],
      order: { 'orders.order_date.day': 'asc' }, timezone: 'UTC' };
    assert.deepEqual((await get('load', daily)).data.map((/** @type {Record<string, string>} */ row) =>
      [row['orders.order_date.day'], row['orders.count']]), [
      ['2026-03-08T00:00:00.000', '1'], ['2026-06-30T00:00:00.000', '1'], ['2026-07-01T00:00:00.000', '1'],
    ]);
    const oneDay = { measures: ['orders.count'],
      timeDimensions: [{ dimension: 'orders.order_date', dateRange: ['2026-07-01', '2026-07-01'] }] };
    assert.deepEqual((await get('load', oneDay)).data, [{ 'orders.count': '1' }]);

    // Mutate a synthetic fixture in place to check marker semantics, not publication.
    await fixtureDriver.query(`UPDATE ${SCHEMA}._publication SET publication_id = 'generation-2'`);
    await fixtureDriver.query(`UPDATE ${SCHEMA}.fct_orders SET publication_id = 'generation-2'`);
    await fixtureDriver.query(`UPDATE ${SCHEMA}.fct_data_availability SET publication_id = 'generation-2'`);
    await fixtureDriver.query(`UPDATE ${SCHEMA}.fct_orders SET total_price_shop_amount = 1 WHERE NOT is_test`);
    await fixtureDriver.query('CHECKPOINT');
    const deadline = Date.now() + 10000;
    let observedNew = false;
    while (Date.now() < deadline) {
      const result = (await get('load', withPublication)).data;
      assert.equal(result.length, 1);
      const row = result[0];
      if (row['orders.publication_id'] === 'generation-2') {
        assert.equal(row['orders.total_price_usd'], '3');
        observedNew = true;
        break;
      }
      assert.equal(row['orders.publication_id'], '00000000-0000-4000-8000-000000000001');
      assert.equal(row['orders.total_price_usd'], '30.338333333');
      await delay(200);
    }
    assert.ok(observedNew, 'new generation and facts eventually arrive together');
    // Ordinary consumer queries need no generation fields or pair coordination.
    // Polling here verifies cache convergence, not an application retry contract.
    const totalsDeadline = Date.now() + 10000;
    let totalsConverged = false;
    while (Date.now() < totalsDeadline) {
      const data = (await get('load', totals)).data;
      assert.equal(data.length, 1);
      assert.equal(data[0]['orders.publication_id'], undefined);
      if (data[0]['orders.total_price_usd'] === '3') {
        totalsConverged = true;
        break;
      }
      assert.equal(data[0]['orders.total_price_usd'], '30.338333333');
      await delay(200);
    }
    assert.ok(totalsConverged, 'ordinary cached totals eventually converge without publication dimensions');
    assert.deepEqual((await get('load', { dimensions: [
      'data_availability.shopify_shop_id', 'data_availability.environment',
      'data_availability.is_loop_snapshot_complete',
    ] })).data, [{
      'data_availability.shopify_shop_id': 'shop-1', 'data_availability.environment': 'ci',
      'data_availability.is_loop_snapshot_complete': false,
    }], 'availability is independently queryable without a publication ID');
    assert.deepEqual((await get('load', { ...withPublication, timeDimensions: [
      { dimension: 'orders.order_date', dateRange: ['2020-01-01', '2020-01-01'] },
    ] })).data, [], 'empty periods do not invent zero or a fact generation');

    // Corrupt marker cardinality must not double-count orders.
    await fixtureDriver.query(`INSERT INTO ${SCHEMA}._publication SELECT * FROM ${SCHEMA}._publication`);
    await fixtureDriver.query('CHECKPOINT');
    assert.deepEqual((await get('load', { ...availability,
      filters: [{ member: 'data_availability.environment', operator: 'equals', values: ['ci'] }],
    })).data, [], 'an uncached status query rejects duplicate markers');
    assert.deepEqual((await get('load', { ...withPublication,
      filters: [{ member: 'orders.shopify_order_id', operator: 'equals', values: ['1'] }],
    })).data, [{
      'orders.publication_id': 'generation-2', 'orders.count': '1',
      'orders.gross_ordered_quantity': '3', 'orders.total_price_usd': '1',
    }]);
    await fixtureDriver.query(`DELETE FROM ${SCHEMA}._publication`);
    await fixtureDriver.query('CHECKPOINT');
    assert.deepEqual((await get('load', { ...availability,
      filters: [{ member: 'data_availability.shopify_shop_id', operator: 'equals', values: ['shop-1'] }],
    })).data, [], 'a missing marker cannot certify availability');
    await fixtureDriver.query(`INSERT INTO ${SCHEMA}._publication VALUES ('generation-2', NULL)`);
    await fixtureDriver.query('CHECKPOINT');
    assert.deepEqual((await get('load', { ...availability,
      dimensions: [...availability.dimensions, 'data_availability.is_loop_enabled'],
    })).data, [], 'a marker without a publication timestamp cannot certify availability');
  } finally { await server.shutdown('SIGTERM'); await rm(directory, { recursive: true, force: true }); }
});

test('the reporting zone comes from ANALYTICS_REPORTING_TIME_ZONE', () => {
  const { execFileSync } = require('node:child_process');
  const zone = execFileSync(process.execPath, ['-e', "process.stdout.write(require('./reporting-timezone.cjs').REPORTING_TIMEZONE)"],
    { cwd: join(__dirname, '..'), env: { ...process.env, ANALYTICS_REPORTING_TIME_ZONE: 'Europe/London' } }).toString();
  assert.equal(zone, 'Europe/London');
});

test('reporting queries use the reporting zone and availability uses UTC', async () => {
  /** @type {any} */ const context = {};
  assert.equal((await queryRewrite({ measures: [] }, context)).timezone, 'America/Los_Angeles');
  assert.equal((await queryRewrite({ measures: [], timezone: 'UTC' }, context)).timezone, 'America/Los_Angeles');
  assert.equal((await queryRewrite({ measures: [], dimensions: ['data_availability.shopify_coverage_ended_at'],
    timezone: 'America/Los_Angeles' }, context)).timezone, 'UTC');
  assert.equal((await queryRewrite({ measures: [], timeDimensions: [{ dimension: 'data_availability.evidence_captured_at',
    granularity: 'hour' }] }, context)).timezone, 'UTC');
  await assert.rejects(queryRewrite({ measures: ['orders.net_revenue_usd'],
    dimensions: ['data_availability.publication_id'] }, context), /separately/);
});
