const { test } = require('node:test');
const assert = require('node:assert/strict');
const { publicationRefreshKey, publishedMarts } = require('../publication.cjs');

test('serving uses the Snowflake publication marker only', () => {
  assert.equal(publicationRefreshKey('analytics_marts').sql(),
    'SELECT publication_id FROM analytics_marts._publication');
  assert.equal(publicationRefreshKey('analytics_marts').every, '1 second');
  assert.throws(() => publishedMarts({ CUBEJS_DB_TYPE: 'duckdb', ANALYTICS_MARTS_SCHEMA: 'analytics_marts' }), /backend/);
  assert.throws(() => publicationRefreshKey('unsafe.schema'));
});

test('governed order consumers require explicit local DuckDB opt-in', () => {
  const base = { CUBEJS_DB_TYPE: 'snowflake', ANALYTICS_MARTS_SCHEMA: 'analytics_marts' };
  for (const environment of ['local', 'ci', 'development', 'production']) {
    for (const flag of [undefined, 'false']) {
      assert.equal(publishedMarts({ ...base, ANALYTICS_ENVIRONMENT: environment,
        GOVERNED_ORDER_METRICS_ENABLED: flag }).governedOrdersEnabled, false);
    }
    assert.throws(() => publishedMarts({ ...base, ANALYTICS_ENVIRONMENT: environment,
      GOVERNED_ORDER_METRICS_ENABLED: 'true' }), /local DuckDB/);
    assert.throws(() => publishedMarts({ ...base, ANALYTICS_ENVIRONMENT: environment,
      GOVERNED_ORDER_METRICS_ENABLED: 'yes' }), /true or false/);
  }
  for (const environment of ['ci', 'development', 'production']) {
    assert.throws(() => publishedMarts({ ...base, ANALYTICS_ENVIRONMENT: environment,
      CUBEJS_DB_TYPE: 'duckdb', GOVERNED_ORDER_METRICS_ENABLED: 'true' }), /local DuckDB/);
  }
  assert.equal(publishedMarts({ ...base, ANALYTICS_ENVIRONMENT: 'local',
    CUBEJS_DB_TYPE: 'duckdb', GOVERNED_ORDER_METRICS_ENABLED: 'true' }).governedOrdersEnabled, true);
});

test('customer routing and refresh use only the customer publication', () => {
  const { publishedMarts } = require('../publication.cjs');
  const env = { CUBEJS_DB_TYPE: 'snowflake', ANALYTICS_MARTS_SCHEMA: 'orders_marts', ANALYTICS_CUSTOMERS_MARTS_SCHEMA: 'customers_marts' };
  const orders = publishedMarts(env);
  const customers = publishedMarts(env, 'customers');
  assert.equal(orders.table('fct_orders'), 'orders_marts.fct_orders');
  assert.equal(customers.table('dim_customers'), 'customers_marts.dim_customers');
  assert.equal(customers.refreshKey.sql(), 'SELECT publication_id FROM customers_marts._publication');
  assert.equal(customers.dataSource, 'customers');
  assert.equal(orders.customerJoinEnabled, false);
  assert.throws(() => publishedMarts({ ...env, ANALYTICS_CUSTOMERS_MARTS_SCHEMA: 'orders_marts' }, 'customers'), /distinct/);
});
