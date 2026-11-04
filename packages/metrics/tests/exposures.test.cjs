const { test } = require('node:test');
const assert = require('node:assert/strict');
const { existsSync, readFileSync } = require('node:fs');
const { join } = require('node:path');
const { renderExposures } = require('../exposures.cjs');

// The check image copies the transformations file beside the package; a
// checkout reads it from the sibling package.
const CHECKED_IN = [
  join(__dirname, '..', 'dbt-exposures.yml'),
  join(__dirname, '..', '..', 'transformations', 'dbt', 'models', 'exposures', '_cube__exposures.yml'),
].find(path => existsSync(path));

test('checked-in dbt exposures match the Cube model', { timeout: 60000 }, async () => {
  assert.ok(CHECKED_IN, 'dbt exposures file is missing');
  assert.equal(await renderExposures(), readFileSync(CHECKED_IN, 'utf8'),
    'Cube views changed: run scripts/platform local exposures and commit the result');
});

test('disabled governed consumers introduce no new model or exposure dependencies', { timeout: 60000 }, async () => {
  const { compileExposures } = require('../exposures.cjs');
  const views = await compileExposures(false);
  assert.ok(!views.some(view => view.view === 'order_lines'));
  const fields = views.flatMap(view => view.fields);
  assert.ok(!fields.some(field => /aov_|asp_|orders\.net_sales_usd/.test(field.field)));
  assert.ok(!fields.some(field => field.columns.some(column =>
    /is_aov_eligible|aov_exclusion_reasons|aov_unresolved_requirements|fct_order_lines\./.test(column))));
  assert.ok(fields.some(field => field.field === 'orders.net_revenue_usd'));
});

test('revenue lineage includes the eligibility filter', { timeout: 60000 }, async () => {
  const { compileExposures } = require('../exposures.cjs');
  const views = await compileExposures(false, undefined, true);
  const orders = views.find(view => view.view === 'orders');
  assert.ok(orders);
  const revenue = orders.fields.find(field => field.field === 'orders.net_revenue_usd');
  assert.ok(revenue);
  assert.deepEqual(revenue.columns, ['fct_orders.net_revenue_shop_amount', 'fct_orders.is_revenue_eligible']);
  const aov = orders.fields.find(field => field.field === 'orders.aov_usd');
  assert.deepEqual(aov?.columns, ['fct_orders.net_revenue_shop_amount', 'fct_orders.is_aov_eligible']);
  const lines = views.find(view => view.view === 'order_lines');
  const asp = lines?.fields.find(field => field.field === 'order_lines.asp_usd');
  assert.deepEqual(asp?.columns, ['fct_order_lines.net_merchandise_shop_amount',
    'fct_order_lines.is_asp_eligible', 'fct_order_lines.net_ordered_quantity']);
});
