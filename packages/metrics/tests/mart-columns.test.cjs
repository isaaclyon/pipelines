const { test } = require('node:test');
const assert = require('node:assert/strict');
const { contractColumns, dbtDimensions } = require('../mart-columns.cjs');
const { compileExposures } = require('../exposures.cjs');

// dbt owns the meaning of published columns (ADR 0014, 2026-10-22 addendum).
// Cubes whose direct-column dimensions read their metadata from dbt:
const FROM_DBT = ['shopify_customers', 'shopify_orders', 'shopify_order_lines'];
const KINDS = { string: 'string dimension', number: 'number dimension', boolean: 'boolean dimension', time: 'time dimension' };

test('direct-column dimensions take description, type and PII from the dbt contract', { timeout: 60000 }, async () => {
  for (const enabled of [false, true]) for (const governed of [false, true]) {
    const inventory = (await compileExposures(enabled, undefined, governed)).find(exposure => exposure.isInventory);
    assert.ok(inventory);
    const checked = inventory.fields.filter(field => FROM_DBT.includes(field.field.split('.')[0] ?? '')
      && field.directColumn && field.directColumn.split('.')[1] === field.field.split('.')[1]);
    assert.ok(checked.length > (enabled ? 50 : 25), 'expected the customers and orders dimensions');
    for (const field of checked) {
      const [mart = '', name = ''] = String(field.directColumn).split('.');
      const column = contractColumns(mart).get(name);
      assert.ok(column, field.field);
      const [dimension] = Object.values(dbtDimensions(mart, [name]));
      assert.equal(field.description, column.description, field.field);
      assert.equal(field.pii, column.pii, field.field);
      assert.equal(field.kind, KINDS[/** @type {keyof typeof KINDS} */ (dimension?.type)], field.field);
    }
  }
});

test('dbt types map to Cube types and unknown columns or types fail', () => {
  const [locale, created, count, amount, verified] = Object.values(dbtDimensions('dim_customers',
    ['locale', 'created_at', 'shopify_order_count', 'shopify_lifetime_spent_amount', 'is_email_verified']));
  assert.deepEqual([locale, created, count, amount, verified].map(item => item?.type),
    ['string', 'time', 'number', 'number', 'boolean']);
  assert.equal(locale?.sql(), 'locale');
  assert.deepEqual({ ...locale, sql: undefined }, {
    sql: undefined, type: 'string', meta: { pii: false },
    description: contractColumns('dim_customers').get('locale')?.description,
  });
  assert.throws(() => dbtDimensions('dim_customers', ['not_a_column']), /dim_customers\.not_a_column.*contract/);
  assert.throws(() => dbtDimensions('missing_mart', ['locale']), /missing_mart.*contract/);
  // Restricted marts hold personal data the Cube reader cannot see.
  assert.throws(() => dbtDimensions('dim_customer_details', ['shopify_shop_id']), /dim_customer_details.*restricted/);
});
