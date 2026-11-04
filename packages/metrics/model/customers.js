// @ts-nocheck Cube model DSL: cube, view and member references are compiler globals.
// Resolved by Node from the Cube configuration directory (see publication.cjs).
const { publishedMarts } = require('../publication.cjs');
const { dbtDimensions } = require('../mart-columns.cjs');

// Shopify customers over the published dim_customers mart, as an analytical
// view. The mart holds source customer fields; this cube selects an
// explicit column allowlist. Approved default-address city and postal code are
// included; names, direct contact details, street lines, coordinates, notes,
// tags and metafields never enter a query.
// Add a column here only after a reviewed access decision. Cube has no PII
// access policy yet, so restricted data stays unavailable through Cube.
const marts = publishedMarts(undefined, 'customers');
if (marts.customersEnabled) {

// Descriptions, types and PII flags come from the dbt contract for dim_customers.
const columns = [
  'shopify_shop_id', 'shopify_customer_id', 'created_at', 'updated_at', 'locale', 'account_state',
  'is_email_verified', 'product_subscriber_status', 'is_data_sale_opted_out', 'is_tax_exempt',
  // Shopify's own summaries. The amount is in each customer's currency, so
  // there is no sum measure.
  'shopify_order_count', 'shopify_lifetime_spent_amount', 'shopify_lifetime_spent_currency_code',
  'shopify_last_order_id', 'shopify_predicted_spend_tier', 'shopify_rfm_group',
  'default_address_country_code', 'default_address_country_name', 'default_address_province_code',
  'default_address_province_name', 'default_address_time_zone', 'default_address_city', 'default_address_postal_code',
];

cube('shopify_customers', {
  data_source: marts.dataSource,
  sql: 'SELECT ' + columns.join(', ') + ' FROM ' + marts.table('dim_customers'),
  refreshKey: marts.refreshKey,
  public: false,

  dimensions: {
    customer_key: {
      meta: { pii: false },
      sql: `${CUBE}.shopify_shop_id || '/' || ${CUBE}.shopify_customer_id`, type: 'string', primaryKey: true, public: false,
      description: 'Shop ID and customer ID joined by a slash; unique per customer',
    },
    ...dbtDimensions('dim_customers', columns),
  },

  measures: {
    count: { meta: { pii: false }, type: 'count', description: 'Number of Shopify customers' },
  },
});

view('customers', {
  description: 'Shopify customers, one row per customer, with an allowlist of approved, non-contact fields. '
    + 'Order counts and spending are Shopify summaries, not governed metrics.',
  cubes: [
    { joinPath: shopify_customers, includes: columns.concat('count') },
  ],
});
}
