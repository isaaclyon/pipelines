// @ts-nocheck Cube model DSL: cube, view and member references are compiler globals.
// Resolved by Node from the Cube configuration directory (see publication.cjs).
const { publishedMarts } = require('../publication.cjs');
const { dbtDimensions } = require('../mart-columns.cjs');

// Shopify orders over the published fct_orders mart and an order-geography mart.
// Consumers query the `orders` view; the cubes stay hidden so the two-table
// split is invisible. Cube joins the geography mart only when a query uses one
// of its fields.
const marts = publishedMarts();

// Shop amounts are in the shop currency, which dbt tests require to be USD.
// Presentment amounts mix currencies, so they are not summed.
const usd = {
  total_price: { description: 'Sum of order totals including tax and shipping, before later refunds', pii: false },
  current_total_price: { description: 'Sum of order totals after edits, returns and refunds', pii: false },
  subtotal_price: { description: 'Sum of line subtotals after discounts, before shipping and tax', pii: false },
  total_discounts: { description: 'Sum of discounts applied to orders', pii: false },
  total_tax: { description: 'Sum of tax charged on orders; orders before extraction revision 2 add nothing', pii: false },
  total_shipping_price: { description: 'Sum of shipping charged after shipping discounts; orders before extraction revision 2 add nothing', pii: false },
  total_refunded: { description: 'Sum of amounts refunded including tax; a reconciliation input, not a commercial refund measure', pii: false },
  original_line_value: { description: 'Sum of quantity times original unit price, before discounts, edits or refunds; not gross sales', pii: false },
};

// fct_orders columns exposed under their own names. Descriptions, types and
// PII flags come from the dbt contract. The classification flags follow: D2C
// is the residual of the commercial rules; retail, specialty and bulk are
// exclusive subchannels; the other characteristics can overlap. Null flags
// mean the evidence is unavailable, not false; filter on true or false.
const orderColumns = [
  'publication_id', 'shopify_shop_id', 'shopify_order_id', 'order_name', 'shopify_customer_id',
  'order_date_source', 'processed_at', 'cancelled_at', 'is_cancelled', 'cancel_reason', 'source_name',
  'reporting_market', 'is_draft_order', 'is_revenue_eligible', 'revenue_exclusion_reasons',
  'revenue_unresolved_requirements',
  ...(marts.governedOrdersEnabled ? ['is_aov_eligible', 'aov_exclusion_reasons', 'aov_unresolved_requirements'] : []),
  'financial_status', 'fulfillment_status', 'presentment_currency_code',
  'has_classification_evidence', 'is_d2c', 'is_commercial', 'is_retail', 'is_specialty', 'is_bulk',
  'is_employee_order', 'is_gifted', 'is_replacement', 'is_exchange', 'is_shopify_collective', 'is_broadcast',
];

// Test orders are excluded from every query.
cube('shopify_orders', {
  sql: 'SELECT * FROM ' + marts.table('fct_orders') + ' WHERE NOT is_test',
  refreshKey: marts.refreshKey,
  public: false,

  joins: {
    // Customers are keyed by shop and customer; a customer row can have many orders.
    ...(marts.customersEnabled && marts.customerJoinEnabled ? {
      shopify_customers: {
        relationship: 'many_to_one',
        sql: `${CUBE}.shopify_shop_id = ${shopify_customers}.shopify_shop_id
          AND ${CUBE}.shopify_customer_id = ${shopify_customers}.shopify_customer_id`,
      },
    } : {}),
    shopify_order_geography: {
      relationship: 'one_to_one',
      sql: `${CUBE}.shopify_shop_id = ${shopify_order_geography}.shopify_shop_id
        AND ${CUBE}.shopify_order_id = ${shopify_order_geography}.shopify_order_id`,
    },
  },

  dimensions: {
    order_key: {
      meta: { pii: false },
      sql: `${CUBE}.shopify_shop_id || '/' || ${CUBE}.shopify_order_id`, type: 'string', primaryKey: true, public: false,
      description: 'Shop ID and order ID joined by a slash; unique per order',
    },
    // The UTC instant behind the mart's order_date. Cube groups it into days in
    // the query timezone, which reporting-timezone.cjs fixes to the reporting time zone.
    order_date: { meta: { pii: false }, sql: 'ordered_at', type: 'time', description: 'Order date in the reporting time zone; processed_at, or created_at when missing' },
    ...dbtDimensions('fct_orders', orderColumns),
  },

  measures: {
    ...Object.fromEntries([
      ['gross_sales', 'gross_merchandise', 'Gross merchandise sales, excluding gift-card sale value', false],
      ['discounts', 'merchandise_discount', 'Signed negative merchandise discounts', false],
      ['merchandise_refunds', 'merchandise_refund', 'Signed negative merchandise refunds, restated on original order date', false],
      ['shipping_revenue', 'shipping', 'Shipping charged after shipping discounts', false],
      ['shipping_refunds', 'shipping_refund', 'Signed negative shipping refunds, restated on original order date', false],
      ...(marts.governedOrdersEnabled ? [['net_sales', 'net_merchandise', 'Gross merchandise sales plus signed discounts and merchandise refunds, excluding shipping', false]] : []),
      ['net_revenue', 'net_revenue', 'Gross sales plus signed discounts and merchandise refunds plus shipping and signed shipping refunds', false],
    ].map(([name, column, description, pii]) => [name + '_usd', {
      meta: { pii },
      sql: column + '_shop_amount', type: 'sum', format: 'currency',
      filters: [{ sql: `${CUBE}.is_revenue_eligible` }],
      description: description + '. USD, eligible resolved orders only; excludes tests, tax and separate handling fees. Unknown evidence contributes no amount; inspect revenue_coverage',
    }])),
    ...(marts.governedOrdersEnabled ? {
    aov_revenue_usd: {
      meta: { pii: false }, sql: 'net_revenue_shop_amount', type: 'sum', format: 'currency',
      filters: [{ sql: `${CUBE}.is_aov_eligible` }],
      description: 'USD net revenue of explicitly AOV-eligible orders, including partial refunds; unknown eligibility contributes no amount',
    },
    aov_order_count: {
      meta: { pii: false }, type: 'count', filters: [{ sql: `${CUBE}.is_aov_eligible` }],
      description: 'Number of explicitly AOV-eligible whole orders, excluding tests and unresolved eligibility',
    },
    aov_usd: {
      meta: { pii: false }, type: 'number', format: 'currency',
      sql: `${aov_revenue_usd} / NULLIF(${aov_order_count}, 0)`,
      description: 'USD average order value: summed AOV-eligible net revenue divided by eligible whole-order count; null for a zero denominator',
    },
    } : {}),
    count: { meta: { pii: false }, type: 'count', description: 'Number of orders, excluding test orders' },
    line_count: { meta: { pii: false }, sql: 'line_count', type: 'sum', description: 'Number of order lines, including removed and refunded lines' },
    gross_ordered_quantity: {
      meta: { pii: false },
      sql: 'gross_ordered_quantity', type: 'sum', description: 'Units ordered before cancellations, removals and refunds',
    },
    ...Object.fromEntries(Object.entries(usd).map(([name, { description, pii }]) => [name + '_usd', {
      meta: { pii },
      sql: name + '_shop_amount', type: 'sum', format: 'currency', description: description + '. In USD',
    }])),
  },
});

// Both configurations expose approved city, postal, province and country data.
// With customers enabled, dim_order_geography keeps direct contact details out
// of the analytical reader. Order-only deployments read dim_order_contacts.
cube('shopify_order_geography', {
  sqlTable: marts.table(marts.customersEnabled ? 'dim_order_geography' : 'dim_order_contacts'),
  refreshKey: marts.refreshKey,
  public: false,

  dimensions: {
    order_key: {
      meta: { pii: false },
      sql: `${CUBE}.shopify_shop_id || '/' || ${CUBE}.shopify_order_id`, type: 'string', primaryKey: true, public: false,
      description: 'Shop ID and order ID joined by a slash; unique per order',
    },
    ...Object.fromEntries(['shipping', 'billing'].flatMap(kind => [
      ['province_code', 'province_code', 'province or state code', false],
      ['country_code', 'country_code', 'ISO country code', false],
      ['city', 'city', 'city', false],
      ['postal_code', 'zip', 'postal code', false],
    ].map(([name, column, label, pii]) => [kind + '_' + name, {
      meta: { pii },
      sql: kind + '_address_' + column, type: 'string', description: `${kind[0].toUpperCase()}${kind.slice(1)} address ${label}, as most recently recorded on the order`,
    }]))),
  },
});

// Customer activation preserves the same approved order-geography members.
view('orders', {
  description: 'Shopify orders at their latest known state, one row per order. Test orders are excluded; '
    + 'cancelled orders are included. Days are in the reporting time zone.',
  cubes: [
    { joinPath: shopify_orders, includes: '*' },
    {
      joinPath: shopify_orders.shopify_order_geography,
      includes: ['shipping_province_code', 'shipping_country_code', 'billing_province_code', 'billing_country_code',
        'shipping_city', 'shipping_postal_code', 'billing_city', 'billing_postal_code'],
    },
  ],
});


// Coverage is intentionally independent of the orders view's test exclusion.
cube('shopify_revenue_coverage', {
  sqlTable: marts.table('fct_orders'),
  refreshKey: marts.refreshKey,
  public: false,
  dimensions: {
    order_key: { meta: { pii: false },
      sql: `${CUBE}.shopify_shop_id || '/' || ${CUBE}.shopify_order_id`, type: 'string', primaryKey: true, public: false,
      description: 'Shop and order source identity joined by a slash; unique per order',
    },
    publication_id: { meta: { pii: false },
      sql: 'publication_id', type: 'string', description: 'Optional diagnostic generation stored with these coverage facts; separate queries may observe different publications',
    },
    order_date: { meta: { pii: false },
      sql: 'ordered_at', type: 'time', description: 'Reporting-zone order date from processed_at with created_at fallback',
    },
    reporting_market: { meta: { pii: false },
      sql: 'reporting_market', type: 'string', description: 'Configured separate market for its shipping country; home market for other destinations and missing country, not actual geography',
    },
    is_draft_order: { meta: { pii: false },
      sql: 'is_draft_order', type: 'boolean', description: 'Exact shopify_draft_order source marker; null for missing source name',
    },
    is_d2c: { meta: { pii: false },
      sql: 'is_d2c', type: 'boolean', description: 'D2C classification, retaining null when channel evidence is missing',
    },
    is_test: { meta: { pii: false },
      sql: 'is_test', type: 'boolean', description: 'Source test-order flag; tests remain in coverage',
    },
    ...(marts.governedOrdersEnabled ? dbtDimensions('fct_orders', ['is_aov_eligible', 'aov_exclusion_reasons', 'aov_unresolved_requirements']) : {}),
    is_revenue_eligible: { meta: { pii: false },
      sql: 'is_revenue_eligible', type: 'boolean', description: 'True eligible, false proven excluded, null unresolved',
    },
    revenue_exclusion_reasons: { meta: { pii: false },
      sql: 'revenue_exclusion_reasons', type: 'string', description: 'All proven revenue exclusions as pipe-delimited identifiers',
    },
    revenue_unresolved_requirements: { meta: { pii: false },
      sql: 'revenue_unresolved_requirements', type: 'string', description: 'All unknown requirements as pipe-delimited identifiers; requirement counts can overlap',
    },
  },
  measures: {
    ...Object.fromEntries((marts.governedOrdersEnabled ? [
      ['eligible', '', 'explicitly AOV-eligible', false],
      ['excluded', ' = FALSE', 'proven AOV-excluded', false],
      ['unresolved', ' IS NULL', 'unknown AOV eligibility', false],
    ] : []).map(([state, predicate, description, pii]) => [`aov_${state}_count`, {
      meta: { pii }, type: 'count', filters: [{ sql: `${CUBE}.is_aov_eligible${predicate}` }],
      description: `Orders with ${description}; coverage retains tests and unknown scope, not an estimate of omitted value`,
    }])),
    count: { meta: { pii: false }, type: 'count', description: 'All retained orders in the requested scope, including tests and unresolved eligibility' },
    eligible_count: { meta: { pii: false },
      type: 'count', filters: [{ sql: `${CUBE}.is_revenue_eligible` }],
      description: 'Orders with a true revenue verdict; coverage retains source test rows rather than applying the orders-view filter',
    },
    excluded_count: { meta: { pii: false },
      type: 'count', filters: [{ sql: `${CUBE}.is_revenue_eligible = FALSE` }],
      description: 'Orders with at least one proven revenue exclusion, including excluded orders that also have unresolved requirements',
    },
    unresolved_count: { meta: { pii: false },
      type: 'count', filters: [{ sql: `${CUBE}.is_revenue_eligible IS NULL` }],
      description: 'Orders with unknown revenue eligibility and no proven exclusion; not an estimate of missing revenue',
    },
  },
});

view('revenue_coverage', {
  description: 'Order-grain revenue' + (marts.governedOrdersEnabled ? ' and AOV' : '')
    + ' coverage including test orders. Group by channel, draft and test flags to keep unknown scope visible. Counts do not prove source coverage or freshness.',
  cubes: [{ joinPath: shopify_revenue_coverage, includes: '*' }],
});
