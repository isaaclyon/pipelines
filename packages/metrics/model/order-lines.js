// @ts-nocheck Cube model DSL globals are supplied by the schema compiler.
const { publishedMarts } = require('../publication.cjs');
const { dbtDimensions } = require('../mart-columns.cjs');
const marts = publishedMarts();

if (marts.governedOrdersEnabled) {
cube('shopify_order_lines', {
  sql: 'SELECT * FROM ' + marts.table('fct_order_lines') + ' WHERE NOT is_test',
  refreshKey: marts.refreshKey,
  public: false,
  joins: {
    shopify_orders: {
      relationship: 'many_to_one',
      sql: `${CUBE}.shopify_shop_id = ${shopify_orders}.shopify_shop_id
        AND ${CUBE}.shopify_order_id = ${shopify_orders}.shopify_order_id`,
    },
  },
  dimensions: {
    line_key: {
      meta: { pii: false }, type: 'string', primaryKey: true, public: false,
      sql: `${CUBE}.shopify_shop_id || '/' || ${CUBE}.shopify_order_id || '/' || ${CUBE}.shopify_line_item_id`,
      description: 'Shop, order and line source identity; unique per retained source line',
    },
    order_date: {
      meta: { pii: false }, sql: 'ordered_at', type: 'time',
      description: 'Original order instant, processed_at with created_at fallback; reporting days use the reporting time zone',
    },
    ...dbtDimensions('fct_order_lines', [
      'shopify_shop_id', 'shopify_order_id', 'shopify_line_item_id', 'sku', 'is_gift_card_item',
      'is_revenue_eligible', 'revenue_exclusion_reasons', 'revenue_unresolved_requirements',
      'is_asp_eligible', 'asp_exclusion_reasons', 'asp_unresolved_requirements',
    ]),
  },
  measures: {
    ...Object.fromEntries([
      ['net_sales_usd', 'net_merchandise_shop_amount', 'is_revenue_eligible', 'USD net merchandise sales, including signed refunds, excluding shipping, tax and gift-card sale value', false],
      ['net_ordered_quantity', 'net_ordered_quantity', 'is_revenue_eligible', 'Default net ordered units excluding gift-card sale units and using revenue eligibility, not ASP eligibility; quantity reductions count once and amount-only refunds do not remove units', false],
      ['asp_net_sales_usd', 'net_merchandise_shop_amount', 'is_asp_eligible', 'USD net merchandise sales of explicitly ASP-eligible lines, excluding shipping and tax', false],
      ['asp_net_ordered_quantity', 'net_ordered_quantity', 'is_asp_eligible', 'Net ordered units of the same explicitly ASP-eligible lines as the ASP numerator', false],
    ].map(([name, column, eligibility, description, pii]) => [name, {
      meta: { pii }, sql: column, type: 'sum',
      filters: [{ sql: `${CUBE}.${eligibility}` },
        ...(name === 'net_ordered_quantity' ? [{ sql: `${CUBE}.is_gift_card_item = FALSE` }] : [])],
      description: description + '. Tests are excluded; unknown evidence contributes no value, not zero',
    }])),
    unresolved_quantity_count: {
      meta: { pii: false }, type: 'count',
      filters: [{ sql: `${CUBE}.is_revenue_eligible` },
        { sql: `${CUBE}.is_gift_card_item = FALSE` },
        { sql: `${CUBE}.net_ordered_quantity IS NULL` }],
      description: 'Revenue-eligible non-gift-card lines with unknown net units; inspect alongside default net ordered quantity, never substitute zero',
    },
    asp_usd: {
      meta: { pii: false }, type: 'number', format: 'currency',
      sql: `${asp_net_sales_usd} / NULLIF(${asp_net_ordered_quantity}, 0)`,
      description: 'USD average selling price: summed ASP-eligible net sales divided by summed eligible net ordered units; null for a zero denominator',
    },
    ...Object.fromEntries(['revenue', 'asp'].flatMap(metric => [
      ['eligible', '', 'explicitly eligible', false],
      ['excluded', ' = FALSE', 'proven excluded', false],
      ['unresolved', ' IS NULL', 'unknown eligibility', false],
    ].map(([state, predicate, description, pii]) => [`${state}_${metric}_count`, {
      meta: { pii }, type: 'count',
      filters: [{ sql: `${CUBE}.is_${metric}_eligible${predicate}` }],
      description: `Retained non-test lines with ${description} for ${metric}; counts are not estimates of missing amounts or units`,
    }]))),
  },
});

view('order_lines', {
  description: 'Source-qualified Shopify line reporting, excluding tests. Refunds and edits restate the original order day. Whole-order AOV is available only through orders, never as a line measure.',
  cubes: [
    { joinPath: shopify_order_lines, includes: '*' },
    { joinPath: shopify_order_lines.shopify_orders,
      includes: ['reporting_market', 'is_d2c', 'is_draft_order'] },
  ],
});
}
