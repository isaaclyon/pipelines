const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');
const SCHEMA = 'ci_marts';

/** Synthetic model fixture; this is not a publication or permissions test. */
async function publish(/** @type {string} */ path) {
  const writer = new DuckDBDriver({ databasePath: path });
  const usd = ['total_price', 'current_total_price', 'subtotal_price', 'total_discounts',
    'total_tax', 'total_shipping_price', 'total_refunded', 'original_line_value']
    .map(name => `cast(amount as decimal(38, 9)) as ${name}_shop_amount`).join(', ');
  try {
    await writer.query(`CREATE SCHEMA ${SCHEMA}`);
    // 1 is just after and 2 just before Pacific midnight on 1 July; 3 is on the
    // spring-forward day; 4 is a test order the view excludes.
    await writer.query(`CREATE TABLE ${SCHEMA}.fct_orders AS
      SELECT '00000000-0000-4000-8000-000000000001' AS publication_id,
        'shop-1' AS shopify_shop_id, shopify_order_id, '#' || shopify_order_id AS order_name, NULL::varchar AS shopify_customer_id,
        ordered_at::timestamp AS ordered_at, 'processed_at' AS order_date_source,
        ordered_at::timestamp AS processed_at, NULL::timestamp AS cancelled_at, false AS is_cancelled,
        is_test, NULL::varchar AS cancel_reason, 'web' AS source_name, 'PAID' AS financial_status,
        CASE WHEN shopify_order_id = '2' THEN 'CA' ELSE 'US' END AS reporting_market,
        shopify_order_id = '2' AS is_draft_order,
        CASE WHEN shopify_order_id = '3' THEN NULL::boolean ELSE shopify_order_id <> '2' END AS is_revenue_eligible,
        ''::varchar AS revenue_exclusion_reasons, ''::varchar AS revenue_unresolved_requirements,
        100::decimal(38,9) AS gross_merchandise_shop_amount,
        -10::decimal(38,9) AS merchandise_discount_shop_amount,
        -20::decimal(38,9) AS merchandise_refund_shop_amount,
        8::decimal(38,9) AS shipping_shop_amount,
        -2::decimal(38,9) AS shipping_refund_shop_amount,
        70::decimal(38,9) AS net_merchandise_shop_amount,
        CASE WHEN shopify_order_id = '2' THEN 20 ELSE 76 END::decimal(38,9) AS net_revenue_shop_amount,
        CASE WHEN shopify_order_id = '3' THEN NULL::boolean ELSE shopify_order_id <> '4' END AS is_aov_eligible,
        ''::varchar AS aov_exclusion_reasons, ''::varchar AS aov_unresolved_requirements,
        'FULFILLED' AS fulfillment_status, 'CAD' AS presentment_currency_code, line_count::bigint AS line_count,
        quantity::bigint AS gross_ordered_quantity, ${usd},
        evidence AS has_classification_evidence, d2c AS is_d2c, NOT d2c AS is_commercial,
        false AS is_retail, false AS is_specialty, NOT d2c AS is_bulk, NULL::boolean AS is_employee_order,
        NULL::boolean AS is_gifted, false AS is_replacement, false AS is_exchange,
        false AS is_shopify_collective, d2c AS is_broadcast
      FROM (VALUES
        ('1', '2026-07-01 07:30:00', false, 2, 3, '10.005', true, true),
        ('2', '2026-07-01 06:30:00', false, 1, 1, '0.333333333', true, false),
        ('3', '2026-03-08 10:30:00', false, 1, 4, '20', false, NULL::boolean),
        ('4', '2026-07-01 12:00:00', true, 1, 100, '999', true, true)
      ) t(shopify_order_id, ordered_at, is_test, line_count, quantity, amount, evidence, d2c)`);
    await writer.query(`CREATE TABLE ${SCHEMA}.fct_order_lines AS
      SELECT 'shop-1' AS shopify_shop_id, shopify_order_id, shopify_line_item_id,
        'SKU-' || shopify_line_item_id AS sku, timestamp '2026-07-01 07:30:00' AS ordered_at,
        shopify_order_id = '4' AS is_test, shopify_line_item_id = 'gift-card' AS is_gift_card_item,
        revenue AS is_revenue_eligible, asp AS is_asp_eligible,
        '' AS revenue_exclusion_reasons, '' AS revenue_unresolved_requirements,
        '' AS asp_exclusion_reasons, '' AS asp_unresolved_requirements,
        quantity::bigint AS net_ordered_quantity, amount::decimal(38,9) AS net_merchandise_shop_amount
      FROM (VALUES
        ('1', '1', 1, 20, true, true),
        ('1', '2', 3, 30, true, true),
        ('2', '1', 2, -10, true, false),
        ('3', '1', NULL, NULL, NULL, NULL),
        ('4', '1', 100, 999, true, true),
        ('2', '2', 2, 0, true, true),
        ('2', '3', 0, 0, true, true),
        ('2', '4', -2, -12, true, true),
        ('1', 'gift-card', 10, 0, true, false),
        ('2', 'unknown-units', NULL, 0, true, NULL)
      ) t(shopify_order_id, shopify_line_item_id, quantity, amount, revenue, asp)`);
    await writer.query(`CREATE TABLE ${SCHEMA}.dim_order_contacts AS
      SELECT 'shop-1' AS shopify_shop_id, shopify_order_id, province AS shipping_address_province_code,
        'US' AS shipping_address_country_code, 'Probe Ship City' AS shipping_address_city, '84601-ship' AS shipping_address_zip,
        'Probe Bill City' AS billing_address_city, 'WA' AS billing_address_province_code,
        '98101-bill' AS billing_address_zip, 'US' AS billing_address_country_code
      FROM (VALUES ('1', 'UT'), ('2', 'UT'), ('3', 'CA'), ('4', 'CA')) t(shopify_order_id, province)`);
    await writer.query(`CREATE TABLE ${SCHEMA}._publication AS
      SELECT '00000000-0000-4000-8000-000000000001' AS publication_id, current_timestamp AS published_at`);
    await writer.query(`CREATE TABLE ${SCHEMA}.fct_data_availability AS
      SELECT 'shopify_orders/shop-1' AS availability_id, publication_id, 'shop-1' AS shopify_shop_id,
        'ci' AS environment, published_at::timestamp AS evidence_captured_at,
        timestamp '2026-06-01 00:00:00' AS shopify_coverage_started_at,
        timestamp '2026-07-02 07:00:00' AS shopify_coverage_ended_at,
        NULL::timestamp AS loop_snapshot_ended_at, false AS is_loop_snapshot_complete,
        false AS is_loop_enabled, 'Fixture coverage limitations' AS coverage_limitations
      FROM ${SCHEMA}._publication`);
    await writer.query('CHECKPOINT');
  } finally { await writer.release(); }
}

module.exports = { publish, SCHEMA };
