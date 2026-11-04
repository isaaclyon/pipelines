{{ config(meta={'dagster': {'ref': {'name': 'fct_order_lines'}}}) }}

-- Compare every source line and reporting field to staging, including nulls.
-- A full join rejects missing and extra lines; parent joins never hide orphans.
with lines as (
    select * from {{ ref('stg_shopify_orders__order_lines') }}
),

orders as (
    select * from {{ ref('stg_shopify_orders__orders') }}
),

published as (
    select * from {{ ref('fct_order_lines') }}
)

select
    lines.shop_id,
    lines.order_id,
    lines.line_item_id,
    published.shopify_line_item_id
from lines
full outer join published
    on
        lines.shop_id = published.shopify_shop_id
        and lines.order_id = published.shopify_order_id
        and lines.line_item_id = published.shopify_line_item_id
left join orders
    on lines.shop_id = orders.shop_id and lines.order_id = orders.order_id
where
    lines.line_item_id is null
    or published.shopify_line_item_id is null
    or orders.order_id is null
    or published.shopify_shop_id is distinct from lines.shop_id
    or published.shopify_order_id is distinct from lines.order_id
    or published.shopify_line_item_id is distinct from lines.line_item_id
    or published.sku is distinct from lines.sku
    or published.line_name is distinct from lines.line_name
    or published.title is distinct from lines.title
    or published.variant_title is distinct from lines.variant_title
    or published.shopify_product_id is distinct from lines.product_id
    or published.shopify_variant_id is distinct from lines.variant_id
    or published.is_gift_card_item is distinct from lines.is_gift_card
    or published.gross_ordered_quantity
    is distinct from lines.order_line_quantity
    or published.current_quantity is distinct from lines.current_quantity
    or published.original_unit_price_shop_amount
    is distinct from lines.original_unit_price_shop_amount
    or published.original_unit_price_presentment_amount
    is distinct from lines.original_unit_price_presentment_amount
    or published.discounted_total_shop_amount
    is distinct from lines.discounted_total_shop_amount
    or published.discounted_total_presentment_amount
    is distinct from lines.discounted_total_presentment_amount
    or published.original_line_value_shop_amount is distinct from cast(
        lines.order_line_quantity
        * lines.original_unit_price_shop_amount as decimal(38, 9)
    )
    or published.original_line_value_presentment_amount is distinct from cast(
        lines.order_line_quantity
        * lines.original_unit_price_presentment_amount as decimal(38, 9)
    )
    or published.order_name is distinct from orders.order_name
    or published.shopify_customer_id is distinct from orders.customer_id
    or published.created_at is distinct from orders.created_at
    or published.processed_at is distinct from orders.processed_at
    or published.updated_at is distinct from orders.updated_at
    or published.cancelled_at is distinct from orders.cancelled_at
    or published.closed_at is distinct from orders.closed_at
    or published.ordered_at is distinct from orders.ordered_at
    or published.order_date_source is distinct from orders.order_date_source
    or published.order_date is distinct from orders.order_date
    or published.cancel_reason is distinct from orders.cancel_reason
    or published.is_test is distinct from orders.is_test
    or published.source_name is distinct from orders.source_name
    or published.financial_status is distinct from orders.financial_status
    or published.fulfillment_status is distinct from orders.fulfillment_status
    or published.is_tax_included is distinct from orders.is_tax_included
    or published.shop_currency_code is distinct from orders.shop_currency_code
    or published.presentment_currency_code
    is distinct from orders.presentment_currency_code
    or published.source_observed_at is distinct from orders.source_observed_at
    or published.is_cancelled is distinct from (orders.cancelled_at is not null)
    or published.billing_address_city
    is distinct from orders.billing_address_city
    or published.billing_address_province
    is distinct from orders.billing_address_province
    or published.billing_address_province_code
    is distinct from orders.billing_address_province_code
    or published.billing_address_zip is distinct from orders.billing_address_zip
    or published.billing_address_country
    is distinct from orders.billing_address_country
    or published.billing_address_country_code
    is distinct from orders.billing_address_country_code
    or published.shipping_address_city
    is distinct from orders.shipping_address_city
    or published.shipping_address_province
    is distinct from orders.shipping_address_province
    or published.shipping_address_province_code
    is distinct from orders.shipping_address_province_code
    or published.shipping_address_zip
    is distinct from orders.shipping_address_zip
    or published.shipping_address_country
    is distinct from orders.shipping_address_country
    or published.shipping_address_country_code
    is distinct from orders.shipping_address_country_code
