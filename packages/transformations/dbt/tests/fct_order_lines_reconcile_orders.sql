{{ config(meta={'dagster': {'ref': {'name': 'fct_order_lines'}}}) }}

-- Reconcile each shop/order separately, in both currencies. Empty orders keep
-- zero rollups and do not acquire synthetic line rows.
with lines as (
    select
        shopify_shop_id,
        shopify_order_id,
        count(*) as line_count,
        coalesce(sum(gross_ordered_quantity), 0) as gross_ordered_quantity,
        coalesce(sum(original_line_value_shop_amount), 0) as shop_amount,
        coalesce(sum(original_line_value_presentment_amount), 0)
            as presentment_amount
    from {{ ref('fct_order_lines') }}
    group by shopify_shop_id, shopify_order_id
),

orders as (
    select * from {{ ref('fct_orders') }}
)

select
    orders.shopify_shop_id,
    orders.shopify_order_id,
    lines.shopify_order_id as line_order_id
from orders
full outer join lines
    on
        orders.shopify_shop_id = lines.shopify_shop_id
        and orders.shopify_order_id = lines.shopify_order_id
where
    orders.shopify_order_id is null
    or orders.line_count is distinct from coalesce(lines.line_count, 0)
    or orders.gross_ordered_quantity
    is distinct from coalesce(lines.gross_ordered_quantity, 0)
    or orders.original_line_value_shop_amount
    is distinct from coalesce(lines.shop_amount, 0)
    or orders.original_line_value_presentment_amount
    is distinct from coalesce(lines.presentment_amount, 0)
