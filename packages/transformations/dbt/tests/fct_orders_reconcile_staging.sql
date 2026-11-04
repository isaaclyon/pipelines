{{ config(meta={'dagster': {'ref': {'name': 'fct_orders'}}}) }}

-- fct_orders keeps every staged order once and carries line totals that match
-- staged lines exactly, so the join to line rollups neither drops nor multiplies.
with staged as (
    select
        (select count(*) from {{ ref('stg_shopify_orders__orders') }}) as order_count,
        coalesce(sum(order_line_quantity), 0) as order_line_quantity,
        coalesce(sum(order_line_quantity * original_unit_price_shop_amount), 0) as line_value
    from {{ ref('stg_shopify_orders__order_lines') }}
),

published as (
    select
        count(*) as order_count,
        coalesce(sum(gross_ordered_quantity), 0) as order_line_quantity,
        coalesce(sum(original_line_value_shop_amount), 0) as line_value
    from {{ ref('fct_orders') }}
)

select
    staged.order_count as staged_order_count,
    published.order_count as published_order_count,
    staged.order_line_quantity as staged_quantity,
    published.order_line_quantity as published_quantity,
    staged.line_value as staged_line_value,
    published.line_value as published_line_value
from staged
cross join published
where
    staged.order_count <> published.order_count
    or staged.order_line_quantity <> published.order_line_quantity
    or staged.line_value <> published.line_value
