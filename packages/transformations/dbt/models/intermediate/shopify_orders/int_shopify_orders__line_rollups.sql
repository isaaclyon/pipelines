-- Roll lines up to one row per order before joining to orders, so line detail
-- cannot multiply order rows.
with lines as (
    select * from {{ ref('stg_shopify_orders__order_lines') }}
),

rolled_up as (
    select
        shop_id,
        order_id,
        cast(count(*) as bigint) as line_count,
        cast(sum(order_line_quantity) as bigint) as gross_ordered_quantity,
        cast(
            sum(order_line_quantity * original_unit_price_shop_amount)
            as decimal(38, 9)
        ) as original_line_value_shop_amount,
        cast(
            sum(order_line_quantity * original_unit_price_presentment_amount)
            as decimal(38, 9)
        ) as original_line_value_presentment_amount
    from lines
    group by shop_id, order_id
)

select * from rolled_up
