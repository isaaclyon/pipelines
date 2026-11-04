{{ config(meta={'dagster': {'ref': {'name': 'fct_orders'}}}) }}

-- Compare every line and every order. Unknown quantities must stay unknown;
-- SUM must not hide an unresolved line by returning a partial total.
with line_failures as (
    select published.shopify_shop_id, published.shopify_order_id
    from {{ ref('fct_order_lines') }} as published
    full outer join {{ ref('int_shopify_order_lines__net_units') }} as expected
        on
            published.shopify_shop_id = expected.shop_id
            and published.shopify_order_id = expected.order_id
            and published.shopify_line_item_id = expected.line_item_id
    where
        published.shopify_line_item_id is null
        or expected.line_item_id is null
        or published.net_ordered_quantity is distinct from expected.net_ordered_quantity
        or published.net_quantity_status is distinct from expected.net_quantity_status
),

order_failures as (
    select published.shopify_shop_id, published.shopify_order_id
    from {{ ref('fct_orders') }} as published
    left join {{ ref('int_shopify_orders__net_units') }} as expected
        on
            published.shopify_shop_id = expected.shop_id
            and published.shopify_order_id = expected.order_id
    where
        published.net_ordered_quantity is distinct from case
            when expected.order_id is null then 0
            else expected.net_ordered_quantity
        end
        or published.unresolved_quantity_line_count
        is distinct from coalesce(expected.unresolved_quantity_line_count, 0)
)

select * from line_failures
union all
select * from order_failures
