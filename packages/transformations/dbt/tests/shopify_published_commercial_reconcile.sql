{{ config(meta={'dagster': {'ref': {'name': 'fct_orders'}}}) }}

{%- set components = [
    ('merchandise_discount', 'merchandise_discount_shop_amount', 'merchandise_discount_unallocated_shop_amount'),
    ('merchandise_refund', 'merchandise_refund_shop_amount', 'merchandise_refund_unallocated_shop_amount'),
    ('shipping', 'shipping_shop_amount', 'shipping_unallocated_shop_amount'),
    ('shipping_refund', 'shipping_refund_shop_amount', 'shipping_refund_unallocated_shop_amount')
] -%}
-- Reconcile by shop and order, never by global totals. Unknown lines do not
-- become zero. An incomplete allocation can retain an explicit known residual.
with lines as (
    select
        shopify_shop_id,
        shopify_order_id,
        {% for component, amount, residual in components %}
            sum({{ amount }}) as {{ component }}_total,
            sum(case when {{ amount }} is null then 1 else 0 end)
                as {{ component }}_unknown
            {%- if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('fct_order_lines') }}
    group by shopify_shop_id, shopify_order_id
),

orders as (
    select * from {{ ref('fct_orders') }}
)

{% for component, amount, residual in components %}
    select
        orders.shopify_shop_id,
        orders.shopify_order_id,
        '{{ component }}' as failed_component
    from orders
    left join lines
        on
            orders.shopify_shop_id = lines.shopify_shop_id
            and orders.shopify_order_id = lines.shopify_order_id
    where
        (orders.is_allocation_complete and (
            orders.{{ amount }} is null
            or orders.{{ residual }} is null
            or orders.{{ residual }} <> 0
            or coalesce(lines.{{ component }}_unknown, 0) > 0
        ))
        or (
            orders.{{ amount }} is not null
            and orders.{{ residual }} is not null
            and coalesce(lines.{{ component }}_unknown, 0) = 0
            and orders.{{ amount }}
            <> coalesce(lines.{{ component }}_total, 0) + orders.{{ residual }}
        )
    union all
{% endfor %}
select
    shopify_shop_id,
    shopify_order_id,
    'net_revenue' as failed_component
from orders
where is_commercial_resolved and (
    net_revenue_shop_amount is null
    or net_merchandise_shop_amount is null
    or net_shipping_shop_amount is null
    or net_revenue_shop_amount
    <> net_merchandise_shop_amount + net_shipping_shop_amount
)
