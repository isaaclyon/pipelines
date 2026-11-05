{{ config(meta={'dagster': {'ref': {'name': 'stg_shopify_orders__order_lines'}}}) }}

-- Every amount uses its order's shop or presentment currency, so summing an
-- order's amounts never mixes currencies. A missing currency code fails; an amount's code may be missing
-- only when the amount itself is.
{% set money = ['total_price', 'current_total_price', 'original_total_price',
    'subtotal_price', 'current_subtotal_price', 'total_discounts', 'total_tax',
    'current_total_tax', 'total_shipping_price', 'total_refunded'] %}
{% set line_money = ['original_unit_price', 'discounted_total'] %}
{% set pairs = [('shop', 'currency_code'),
    ('presentment', 'presentment_currency_code')] %}

with orders as (
    select * from {{ source('shopify_orders', 'orders') }}
),

lines as (
    select * from {{ sparse_source('shopify_orders', 'orders__lines') }}
),

order_mismatches as (
    select
        shop_id,
        order_id,
        'order' as level
    from orders
    where
        currency_code is null
        or presentment_currency_code is null
        {%- for name in money %}{% for kind, code in pairs %}
        or (
            {{ name }}_{{ kind }}_amount is not null
            and {{ name }}_{{ kind }}_currency is distinct from {{ code }}
        )
        {%- endfor %}{% endfor %}
),

line_mismatches as (
    select
        lines.shop_id,
        lines.order_id,
        'line' as level
    from lines
    inner join orders
        on lines.shop_id = orders.shop_id and lines.order_id = orders.order_id
    where
        false
        {%- for name in line_money %}{% for kind, code in pairs %}
        or (
            lines.{{ name }}_{{ kind }}_amount is not null
            and lines.{{ name }}_{{ kind }}_currency
            is distinct from orders.{{ code }}
        )
        {%- endfor %}{% endfor %}
)

select * from order_mismatches
union all
select * from line_mismatches
union all
-- Parent currency declarations require a matching staged order for every
-- line; otherwise the amount has no associated currency.
select lines.shop_id, lines.order_id, 'orphan_line' as level
from {{ ref('stg_shopify_orders__order_lines') }} as lines
left join {{ ref('stg_shopify_orders__orders') }} as parent
    on lines.shop_id = parent.shop_id and lines.order_id = parent.order_id
where parent.order_id is null
