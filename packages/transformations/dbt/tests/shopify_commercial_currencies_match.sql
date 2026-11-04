{{ config(meta={'dagster': {'ref': {'name': 'fct_orders'}}}) }}

-- depends_on: {{ ref('fct_orders') }}
{{ config(tags=['shopify_commercial_cross_model']) }}
-- Check raw children so an unused staging view is not required by publication.
-- Every commercial child money amount uses the currency of its order: shop
-- money in the order's shop currency and presentment money in its presentment
-- currency. A child whose order is missing fails, because its currency is unknown.
-- Discount application values are excluded; their currency is not an order currency claim.
with orders as (
    select
        shop_id,
        order_id,
        currency_code,
        presentment_currency_code
    from {{ source('shopify_orders', 'orders') }}
)

select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__agreement_sales' as model_name
from {{ sparse_source('shopify_orders', 'orders__agreements__sales') }} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.additional_fee_price_shop_amount is not null
        and c.additional_fee_price_shop_currency is distinct from o.currency_code
    )
    or (
        c.additional_fee_price_presentment_amount is not null
        and c.additional_fee_price_presentment_currency is distinct from o.presentment_currency_code
    )
    or (
        c.total_amount_shop_amount is not null
        and c.total_amount_shop_currency is distinct from o.currency_code
    )
    or (
        c.total_amount_presentment_amount is not null
        and c.total_amount_presentment_currency is distinct from o.presentment_currency_code
    )
    or (
        c.total_tax_amount_shop_amount is not null
        and c.total_tax_amount_shop_currency is distinct from o.currency_code
    )
    or (
        c.total_tax_amount_presentment_amount is not null
        and c.total_tax_amount_presentment_currency is distinct from o.presentment_currency_code
    )
    or (
        c.total_discount_amount_before_taxes_shop_amount is not null
        and c.total_discount_amount_before_taxes_shop_currency is distinct from o.currency_code
    )
    or (
        c.total_discount_amount_before_taxes_presentment_amount is not null
        and c.total_discount_amount_before_taxes_presentment_currency is distinct from o.presentment_currency_code
    )
    or (
        c.total_discount_amount_after_taxes_shop_amount is not null
        and c.total_discount_amount_after_taxes_shop_currency is distinct from o.currency_code
    )
    or (
        c.total_discount_amount_after_taxes_presentment_amount is not null
        and c.total_discount_amount_after_taxes_presentment_currency is distinct from o.presentment_currency_code
    )
union all
select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__agreement_sale_taxes' as model_name
from {{
    sparse_source('shopify_orders', 'orders__agreements__sales__taxes')
}} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.amount_shop_amount is not null
        and c.amount_shop_currency is distinct from o.currency_code
    )
    or (
        c.amount_presentment_amount is not null
        and c.amount_presentment_currency is distinct from o.presentment_currency_code
    )
    or (
        c.price_shop_amount is not null
        and c.price_shop_currency is distinct from o.currency_code
    )
    or (
        c.price_presentment_amount is not null
        and c.price_presentment_currency is distinct from o.presentment_currency_code
    )
union all
select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__refund_lines' as model_name
from {{ sparse_source('shopify_orders', 'orders__refunds__lines') }} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.subtotal_shop_amount is not null
        and c.subtotal_shop_currency is distinct from o.currency_code
    )
    or (
        c.subtotal_presentment_amount is not null
        and c.subtotal_presentment_currency is distinct from o.presentment_currency_code
    )
    or (
        c.total_tax_shop_amount is not null
        and c.total_tax_shop_currency is distinct from o.currency_code
    )
    or (
        c.total_tax_presentment_amount is not null
        and c.total_tax_presentment_currency is distinct from o.presentment_currency_code
    )
union all
select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__refund_adjustments' as model_name
from {{ sparse_source('shopify_orders', 'orders__refunds__adjustments') }} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.amount_shop_amount is not null
        and c.amount_shop_currency is distinct from o.currency_code
    )
    or (
        c.amount_presentment_amount is not null
        and c.amount_presentment_currency is distinct from o.presentment_currency_code
    )
    or (
        c.tax_amount_shop_amount is not null
        and c.tax_amount_shop_currency is distinct from o.currency_code
    )
    or (
        c.tax_amount_presentment_amount is not null
        and c.tax_amount_presentment_currency is distinct from o.presentment_currency_code
    )
union all
select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__refund_shipping_lines' as model_name
from {{
    sparse_source('shopify_orders', 'orders__refunds__shipping_lines')
}} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.subtotal_amount_shop_amount is not null
        and c.subtotal_amount_shop_currency is distinct from o.currency_code
    )
    or (
        c.subtotal_amount_presentment_amount is not null
        and c.subtotal_amount_presentment_currency is distinct from o.presentment_currency_code
    )
    or (
        c.tax_amount_shop_amount is not null
        and c.tax_amount_shop_currency is distinct from o.currency_code
    )
    or (
        c.tax_amount_presentment_amount is not null
        and c.tax_amount_presentment_currency is distinct from o.presentment_currency_code
    )
union all
select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__refund_duties' as model_name
from {{ sparse_source('shopify_orders', 'orders__refunds__duties') }} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.amount_shop_amount is not null
        and c.amount_shop_currency is distinct from o.currency_code
    )
    or (
        c.amount_presentment_amount is not null
        and c.amount_presentment_currency is distinct from o.presentment_currency_code
    )
union all
select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__shipping_lines' as model_name
from {{ sparse_source('shopify_orders', 'orders__shipping_lines') }} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.original_price_shop_amount is not null
        and c.original_price_shop_currency is distinct from o.currency_code
    )
    or (
        c.original_price_presentment_amount is not null
        and c.original_price_presentment_currency is distinct from o.presentment_currency_code
    )
    or (
        c.discounted_price_shop_amount is not null
        and c.discounted_price_shop_currency is distinct from o.currency_code
    )
    or (
        c.discounted_price_presentment_amount is not null
        and c.discounted_price_presentment_currency is distinct from o.presentment_currency_code
    )
    or (
        c.current_discounted_price_shop_amount is not null
        and c.current_discounted_price_shop_currency is distinct from o.currency_code
    )
    or (
        c.current_discounted_price_presentment_amount is not null
        and c.current_discounted_price_presentment_currency is distinct from o.presentment_currency_code
    )
union all
select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__line_discount_allocations' as model_name
from {{
    sparse_source('shopify_orders', 'orders__lines__discount_allocations')
}} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.allocated_amount_shop_amount is not null
        and c.allocated_amount_shop_currency is distinct from o.currency_code
    )
    or (
        c.allocated_amount_presentment_amount is not null
        and c.allocated_amount_presentment_currency is distinct from o.presentment_currency_code
    )
union all
select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__shipping_line_discount_allocations' as model_name
from {{
        sparse_source('shopify_orders', 'orders__shipping_lines__discount_allocations')
    }} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.allocated_amount_shop_amount is not null
        and c.allocated_amount_shop_currency is distinct from o.currency_code
    )
    or (
        c.allocated_amount_presentment_amount is not null
        and c.allocated_amount_presentment_currency is distinct from o.presentment_currency_code
    )
union all
select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__line_tax_lines' as model_name
from {{ sparse_source('shopify_orders', 'orders__lines__tax_lines') }} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.price_shop_amount is not null
        and c.price_shop_currency is distinct from o.currency_code
    )
    or (
        c.price_presentment_amount is not null
        and c.price_presentment_currency is distinct from o.presentment_currency_code
    )
union all
select
    c.shop_id,
    c.order_id,
    'stg_shopify_orders__shipping_line_tax_lines' as model_name
from {{
    sparse_source('shopify_orders', 'orders__shipping_lines__tax_lines')
}} as c
left join orders as o
    on c.shop_id = o.shop_id and c.order_id = o.order_id
where
    o.order_id is null
    or (
        c.price_shop_amount is not null
        and c.price_shop_currency is distinct from o.currency_code
    )
    or (
        c.price_presentment_amount is not null
        and c.price_presentment_currency is distinct from o.presentment_currency_code
    )
