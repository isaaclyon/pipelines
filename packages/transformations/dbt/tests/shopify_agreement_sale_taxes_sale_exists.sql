{{ config(meta={'dagster': {'ref': {'name': 'stg_shopify_orders__orders'}}}) }}

-- depends_on: {{ ref('stg_shopify_orders__orders') }}
-- Validate the raw child, which keeps rows the staging view would not join.
{{ config(tags=['shopify_commercial_cross_model']) }}
select
    taxes.shop_id,
    taxes.order_id,
    taxes.agreement_id,
    taxes.sale_id
from {{
    sparse_source('shopify_orders', 'orders__agreements__sales__taxes')
}} as taxes
left join {{
    sparse_source('shopify_orders', 'orders__agreements__sales')
}} as sales
    on
        taxes.shop_id = sales.shop_id
        and taxes.order_id = sales.order_id
        and taxes.agreement_id = sales.agreement_id
        and taxes.sale_id = sales.sale_id
where taxes.sale_id is not null and sales.sale_id is null
