{{ config(meta={'dagster': {'ref': {'name': 'fct_orders'}}}) }}

-- depends_on: {{ ref('fct_orders') }}
-- Validate this raw child even when the optional tax-detail view is not selected.
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
