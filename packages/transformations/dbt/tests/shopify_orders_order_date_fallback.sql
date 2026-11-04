-- Orders whose order_date falls back to created_at because Shopify has no
-- processing time. A visible count; publication continues.
{{ config(severity='warn') }}

select
    shop_id,
    order_id,
    ordered_at
from {{ ref('stg_shopify_orders__orders') }}
where order_date_source = 'created_at'
