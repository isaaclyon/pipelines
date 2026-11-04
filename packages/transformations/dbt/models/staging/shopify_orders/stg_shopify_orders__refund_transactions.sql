with source as (
    select *
    from {{ sparse_source('shopify_orders', 'orders__refunds__transactions') }}
)

select
    shop_id,
    order_id,
    refund_id,
    transaction_id
from source
