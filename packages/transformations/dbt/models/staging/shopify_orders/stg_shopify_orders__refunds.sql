with source as (
    select * from {{ sparse_source('shopify_orders', 'orders__refunds') }}
),

renamed as (
    select
        shop_id,
        order_id,
        refund_id,
        return_id,
        {{ utc_timestamp('created_at') }} as created_at,
        {{ utc_timestamp('updated_at') }} as updated_at,
        {{ utc_timestamp('processed_at') }} as processed_at,
        total_refunded_shop_currency as shop_currency_code,
        total_refunded_presentment_currency as presentment_currency_code,
        total_refunded_shop_amount,
        total_refunded_presentment_amount
    from source
)

select * from renamed
