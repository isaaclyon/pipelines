with source as (
    select *
    from {{ sparse_source('shopify_orders', 'orders__refunds__duties') }}
),

renamed as (
    select
        shop_id,
        order_id,
        refund_id,
        cast(position as bigint) as duty_position,
        duty_id,
        amount_shop_amount as refund_shop_amount,
        amount_shop_currency as refund_shop_currency_code,
        amount_presentment_amount as refund_presentment_amount,
        amount_presentment_currency as refund_presentment_currency_code
    from source
)

select * from renamed
