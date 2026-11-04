with source as (
    select * from {{ sparse_source('shopify_orders', 'orders__transactions') }}
),

renamed as (
    select
        shop_id,
        order_id,
        transaction_id,
        parent_transaction_id,
        {{ utc_timestamp('created_at') }} as created_at,
        {{ utc_timestamp('processed_at') }} as processed_at,
        upper(trim(kind)) as kind,
        upper(trim(status)) as status,
        gateway,
        test as is_test,
        error_code,
        amount_shop_currency as shop_currency_code,
        amount_presentment_currency as presentment_currency_code,
        amount_shop_amount as transaction_shop_amount,
        amount_presentment_amount as transaction_presentment_amount
    from source
)

select * from renamed
