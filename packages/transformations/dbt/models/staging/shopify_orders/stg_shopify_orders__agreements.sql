with source as (
    select *
    from {{ sparse_source('shopify_orders', 'orders__agreements') }}
),

renamed as (
    select
        shop_id,
        order_id,
        agreement_id,
        type as agreement_type,
        reason,
        refund_id,
        return_id,
        {{ utc_timestamp('happened_at') }} as happened_at
    from source
)

select * from renamed
