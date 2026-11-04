with source as (
    select *
    from {{ sparse_source('shopify_orders', 'orders__refunds__lines') }}
),

renamed as (
    select
        shop_id,
        order_id,
        refund_id,
        refund_line_item_id,
        line_item_id,
        cast(quantity as bigint) as refunded_quantity,
        subtotal_shop_amount,
        subtotal_shop_currency as subtotal_shop_currency_code,
        subtotal_presentment_amount,
        subtotal_presentment_currency as subtotal_presentment_currency_code,
        total_tax_shop_amount,
        total_tax_shop_currency as total_tax_shop_currency_code,
        total_tax_presentment_amount,
        total_tax_presentment_currency as total_tax_presentment_currency_code
    from source
)

select * from renamed
