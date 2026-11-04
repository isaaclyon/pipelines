with source as (
    select *
    from {{ sparse_source('shopify_orders', 'orders__refunds__adjustments') }}
),

renamed as (
    select
        shop_id,
        order_id,
        refund_id,
        adjustment_id,
        reason as adjustment_reason,
        amount_shop_amount as adjustment_shop_amount,
        amount_shop_currency as adjustment_shop_currency_code,
        amount_presentment_amount as adjustment_presentment_amount,
        amount_presentment_currency as adjustment_presentment_currency_code,
        tax_amount_shop_amount as adjustment_tax_shop_amount,
        tax_amount_shop_currency as adjustment_tax_shop_currency_code,
        tax_amount_presentment_amount as adjustment_tax_presentment_amount,
        tax_amount_presentment_currency
            as adjustment_tax_presentment_currency_code
    from source
)

select * from renamed
