with source as (
    select *
    from {{
        sparse_source('shopify_orders', 'orders__refunds__shipping_lines')
    }}
),

renamed as (
    select
        shop_id,
        order_id,
        refund_id,
        refund_shipping_line_id,
        shipping_line_id,
        subtotal_amount_shop_amount as subtotal_shop_amount,
        subtotal_amount_shop_currency as subtotal_shop_currency_code,
        subtotal_amount_presentment_amount as subtotal_presentment_amount,
        subtotal_amount_presentment_currency
            as subtotal_presentment_currency_code,
        tax_amount_shop_amount as tax_shop_amount,
        tax_amount_shop_currency as tax_shop_currency_code,
        tax_amount_presentment_amount as tax_presentment_amount,
        tax_amount_presentment_currency as tax_presentment_currency_code
    from source
)

select * from renamed
