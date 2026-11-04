with source as (
    select *
    from {{
        sparse_source('shopify_orders', 'orders__agreements__sales__taxes')
    }}
),

renamed as (
    select
        shop_id,
        order_id,
        agreement_id,
        sale_id,
        sale_tax_id,
        cast(position as bigint) as sale_tax_position,
        title as tax_title,
        rate as tax_rate,
        channel_liable as is_channel_liable,
        amount_shop_amount as sale_tax_shop_amount,
        amount_shop_currency as sale_tax_shop_currency_code,
        amount_presentment_amount as sale_tax_presentment_amount,
        amount_presentment_currency as sale_tax_presentment_currency_code,
        price_shop_amount as tax_price_shop_amount,
        price_shop_currency as tax_price_shop_currency_code,
        price_presentment_amount as tax_price_presentment_amount,
        price_presentment_currency as tax_price_presentment_currency_code
    from source
)

select * from renamed
