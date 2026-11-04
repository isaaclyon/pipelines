with source as (
    select *
    from {{
        sparse_source('shopify_orders', 'orders__shipping_lines__tax_lines')
    }}
),

renamed as (
    select
        shop_id,
        order_id,
        shipping_line_id,
        cast(position as bigint) as tax_line_position,
        title as tax_title,
        rate as tax_rate,
        channel_liable as is_channel_liable,
        price_shop_amount as tax_price_shop_amount,
        price_shop_currency as tax_price_shop_currency_code,
        price_presentment_amount as tax_price_presentment_amount,
        price_presentment_currency as tax_price_presentment_currency_code
    from source
)

select * from renamed
