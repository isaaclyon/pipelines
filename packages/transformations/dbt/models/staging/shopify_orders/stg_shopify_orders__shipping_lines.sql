with source as (
    select *
    from {{ sparse_source('shopify_orders', 'orders__shipping_lines') }}
),

renamed as (
    select
        shop_id,
        order_id,
        shipping_line_id,
        title as shipping_title,
        code as shipping_code,
        source as shipping_source,
        is_removed,
        original_price_shop_amount,
        original_price_shop_currency as original_price_shop_currency_code,
        original_price_presentment_amount,
        original_price_presentment_currency
            as original_price_presentment_currency_code,
        discounted_price_shop_amount,
        discounted_price_shop_currency as discounted_price_shop_currency_code,
        discounted_price_presentment_amount,
        discounted_price_presentment_currency
            as discounted_price_presentment_currency_code,
        current_discounted_price_shop_amount,
        current_discounted_price_shop_currency
            as current_discounted_price_shop_currency_code,
        current_discounted_price_presentment_amount,
        current_discounted_price_presentment_currency
            as current_discounted_price_presentment_currency_code
    from source
)

select * from renamed
