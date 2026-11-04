with source as (
    select *
    from {{ sparse_source('shopify_orders', 'orders__agreements__sales') }}
),

renamed as (
    select
        shop_id,
        order_id,
        agreement_id,
        sale_id,
        type as sale_type,
        cast(quantity as bigint) as signed_quantity,
        line_item_id,
        shipping_line_id,
        duty_id,
        fee_id,
        fee_type,
        additional_fee_id,
        additional_fee_name,
        additional_fee_price_shop_amount,
        additional_fee_price_shop_currency
            as additional_fee_price_shop_currency_code,
        additional_fee_price_presentment_amount,
        additional_fee_price_presentment_currency
            as additional_fee_price_presentment_currency_code,
        total_amount_shop_amount,
        total_amount_shop_currency as total_amount_shop_currency_code,
        total_amount_presentment_amount,
        total_amount_presentment_currency
            as total_amount_presentment_currency_code,
        total_tax_amount_shop_amount,
        total_tax_amount_shop_currency as total_tax_amount_shop_currency_code,
        total_tax_amount_presentment_amount,
        total_tax_amount_presentment_currency
            as total_tax_amount_presentment_currency_code,
        total_discount_amount_before_taxes_shop_amount,
        total_discount_amount_before_taxes_shop_currency
            as total_discount_amount_before_taxes_shop_currency_code,
        total_discount_amount_before_taxes_presentment_amount,
        total_discount_amount_before_taxes_presentment_currency
            as total_discount_amount_before_taxes_presentment_currency_code,
        total_discount_amount_after_taxes_shop_amount,
        total_discount_amount_after_taxes_shop_currency
            as total_discount_amount_after_taxes_shop_currency_code,
        total_discount_amount_after_taxes_presentment_amount,
        total_discount_amount_after_taxes_presentment_currency
            as total_discount_amount_after_taxes_presentment_currency_code,
        upper(trim(action_type)) as action_type,
        upper(trim(line_type)) as line_type
    from source
)

select * from renamed
