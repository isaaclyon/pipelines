with source as (
    select *
    from {{
        sparse_source('shopify_orders', 'orders__shipping_lines__discount_allocations')
    }}
),

renamed as (
    select
        shop_id,
        order_id,
        shipping_line_id,
        cast(position as bigint) as allocation_position,
        cast(application_index as bigint) as application_index,
        allocated_amount_shop_amount,
        allocated_amount_shop_currency as allocated_amount_shop_currency_code,
        allocated_amount_presentment_amount,
        allocated_amount_presentment_currency
            as allocated_amount_presentment_currency_code
    from source
)

select * from renamed
