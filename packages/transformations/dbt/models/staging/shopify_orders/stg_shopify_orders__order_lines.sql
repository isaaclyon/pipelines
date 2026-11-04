with source as (
    select * from {{ sparse_source('shopify_orders', 'orders__lines') }}
),

renamed as (
    select
        shop_id,
        order_id,
        line_item_id,
        sku,
        name as line_name,
        title,
        variant_title,
        product_id,
        variant_id,
        is_gift_card,
        cast(quantity as bigint) as order_line_quantity,
        cast(current_quantity as bigint) as current_quantity,
        original_unit_price_shop_amount,
        original_unit_price_presentment_amount,
        discounted_total_shop_amount,
        discounted_total_presentment_amount,
        {{ shopify_line_fulfillment_columns(source(
            "shopify_orders", "orders__lines"
        )) }}
    from source
)

select * from renamed
