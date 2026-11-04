with source as (
    select *
    from {{ sparse_source('shopify_orders', 'orders__discount_applications') }}
),

renamed as (
    select
        shop_id,
        order_id,
        cast(application_index as bigint) as application_index,
        type as discount_application_type,
        code as discount_code,
        title as discount_title,
        description as discount_description,
        value_type as discount_value_type,
        value_amount as discount_value_amount,
        value_currency as discount_value_currency_code,
        value_percentage as discount_value_percentage,
        upper(trim(allocation_method)) as allocation_method,
        upper(trim(target_selection)) as target_selection,
        upper(trim(target_type)) as target_type
    from source
)

select * from renamed
