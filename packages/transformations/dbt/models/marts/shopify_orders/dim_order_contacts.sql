with orders as (
    select * from {{ ref('stg_shopify_orders__orders') }}
),

contacts as (
    select
        shop_id as shopify_shop_id,
        order_id as shopify_order_id,
        order_email,
        order_phone,
        billing_address_first_name,
        billing_address_last_name,
        billing_address_company,
        billing_address_address1,
        billing_address_address2,
        billing_address_city,
        billing_address_province,
        billing_address_province_code,
        billing_address_zip,
        billing_address_country,
        billing_address_country_code,
        billing_address_phone,
        shipping_address_first_name,
        shipping_address_last_name,
        shipping_address_company,
        shipping_address_address1,
        shipping_address_address2,
        shipping_address_city,
        shipping_address_province,
        shipping_address_province_code,
        shipping_address_zip,
        shipping_address_country,
        shipping_address_country_code,
        shipping_address_phone
    from orders
)

select * from contacts
