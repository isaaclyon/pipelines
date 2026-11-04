-- Approved analytical order geography. Names, email, phone and street lines
-- stay in the restricted dim_order_contacts.
select
    shopify_shop_id,
    shopify_order_id,
    billing_address_city,
    billing_address_zip,
    billing_address_province,
    billing_address_province_code,
    billing_address_country,
    billing_address_country_code,
    shipping_address_city,
    shipping_address_zip,
    shipping_address_province,
    shipping_address_province_code,
    shipping_address_country,
    shipping_address_country_code
from {{ ref('dim_order_contacts') }}
