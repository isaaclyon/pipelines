with source as (
    select * from {{ source('shopify_customers', 'customers__addresses') }}
)

select
    shop_id,
    customer_id,
    address_id,
    first_name,
    last_name,
    full_name,
    company,
    phone_number,
    address_line_1,
    address_line_2,
    postal_code,
    city,
    province_name,
    province_code,
    country_name,
    country_code,
    time_zone,
    address_validation_status,
    is_default_address,
    are_coordinates_validated as is_coordinates_validated,
    latitude,
    longitude
from source
