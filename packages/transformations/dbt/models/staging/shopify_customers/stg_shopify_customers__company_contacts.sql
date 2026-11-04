with source as (
    select *
    from {{ source('shopify_customers', 'customers__company_contacts') }}
)

select
    shop_id,
    customer_id,
    company_contact_id,
    company_id,
    company_location_id,
    is_main_contact,
    title,
    cast(_dlt_list_idx as bigint) as company_contact_position
from source
