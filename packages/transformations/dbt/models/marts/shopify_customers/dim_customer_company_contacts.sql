select
    shop_id as shopify_shop_id,
    customer_id as shopify_customer_id,
    company_contact_id as shopify_company_contact_id,
    company_id as shopify_company_id,
    company_location_id as shopify_company_location_id,
    is_main_contact,
    title,
    company_contact_position
from {{ ref('stg_shopify_customers__company_contacts') }}
