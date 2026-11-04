select
    shop_id as shopify_shop_id,
    customer_id as shopify_customer_id,
    tax_exemption_position,
    code
from {{ ref('stg_shopify_customers__tax_exemptions') }}
