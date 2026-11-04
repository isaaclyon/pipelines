select
    shop_id as shopify_shop_id,
    customer_id as shopify_customer_id,
    tag_position,
    tag
from {{ ref('stg_shopify_customers__tags') }}
