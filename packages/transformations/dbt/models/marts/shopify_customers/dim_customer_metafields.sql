select
    shop_id as shopify_shop_id,
    customer_id as shopify_customer_id,
    metafield_id as shopify_metafield_id,
    metafield_namespace,
    metafield_key,
    metafield_type,
    metafield_value,
    created_at,
    updated_at
from {{ ref('stg_shopify_customers__metafields') }}
