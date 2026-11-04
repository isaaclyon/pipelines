with source as (
    select * from {{ source('shopify_customers', 'customers__metafields') }}
)

select
    shop_id,
    customer_id,
    metafield_id,
    namespace as metafield_namespace,
    key as metafield_key,
    type as metafield_type,
    value as metafield_value,
    {{ utc_timestamp('created_at') }} as created_at,
    {{ utc_timestamp('updated_at') }} as updated_at
from source
