with source as (
    select * from {{ source('shopify_customers', 'customers__tags') }}
)

select
    shop_id,
    customer_id,
    cast(position as bigint) as tag_position,
    tag
from source
