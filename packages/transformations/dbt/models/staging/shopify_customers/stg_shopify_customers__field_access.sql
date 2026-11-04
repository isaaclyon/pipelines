with source as (
    select * from {{ source('shopify_customers', 'customer_field_access') }}
)

select
    shop_id,
    batch_id,
    field_group,
    status,
    {{ utc_timestamp('checked_at') }} as checked_at
from source
