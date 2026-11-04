with source as (
    select * from {{ source('shopify_customers', 'customers__tax_exemptions') }}
)

select
    shop_id,
    customer_id,
    cast(position as bigint) as tax_exemption_position,
    code
from source
