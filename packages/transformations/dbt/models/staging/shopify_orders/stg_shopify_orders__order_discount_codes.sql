-- Source discount codes applied to orders. As with tags, an order observed at
-- extraction revision 2 or later with no rows here has no discount codes.
with source as (
    select * from {{
        sparse_source('shopify_orders', 'orders__discount_codes')
    }}
)

select
    shop_id,
    order_id,
    code as discount_code,
    lower(trim(code)) as normalized_discount_code
from source
