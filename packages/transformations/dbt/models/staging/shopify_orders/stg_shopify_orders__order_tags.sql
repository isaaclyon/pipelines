-- Source order tags. An order observed at extraction revision 2 or later with
-- no rows here has an empty tag list; earlier observations never selected tags.
with source as (
    select * from {{ sparse_source('shopify_orders', 'orders__tags') }}
)

select
    shop_id,
    order_id,
    tag,
    lower(trim(tag)) as normalized_tag
from source
