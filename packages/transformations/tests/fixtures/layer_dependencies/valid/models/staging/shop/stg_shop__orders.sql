-- Comments mentioning from raw.orders are ignored.
select order_id, customer_id, country_code, created_at
from {{ source('shop', 'orders') }}
