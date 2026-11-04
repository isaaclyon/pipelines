select distinct order_id, customer_id, created_at
from {{ ref('stg_shop__orders') }}
