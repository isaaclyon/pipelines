select order_id, dbt_valid_from
from {{ ref('orders_snapshot') }}
