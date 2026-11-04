select order_id, country_name
from {{ ref('int_orders__customers') }}
