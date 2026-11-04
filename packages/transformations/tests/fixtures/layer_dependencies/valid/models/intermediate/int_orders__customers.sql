select dated.order_id, customers.country_name
from {{ ref('int_orders__dated') }} as dated, {{ ref('stg_shop__customers') }} as customers
