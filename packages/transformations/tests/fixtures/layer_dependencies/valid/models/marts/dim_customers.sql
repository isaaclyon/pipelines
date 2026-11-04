select customers.customer_id, count(orders.order_id) as order_count
from {{ ref('stg_shop__customers') }} as customers
left join {{ ref('fct_orders') }} as orders on true
group by 1
