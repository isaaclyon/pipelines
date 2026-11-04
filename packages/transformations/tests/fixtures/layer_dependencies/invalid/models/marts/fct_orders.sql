select orders.*
from {{ ref('stg_shop__orders') }} as orders
inner join raw.shop.customers as customers on orders.customer_id = customers.customer_id
left join {{ target.schema }}.countries as countries on true
left join "legacy"."orders" as legacy on true
left join 'orders.parquet' as files on true
