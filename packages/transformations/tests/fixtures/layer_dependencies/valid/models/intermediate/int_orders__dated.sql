{{ config(materialized='ephemeral') }}

with recursive orders as (
    select
        order_id,
        customer_id,
        extract(year from created_at) as order_year,
        substring('from x' from 1 for 2) as label
    from {{ ref('stg_shop__orders_deduplicated') }}
    where customer_id is not distinct from customer_id
)

select orders.*, tags.tag
from orders
cross join unnest(['a', 'b']) as tags (tag)
where order_id in (select order_id from {{ ref('stg_shop__orders') }})
