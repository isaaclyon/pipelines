select order_id from {{ source('shop', 'orders') }}
except
select order_id from {{ ref('fct_orders') }}
