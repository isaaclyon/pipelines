select order_id from {{ ref('stg_shop__orders') }}
except
select order_id from {{ ref('fct_orders') }}
