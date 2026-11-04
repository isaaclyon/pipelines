select order_id from {{ source('shop', 'orders') }}
