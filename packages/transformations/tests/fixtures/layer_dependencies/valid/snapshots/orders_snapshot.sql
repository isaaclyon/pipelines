{% snapshot orders_snapshot %}
{{ config(unique_key='order_id', strategy='check', check_cols='all') }}
select order_id, customer_id from {{ source('shop', 'orders') }}
{% endsnapshot %}
