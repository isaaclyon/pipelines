{% snapshot orders_snapshot %}
{{ config(unique_key='id', strategy='check', check_cols='all') }}
select * from {{ ref('stg_shop__orders') }}
{% endsnapshot %}
