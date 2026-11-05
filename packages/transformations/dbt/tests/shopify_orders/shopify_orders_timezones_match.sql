{{ config(meta={'dagster': {'ref': {'name': 'stg_shopify_orders__orders'}}}) }}

-- Verify UTC normalization against raw instants and the reporting-zone
-- order date and its fallback. Unit tests cover DST and midnight.
{% set instants = ['created_at', 'processed_at', 'updated_at',
    'cancelled_at', 'closed_at'] %}
with staged as (
    select * from {{ ref('stg_shopify_orders__orders') }}
),

raw_orders as (
    select * from {{ source('shopify_orders', 'orders') }}
)

select
    staged.shop_id,
    staged.order_id
from staged
inner join raw_orders as original
    on staged.shop_id = original.shop_id and staged.order_id = original.order_id
where
    staged.ordered_at is distinct from {{ utc_timestamp(
        'coalesce(original.processed_at, original.created_at)'
    ) }}
    or staged.order_date is distinct from {{ local_date(
        'staged.ordered_at', reporting_time_zone()
    ) }}
    or staged.source_observed_at
    is distinct from {{ utc_timestamp('original._observed_at') }}
    {% for name in instants %}
        or staged.{{ name }}
        is distinct from {{ utc_timestamp('original.' ~ name) }}
    {% endfor %}
