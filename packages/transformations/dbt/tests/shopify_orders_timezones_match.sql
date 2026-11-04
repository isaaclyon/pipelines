{{ config(meta={'dagster': {'ref': {'name': 'fct_orders'}}}) }}

-- Verify UTC normalization against raw instants and preserve the Pacific date
-- and fallback through publication. Unit tests cover DST and midnight.
{% set instants = ['created_at', 'processed_at', 'updated_at',
    'cancelled_at', 'closed_at'] %}
with staged as (
    select * from {{ ref('stg_shopify_orders__orders') }}
),

raw_orders as (
    select * from {{ source('shopify_orders', 'orders') }}
),

published as (
    select * from {{ ref('fct_orders') }}
)

select
    staged.shop_id,
    staged.order_id
from staged
inner join raw_orders as original
    on staged.shop_id = original.shop_id and staged.order_id = original.order_id
inner join published
    on
        staged.shop_id = published.shopify_shop_id
        and staged.order_id = published.shopify_order_id
where
    staged.ordered_at is distinct from {{ utc_timestamp(
        'coalesce(original.processed_at, original.created_at)'
    ) }}
    or staged.order_date is distinct from {{ local_date(
        'staged.ordered_at', 'America/Los_Angeles'
    ) }}
    or staged.source_observed_at
    is distinct from {{ utc_timestamp('original._observed_at') }}
    or published.source_observed_at is distinct from staged.source_observed_at
    or published.ordered_at is distinct from staged.ordered_at
    or published.order_date is distinct from staged.order_date
    {% for name in instants %}
        or staged.{{ name }}
        is distinct from {{ utc_timestamp('original.' ~ name) }}
        or published.{{ name }} is distinct from staged.{{ name }}
    {% endfor %}
