{% set publication_id = env_var('DBT_PUBLICATION_ID', '') %}
{% set publication_id = publication_id | replace("'", "''") %}
{% set environment = env_var('ANALYTICS_ENVIRONMENT', 'unknown') %}
{% set loop_enabled = env_var('LOOP_RETURNS_ENABLED', 'false') == 'true' %}

with shops as (
    select shop_id from {{ ref('stg_shopify_orders__orders') }}
    union
    select shop_id from {{ ref('stg_shopify_orders__ingestion_batches') }}
    union
    select shop_id from {{ ref('stg_loop_returns__snapshots') }}
),

-- Keep an explicit unknown row even before any shop has source evidence.
known_shops as (
    select shop_id from shops
    union all
    select cast(null as varchar) as shop_id
    where not exists (select 1 from shops)
),

coverage as (
    select
        *,
        row_number() over (
            partition by shop_id order by coverage_ended_at desc
        ) as recency
    from {{ ref('int_shopify_orders__window_coverage') }}
),

loop_snapshots as (
    select
        *,
        row_number() over (
            partition by shop_id
            order by
                snapshot_ended_at desc, extraction_completed_at desc,
                snapshot_id desc, snapshot_load_id desc
        ) as recency
    from {{ ref('stg_loop_returns__snapshots') }}
)

select
    'shopify_orders/' || coalesce(known_shops.shop_id, 'unknown')
        as availability_id,
    known_shops.shop_id as shopify_shop_id,
    cast(
        nullif('{{ publication_id }}', '')
        as varchar
    ) as publication_id,
    '{{ environment | replace("'", "''") }}'
        as environment,
    {{ utc_timestamp('current_timestamp') }} as evidence_captured_at,
    coverage.coverage_started_at as shopify_coverage_started_at,
    coverage.coverage_ended_at as shopify_coverage_ended_at,
    loop_snapshots.snapshot_ended_at as loop_snapshot_ended_at,
    coalesce(loop_snapshots.is_snapshot_complete, false)
        as is_loop_snapshot_complete,
    cast({{ 'true' if loop_enabled else 'false' }} as boolean)
        as is_loop_enabled,
    'Shopify updated_at windows do not prove the initial order-date population '
    || 'or deletion coverage. Loop snapshot evidence has no retained '
    || 'history-start boundary. Unknown eligibility and scope require '
    || 'separate revenue coverage.'
        as coverage_limitations
from known_shops
left join coverage
    on known_shops.shop_id = coverage.shop_id and coverage.recency = 1
left join loop_snapshots
    on
        known_shops.shop_id = loop_snapshots.shop_id
        and loop_snapshots.recency = 1
