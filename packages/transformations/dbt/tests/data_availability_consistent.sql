{{ config(meta={'dagster': {'ref': {'name': 'fct_data_availability'}}}) }}

with availability as (
    select * from {{ ref('fct_data_availability') }}
),

coverage as (
    select * from {{ ref('int_shopify_orders__window_coverage') }}
),

latest_coverage as (
    select shop_id, max(coverage_ended_at) as coverage_ended_at
    from coverage
    group by shop_id
),

snapshots as (
    select * from {{ ref('stg_loop_returns__snapshots') }}
),

fact_generations as (
    select distinct shopify_shop_id, publication_id
    from {{ ref('fct_orders') }}
)

select availability.availability_id
from availability
left join coverage
    on
        availability.shopify_shop_id = coverage.shop_id
        and availability.shopify_coverage_started_at = coverage.coverage_started_at
        and availability.shopify_coverage_ended_at = coverage.coverage_ended_at
left join latest_coverage
    on availability.shopify_shop_id = latest_coverage.shop_id
left join snapshots
    on
        availability.shopify_shop_id = snapshots.shop_id
        and availability.loop_snapshot_ended_at = snapshots.snapshot_ended_at
        and snapshots.is_snapshot_complete
left join fact_generations
    on availability.shopify_shop_id = fact_generations.shopify_shop_id
where
    (availability.shopify_coverage_ended_at is not null and coverage.shop_id is null)
    or availability.shopify_coverage_ended_at
    is distinct from latest_coverage.coverage_ended_at
    or (availability.is_loop_snapshot_complete and snapshots.shop_id is null)
    or (
        fact_generations.shopify_shop_id is not null
        and availability.publication_id
        is distinct from fact_generations.publication_id
    )
union all
select cast('missing_availability' as varchar)
where not exists (select 1 from availability)
