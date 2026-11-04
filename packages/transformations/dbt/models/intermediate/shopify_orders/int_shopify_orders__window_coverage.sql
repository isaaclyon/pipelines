-- Connected updated-at intervals, not MIN(start)/MAX(end) across unknown gaps.
with verified as (
    select distinct
        shop_id,
        window_started_at,
        window_ended_at
    from {{ ref('stg_shopify_orders__ingestion_batches') }}
    where
        is_coverage_verified
        and window_started_at < window_ended_at
),

prior_ends as (
    select
        *,
        max(window_ended_at) over (
            partition by shop_id
            order by window_started_at, window_ended_at
            rows between unbounded preceding and 1 preceding
        ) as prior_ended_at
    from verified
),

boundaries as (
    select
        *,
        case
            when prior_ended_at >= window_started_at then 0 else 1
        end as starts_interval
    from prior_ends
),

intervals as (
    select
        *,
        sum(starts_interval) over (
            partition by shop_id
            order by window_started_at, window_ended_at
            rows between unbounded preceding and current row
        ) as interval_number
    from boundaries
)

select
    shop_id,
    min(window_started_at) as coverage_started_at,
    max(window_ended_at) as coverage_ended_at
from intervals
group by shop_id, interval_number
