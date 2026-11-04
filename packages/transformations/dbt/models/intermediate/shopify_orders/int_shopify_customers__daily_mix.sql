-- Home-market D2C customer-day mix. The daily population uses the home-market
-- D2C scope; prior history uses every market, channel and source. A prior
-- AOV-true day proves repeat. No negative-history evidence exists yet, so a
-- customer without that proof stays unknown: this model never classifies
-- anyone new.
with orders as (
    select
        orders.shop_id,
        orders.customer_id,
        orders.order_date,
        eligibility.is_aov_eligible,
        eligibility.is_aov_eligible
        and classifications.is_d2c
        and not orders.is_test
        and coalesce(orders.source_name, '') != 'shopify_draft_order'
        and orders.is_home_reporting_market
            as is_qualifying
    from {{ ref('stg_shopify_orders__orders') }} as orders
    left join
        {{ ref('int_shopify_orders__average_eligibility') }} as eligibility
        on
            orders.shop_id = eligibility.shop_id
            and orders.order_id = eligibility.order_id
    left join
        {{ ref('int_shopify_orders__classifications') }} as classifications
        on
            orders.shop_id = classifications.shop_id
            and orders.order_id = classifications.order_id
    where orders.customer_id is not null and orders.order_date is not null
),

customer_days as (
    select
        shop_id,
        customer_id,
        order_date,
        count(case when is_qualifying then 1 end) as qualifying_order_count,
        count(case when is_aov_eligible then 1 end) as aov_order_count,
        count(case when is_aov_eligible is null then 1 end)
            as unresolved_order_count
    from orders
    group by shop_id, customer_id, order_date
),

-- One row per customer and day, so preceding rows are strictly earlier days.
history as (
    select
        *,
        coalesce(sum(aov_order_count) over (
            partition by shop_id, customer_id order by order_date
            rows between unbounded preceding and 1 preceding
        ), 0) as prior_aov_order_count,
        coalesce(sum(unresolved_order_count) over (
            partition by shop_id, customer_id order by order_date
            rows between unbounded preceding and 1 preceding
        ), 0) as prior_unresolved_order_count,
        min(case when aov_order_count > 0 then order_date end) over (
            partition by shop_id, customer_id order by order_date
            rows between unbounded preceding and 1 preceding
        ) as first_prior_qualifying_order_date
    from customer_days
)

select
    shop_id,
    customer_id,
    order_date,
    qualifying_order_count,
    first_prior_qualifying_order_date,
    case
        when prior_aov_order_count > 0 then 'repeat' else 'unknown'
    end as customer_mix_verdict,
    case
        when prior_aov_order_count > 0 then ''
        else
            (case
                when prior_unresolved_order_count > 0
                    then 'unresolved_prior_eligibility|'
                else ''
            end)
            || 'unproven_history_coverage|'
    end as unknown_reasons
from history
where qualifying_order_count > 0
