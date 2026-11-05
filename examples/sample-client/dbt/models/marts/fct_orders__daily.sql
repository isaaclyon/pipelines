-- Non-test Shopify orders per reporting day.
select
    order_date,
    min(ordered_at) as first_ordered_at,
    count(*) as order_count
from {{ ref('analytics_toolkit', 'stg_shopify_orders__orders') }}
where not is_test
group by order_date
