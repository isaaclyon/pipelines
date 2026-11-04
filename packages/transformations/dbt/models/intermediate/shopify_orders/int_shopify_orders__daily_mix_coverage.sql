-- Coverage disclosed beside the home-market D2C customer-day mix. These orders
-- and customers are outside the mix population and never join its denominator.
-- Scope is the daily population's test, draft-marker and market rules.
with scoped as (
    select
        orders.shop_id,
        orders.customer_id,
        orders.order_date,
        eligibility.is_aov_eligible,
        classifications.is_d2c
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
    where
        orders.order_date is not null
        and not orders.is_test
        and coalesce(orders.source_name, '') != 'shopify_draft_order'
        and orders.is_home_reporting_market
),

-- Identified D2C customers whose only possibly qualifying orders that day have
-- unknown AOV eligibility.
eligibility_unknown_customers as (
    select
        shop_id,
        order_date,
        customer_id
    from scoped
    where customer_id is not null and is_d2c
    group by shop_id, order_date, customer_id
    having
        count(case when is_aov_eligible then 1 end) = 0
        and count(case when is_aov_eligible is null then 1 end) > 0
),

orders_by_day as (
    select
        shop_id,
        order_date,
        count(
            case
                when customer_id is null and is_aov_eligible and is_d2c then 1
            end
        ) as unidentified_qualifying_order_count,
        count(
            case
                when
                    is_d2c is null and coalesce(is_aov_eligible, true)
                    then 1
            end
        ) as d2c_unknown_order_count
    from scoped
    group by shop_id, order_date
),

customers_by_day as (
    select
        shop_id,
        order_date,
        count(*) as eligibility_unknown_customer_count
    from eligibility_unknown_customers
    group by shop_id, order_date
)

select
    orders_by_day.shop_id,
    orders_by_day.order_date,
    orders_by_day.unidentified_qualifying_order_count,
    orders_by_day.d2c_unknown_order_count,
    coalesce(customers_by_day.eligibility_unknown_customer_count, 0)
        as eligibility_unknown_customer_count
from orders_by_day
left join customers_by_day
    on
        orders_by_day.shop_id = customers_by_day.shop_id
        and orders_by_day.order_date = customers_by_day.order_date
