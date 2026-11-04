-- Revenue supplies shared payment/commercial prerequisites, not the population
-- for averages. AOV adds retained-value/refund rules; ASP does not.
with evidence as (
    select
        orders.shop_id,
        orders.order_id,
        revenue.is_revenue_eligible,
        classifications.is_gifted,
        classifications.is_replacement,
        classifications.is_exchange,
        payments.is_fully_refunded,
        commercial.net_revenue_shop_amount,
        orders.cancelled_at is not null as is_cancelled,
        coalesce(revenue.revenue_exclusion_reasons, '')
        || (case
            when orders.cancelled_at is not null
                then 'cancelled_order|'
            else ''
        end)
        || (case
            when classifications.is_gifted
                then 'gifted_order|'
            else ''
        end)
        || (case
            when classifications.is_replacement
                then 'replacement_order|'
            else ''
        end)
        || (case
            when classifications.is_exchange
                then 'exchange_order|'
            else ''
        end)
            as asp_order_exclusion_reasons,
        coalesce(revenue.revenue_unresolved_requirements, '')
        || (case
            when revenue.order_id is null
                then 'revenue_evidence_missing|'
            else ''
        end)
        || (case
            when classifications.is_gifted is null
                then 'gifting_status_unknown|'
            else ''
        end)
        || (case
            when classifications.is_replacement is null
                then 'replacement_status_unknown|'
            else ''
        end)
        || (case
            when classifications.is_exchange is null
                then 'exchange_status_unknown|'
            else ''
        end)
            as asp_order_unresolved_requirements
    from {{ ref('stg_shopify_orders__orders') }} as orders
    left join {{ ref('int_shopify_orders__revenue_eligibility') }} as revenue
        on
            orders.shop_id = revenue.shop_id
            and orders.order_id = revenue.order_id
    left join
        {{ ref('int_shopify_orders__classifications') }} as classifications
        on
            orders.shop_id = classifications.shop_id
            and orders.order_id = classifications.order_id
    left join {{ ref('int_shopify_orders__payment_history') }} as payments
        on
            orders.shop_id = payments.shop_id
            and orders.order_id = payments.order_id
    left join
        {{ ref('int_shopify_orders__commercial_components') }} as commercial
        on
            orders.shop_id = commercial.shop_id
            and orders.order_id = commercial.order_id
),

parent_scope as (
    select
        *,
        case
            when
                is_revenue_eligible = false or is_cancelled
                or is_gifted or is_replacement or is_exchange then false
            when
                is_revenue_eligible is null or is_gifted is null
                or is_replacement is null or is_exchange is null then null
            else true
        end as is_asp_order_eligible
    from evidence
)

select
    shop_id,
    order_id,
    is_asp_order_eligible,
    asp_order_exclusion_reasons,
    asp_order_unresolved_requirements,
    case
        when
            is_asp_order_eligible = false or is_fully_refunded
            or net_revenue_shop_amount <= 0 then false
        when
            is_asp_order_eligible is null or is_fully_refunded is null
            or net_revenue_shop_amount is null then null
        else true
    end as is_aov_eligible,
    asp_order_exclusion_reasons
    || (case when is_fully_refunded then 'fully_refunded|' else '' end)
    || (case
        when net_revenue_shop_amount <= 0
            then 'nonpositive_commercial_value|'
        else ''
    end)
        as aov_exclusion_reasons,
    asp_order_unresolved_requirements
    || (case
        when is_fully_refunded is null
            then 'refund_status_unknown|'
        else ''
    end)
    || (case
        when net_revenue_shop_amount is null
            then 'retained_commercial_value_unknown|'
        else ''
    end)
        as aov_unresolved_requirements
from parent_scope
