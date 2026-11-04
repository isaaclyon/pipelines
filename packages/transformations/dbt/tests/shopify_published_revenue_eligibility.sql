{{ config(meta={'dagster': {'ref': {'name': 'fct_orders'}}}) }}

-- Preserve order-grain decisions and every reason from their owning model.
-- A candidate with contradictory proof cannot replace the published answer.
select
    coalesce(orders.shopify_shop_id, evidence.shop_id) as shop_id,
    coalesce(orders.shopify_order_id, evidence.order_id) as order_id
from {{ ref('fct_orders') }} as orders
full outer join {{ ref('int_shopify_orders__revenue_eligibility') }} as evidence
    on
        orders.shopify_shop_id = evidence.shop_id
        and orders.shopify_order_id = evidence.order_id
where
    orders.shopify_order_id is null
    or evidence.order_id is null
    or orders.is_revenue_eligible is distinct from evidence.is_revenue_eligible
    or orders.revenue_exclusion_reasons
    is distinct from evidence.revenue_exclusion_reasons
    or orders.revenue_unresolved_requirements
    is distinct from evidence.revenue_unresolved_requirements
    or (orders.is_revenue_eligible and (
        orders.has_fully_paid_current_obligation is distinct from true
        or orders.is_payment_observation_complete is distinct from true
        or orders.is_payment_obligation_verified is distinct from true
        or orders.is_payment_chronology_resolved is distinct from true
        or orders.is_commercial_resolved is distinct from true
        or orders.is_test is distinct from false
        or orders.revenue_exclusion_reasons <> ''
        or orders.revenue_unresolved_requirements <> ''
    ))
    or (orders.is_partially_refunded and orders.is_fully_refunded)
