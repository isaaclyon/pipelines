{{ config(meta={'dagster': {'ref': {'name': 'fct_orders'}}}) }}

-- Reconcile the published verdicts and every reason to their owning models.
select
    coalesce(orders.shopify_shop_id, evidence.shop_id) as shop_id,
    coalesce(orders.shopify_order_id, evidence.order_id) as order_id
from {{ ref('fct_orders') }} as orders
full outer join {{ ref('int_shopify_orders__average_eligibility') }} as evidence
    on
        orders.shopify_shop_id = evidence.shop_id
        and orders.shopify_order_id = evidence.order_id
where
    orders.shopify_order_id is null
    or evidence.order_id is null
    or orders.is_aov_eligible is distinct from evidence.is_aov_eligible
    or orders.aov_exclusion_reasons
    is distinct from evidence.aov_exclusion_reasons
    or orders.aov_unresolved_requirements
    is distinct from evidence.aov_unresolved_requirements
    or (orders.is_aov_eligible and (
        orders.is_revenue_eligible is distinct from true
        or orders.is_cancelled is distinct from false
        or orders.is_gifted is distinct from false
        or orders.is_replacement is distinct from false
        or orders.is_exchange is distinct from false
        or orders.is_fully_refunded is distinct from false
        or orders.net_revenue_shop_amount is null
        or orders.net_revenue_shop_amount <= 0
    ))

union all

select
    coalesce(lines.shopify_shop_id, evidence.shop_id) as shop_id,
    coalesce(lines.shopify_order_id, evidence.order_id) as order_id
from {{ ref('fct_order_lines') }} as lines
full outer join {{ ref('int_shopify_order_lines__asp_eligibility') }} as evidence
    on
        lines.shopify_shop_id = evidence.shop_id
        and lines.shopify_order_id = evidence.order_id
        and lines.shopify_line_item_id = evidence.line_item_id
left join {{ ref('fct_orders') }} as orders
    on
        lines.shopify_shop_id = orders.shopify_shop_id
        and lines.shopify_order_id = orders.shopify_order_id
where
    lines.shopify_line_item_id is null
    or evidence.line_item_id is null
    or orders.shopify_order_id is null
    or lines.is_asp_eligible is distinct from evidence.is_asp_eligible
    or lines.asp_exclusion_reasons is distinct from evidence.asp_exclusion_reasons
    or lines.asp_unresolved_requirements
    is distinct from evidence.asp_unresolved_requirements
    or lines.is_revenue_eligible is distinct from orders.is_revenue_eligible
    or lines.revenue_exclusion_reasons
    is distinct from orders.revenue_exclusion_reasons
    or lines.revenue_unresolved_requirements
    is distinct from orders.revenue_unresolved_requirements
    or lines.is_aov_eligible is distinct from orders.is_aov_eligible
    or lines.aov_exclusion_reasons is distinct from orders.aov_exclusion_reasons
    or lines.aov_unresolved_requirements
    is distinct from orders.aov_unresolved_requirements
    or (lines.is_asp_eligible and (
        orders.is_revenue_eligible is distinct from true
        or orders.is_cancelled is distinct from false
        or orders.is_gifted is distinct from false
        or orders.is_replacement is distinct from false
        or orders.is_exchange is distinct from false
        or lines.is_gift_card_item is distinct from false
        or lines.net_ordered_quantity is null
        or lines.net_merchandise_shop_amount is null
    ))
