-- Revenue-specific decisions. Cancellation, gifting, replacements, exchanges
-- and channels are deliberately not universal revenue exclusions.
with gift_card_orders as (
    select distinct
        shop_id,
        order_id
    from {{ ref('stg_shopify_orders__order_lines') }}
    where is_gift_card
),

evidence as (
    select
        orders.shop_id,
        orders.order_id,
        orders.is_test,
        payments.has_fully_paid_current_obligation,
        coalesce(payments.is_payment_observation_complete, false)
            as is_payment_observation_complete,
        coalesce(payments.is_payment_obligation_verified, false)
            as is_payment_obligation_verified,
        coalesce(payments.is_payment_chronology_resolved, false)
            as is_payment_chronology_resolved,
        coalesce(
            commercial.is_commercial_resolved
            and commercial.unknown_gift_card_line_count = 0, false)
            as commercial_value_known,
        coalesce(
            commercial.is_commercial_resolved
            and gift_card_orders.order_id is not null
            and commercial.non_gift_card_line_count = 0
            and commercial.unknown_gift_card_line_count = 0, false)
            as is_gift_card_only
    from {{ ref('stg_shopify_orders__orders') }} as orders
    left join gift_card_orders
        on
            orders.shop_id = gift_card_orders.shop_id
            and orders.order_id = gift_card_orders.order_id
    left join {{ ref('int_shopify_orders__payment_history') }} as payments
        on
            orders.shop_id = payments.shop_id
            and orders.order_id = payments.order_id
    left join
        {{ ref('int_shopify_orders__commercial_components') }} as commercial
        on
            orders.shop_id = commercial.shop_id
            and orders.order_id = commercial.order_id
)

select
    shop_id,
    order_id,
    case
        when
            is_test or is_gift_card_only
            or has_fully_paid_current_obligation = false then false
        when
            is_test is null or has_fully_paid_current_obligation is null
            or not commercial_value_known then null
        else true
    end as is_revenue_eligible,
    (case when is_test then 'test_order|' else '' end)
    || (case when is_gift_card_only then 'gift_card_only|' else '' end)
    || (case
        when has_fully_paid_current_obligation = false
            then 'payment_not_fully_collected|'
        else ''
    end)
        as revenue_exclusion_reasons,
    (case when is_test is null then 'test_status_unknown|' else '' end)
    || (case
        when not is_payment_observation_complete
            then 'payment_evidence_incomplete|'
        else ''
    end)
    || (case
        when not is_payment_obligation_verified
            then 'payment_obligation_unknown|'
        else ''
    end)
    || (case
        when not is_payment_chronology_resolved
            then 'payment_chronology_unresolved|'
        else ''
    end)
    || (case
        when not commercial_value_known
            then 'commercial_value_unknown|'
        else ''
    end)
        as revenue_unresolved_requirements
from evidence
