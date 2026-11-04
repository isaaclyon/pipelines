{% set publication_id = env_var("DBT_PUBLICATION_ID", "") %}

with orders as (
    select * from {{ ref('stg_shopify_orders__orders') }}
),

line_rollups as (
    select * from {{ ref('int_shopify_orders__line_rollups') }}
),

net_units as (
    select * from {{ ref('int_shopify_orders__net_units') }}
),

commercial as (
    select * from {{ ref('int_shopify_orders__commercial_components') }}
),

allocations as (
    select * from {{ ref('int_shopify_orders__allocations') }}
),

payment_history as (
    select * from {{ ref('int_shopify_orders__payment_history') }}
),

revenue_eligibility as (
    select * from {{ ref('int_shopify_orders__revenue_eligibility') }}
),

average_eligibility as (
    select * from {{ ref('int_shopify_orders__average_eligibility') }}
),

classifications as (
    select * from {{ ref('int_shopify_orders__classifications') }}
),

joined as (
    select
        cast(nullif('{{ publication_id }}', '') as varchar)
            as publication_id,
        orders.shop_id as shopify_shop_id,
        orders.order_id as shopify_order_id,
        orders.order_name,
        orders.customer_id as shopify_customer_id,
        orders.created_at,
        orders.processed_at,
        orders.updated_at,
        orders.cancelled_at,
        orders.closed_at,
        orders.ordered_at,
        orders.order_date_source,
        orders.order_date,
        orders.cancel_reason,
        orders.is_test,
        orders.source_name,
        orders.financial_status,
        orders.fulfillment_status,
        orders.is_tax_included,
        orders.shop_currency_code,
        orders.presentment_currency_code,
        orders.total_price_shop_amount,
        orders.total_price_presentment_amount,
        orders.current_total_price_shop_amount,
        orders.current_total_price_presentment_amount,
        orders.original_total_price_shop_amount,
        orders.original_total_price_presentment_amount,
        orders.subtotal_price_shop_amount,
        orders.subtotal_price_presentment_amount,
        orders.current_subtotal_price_shop_amount,
        orders.current_subtotal_price_presentment_amount,
        orders.total_discounts_shop_amount,
        orders.total_discounts_presentment_amount,
        orders.total_tax_shop_amount,
        orders.total_tax_presentment_amount,
        orders.current_total_tax_shop_amount,
        orders.current_total_tax_presentment_amount,
        orders.total_shipping_price_shop_amount,
        orders.total_shipping_price_presentment_amount,
        orders.total_refunded_shop_amount,
        orders.total_refunded_presentment_amount,
        orders.source_observed_at,
        payment_history.is_payment_observation_complete,
        payment_history.is_payment_obligation_verified,
        payment_history.is_payment_chronology_resolved,
        payment_history.has_fully_paid_current_obligation,
        payment_history.net_customer_payment_shop_amount,
        payment_history.is_partially_refunded,
        payment_history.is_fully_refunded,
        revenue_eligibility.is_revenue_eligible,
        revenue_eligibility.revenue_exclusion_reasons,
        revenue_eligibility.revenue_unresolved_requirements,
        average_eligibility.is_aov_eligible,
        average_eligibility.aov_exclusion_reasons,
        average_eligibility.aov_unresolved_requirements,
        classifications.has_classification_evidence,
        classifications.has_retail_evidence,
        classifications.has_specialty_evidence,
        classifications.has_bulk_evidence,
        classifications.has_gifting_tag_evidence,
        classifications.is_d2c,
        classifications.is_commercial,
        classifications.is_retail,
        classifications.is_specialty,
        classifications.is_bulk,
        classifications.is_employee_order,
        classifications.is_gifted,
        classifications.is_replacement,
        classifications.is_exchange,
        classifications.is_shopify_collective,
        classifications.is_broadcast,
        cast(case
            when net_units.order_id is null then 0
            else net_units.net_ordered_quantity
        end as bigint) as net_ordered_quantity,
        commercial.commercial_resolution_status,
        commercial.is_commercial_resolved,
        commercial.agreement_count,
        commercial.sale_count,
        commercial.unresolved_sale_count,
        commercial.refund_object_count,
        commercial.refund_without_agreement_count,
        commercial.refund_object_unresolved_count,
        commercial.unfinalized_fee_count,
        commercial.unresolved_adjustment_count,
        commercial.non_gift_card_line_count,
        commercial.unknown_gift_card_line_count,
        commercial.unresolved_refund_payment_count,
        commercial.gross_merchandise_shop_amount,
        commercial.merchandise_discount_shop_amount,
        commercial.merchandise_refund_shop_amount,
        commercial.net_merchandise_shop_amount,
        commercial.shipping_shop_amount,
        commercial.shipping_refund_shop_amount,
        commercial.net_shipping_shop_amount,
        commercial.net_revenue_shop_amount,
        commercial.duty_shop_amount,
        commercial.gift_card_liability_shop_amount,
        commercial.tax_shop_amount,
        commercial.return_shipping_fee_shop_amount,
        commercial.unfinalized_fee_shop_amount,
        commercial.additional_fee_shop_amount,
        commercial.unresolved_adjustment_shop_amount,
        commercial.unresolved_adjustment_tax_shop_amount,
        commercial.unassigned_shipping_shop_amount,
        commercial.unassigned_shipping_refund_shop_amount,
        commercial.ledger_total_shop_amount,
        commercial.ledger_header_difference_shop_amount,
        commercial.refund_object_shop_amount,
        commercial.refund_linked_sale_total_shop_amount,
        commercial.refund_agreement_difference_shop_amount,
        commercial.successful_collected_shop_amount,
        commercial.successful_refund_object_payment_shop_amount,
        commercial.failed_attempt_reversal_shop_amount,
        commercial.unresolved_refund_payment_shop_amount,
        commercial.reporting_currency_code,
        commercial.unresolved_handling_fee_count,
        commercial.handling_fee_shop_amount,
        commercial.unresolved_payment_activity_count,
        commercial.payment_transaction_count,
        allocations.merchandise_discount_unallocated_shop_amount,
        allocations.merchandise_refund_unallocated_shop_amount,
        allocations.shipping_unallocated_shop_amount,
        allocations.shipping_refund_unallocated_shop_amount,
        allocations.allocation_status,
        allocations.is_allocation_complete,
        orders.cancelled_at is not null as is_cancelled,
        orders.source_name = 'shopify_draft_order' as is_draft_order,
        orders.reporting_market,
        coalesce(
            line_rollups.line_count, cast(0 as bigint)
        ) as line_count,
        coalesce(
            line_rollups.gross_ordered_quantity, cast(0 as bigint)
        ) as gross_ordered_quantity,
        coalesce(
            line_rollups.original_line_value_shop_amount,
            cast(0 as decimal(38, 9))
        ) as original_line_value_shop_amount,
        coalesce(
            line_rollups.original_line_value_presentment_amount,
            cast(0 as decimal(38, 9))
        ) as original_line_value_presentment_amount,
        coalesce(net_units.unresolved_quantity_line_count, cast(0 as bigint))
            as unresolved_quantity_line_count
    from orders
    left join payment_history
        on
            orders.shop_id = payment_history.shop_id
            and orders.order_id = payment_history.order_id
    left join revenue_eligibility
        on
            orders.shop_id = revenue_eligibility.shop_id
            and orders.order_id = revenue_eligibility.order_id
    left join average_eligibility
        on
            orders.shop_id = average_eligibility.shop_id
            and orders.order_id = average_eligibility.order_id
    left join allocations
        on
            orders.shop_id = allocations.shop_id
            and orders.order_id = allocations.order_id
    left join commercial
        on
            orders.shop_id = commercial.shop_id
            and orders.order_id = commercial.order_id
    left join line_rollups
        on
            orders.shop_id = line_rollups.shop_id
            and orders.order_id = line_rollups.order_id
    left join net_units
        on
            orders.shop_id = net_units.shop_id
            and orders.order_id = net_units.order_id
    left join classifications
        on
            orders.shop_id = classifications.shop_id
            and orders.order_id = classifications.order_id
)

select * from joined
