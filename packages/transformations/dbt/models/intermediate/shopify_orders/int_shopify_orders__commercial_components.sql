-- One row per source order. Components come from the sales ledger, refund
-- objects,
-- refund adjustments and payment reconciliation. Nothing here is an allocation
-- or
-- an eligibility decision.
--
-- Known-zero rule: a component is zero only when every sale for the order is
-- resolved and the component has no rows. Any unresolved sale, missing
-- agreement
-- evidence or missing money keeps the affected component NULL.
--
-- Net money (net_merchandise, net_shipping, net_revenue) is NULL unless
-- commercial_resolution_status is 'complete'. Unfinalized fees and unexplained
-- adjustments block that status until matching evidence is integrated.
with orders as (
    select
        shop_id,
        order_id,
        current_total_price_shop_amount
    from {{ ref('stg_shopify_orders__orders') }}
),

agreements as (
    select
        shop_id,
        order_id,
        agreement_id,
        reason,
        happened_at,
        return_id,
        refund_id
    from {{ ref('stg_shopify_orders__agreements') }}
),

agreement_rollup as (
    select
        shop_id,
        order_id,
        cast(count(*) as bigint) as agreement_count,
        cast(sum(case when reason = 'ORDER' then 1 else 0 end) as bigint)
            as order_agreement_count
    from agreements
    group by shop_id, order_id
),

line_rollup as (
    select
        shop_id,
        order_id,
        cast(sum(case when is_gift_card = false then 1 else 0 end) as bigint)
            as non_gift_card_line_count,
        cast(sum(case when is_gift_card is null then 1 else 0 end) as bigint)
            as unknown_gift_card_line_count
    from {{ ref('stg_shopify_orders__order_lines') }}
    group by shop_id, order_id
),

-- Direct refund identity wins. Return identity is accepted only one-to-one.
ledger_rollup as (
    select
        shop_id,
        order_id,
        cast(count(*) as bigint) as sale_count,
        cast(
            sum(
                case when resolution_status <> 'resolved' then 1 else 0 end
            ) as bigint
        ) as unresolved_sale_count,
        cast(
            sum(
                case when sale_kind = 'unfinalized_fee' then 1 else 0 end
            ) as bigint
        ) as unfinalized_fee_count,
        coalesce(sum(gross_merchandise_shop_amount), 0)
            as gross_merchandise_total_shop_amount,
        coalesce(sum(merchandise_discount_shop_amount), 0)
            as merchandise_discount_total_shop_amount,
        coalesce(sum(merchandise_refund_shop_amount), 0)
            as merchandise_refund_total_shop_amount,
        coalesce(sum(shipping_shop_amount), 0) as shipping_total_shop_amount,
        coalesce(sum(shipping_refund_shop_amount), 0)
            as shipping_refund_total_shop_amount,
        coalesce(sum(gift_card_liability_shop_amount), 0)
            as gift_card_liability_total_shop_amount,
        coalesce(sum(handling_fee_shop_amount), 0)
            as ledger_handling_fee_total_shop_amount,
        coalesce(sum(duty_shop_amount), 0) as duty_total_shop_amount,
        coalesce(sum(return_shipping_fee_shop_amount), 0)
            as return_shipping_fee_total_shop_amount,
        coalesce(sum(unfinalized_fee_shop_amount), 0)
            as unfinalized_fee_total_shop_amount,
        coalesce(sum(additional_fee_shop_amount), 0)
            as additional_fee_total_shop_amount,
        coalesce(
            sum(
                case
                    when
                        resolution_status = 'resolved'
                        then sale_tax_shop_amount
                end
            ),
            0
        ) as tax_total_shop_amount,
        coalesce(
            sum(
                case
                    when
                        resolution_status = 'resolved'
                        then sale_total_shop_amount
                end
            ),
            0
        ) as ledger_total_shop_amount
    from {{ ref('int_shopify_sales__ledger') }}
    group by shop_id, order_id
),

refund_rollup as (
    select
        shop_id,
        order_id,
        cast(count(*) as bigint) as refund_object_count,
        cast(sum(without_agreement_count) as bigint)
            as refund_without_agreement_count,
        cast(sum(component_difference_count) as bigint)
            as refund_component_mismatch_count,
        cast(sum(payment_difference_count) as bigint)
            as refund_payment_mismatch_count,
        cast(
            sum(
                case
                    when
                        coalesce(shop_currency_code, '') = 'USD'
                        and total_refunded_shop_amount is not null
                        then 0
                    else 1
                end
            ) as bigint
        ) as refund_object_unresolved_count,
        sum(commercial_sale_total_shop_amount)
            as refund_linked_sale_total_shop_amount,
        sum(
            case
                when
                    coalesce(shop_currency_code, '') = 'USD'
                    then total_refunded_shop_amount
            end
        ) as refund_object_total_shop_amount
    from {{ ref('int_shopify_refunds__commercial_reconciliation') }}
    group by shop_id, order_id
),

-- Every refund adjustment is unexplained until finalized evidence explains it.
-- An adjustment never becomes a merchandise refund, shipping refund or fee
-- here.
handling_fee_rollup as (
    select
        shop_id,
        order_id,
        cast(sum(case when handling_fee_status not in (
            'no_adjustment_records', 'matched_loop_handling_fee'
        ) then 1 else 0 end) as bigint) as unresolved_handling_fee_count,
        cast(sum(handling_fee_shop_amount) as decimal(38, 9))
            as handling_fee_shop_amount
    from {{ ref('int_shopify_refunds__handling_fees') }}
    group by shop_id, order_id
),

unresolved_adjustments as (
    select adjustments.*
    from {{ ref('stg_shopify_orders__refund_adjustments') }} as adjustments
    left join {{ ref('int_shopify_refunds__handling_fees') }} as fees
        on
            adjustments.shop_id = fees.shop_id
            and adjustments.order_id = fees.order_id
            and adjustments.refund_id = fees.refund_id
    where coalesce(fees.handling_fee_status, '') <> 'matched_loop_handling_fee'
),

adjustment_rollup as (
    select
        shop_id,
        order_id,
        cast(count(*) as bigint) as unresolved_adjustment_count,
        cast(
            sum(
                case
                    when
                        coalesce(adjustment_shop_currency_code, '') = 'USD'
                        and coalesce(adjustment_tax_shop_currency_code, '')
                        = 'USD'
                        and adjustment_shop_amount is not null
                        and adjustment_tax_shop_amount is not null
                        then 0
                    else 1
                end
            ) as bigint
        ) as adjustment_currency_unresolved_count,
        sum(
            case
                when
                    coalesce(adjustment_shop_currency_code, '') = 'USD'
                    then adjustment_shop_amount
            end
        ) as unresolved_adjustment_total_shop_amount,
        sum(
            case
                when
                    coalesce(adjustment_tax_shop_currency_code, '') = 'USD'
                    then adjustment_tax_shop_amount
            end
        ) as unresolved_adjustment_tax_total_shop_amount
    from unresolved_adjustments
    group by shop_id, order_id
),

joined as (
    select
        orders.shop_id,
        orders.order_id,
        orders.current_total_price_shop_amount,
        ledger_rollup.gross_merchandise_total_shop_amount,
        ledger_rollup.merchandise_discount_total_shop_amount,
        ledger_rollup.merchandise_refund_total_shop_amount,
        ledger_rollup.shipping_total_shop_amount,
        ledger_rollup.shipping_refund_total_shop_amount,
        ledger_rollup.gift_card_liability_total_shop_amount,
        ledger_rollup.duty_total_shop_amount,
        ledger_rollup.ledger_handling_fee_total_shop_amount,
        ledger_rollup.return_shipping_fee_total_shop_amount,
        ledger_rollup.unfinalized_fee_total_shop_amount,
        ledger_rollup.additional_fee_total_shop_amount,
        ledger_rollup.tax_total_shop_amount,
        ledger_rollup.ledger_total_shop_amount,
        payment_reconciliation.collected_shop_amount,
        payment_reconciliation.refund_object_payment_shop_amount,
        payment_reconciliation.failed_attempt_reversal_shop_amount,
        payment_reconciliation.unresolved_refund_payment_shop_amount,
        coalesce(agreement_rollup.agreement_count, 0) as agreement_count,
        coalesce(agreement_rollup.order_agreement_count, 0)
            as order_agreement_count,
        coalesce(ledger_rollup.sale_count, 0) as sale_count,
        coalesce(ledger_rollup.unresolved_sale_count, 0)
            as unresolved_sale_count,
        coalesce(ledger_rollup.unfinalized_fee_count, 0)
            as unfinalized_fee_count,
        coalesce(refund_rollup.refund_without_agreement_count, 0)
            as refund_without_agreement_count,
        coalesce(refund_rollup.refund_object_unresolved_count, 0)
            as refund_object_unresolved_count,
        coalesce(refund_rollup.refund_object_count, 0) as refund_object_count,
        coalesce(refund_rollup.refund_component_mismatch_count, 0)
            as refund_component_mismatch_count,
        coalesce(refund_rollup.refund_payment_mismatch_count, 0)
            as refund_payment_mismatch_count,
        coalesce(adjustment_rollup.unresolved_adjustment_count, 0)
            as unresolved_adjustment_count,
        coalesce(adjustment_rollup.adjustment_currency_unresolved_count, 0)
            as adjustment_currency_unresolved_count,
        coalesce(payment_reconciliation.unresolved_refund_payment_count, 0)
            as unresolved_refund_payment_count,
        coalesce(payment_reconciliation.payment_currency_unresolved_count, 0)
            as payment_currency_unresolved_count,
        coalesce(line_rollup.non_gift_card_line_count, 0)
            as non_gift_card_line_count,
        coalesce(line_rollup.unknown_gift_card_line_count, 0)
            as unknown_gift_card_line_count,
        coalesce(refund_rollup.refund_linked_sale_total_shop_amount, 0)
            as refund_linked_sale_total_shop_amount,
        coalesce(refund_rollup.refund_object_total_shop_amount, 0)
            as refund_object_total_shop_amount,
        coalesce(adjustment_rollup.unresolved_adjustment_total_shop_amount, 0)
            as unresolved_adjustment_total_shop_amount,
        coalesce(
            adjustment_rollup.unresolved_adjustment_tax_total_shop_amount, 0
        ) as unresolved_adjustment_tax_total_shop_amount,
        coalesce(fees.unresolved_handling_fee_count, cast(0 as bigint))
            as unresolved_handling_fee_count,
        case
            when coalesce(fees.unresolved_handling_fee_count, 0) = 0
                then
                    coalesce(
                        fees.handling_fee_shop_amount, cast(0 as decimal(38, 9))
                    )
        end as handling_fee_shop_amount,
        coalesce(
            payment_reconciliation.payment_transaction_count, cast(0 as bigint)
        )
            as payment_transaction_count,
        coalesce(
            payment_reconciliation.unresolved_payment_activity_count,
            cast(0 as bigint)
        )
            as unresolved_payment_activity_count
    from orders
    left join agreement_rollup
        on
            orders.shop_id = agreement_rollup.shop_id
            and orders.order_id = agreement_rollup.order_id
    left join ledger_rollup
        on
            orders.shop_id = ledger_rollup.shop_id
            and orders.order_id = ledger_rollup.order_id
    left join refund_rollup
        on
            orders.shop_id = refund_rollup.shop_id
            and orders.order_id = refund_rollup.order_id
    left join adjustment_rollup
        on
            orders.shop_id = adjustment_rollup.shop_id
            and orders.order_id = adjustment_rollup.order_id
    left join handling_fee_rollup as fees
        on orders.shop_id = fees.shop_id and orders.order_id = fees.order_id
    left join
        {{ ref('int_shopify_orders__payment_reconciliation') }}
            as payment_reconciliation
        on
            orders.shop_id = payment_reconciliation.shop_id
            and orders.order_id = payment_reconciliation.order_id
    left join line_rollup
        on
            orders.shop_id = line_rollup.shop_id
            and orders.order_id = line_rollup.order_id
),

statused as (
    select
        *,
        case
            when agreement_count = 0 then 'no_agreement_evidence'
            when order_agreement_count = 0 then 'no_order_agreement'
            when sale_count = 0 then 'no_sale_evidence'
            when unresolved_sale_count > 0 then 'unresolved_sales'
            when
                refund_without_agreement_count > 0
                or refund_object_unresolved_count > 0
                then 'unresolved_refund'
            when
                unresolved_handling_fee_count > 0
                then 'unresolved_handling_fee'
            when unfinalized_fee_count > 0 then 'unfinalized_fee'
            when unresolved_adjustment_count > 0 then 'unresolved_adjustment'
            when
                unresolved_refund_payment_count > 0
                then 'unresolved_payment_refund'
            when
                payment_currency_unresolved_count > 0
                then 'unresolved_payment_currency'
            when payment_transaction_count = 0 then 'no_payment_evidence'
            when
                unresolved_payment_activity_count > 0
                then 'unresolved_payment_activity'
            when
                current_total_price_shop_amount is null
                or ledger_total_shop_amount
                - ledger_handling_fee_total_shop_amount
                <> current_total_price_shop_amount
                then 'unresolved_header_difference'
            when
                refund_component_mismatch_count > 0
                then 'unresolved_refund_difference'
            when
                refund_object_total_shop_amount
                + refund_linked_sale_total_shop_amount
                + handling_fee_shop_amount
                - ledger_handling_fee_total_shop_amount
                <> 0
                then 'unresolved_refund_difference'
            when
                refund_payment_mismatch_count > 0
                or refund_object_total_shop_amount
                <> refund_object_payment_shop_amount
                then 'unresolved_refund_payment_difference'
            else 'complete'
        end as commercial_resolution_status,
        -- Ledger components are complete whenever every sale resolved and an
        -- order agreement exists, even if a later check blocks net money.
        sale_count > 0
        and unresolved_sale_count = 0
        and order_agreement_count > 0 as has_known_sales
    from joined
)

select
    shop_id,
    order_id,
    commercial_resolution_status,
    agreement_count,
    sale_count,
    unresolved_sale_count,
    refund_object_count,
    refund_without_agreement_count,
    refund_object_unresolved_count,
    unfinalized_fee_count,
    unresolved_adjustment_count,
    non_gift_card_line_count,
    unknown_gift_card_line_count,
    unresolved_handling_fee_count,
    handling_fee_shop_amount,
    payment_transaction_count,
    unresolved_payment_activity_count,
    collected_shop_amount as successful_collected_shop_amount,
    refund_object_payment_shop_amount
        as successful_refund_object_payment_shop_amount,
    failed_attempt_reversal_shop_amount,
    unresolved_refund_payment_shop_amount,
    unresolved_refund_payment_count,
    'USD' as reporting_currency_code,
    commercial_resolution_status = 'complete' as is_commercial_resolved,
    case when has_known_sales then gross_merchandise_total_shop_amount end
        as gross_merchandise_shop_amount,
    case when has_known_sales then merchandise_discount_total_shop_amount end
        as merchandise_discount_shop_amount,
    case when has_known_sales then merchandise_refund_total_shop_amount end
        as merchandise_refund_shop_amount,
    case
        when commercial_resolution_status = 'complete'
            then
                gross_merchandise_total_shop_amount
                + merchandise_discount_total_shop_amount
                + merchandise_refund_total_shop_amount
    end as net_merchandise_shop_amount,
    case when has_known_sales then shipping_total_shop_amount end
        as shipping_shop_amount,
    case when has_known_sales then shipping_refund_total_shop_amount end
        as shipping_refund_shop_amount,
    case
        when commercial_resolution_status = 'complete'
            then shipping_total_shop_amount + shipping_refund_total_shop_amount
    end as net_shipping_shop_amount,
    case
        when commercial_resolution_status = 'complete'
            then
                gross_merchandise_total_shop_amount
                + merchandise_discount_total_shop_amount
                + merchandise_refund_total_shop_amount
                + shipping_total_shop_amount
                + shipping_refund_total_shop_amount
    end as net_revenue_shop_amount,
    case when has_known_sales then duty_total_shop_amount end
        as duty_shop_amount,
    case when has_known_sales then gift_card_liability_total_shop_amount end
        as gift_card_liability_shop_amount,
    case when has_known_sales then tax_total_shop_amount end as tax_shop_amount,
    case when has_known_sales then return_shipping_fee_total_shop_amount end
        as return_shipping_fee_shop_amount,
    case when has_known_sales then unfinalized_fee_total_shop_amount end
        as unfinalized_fee_shop_amount,
    case when has_known_sales then additional_fee_total_shop_amount end
        as additional_fee_shop_amount,
    case
        when
            adjustment_currency_unresolved_count = 0
            then unresolved_adjustment_total_shop_amount
    end as unresolved_adjustment_shop_amount,
    case
        when
            adjustment_currency_unresolved_count = 0
            then unresolved_adjustment_tax_total_shop_amount
    end as unresolved_adjustment_tax_shop_amount,
    case when has_known_sales then shipping_total_shop_amount end
        as unassigned_shipping_shop_amount,
    case when has_known_sales then shipping_refund_total_shop_amount end
        as unassigned_shipping_refund_shop_amount,
    case when has_known_sales then ledger_total_shop_amount end
        as ledger_total_shop_amount,
    case
        when has_known_sales and current_total_price_shop_amount is not null
            then
                ledger_total_shop_amount
                - ledger_handling_fee_total_shop_amount
                - current_total_price_shop_amount
    end as ledger_header_difference_shop_amount,
    case
        when
            refund_object_unresolved_count = 0
            then refund_object_total_shop_amount
    end as refund_object_shop_amount,
    case when has_known_sales then refund_linked_sale_total_shop_amount end
        as refund_linked_sale_total_shop_amount,
    case
        when has_known_sales and refund_object_unresolved_count = 0
            then
                refund_object_total_shop_amount
                + refund_linked_sale_total_shop_amount
                + handling_fee_shop_amount
                - ledger_handling_fee_total_shop_amount
    end as refund_agreement_difference_shop_amount
from statused
