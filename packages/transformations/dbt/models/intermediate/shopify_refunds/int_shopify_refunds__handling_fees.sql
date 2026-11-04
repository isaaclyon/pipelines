-- Resolve the Loop return handling fee inside each Shopify refund.
--
-- A fee resolves only when one Loop return and one Shopify refund match in
-- both directions and every strict check passes. Null amounts are unknown,
-- never zero.
-- Absent child rows count as zero only where the rules below say so.
-- Shipping and duty refunds are other components and must be known zero.
with refunds as (
    select
        shop_id,
        order_id,
        refund_id,
        total_refunded_shop_amount,
        shop_currency_code,
        presentment_currency_code
    from {{ ref('stg_shopify_orders__refunds') }}
),

refund_line_totals as (
    select
        shop_id,
        order_id,
        refund_id,
        count(*) as line_count,
        sum(
            case
                when
                    subtotal_shop_amount is null
                    or total_tax_shop_amount is null
                    then 1
                else 0
            end
        ) as unknown_amount_count,
        sum(
            case
                when
                    subtotal_shop_currency_code = 'USD'
                    and subtotal_presentment_currency_code = 'USD'
                    and total_tax_shop_currency_code = 'USD'
                    and total_tax_presentment_currency_code = 'USD'
                    then 0
                else 1
            end
        ) as non_usd_count,
        sum(subtotal_shop_amount + total_tax_shop_amount) as line_total_amount
    from {{ ref('stg_shopify_orders__refund_lines') }}
    group by shop_id, order_id, refund_id
),

refund_adjustment_totals as (
    select
        shop_id,
        order_id,
        refund_id,
        count(*) as adjustment_count,
        sum(
            case
                when
                    adjustment_shop_amount is null
                    or adjustment_shop_amount <= 0
                    then 1
                else 0
            end
        ) as unknown_or_nonpositive_count,
        sum(
            case
                when
                    adjustment_tax_shop_amount is null
                    or adjustment_tax_shop_amount <> 0
                    then 1
                else 0
            end
        ) as tax_nonzero_or_unknown_count,
        sum(
            case
                when
                    adjustment_shop_currency_code = 'USD'
                    and adjustment_presentment_currency_code = 'USD'
                    and adjustment_tax_shop_currency_code = 'USD'
                    and adjustment_tax_presentment_currency_code = 'USD'
                    then 0
                else 1
            end
        ) as non_usd_count,
        sum(adjustment_shop_amount) as adjustment_total_amount
    from {{ ref('stg_shopify_orders__refund_adjustments') }}
    group by shop_id, order_id, refund_id
),

-- Shipping and duty refunds: a row with an unknown or nonzero amount is
-- flagged.
refund_other_flags as (
    select
        shop_id,
        order_id,
        refund_id,
        sum(flagged) as other_component_flag_count
    from (
        select
            shop_id,
            order_id,
            refund_id,
            case
                when
                    subtotal_shop_amount is null
                    or subtotal_shop_amount <> 0
                    or tax_shop_amount is null
                    or tax_shop_amount <> 0
                    then 1
                else 0
            end as flagged
        from {{ ref('stg_shopify_orders__refund_shipping_lines') }}
        union all
        select
            shop_id,
            order_id,
            refund_id,
            case
                when refund_shop_amount is null or refund_shop_amount <> 0
                    then 1
                else 0
            end as flagged
        from {{ ref('stg_shopify_orders__refund_duties') }}
    ) as component_rows
    group by shop_id, order_id, refund_id
),

refund_facts as (
    select
        refunds.shop_id,
        refunds.order_id,
        refunds.refund_id,
        refunds.total_refunded_shop_amount,
        line_totals.line_total_amount,
        adjustments.adjustment_total_amount,
        case
            when
                refunds.shop_currency_code = 'USD'
                and refunds.presentment_currency_code = 'USD'
                then 0
            else 1
        end as refund_non_usd_count,
        coalesce(line_totals.line_count, 0) as line_count,
        coalesce(line_totals.unknown_amount_count, 0)
            as line_unknown_count,
        coalesce(line_totals.non_usd_count, 0) as line_non_usd_count,
        coalesce(adjustments.adjustment_count, 0) as adjustment_count,
        coalesce(adjustments.unknown_or_nonpositive_count, 0)
            as adjustment_unknown_count,
        coalesce(adjustments.tax_nonzero_or_unknown_count, 0)
            as adjustment_tax_flag_count,
        coalesce(adjustments.non_usd_count, 0) as adjustment_non_usd_count,
        coalesce(other_flags.other_component_flag_count, 0)
            as other_component_flag_count
    from refunds
    left join refund_line_totals as line_totals
        on
            refunds.shop_id = line_totals.shop_id
            and refunds.order_id = line_totals.order_id
            and refunds.refund_id = line_totals.refund_id
    left join refund_adjustment_totals as adjustments
        on
            refunds.shop_id = adjustments.shop_id
            and refunds.order_id = adjustments.order_id
            and refunds.refund_id = adjustments.refund_id
    left join refund_other_flags as other_flags
        on
            refunds.shop_id = other_flags.shop_id
            and refunds.order_id = other_flags.order_id
            and refunds.refund_id = other_flags.refund_id
),

-- Loop returns whose refund amount equals a Shopify refund total on the same
-- order. Uniqueness counts use these amount-compatible pairs. An open or
-- otherwise unqualified duplicate therefore still blocks a match.
amount_candidates as (
    select
        facts.shop_id,
        facts.order_id,
        facts.refund_id,
        facts.total_refunded_shop_amount,
        facts.refund_non_usd_count,
        facts.line_count,
        facts.line_unknown_count,
        facts.line_non_usd_count,
        facts.line_total_amount,
        facts.adjustment_count,
        facts.adjustment_unknown_count,
        facts.adjustment_tax_flag_count,
        facts.adjustment_non_usd_count,
        facts.adjustment_total_amount,
        facts.other_component_flag_count,
        headers.return_id,
        headers.return_state,
        headers.is_snapshot_complete,
        headers.currency_code,
        headers.return_total_amount,
        headers.handling_fee_amount,
        headers.gift_card_amount,
        headers.exchange_total_amount,
        headers.upsell_amount
    from refund_facts as facts
    inner join {{ ref('stg_loop_returns__headers') }} as headers
        on
            facts.shop_id = headers.shop_id
            and facts.order_id = headers.order_id
            and facts.total_refunded_shop_amount = headers.refund_amount
),

candidate_checks as (
    select
        shop_id,
        order_id,
        refund_id,
        return_id,
        handling_fee_amount,
        case
            when
                coalesce(currency_code, '') <> 'USD'
                or refund_non_usd_count > 0
                or line_non_usd_count > 0
                or adjustment_non_usd_count > 0
                then 'unresolved_currency_not_usd'
            when coalesce(lower(trim(return_state)), '') <> 'closed'
                then 'unresolved_loop_return_open'
            when coalesce(is_snapshot_complete, false) = false
                then 'unresolved_loop_snapshot_incomplete'
            when
                gift_card_amount is null or gift_card_amount <> 0
                or exchange_total_amount is null or exchange_total_amount <> 0
                or upsell_amount is null or upsell_amount <> 0
                then 'unresolved_other_loop_amount'
            when handling_fee_amount is null or handling_fee_amount <= 0
                then 'unresolved_handling_fee_not_positive'
            when
                return_total_amount is null
                or return_total_amount
                <> total_refunded_shop_amount + handling_fee_amount
                then 'unresolved_return_total_mismatch'
            when
                line_count = 0
                or line_unknown_count > 0
                or line_total_amount is null
                or line_total_amount <> return_total_amount
                then 'unresolved_refund_lines_mismatch'
            when other_component_flag_count > 0
                then 'unresolved_other_refund_components'
            when adjustment_unknown_count > 0
                then 'unresolved_adjustment_unknown'
            when adjustment_tax_flag_count > 0
                then 'unresolved_adjustment_tax'
            when
                adjustment_total_amount is null
                or adjustment_total_amount <> handling_fee_amount
                then 'unresolved_adjustment_sum_mismatch'
            else 'qualified'
        end as check_outcome,
        count(*) over (
            partition by shop_id, order_id, refund_id
        ) as refund_candidate_count,
        count(*) over (
            partition by shop_id, order_id, return_id
        ) as return_candidate_count
    from amount_candidates
),

candidate_outcomes as (
    select
        shop_id,
        order_id,
        refund_id,
        return_id,
        handling_fee_amount,
        case
            when refund_candidate_count > 1
                then 'unresolved_ambiguous_loop_return'
            when check_outcome <> 'qualified'
                then check_outcome
            when return_candidate_count > 1
                then 'unresolved_ambiguous_refund'
            else 'matched_loop_handling_fee'
        end as candidate_outcome,
        case
            when handling_fee_amount is null or handling_fee_amount <> 0
                then 1
            else 0
        end as nonzero_fee_candidate_flag
    from candidate_checks
),

refund_outcomes as (
    select
        facts.shop_id,
        facts.order_id,
        facts.refund_id,
        facts.adjustment_count,
        coalesce(sum(outcomes.nonzero_fee_candidate_flag), 0)
            as nonzero_fee_candidate_count,
        count(outcomes.candidate_outcome) as candidate_count,
        max(outcomes.candidate_outcome) as candidate_outcome,
        max(
            case
                when outcomes.candidate_outcome = 'matched_loop_handling_fee'
                    then outcomes.return_id
            end
        ) as matched_return_id,
        max(
            case
                when outcomes.candidate_outcome = 'matched_loop_handling_fee'
                    then outcomes.handling_fee_amount
            end
        ) as matched_handling_fee_amount
    from refund_facts as facts
    left join candidate_outcomes as outcomes
        on
            facts.shop_id = outcomes.shop_id
            and facts.order_id = outcomes.order_id
            and facts.refund_id = outcomes.refund_id
    group by
        facts.shop_id,
        facts.order_id,
        facts.refund_id,
        facts.adjustment_count
),

resolved as (
    select
        shop_id,
        order_id,
        refund_id,
        case
            when adjustment_count = 0 and nonzero_fee_candidate_count = 0
                then 'no_adjustment_records'
            when adjustment_count = 0
                then 'unresolved_no_adjustment_loop_fee'
            when candidate_count = 0
                then 'unresolved_no_loop_return'
            else candidate_outcome
        end as handling_fee_status,
        case
            when candidate_outcome = 'matched_loop_handling_fee'
                then matched_return_id
        end as loop_return_id,
        case
            when adjustment_count = 0 and nonzero_fee_candidate_count = 0
                then cast(0 as decimal(38, 9))
            when candidate_outcome = 'matched_loop_handling_fee'
                then matched_handling_fee_amount
        end as handling_fee_shop_amount
    from refund_outcomes
)

select
    shop_id,
    order_id,
    refund_id,
    loop_return_id,
    cast(handling_fee_shop_amount as decimal(38, 9))
        as handling_fee_shop_amount,
    cast('USD' as varchar) as reporting_currency_code,
    cast(handling_fee_status as varchar) as handling_fee_status
from resolved
