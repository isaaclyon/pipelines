-- One row per refund. Preserve direct identities first. Otherwise require a
-- unique exact agreement amount and source timestamp in both directions.
-- Check commercial and payment amounts per refund so differences cannot cancel.
with agreements as (
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

direct_candidates as (
    select
        refunds.shop_id,
        refunds.order_id,
        refunds.refund_id,
        agreements.agreement_id,
        agreements.refund_id as direct_refund_id,
        count(*) over (
            partition by
                agreements.shop_id, agreements.order_id,
                agreements.agreement_id
        ) as agreement_candidate_count,
        count(*) over (
            partition by
                refunds.shop_id, refunds.order_id,
                refunds.refund_id
        ) as refund_candidate_count
    from {{ ref('stg_shopify_orders__refunds') }} as refunds
    inner join agreements
        on
            refunds.shop_id = agreements.shop_id
            and refunds.order_id = agreements.order_id
            and (refunds.refund_id = agreements.refund_id or (
                agreements.refund_id is null
                and refunds.return_id = agreements.return_id
            ))
),

direct_links as (
    select
        shop_id,
        order_id,
        refund_id,
        agreement_id
    from direct_candidates
    where
        direct_refund_id is not null
        or (agreement_candidate_count = 1 and refund_candidate_count = 1)
),

agreement_totals as (
    select
        shop_id,
        order_id,
        agreement_id,
        sum(sale_total_shop_amount) as agreement_total_shop_amount,
        sum(case when resolution_status <> 'resolved' then 1 else 0 end)
            as unresolved_sale_count
    from {{ ref('int_shopify_sales__ledger') }}
    group by shop_id, order_id, agreement_id
),

-- Shopify edit refunds carry refund objects but no agreement refund identity.
-- Match only a unique exact USD reduction, after the agreement, in both
-- directions.
-- No fuzzy amount match, payment residual allocation, or arbitrary time
-- tolerance.
amount_candidates as (
    select
        refunds.shop_id,
        refunds.order_id,
        refunds.refund_id,
        agreements.agreement_id,
        count(*) over (
            partition by
                refunds.shop_id, refunds.order_id,
                refunds.refund_id
        ) as refund_candidate_count,
        count(*) over (
            partition by
                agreements.shop_id, agreements.order_id,
                agreements.agreement_id
        ) as agreement_candidate_count
    from {{ ref('stg_shopify_orders__refunds') }} as refunds
    inner join agreements
        on
            refunds.shop_id = agreements.shop_id
            and refunds.order_id = agreements.order_id
            and agreements.reason in ('ORDER_EDIT', 'RETURN')
            and agreements.refund_id is null
            and refunds.created_at >= agreements.happened_at
    inner join agreement_totals as totals
        on
            agreements.shop_id = totals.shop_id
            and agreements.order_id = totals.order_id
            and agreements.agreement_id = totals.agreement_id
            and totals.unresolved_sale_count = 0
            and totals.agreement_total_shop_amount < 0
            and refunds.shop_currency_code = 'USD'
            and refunds.total_refunded_shop_amount
            = -totals.agreement_total_shop_amount
    where not exists (
        select 1 from direct_links
        where
            direct_links.shop_id = refunds.shop_id
            and direct_links.order_id = refunds.order_id
            and (
                direct_links.refund_id = refunds.refund_id
                or direct_links.agreement_id = agreements.agreement_id
            )
    )
),

resolved_refund_agreements as (
    select
        shop_id,
        order_id,
        refund_id,
        agreement_id
    from direct_links
    union all
    select
        shop_id,
        order_id,
        refund_id,
        agreement_id
    from amount_candidates
    where refund_candidate_count = 1 and agreement_candidate_count = 1
),

mapped_ledger as (
    select
        ledger.*,
        links.refund_id as resolved_refund_id
    from {{ ref('int_shopify_sales__ledger') }} as ledger
    left join resolved_refund_agreements as links
        on
            ledger.shop_id = links.shop_id and ledger.order_id = links.order_id
            and ledger.agreement_id = links.agreement_id
),

refund_ledger_totals as (
    select
        shop_id,
        order_id,
        resolved_refund_id as refund_id,
        coalesce(sum(case
            when resolution_status = 'resolved'
                then sale_total_shop_amount
        end), 0) as commercial_sale_total_shop_amount,
        sum(sale_total_shop_amount - handling_fee_shop_amount)
            as commercial_refund_total_shop_amount
    from mapped_ledger
    where resolved_refund_id is not null
    group by shop_id, order_id, resolved_refund_id
),

refund_payment_totals as (
    select
        links.shop_id,
        links.order_id,
        links.refund_id,
        sum(payment_transactions.transaction_shop_amount)
            as refund_payment_shop_amount
    from {{ ref('stg_shopify_orders__refund_transactions') }} as links
    inner join
        {{ ref('stg_shopify_orders__transactions') }} as payment_transactions
        on
            links.shop_id = payment_transactions.shop_id
            and links.order_id = payment_transactions.order_id
            and links.transaction_id = payment_transactions.transaction_id
            and payment_transactions.kind = 'REFUND'
            and payment_transactions.status = 'SUCCESS'
            and payment_transactions.shop_currency_code = 'USD'
    group by links.shop_id, links.order_id, links.refund_id
),

refund_checks as (
    select
        refunds.shop_id,
        refunds.order_id,
        refunds.refund_id,
        refunds.shop_currency_code,
        refunds.total_refunded_shop_amount,
        refund_totals.commercial_sale_total_shop_amount,
        case
            when
                refund_totals.commercial_refund_total_shop_amount is null
                or refunds.total_refunded_shop_amount
                + refund_totals.commercial_refund_total_shop_amount
                + coalesce(fees.handling_fee_shop_amount, 0) <> 0
                then 1
            else 0
        end as has_component_difference,
        case
            when exists (
                select 1
                from resolved_refund_agreements as links
                where
                    links.shop_id = refunds.shop_id
                    and links.order_id = refunds.order_id
                    and links.refund_id = refunds.refund_id
            ) then 0
            else 1
        end as is_without_agreement,
        case
            when
                refunds.total_refunded_shop_amount
                <> coalesce(payments.refund_payment_shop_amount, 0)
                then 1
            else 0
        end as has_payment_difference
    from {{ ref('stg_shopify_orders__refunds') }} as refunds
    left join refund_payment_totals as payments
        on
            refunds.shop_id = payments.shop_id
            and refunds.order_id = payments.order_id
            and refunds.refund_id = payments.refund_id
    left join refund_ledger_totals as refund_totals
        on
            refunds.shop_id = refund_totals.shop_id
            and refunds.order_id = refund_totals.order_id
            and refunds.refund_id = refund_totals.refund_id
    left join {{ ref('int_shopify_refunds__handling_fees') }} as fees
        on
            refunds.shop_id = fees.shop_id and refunds.order_id = fees.order_id
            and refunds.refund_id = fees.refund_id
)

select
    shop_id,
    order_id,
    refund_id,
    shop_currency_code,
    total_refunded_shop_amount,
    commercial_sale_total_shop_amount,
    cast(is_without_agreement as bigint) as without_agreement_count,
    cast(has_component_difference as bigint) as component_difference_count,
    cast(has_payment_difference as bigint) as payment_difference_count,
    'USD' as reporting_currency_code
from refund_checks
