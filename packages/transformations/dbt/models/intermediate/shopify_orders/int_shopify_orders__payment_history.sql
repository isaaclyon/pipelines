-- Reconstruct tax-inclusive obligations and net cash at logical event times.
-- A linked refund's cash and credit are evaluated together, not as an interim
-- credit that could incorrectly make an underpaid order appear fully paid.
with orders as (
    select * from {{ ref('stg_shopify_orders__orders') }}
),

agreements as (
    select
        agreements.shop_id,
        agreements.order_id,
        agreements.agreement_id,
        agreements.reason,
        agreements.refund_id,
        agreements.happened_at,
        count(sales.sale_id) as sale_count,
        sum(case
            when
                sales.total_amount_shop_amount is null
                or coalesce(sales.total_amount_shop_currency_code, '') <> 'USD'
                then 1
            else 0
        end) as invalid_sale_count,
        sum(sales.total_amount_shop_amount) as obligation_change
    from {{ ref('stg_shopify_orders__agreements') }} as agreements
    left join {{ ref('stg_shopify_orders__agreement_sales') }} as sales
        on
            agreements.shop_id = sales.shop_id
            and agreements.order_id = sales.order_id
            and agreements.agreement_id = sales.agreement_id
    group by
        agreements.shop_id, agreements.order_id,
        agreements.agreement_id, agreements.reason,
        agreements.refund_id, agreements.happened_at
),

obligations as (
    select
        shop_id,
        order_id,
        sum(case when reason = 'ORDER' then 1 else 0 end) as initial_count,
        sum(case when reason = 'ORDER' then obligation_change else 0 end)
            as initial_obligation,
        sum(obligation_change) as current_obligation,
        sum(case
            when
                happened_at is null or sale_count = 0
                or invalid_sale_count > 0
                or coalesce(reason, '') not in ('ORDER', 'ORDER_EDIT', 'REFUND')
                or (reason = 'REFUND' and refund_id is null)
                then 1
            else 0
        end) as invalid_agreement_count,
        max(case when reason in ('ORDER', 'ORDER_EDIT') then happened_at end)
            as latest_edit_at,
        max(happened_at) as latest_agreement_at
    from agreements
    group by shop_id, order_id
),

-- Exclude validated ledger handling fees from the Shopify header.
-- Keep those fees in event obligations; remove them only for header comparison.
ledger_fees as (
    select
        shop_id,
        order_id,
        sum(handling_fee_shop_amount) as handling_fee_shop_amount
    from {{ ref('int_shopify_sales__ledger') }}
    group by shop_id, order_id
),

refund_agreements as (
    select
        shop_id,
        order_id,
        refund_id,
        count(*) as agreement_count,
        max(happened_at) as happened_at
    from agreements
    where reason = 'REFUND'
    group by shop_id, order_id, refund_id
),

refund_links as (
    select
        links.shop_id,
        links.order_id,
        links.transaction_id,
        count(*) as link_count,
        max(refund_agreements.agreement_count) as agreement_count,
        max(refund_agreements.happened_at) as happened_at
    from {{ ref('stg_shopify_orders__refund_transactions') }} as links
    left join refund_agreements
        on
            links.shop_id = refund_agreements.shop_id
            and links.order_id = refund_agreements.order_id
            and links.refund_id = refund_agreements.refund_id
    group by links.shop_id, links.order_id, links.transaction_id
),

payments as (
    select
        flows.*,
        case
            when flows.payment_flow_class = 'refund_object'
                then refund_links.happened_at
            else flows.occurred_at
        end as event_at,
        case when
            flows.payment_flow_class = 'collection'
            and not flows.is_failed_attempt_collection
            then flows.transaction_shop_amount
        when flows.payment_flow_class = 'refund_object'
            then -flows.transaction_shop_amount
        else cast(0 as decimal(38, 9)) end as cash_change,
        case
            when
                flows.payment_flow_class in (
                    'unclassified', 'unresolved_refund'
                )
                -- Unsettled refunds are not cash movements, but their credits
                -- cannot prove full payment until settlement is resolved.
                or (
                    flows.kind = 'REFUND'
                    and flows.status in ('PENDING', 'AWAITING_RESPONSE')
                )
                or (flows.payment_flow_class <> 'noncash' and (
                    flows.occurred_at is null
                    or flows.transaction_shop_amount is null
                    or flows.transaction_shop_amount < 0
                    or coalesce(flows.shop_currency_code, '') <> 'USD'
                ))
                or (flows.payment_flow_class = 'refund_object' and (
                    coalesce(refund_links.link_count, 0) <> 1
                    or coalesce(refund_links.agreement_count, 0) <> 1
                    or refund_links.happened_at is null
                ))
                then 1
            else 0
        end as invalid_payment_count
    from {{ ref('int_shopify_transactions__payment_flows') }} as flows
    left join refund_links
        on
            flows.shop_id = refund_links.shop_id
            and flows.order_id = refund_links.order_id
            and flows.transaction_id = refund_links.transaction_id
),

payment_totals as (
    select
        shop_id,
        order_id,
        count(*) as transaction_count,
        sum(invalid_payment_count) as invalid_payment_count,
        sum(cash_change) as net_cash,
        sum(case when
            payment_flow_class = 'collection'
            and not is_failed_attempt_collection then transaction_shop_amount
        else cast(0 as decimal(38, 9)) end) as collected,
        sum(case
            when payment_flow_class = 'refund_object'
                then transaction_shop_amount
            else cast(0 as decimal(38, 9))
        end)
            as refunded,
        max(occurred_at) as latest_payment_at
    from payments
    group by shop_id, order_id
),

events as (
    select
        shop_id,
        order_id,
        happened_at as event_at,
        obligation_change,
        cast(0 as decimal(38, 9)) as cash_change,
        case when reason = 'ORDER' then 1 else 0 end as initial_count
    from agreements
    union all
    select
        shop_id,
        order_id,
        event_at,
        cast(0 as decimal(38, 9)) as obligation_change,
        cash_change,
        0 as initial_count
    from payments
    where cash_change <> 0
),

event_times as (
    select
        shop_id,
        order_id,
        event_at,
        sum(obligation_change) as obligation_change,
        sum(cash_change) as cash_change,
        sum(initial_count) as initial_count
    from events
    group by shop_id, order_id, event_at
),

balances as (
    select
        shop_id,
        order_id,
        event_at,
        sum(obligation_change) over (
            partition by shop_id, order_id order by event_at
            rows between unbounded preceding and current row
        ) as obligation,
        sum(cash_change) over (
            partition by shop_id, order_id order by event_at
            rows between unbounded preceding and current row
        ) as net_cash,
        sum(initial_count) over (
            partition by shop_id, order_id order by event_at
            rows between unbounded preceding and current row
        ) as initial_count
    from event_times
),

history as (
    select
        balances.shop_id,
        balances.order_id,
        max(case
            when
                balances.event_at >= obligations.latest_edit_at
                and balances.initial_count = 1 and balances.obligation >= 0
                and balances.net_cash >= balances.obligation then 1
            else 0
        end)
            as fully_paid,
        min(balances.net_cash) as minimum_cash,
        min(balances.obligation) as minimum_obligation
    from balances
    inner join obligations
        on
            balances.shop_id = obligations.shop_id
            and balances.order_id = obligations.order_id
    group by balances.shop_id, balances.order_id
),

proof as (
    select
        orders.shop_id,
        orders.order_id,
        orders.source_observed_at,
        coalesce(
            orders.source_observed_at is not null
            and nullif(orders.source_query_version, '') is not null
            and orders.extraction_revision >= 3
            and orders.transactions_count_precision = 'EXACT'
            and orders.transactions_count
            = coalesce(payment_totals.transaction_count, 0),
            false
        ) as is_payment_observation_complete,
        coalesce(
            obligations.initial_count = 1
            and obligations.invalid_agreement_count = 0
            and obligations.initial_obligation
            = orders.original_total_price_shop_amount
            and obligations.current_obligation
            - coalesce(ledger_fees.handling_fee_shop_amount, 0)
            = orders.current_total_price_shop_amount
            and history.minimum_obligation >= 0
            and obligations.latest_agreement_at <= orders.source_observed_at,
            false
        ) as is_payment_obligation_verified,
        coalesce(payment_totals.invalid_payment_count, 0) = 0
        and coalesce(history.minimum_cash, 0) >= 0
        and (
            payment_totals.latest_payment_at is null
            or payment_totals.latest_payment_at <= orders.source_observed_at)
            as is_payment_chronology_resolved,
        coalesce(history.fully_paid, 0) = 1 as fully_paid,
        coalesce(payment_totals.net_cash, cast(0 as decimal(38, 9)))
            as net_cash,
        coalesce(payment_totals.collected, cast(0 as decimal(38, 9)))
            as collected,
        coalesce(payment_totals.refunded, cast(0 as decimal(38, 9))) as refunded
    from orders
    left join ledger_fees
        on
            orders.shop_id = ledger_fees.shop_id
            and orders.order_id = ledger_fees.order_id
    left join obligations
        on
            orders.shop_id = obligations.shop_id
            and orders.order_id = obligations.order_id
    left join payment_totals
        on
            orders.shop_id = payment_totals.shop_id
            and orders.order_id = payment_totals.order_id
    left join history
        on
            orders.shop_id = history.shop_id
            and orders.order_id = history.order_id
)

select
    shop_id,
    order_id,
    source_observed_at,
    is_payment_observation_complete,
    is_payment_obligation_verified,
    is_payment_chronology_resolved,
    'USD' as reporting_currency_code,
    case
        when
            is_payment_observation_complete and is_payment_obligation_verified
            and is_payment_chronology_resolved then fully_paid
    end
        as has_fully_paid_current_obligation,
    case
        when is_payment_observation_complete and is_payment_chronology_resolved
            then net_cash
    end as net_customer_payment_shop_amount,
    case
        when is_payment_observation_complete and is_payment_chronology_resolved
            then refunded > 0 and net_cash > 0
    end as is_partially_refunded,
    case
        when is_payment_observation_complete and is_payment_chronology_resolved
            then collected > 0 and refunded > 0 and net_cash = 0
    end as is_fully_refunded
from proof
