-- Transaction-level evidence shared by payment history and commercial
-- reconciliation. Gross collections retain reversed attempts;
-- payment history can remove both proven movements of those pairs.
with transactions as (
    select
        shop_id,
        order_id,
        transaction_id,
        parent_transaction_id,
        created_at,
        processed_at,
        gateway,
        kind,
        status,
        shop_currency_code,
        transaction_shop_amount
    from {{ ref('stg_shopify_orders__transactions') }}
),

refund_links as (
    select distinct
        shop_id,
        order_id,
        transaction_id
    from {{ ref('stg_shopify_orders__refund_transactions') }}
),

-- The proof is written as joins, not nested correlated subqueries, because
-- Snowflake cannot evaluate subqueries nested inside a subquery that refer to
-- the outer row.
failed_attempt_times as (
    select distinct
        shop_id,
        order_id,
        created_at
    from transactions
    where
        kind in ('SALE', 'CAPTURE', 'AUTHORIZATION')
        and status in ('FAILURE', 'ERROR')
),

refund_counts as (
    select
        shop_id,
        order_id,
        parent_transaction_id,
        count(*) as refund_count
    from transactions
    where kind = 'REFUND'
    group by shop_id, order_id, parent_transaction_id
),

order_agreements as (
    select
        shop_id,
        order_id,
        min(happened_at) as first_dated_at,
        sum(case when happened_at is null then 1 else 0 end) > 0 as has_undated
    from {{ ref('stg_shopify_orders__agreements') }}
    where reason = 'ORDER'
    group by shop_id, order_id
),

failed_attempt_parents as (
    select
        parent.shop_id,
        parent.order_id,
        parent.transaction_id,
        parent.gateway,
        parent.shop_currency_code,
        parent.transaction_shop_amount,
        parent.created_at
    from transactions as parent
    inner join failed_attempt_times as attempts
        on
            parent.shop_id = attempts.shop_id
            and parent.order_id = attempts.order_id
            and parent.created_at = attempts.created_at
    inner join refund_counts
        on
            parent.shop_id = refund_counts.shop_id
            and parent.order_id = refund_counts.order_id
            and parent.transaction_id = refund_counts.parent_transaction_id
            and refund_counts.refund_count = 1
    where
        parent.kind in ('SALE', 'CAPTURE')
        and parent.status = 'SUCCESS'
        and parent.gateway = 'gift_card'
),

proven_reversals as (
    select
        transactions.shop_id,
        transactions.order_id,
        transactions.transaction_id
    from transactions
    inner join failed_attempt_parents as parent
        on
            transactions.shop_id = parent.shop_id
            and transactions.order_id = parent.order_id
            and transactions.parent_transaction_id = parent.transaction_id
    left join order_agreements as agreements
        on
            parent.shop_id = agreements.shop_id
            and parent.order_id = agreements.order_id
    where
        transactions.gateway = parent.gateway
        and transactions.shop_currency_code = parent.shop_currency_code
        and transactions.transaction_shop_amount
        = parent.transaction_shop_amount
        and transactions.transaction_shop_amount > 0
        and parent.created_at <= transactions.created_at
        and not coalesce(
            agreements.has_undated
            or agreements.first_dated_at <= transactions.created_at,
            false
        )
),

classified as (
    select
        transactions.shop_id,
        transactions.order_id,
        transactions.transaction_id,
        transactions.parent_transaction_id,
        transactions.kind,
        transactions.status,
        transactions.shop_currency_code,
        transactions.transaction_shop_amount,
        coalesce(transactions.processed_at, transactions.created_at)
            as occurred_at,
        refund_links.transaction_id is not null as has_refund_object,
        proven_reversals.transaction_id is not null
            as is_proven_failed_attempt_reversal
    from transactions
    left join refund_links
        on
            transactions.shop_id = refund_links.shop_id
            and transactions.order_id = refund_links.order_id
            and transactions.transaction_id = refund_links.transaction_id
    left join proven_reversals
        on
            transactions.shop_id = proven_reversals.shop_id
            and transactions.order_id = proven_reversals.order_id
            and transactions.transaction_id = proven_reversals.transaction_id
),

reversed_collections as (
    select distinct
        shop_id,
        order_id,
        parent_transaction_id
    from classified
    where
        kind = 'REFUND' and status = 'SUCCESS'
        and is_proven_failed_attempt_reversal and not has_refund_object
),

flows as (
    -- Preserve output order while explicitly typing the final indicator.
    select -- noqa: ST06
        classified.shop_id,
        classified.order_id,
        classified.transaction_id,
        classified.parent_transaction_id,
        classified.occurred_at,
        classified.kind,
        classified.status,
        classified.shop_currency_code,
        classified.transaction_shop_amount,
        reversed_collections.parent_transaction_id is not null
            as is_failed_attempt_collection,
        case
            when
                classified.kind in ('SALE', 'CAPTURE')
                and classified.status = 'SUCCESS'
                then 'collection'
            when
                classified.kind = 'REFUND'
                and classified.status = 'SUCCESS'
                and classified.has_refund_object
                then 'refund_object'
            when
                classified.kind = 'REFUND' and classified.status = 'SUCCESS'
                and classified.is_proven_failed_attempt_reversal
                then 'failed_attempt_reversal'
            when
                classified.kind = 'REFUND' and classified.status = 'SUCCESS'
                then 'unresolved_refund'
            when
                classified.kind in (
                    'AUTHORIZATION', 'VOID', 'EMV_AUTHORIZATION'
                )
                and classified.status = 'SUCCESS' then 'noncash'
            when
                classified.kind in (
                    'SALE',
                    'CAPTURE',
                    'REFUND',
                    'AUTHORIZATION',
                    'VOID',
                    'EMV_AUTHORIZATION'
                )
                and classified.status in (
                    'FAILURE', 'ERROR', 'PENDING', 'AWAITING_RESPONSE'
                )
                then 'noncash'
            else 'unclassified'
        end as payment_flow_class,
        cast(case
            when
                classified.kind in ('SALE', 'CAPTURE', 'AUTHORIZATION')
                and classified.status in ('FAILURE', 'ERROR')
                then 1
            else 0
        end as integer) as failed_payment_attempt_count
    from classified
    left join reversed_collections
        on
            classified.shop_id = reversed_collections.shop_id
            and classified.order_id = reversed_collections.order_id
            and classified.transaction_id
            = reversed_collections.parent_transaction_id
)

select * from flows
