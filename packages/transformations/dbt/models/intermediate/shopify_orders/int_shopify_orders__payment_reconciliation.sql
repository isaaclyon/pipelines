-- One row per order with payment transactions. Reconcile gross movements;
-- eligibility consumes the shared transaction evidence separately.
with flows as (
    select * from {{ ref('int_shopify_transactions__payment_flows') }}
),

rolled_up as (
    select
        shop_id,
        order_id,
        cast(count(*) as bigint) as payment_transaction_count,
        cast(sum(failed_payment_attempt_count) as bigint)
            as failed_payment_attempt_count,
        cast(sum(case
            when payment_flow_class = 'unclassified' then 1
            when payment_flow_class <> 'noncash' and (
                transaction_shop_amount is null or transaction_shop_amount < 0
            ) then 1 else 0
        end) as bigint) as unresolved_payment_activity_count,
        cast(
            sum(
                case
                    when
                        payment_flow_class <> 'noncash'
                        and coalesce(shop_currency_code, '') <> 'USD'
                        then 1
                    else 0
                end
            ) as bigint
        ) as payment_currency_unresolved_count,
        cast(
            sum(
                case
                    when payment_flow_class = 'unresolved_refund' then 1 else 0
                end
            ) as bigint
        ) as unresolved_refund_payment_count,
        sum(
            case
                when
                    payment_flow_class = 'collection'
                    and shop_currency_code = 'USD'
                    then transaction_shop_amount
            end
        ) as collected_total_shop_amount,
        sum(
            case
                when
                    payment_flow_class = 'refund_object'
                    and shop_currency_code = 'USD'
                    then transaction_shop_amount
            end
        ) as refund_object_payment_total_shop_amount,
        sum(
            case
                when
                    payment_flow_class = 'failed_attempt_reversal'
                    and shop_currency_code = 'USD'
                    then transaction_shop_amount
            end
        ) as failed_attempt_reversal_total_shop_amount,
        sum(
            case
                when
                    payment_flow_class = 'unresolved_refund'
                    and shop_currency_code = 'USD'
                    then transaction_shop_amount
            end
        ) as unresolved_refund_payment_total_shop_amount
    from flows
    group by shop_id, order_id
)

select
    shop_id,
    order_id,
    payment_transaction_count,
    failed_payment_attempt_count,
    unresolved_payment_activity_count,
    payment_currency_unresolved_count,
    unresolved_refund_payment_count,
    'USD' as reporting_currency_code,
    case
        when
            payment_currency_unresolved_count = 0
            and unresolved_payment_activity_count = 0
            then collected_total_shop_amount
    end as collected_shop_amount,
    case
        when
            payment_currency_unresolved_count = 0
            and unresolved_payment_activity_count = 0
            then
                coalesce(
                    refund_object_payment_total_shop_amount,
                    cast(0 as decimal(38, 9))
                )
    end as refund_object_payment_shop_amount,
    case
        when
            payment_currency_unresolved_count = 0
            and unresolved_payment_activity_count = 0
            then
                coalesce(
                    failed_attempt_reversal_total_shop_amount,
                    cast(0 as decimal(38, 9))
                )
    end as failed_attempt_reversal_shop_amount,
    case
        when
            payment_currency_unresolved_count = 0
            and unresolved_payment_activity_count = 0
            then
                coalesce(
                    unresolved_refund_payment_total_shop_amount,
                    cast(0 as decimal(38, 9))
                )
    end as unresolved_refund_payment_shop_amount
from rolled_up
