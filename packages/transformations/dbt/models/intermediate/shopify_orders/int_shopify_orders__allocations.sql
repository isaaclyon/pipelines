-- One row per source order. Each unallocated amount is the order total minus
-- the sum of final line amounts. It is zero when every line share was
-- assigned. It is NULL when the order total or any line amount is unknown, so
-- a partial sum never looks complete. A nonzero amount means no eligible line
-- received it.
--
-- allocation_status reports the first condition that applies, in this order:
-- unknown_source_amount, unknown_gift_card_flag, unresolved_allocation,
-- no_eligible_lines and complete. Orders with no lines still appear, because
-- their totals must not disappear from the order grain.
with orders as (
    select
        shop_id,
        order_id,
        merchandise_discount_shop_amount as discount_total_shop_amount,
        merchandise_refund_shop_amount as refund_total_shop_amount,
        unassigned_shipping_shop_amount as shipping_total_shop_amount,
        unassigned_shipping_refund_shop_amount
            as shipping_refund_total_shop_amount
    from {{ ref('int_shopify_orders__commercial_components') }}
),

line_rollup as (
    select
        shop_id,
        order_id,
        cast(
            sum(case when is_gift_card_line is null then 1 else 0 end) as bigint
        )
            as unknown_gift_card_line_count,
        cast(
            sum(
                case
                    when merchandise_discount_shop_amount is null then 1 else 0
                end
            ) as bigint
        )
            as unknown_discount_line_count,
        cast(
            sum(
                case
                    when merchandise_refund_shop_amount is null then 1 else 0
                end
            ) as bigint
        )
            as unknown_refund_line_count,
        cast(
            sum(
                case when shipping_shop_amount is null then 1 else 0 end
            ) as bigint
        )
            as unknown_shipping_line_count,
        cast(
            sum(
                case when shipping_refund_shop_amount is null then 1 else 0 end
            ) as bigint
        )
            as unknown_shipping_refund_line_count,
        sum(merchandise_discount_shop_amount) as discount_allocated_shop_amount,
        sum(merchandise_refund_shop_amount) as refund_allocated_shop_amount,
        sum(shipping_shop_amount) as shipping_allocated_shop_amount,
        sum(shipping_refund_shop_amount)
            as shipping_refund_allocated_shop_amount
    from {{ ref('int_shopify_order_lines__allocations') }}
    group by shop_id, order_id
),

unallocated as (
    select
        orders.shop_id,
        orders.order_id,
        orders.discount_total_shop_amount,
        orders.refund_total_shop_amount,
        orders.shipping_total_shop_amount,
        orders.shipping_refund_total_shop_amount,
        coalesce(line_totals.unknown_gift_card_line_count, 0)
            as unknown_gift_card_line_count,
        case
            when
                orders.discount_total_shop_amount is null
                or coalesce(line_totals.unknown_discount_line_count, 0) > 0
                then null
            else
                orders.discount_total_shop_amount
                - coalesce(line_totals.discount_allocated_shop_amount, 0)
        end as merchandise_discount_unallocated_shop_amount,
        case
            when
                orders.refund_total_shop_amount is null
                or coalesce(line_totals.unknown_refund_line_count, 0) > 0
                then null
            else
                orders.refund_total_shop_amount
                - coalesce(line_totals.refund_allocated_shop_amount, 0)
        end as merchandise_refund_unallocated_shop_amount,
        case
            when
                orders.shipping_total_shop_amount is null
                or coalesce(line_totals.unknown_shipping_line_count, 0) > 0
                then null
            else
                orders.shipping_total_shop_amount
                - coalesce(line_totals.shipping_allocated_shop_amount, 0)
        end as shipping_unallocated_shop_amount,
        case
            when
                orders.shipping_refund_total_shop_amount is null
                or coalesce(line_totals.unknown_shipping_refund_line_count, 0)
                > 0
                then null
            else
                orders.shipping_refund_total_shop_amount
                - coalesce(line_totals.shipping_refund_allocated_shop_amount, 0)
        end as shipping_refund_unallocated_shop_amount
    from orders
    left join line_rollup as line_totals
        on
            orders.shop_id = line_totals.shop_id
            and orders.order_id = line_totals.order_id
),

classified as (
    select
        *,
        case
            when
                discount_total_shop_amount is null
                or refund_total_shop_amount is null
                or shipping_total_shop_amount is null
                or shipping_refund_total_shop_amount is null
                then 'unknown_source_amount'
            when
                merchandise_discount_unallocated_shop_amount is null
                or merchandise_refund_unallocated_shop_amount is null
                or shipping_unallocated_shop_amount is null
                or shipping_refund_unallocated_shop_amount is null
                then case
                    when
                        unknown_gift_card_line_count > 0
                        then 'unknown_gift_card_flag'
                    else 'unresolved_allocation'
                end
            when
                merchandise_discount_unallocated_shop_amount <> 0
                or merchandise_refund_unallocated_shop_amount <> 0
                or shipping_unallocated_shop_amount <> 0
                or shipping_refund_unallocated_shop_amount <> 0
                then 'no_eligible_lines'
            else 'complete'
        end as allocation_status
    from unallocated
)

select
    shop_id,
    order_id,
    cast(merchandise_discount_unallocated_shop_amount as decimal(38, 9))
        as merchandise_discount_unallocated_shop_amount,
    cast(merchandise_refund_unallocated_shop_amount as decimal(38, 9))
        as merchandise_refund_unallocated_shop_amount,
    cast(shipping_unallocated_shop_amount as decimal(38, 9))
        as shipping_unallocated_shop_amount,
    cast(shipping_refund_unallocated_shop_amount as decimal(38, 9))
        as shipping_refund_unallocated_shop_amount,
    unknown_gift_card_line_count,
    allocation_status,
    cast('USD' as varchar) as reporting_currency_code,
    allocation_status = 'complete' as is_allocation_complete
from classified
