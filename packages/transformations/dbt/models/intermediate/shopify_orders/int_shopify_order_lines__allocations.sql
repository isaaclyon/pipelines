-- One row per source order line. Final merchandise discount and refund, and
-- final shipping and shipping refund, with the method that resolved each
-- component.
--
-- Direct line amounts from int_shopify_order_lines__commercial_components
-- keep their values. The remainder of a component is the order total minus
-- the sum of direct line amounts. Shipping and shipping refund have no direct
-- line amount, so their remainder is the whole order total.
--
-- shopify_allocate_usd spreads each known nonzero remainder over the eligible
-- lines. Eligible lines are known merchandise lines, not gift cards. A
-- gift-card line takes no share. An unknown gift flag leaves the component
-- unresolved for every line in the order, because the unknown line might be
-- eligible.
--
-- Discount is allocated first. Its weights are gross merchandise. Refund and
-- shipping weights are gross merchandise plus the final discount. Quantity
-- fallback uses the source gross ordered quantity. A remainder of zero needs
-- no weights.
--
-- Final amounts are NULL when the remainder, a direct amount or the
-- allocation is unknown.
--
-- Keep each CTE referenced once where possible. Snowflake repeats the whole
-- plan of a CTE for every reference, and plan compilation, not the data,
-- dominated this model's run time.
with lines as (
    select
        commercial.shop_id,
        commercial.order_id,
        commercial.line_item_id,
        commercial.is_gift_card_line,
        commercial.gross_merchandise_shop_amount,
        commercial.merchandise_discount_shop_amount
            as discount_direct_shop_amount,
        commercial.merchandise_refund_shop_amount as refund_direct_shop_amount,
        source_lines.order_line_quantity
    from
        {{ ref('int_shopify_order_lines__commercial_components') }}
            as commercial
    left join {{ ref('stg_shopify_orders__order_lines') }} as source_lines
        on
            commercial.shop_id = source_lines.shop_id
            and commercial.order_id = source_lines.order_id
            and commercial.line_item_id = source_lines.line_item_id
),

order_components as (
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

-- A direct sum is known only when every line amount in the order is known.
direct_sums as (
    select
        shop_id,
        order_id,
        cast(
            sum(case when is_gift_card_line is null then 1 else 0 end) as bigint
        )
            as unknown_gift_card_line_count,
        cast(
            sum(
                case when discount_direct_shop_amount is null then 1 else 0 end
            ) as bigint
        )
            as unknown_discount_line_count,
        cast(
            sum(
                case when refund_direct_shop_amount is null then 1 else 0 end
            ) as bigint
        )
            as unknown_refund_line_count,
        sum(discount_direct_shop_amount) as discount_direct_total_shop_amount,
        sum(refund_direct_shop_amount) as refund_direct_total_shop_amount
    from lines
    group by shop_id, order_id
),

line_context as (
    select
        lines.shop_id,
        lines.order_id,
        lines.line_item_id,
        lines.is_gift_card_line,
        lines.gross_merchandise_shop_amount,
        lines.order_line_quantity,
        lines.discount_direct_shop_amount,
        lines.refund_direct_shop_amount,
        orders.shipping_total_shop_amount as shipping_remainder_shop_amount,
        orders.shipping_refund_total_shop_amount
            as shipping_refund_remainder_shop_amount,
        coalesce(sums.unknown_gift_card_line_count, 0)
            as order_unknown_gift_card_line_count,
        case
            when coalesce(sums.unknown_discount_line_count, 0) = 0
                then
                    orders.discount_total_shop_amount
                    - coalesce(sums.discount_direct_total_shop_amount, 0)
        end as discount_remainder_shop_amount,
        case
            when coalesce(sums.unknown_refund_line_count, 0) = 0
                then
                    orders.refund_total_shop_amount
                    - coalesce(sums.refund_direct_total_shop_amount, 0)
        end as refund_remainder_shop_amount
    from lines
    left join order_components as orders
        on
            lines.shop_id = orders.shop_id
            and lines.order_id = orders.order_id
    left join direct_sums as sums
        on
            lines.shop_id = sums.shop_id
            and lines.order_id = sums.order_id
),

-- Stage 1: merchandise discount. Its weights come from gross merchandise
-- only.
{% set discount_input %}
        select
            shop_id,
            order_id,
            line_item_id,
            'merchandise_discount' as component,
            discount_remainder_shop_amount as component_shop_amount,
            case when is_gift_card_line = false then gross_merchandise_shop_amount end as value_weight,
            case when is_gift_card_line = false then order_line_quantity end as quantity_weight
        from line_context
        where discount_remainder_shop_amount <> 0
            and coalesce(is_gift_card_line, false) = false
{% endset %}

discount_allocations as (
    {{ shopify_allocate_usd(discount_input) }}
),

line_discount as (
    select
        context.*,
        case
            when
                context.discount_remainder_shop_amount = 0
                or context.is_gift_card_line = true
                then cast(0 as decimal(38, 9))
            else allocation.allocated_shop_amount
        end as discount_allocated_shop_amount,
        case
            when context.discount_remainder_shop_amount = 0 then 'source_only'
            when context.is_gift_card_line = true then 'not_eligible'
            when
                context.discount_remainder_shop_amount is null
                then 'unresolved_remainder'
            when
                context.order_unknown_gift_card_line_count > 0
                then 'unresolved_unknown_gift_card_flag'
            else allocation.allocation_method
        end as discount_allocation_method
    from line_context as context
    left join discount_allocations as allocation
        on
            context.shop_id = allocation.shop_id
            and context.order_id = allocation.order_id
            and context.line_item_id = allocation.line_item_id
),

-- Stage 2: refund, shipping and shipping refund. Weights use the final
-- discount.
{% set other_input %}
        select
            ld.shop_id,
            ld.order_id,
            ld.line_item_id,
            components.component,
            case components.component
                when 'merchandise_refund' then ld.refund_remainder_shop_amount
                when 'shipping' then ld.shipping_remainder_shop_amount
                else ld.shipping_refund_remainder_shop_amount
            end as component_shop_amount,
            case when ld.is_gift_card_line = false
                then ld.gross_merchandise_shop_amount + ld.discount_direct_shop_amount + ld.discount_allocated_shop_amount
            end as value_weight,
            case when ld.is_gift_card_line = false then ld.order_line_quantity end as quantity_weight
        from line_discount as ld
        cross join (
            values ('merchandise_refund'), ('shipping'), ('shipping_refund')
        ) as components (component)
        where coalesce(ld.is_gift_card_line, false) = false
            and case components.component
                when 'merchandise_refund' then ld.refund_remainder_shop_amount
                when 'shipping' then ld.shipping_remainder_shop_amount
                else ld.shipping_refund_remainder_shop_amount
            end <> 0
{% endset %}

other_allocations as (
    {{ shopify_allocate_usd(other_input) }}
),

other_by_line as (
    select
        shop_id,
        order_id,
        line_item_id,
        max(
            case
                when component = 'merchandise_refund' then allocated_shop_amount
            end
        ) as refund_allocated,
        max(
            case
                when component = 'merchandise_refund' then allocation_method
            end
        ) as refund_method,
        max(case when component = 'shipping' then allocated_shop_amount end)
            as shipping_allocated,
        max(case when component = 'shipping' then allocation_method end)
            as shipping_method,
        max(
            case
                when component = 'shipping_refund' then allocated_shop_amount
            end
        ) as shipping_refund_allocated,
        max(
            case when component = 'shipping_refund' then allocation_method end
        ) as shipping_refund_method
    from other_allocations
    group by shop_id, order_id, line_item_id
),

-- Amounts and methods per component. A remainder of zero is source-only, a
-- gift-card line takes no share, and an unresolved remainder or unknown gift
-- flag leaves the share unresolved.
resolved as (
    select
        ld.shop_id,
        ld.order_id,
        ld.line_item_id,
        ld.is_gift_card_line,
        ld.discount_direct_shop_amount,
        ld.discount_allocated_shop_amount,
        ld.discount_allocation_method,
        ld.refund_direct_shop_amount,
        cast('USD' as varchar) as reporting_currency_code,
        cast(
            ld.discount_direct_shop_amount + ld.discount_allocated_shop_amount
            as decimal(38, 9)
        ) as discount_shop_amount,
        case
            when
                ld.refund_remainder_shop_amount = 0
                or ld.is_gift_card_line = true
                then cast(0 as decimal(38, 9))
            else stage_two.refund_allocated
        end as refund_allocated_shop_amount,
        case
            when ld.refund_remainder_shop_amount = 0 then 'source_only'
            when ld.is_gift_card_line = true then 'not_eligible'
            when
                ld.refund_remainder_shop_amount is null
                then 'unresolved_remainder'
            when ld.order_unknown_gift_card_line_count > 0
                then 'unresolved_unknown_gift_card_flag'
            else stage_two.refund_method
        end as refund_allocation_method,
        case
            when
                ld.shipping_remainder_shop_amount = 0
                or ld.is_gift_card_line = true
                then cast(0 as decimal(38, 9))
            else stage_two.shipping_allocated
        end as shipping_allocated_shop_amount,
        case
            when ld.shipping_remainder_shop_amount = 0 then 'source_only'
            when ld.is_gift_card_line = true then 'not_eligible'
            when
                ld.shipping_remainder_shop_amount is null
                then 'unresolved_remainder'
            when ld.order_unknown_gift_card_line_count > 0
                then 'unresolved_unknown_gift_card_flag'
            else stage_two.shipping_method
        end as shipping_allocation_method,
        case
            when
                ld.shipping_refund_remainder_shop_amount = 0
                or ld.is_gift_card_line = true
                then cast(0 as decimal(38, 9))
            else stage_two.shipping_refund_allocated
        end as shipping_refund_allocated_shop_amount,
        case
            when ld.shipping_refund_remainder_shop_amount = 0 then 'source_only'
            when ld.is_gift_card_line = true then 'not_eligible'
            when
                ld.shipping_refund_remainder_shop_amount is null
                then 'unresolved_remainder'
            when ld.order_unknown_gift_card_line_count > 0
                then 'unresolved_unknown_gift_card_flag'
            else stage_two.shipping_refund_method
        end as shipping_refund_allocation_method
    from line_discount as ld
    left join other_by_line as stage_two
        on
            ld.shop_id = stage_two.shop_id
            and ld.order_id = stage_two.order_id
            and ld.line_item_id = stage_two.line_item_id
),

totals as (
    select
        *,
        cast(
            refund_direct_shop_amount + refund_allocated_shop_amount
            as decimal(38, 9)
        ) as refund_shop_amount
    from resolved
)

select
    shop_id,
    order_id,
    line_item_id,
    is_gift_card_line,
    reporting_currency_code,
    discount_direct_shop_amount as merchandise_discount_direct_shop_amount,
    discount_allocated_shop_amount
        as merchandise_discount_allocated_shop_amount,
    discount_shop_amount as merchandise_discount_shop_amount,
    discount_allocation_method as merchandise_discount_allocation_method,
    refund_direct_shop_amount as merchandise_refund_direct_shop_amount,
    refund_allocated_shop_amount as merchandise_refund_allocated_shop_amount,
    refund_shop_amount as merchandise_refund_shop_amount,
    refund_allocation_method as merchandise_refund_allocation_method,
    shipping_allocated_shop_amount as shipping_shop_amount,
    shipping_allocation_method,
    shipping_refund_allocated_shop_amount as shipping_refund_shop_amount,
    shipping_refund_allocation_method
from totals
