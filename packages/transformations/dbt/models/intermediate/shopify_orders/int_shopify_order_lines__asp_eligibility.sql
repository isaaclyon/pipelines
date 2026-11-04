-- Preserve zero-unit residual value and amount-only refunded units. A positive
-- line value or quantity is not a gate. Divide only after aggregation.
with evidence as (
    select
        source_lines.shop_id,
        source_lines.order_id,
        source_lines.line_item_id,
        source_lines.is_gift_card,
        order_scope.is_asp_order_eligible,
        net_units.net_ordered_quantity,
        commercial.gross_merchandise_shop_amount is not null
        and allocations.merchandise_discount_shop_amount is not null
        and allocations.merchandise_refund_shop_amount is not null
            as is_line_value_known,
        coalesce(order_scope.asp_order_exclusion_reasons, '')
        || (case
            when source_lines.is_gift_card then 'gift_card_item|'
            else ''
        end)
            as asp_exclusion_reasons,
        coalesce(order_scope.asp_order_unresolved_requirements, '')
        || (case
            when order_scope.order_id is null
                then 'parent_eligibility_missing|'
            else ''
        end)
        || (case
            when source_lines.is_gift_card is null
                then 'gift_card_status_unknown|'
            else ''
        end)
        || (case
            when net_units.net_ordered_quantity is null
                then 'net_quantity_unknown|'
            else ''
        end)
            as parent_and_quantity_requirements
    from {{ ref('stg_shopify_orders__order_lines') }} as source_lines
    left join
        {{ ref('int_shopify_orders__average_eligibility') }} as order_scope
        on
            source_lines.shop_id = order_scope.shop_id
            and source_lines.order_id = order_scope.order_id
    left join {{ ref('int_shopify_order_lines__net_units') }} as net_units
        on
            source_lines.shop_id = net_units.shop_id
            and source_lines.order_id = net_units.order_id
            and source_lines.line_item_id = net_units.line_item_id
    left join
        {{ ref('int_shopify_order_lines__commercial_components') }}
            as commercial
        on
            source_lines.shop_id = commercial.shop_id
            and source_lines.order_id = commercial.order_id
            and source_lines.line_item_id = commercial.line_item_id
    left join {{ ref('int_shopify_order_lines__allocations') }} as allocations
        on
            source_lines.shop_id = allocations.shop_id
            and source_lines.order_id = allocations.order_id
            and source_lines.line_item_id = allocations.line_item_id
)

select
    shop_id,
    order_id,
    line_item_id,
    asp_exclusion_reasons,
    case
        when is_asp_order_eligible = false or is_gift_card then false
        when
            is_asp_order_eligible is null or is_gift_card is null
            or net_ordered_quantity is null or not is_line_value_known then null
        else true
    end as is_asp_eligible,
    parent_and_quantity_requirements
    || (case
        when not is_line_value_known
            then 'line_commercial_value_unknown|'
        else ''
    end)
        as asp_unresolved_requirements
from evidence
