with evidence as (
    select
        order_lines.shop_id,
        order_lines.order_id,
        order_lines.line_item_id,
        order_lines.current_quantity,
        order_lines.unfulfilled_quantity,
        cast(case
            when orders.order_id is null then 'missing_order_evidence'
            when
                order_lines.current_quantity is null
                or order_lines.order_line_quantity is null
                or order_lines.current_quantity < 0
                or order_lines.current_quantity
                > order_lines.order_line_quantity
                then 'invalid_quantity_evidence'
            when
                orders.cancelled_at is null or order_lines.current_quantity = 0
                then 'current_quantity'
            when order_lines.unfulfilled_quantity is null
                then 'missing_cancellation_evidence'
            when
                order_lines.unfulfilled_quantity < 0
                or order_lines.unfulfilled_quantity
                > order_lines.current_quantity
                then 'invalid_quantity_evidence'
            else 'cancelled_unfulfilled'
        end as varchar) as net_quantity_status
    from {{ ref('stg_shopify_orders__order_lines') }} as order_lines
    left join {{ ref('stg_shopify_orders__orders') }} as orders
        on
            order_lines.shop_id = orders.shop_id
            and order_lines.order_id = orders.order_id
)

select
    shop_id,
    order_id,
    line_item_id,
    cast(case
        when net_quantity_status = 'current_quantity' then current_quantity
        when net_quantity_status = 'cancelled_unfulfilled'
            then current_quantity - unfulfilled_quantity
    end as bigint) as net_ordered_quantity,
    net_quantity_status
from evidence
