-- One row per source order line. Amounts are direct line assignments from the
-- sales ledger: merchandise gross, tax-exclusive discount, merchandise refund
-- and
-- gift-card liability. A line with no sale evidence, or with any unresolved
-- sale
-- assigned to it, returns NULL amounts rather than zero.
with order_lines as (
    select
        shop_id,
        order_id,
        line_item_id,
        is_gift_card
    from {{ ref('stg_shopify_orders__order_lines') }}
),

ledger_by_line as (
    select
        shop_id,
        order_id,
        line_item_id,
        cast(
            sum(
                case when resolution_status <> 'resolved' then 1 else 0 end
            ) as bigint
        ) as unresolved_line_sale_count,
        cast(count(*) as bigint) as line_sale_count,
        coalesce(sum(gross_merchandise_shop_amount), 0)
            as gross_merchandise_total_shop_amount,
        coalesce(sum(merchandise_discount_shop_amount), 0)
            as merchandise_discount_total_shop_amount,
        coalesce(sum(merchandise_refund_shop_amount), 0)
            as merchandise_refund_total_shop_amount,
        coalesce(sum(gift_card_liability_shop_amount), 0)
            as gift_card_liability_total_shop_amount,
        coalesce(
            sum(
                case
                    when sale_kind = 'merchandise' then sale_tax_shop_amount
                    else cast(0 as decimal(38, 9))
                end
            ),
            0
        ) as merchandise_tax_total_shop_amount
    from {{ ref('int_shopify_sales__ledger') }}
    where line_item_id is not null
    group by shop_id, order_id, line_item_id
),

joined as (
    select
        order_lines.shop_id,
        order_lines.order_id,
        order_lines.line_item_id,
        order_lines.is_gift_card as is_gift_card_line,
        ledger_by_line.gross_merchandise_total_shop_amount,
        ledger_by_line.merchandise_discount_total_shop_amount,
        ledger_by_line.merchandise_refund_total_shop_amount,
        ledger_by_line.gift_card_liability_total_shop_amount,
        ledger_by_line.merchandise_tax_total_shop_amount,
        coalesce(ledger_by_line.line_sale_count, 0) as line_sale_count,
        coalesce(ledger_by_line.unresolved_line_sale_count, 0)
            as unresolved_line_sale_count,
        case
            when ledger_by_line.line_item_id is null then 'no_sale_evidence'
            when
                ledger_by_line.unresolved_line_sale_count > 0
                then 'unresolved_sales'
            else 'resolved'
        end as line_commercial_status
    from order_lines
    left join ledger_by_line
        on
            order_lines.shop_id = ledger_by_line.shop_id
            and order_lines.order_id = ledger_by_line.order_id
            and order_lines.line_item_id = ledger_by_line.line_item_id
)

select
    shop_id,
    order_id,
    line_item_id,
    is_gift_card_line,
    line_sale_count,
    unresolved_line_sale_count,
    line_commercial_status,
    'USD' as reporting_currency_code,
    case
        when
            line_commercial_status = 'resolved'
            then gross_merchandise_total_shop_amount
    end as gross_merchandise_shop_amount,
    case
        when
            line_commercial_status = 'resolved'
            then merchandise_discount_total_shop_amount
    end as merchandise_discount_shop_amount,
    case
        when
            line_commercial_status = 'resolved'
            then merchandise_refund_total_shop_amount
    end as merchandise_refund_shop_amount,
    case
        when line_commercial_status = 'resolved'
            then
                gross_merchandise_total_shop_amount
                + merchandise_discount_total_shop_amount
                + merchandise_refund_total_shop_amount
    end as net_merchandise_shop_amount,
    case
        when
            line_commercial_status = 'resolved'
            then gift_card_liability_total_shop_amount
    end as gift_card_liability_shop_amount,
    case
        when
            line_commercial_status = 'resolved'
            then merchandise_tax_total_shop_amount
    end as merchandise_tax_shop_amount
from joined
