-- One row per Shopify sale, with signed tax-exclusive USD components in the
-- shop
-- currency. Source fields stay as received. A sale that fails any check keeps
-- its
-- source values, has resolution_status set to the failed check, and leaves
-- every
-- component NULL, so an unknown amount is never recorded as zero.
--
-- Sign rules: ORDER and UPDATE product sales carry gross merchandise and the
-- tax-exclusive discount (the source total is already net of discount). RETURN
-- product sales carry the tax-exclusive merchandise refund. Every component
-- other than the discount keeps the source sign. The components of a resolved
-- sale sum to sale_net_shop_amount.
with orders as (
    select
        shop_id,
        order_id
    from {{ ref('stg_shopify_orders__orders') }}
),

agreements as (
    select
        shop_id,
        order_id,
        agreement_id,
        agreement_type,
        reason,
        refund_id,
        happened_at
    from {{ ref('stg_shopify_orders__agreements') }}
),

order_lines as (
    select
        shop_id,
        order_id,
        line_item_id,
        is_gift_card
    from {{ ref('stg_shopify_orders__order_lines') }}
),

sales as (
    select
        shop_id,
        order_id,
        agreement_id,
        sale_id,
        sale_type,
        action_type,
        line_type,
        signed_quantity,
        line_item_id,
        shipping_line_id,
        duty_id,
        fee_id,
        fee_type,
        additional_fee_id,
        total_amount_shop_amount,
        total_amount_shop_currency_code,
        total_tax_amount_shop_amount,
        total_tax_amount_shop_currency_code,
        total_discount_amount_before_taxes_shop_amount,
        total_discount_amount_before_taxes_shop_currency_code
    from {{ ref('stg_shopify_orders__agreement_sales') }}
),

joined as (
    select
        sales.shop_id,
        sales.order_id,
        sales.agreement_id,
        sales.sale_id,
        agreements.agreement_type,
        agreements.reason as agreement_reason,
        agreements.refund_id as agreement_refund_id,
        agreements.happened_at as agreement_happened_at,
        fees.handling_fee_status,
        fees.handling_fee_shop_amount as matched_handling_fee_shop_amount,
        sales.sale_type,
        sales.action_type,
        sales.line_type,
        sales.signed_quantity,
        sales.line_item_id,
        sales.shipping_line_id,
        sales.duty_id,
        sales.fee_id,
        sales.fee_type,
        sales.additional_fee_id,
        order_lines.is_gift_card as is_gift_card_line,
        sales.total_amount_shop_amount as sale_total_shop_amount,
        sales.total_amount_shop_currency_code,
        sales.total_tax_amount_shop_amount as sale_tax_shop_amount,
        sales.total_tax_amount_shop_currency_code,
        sales.total_discount_amount_before_taxes_shop_amount
            as source_discount_before_taxes_shop_amount,
        sales.total_discount_amount_before_taxes_shop_currency_code
            as source_discount_currency_code,
        orders.order_id is not null as has_order,
        agreements.agreement_id is not null as has_agreement,
        sum(case when sales.sale_type = 'AdjustmentSale' then 1 else 0 end)
            over (
                partition by sales.shop_id, sales.order_id, agreements.refund_id
            ) as adjustment_sale_count,
        order_lines.line_item_id is not null as has_order_line
    from sales
    left join orders
        on
            sales.shop_id = orders.shop_id
            and sales.order_id = orders.order_id
    left join agreements
        on
            sales.shop_id = agreements.shop_id
            and sales.order_id = agreements.order_id
            and sales.agreement_id = agreements.agreement_id
    left join {{ ref('int_shopify_refunds__handling_fees') }} as fees
        on
            agreements.shop_id = fees.shop_id
            and agreements.order_id = fees.order_id
            and agreements.refund_id = fees.refund_id
    left join order_lines
        on
            sales.shop_id = order_lines.shop_id
            and sales.order_id = order_lines.order_id
            and sales.line_item_id = order_lines.line_item_id
),

-- Sale kind comes from the concrete Shopify sale type. Action group is ORDER
-- and
-- UPDATE (charge) or RETURN. Other actions have no group.
classified as (
    select
        *,
        case
            when
                sale_type = 'AdjustmentSale'
                and handling_fee_status = 'matched_loop_handling_fee'
                and adjustment_sale_count = 1
                and sale_total_shop_amount = matched_handling_fee_shop_amount
                and sale_tax_shop_amount = 0 then 'handling_fee'
            when sale_type = 'GiftCardSale' then 'gift_card'
            when
                sale_type = 'ProductSale' and is_gift_card_line
                then 'gift_card'
            when sale_type = 'ProductSale' then 'merchandise'
            when sale_type = 'ShippingLineSale' then 'shipping'
            when sale_type = 'DutySale' then 'duty'
            when sale_type = 'FeeSale' and fee_type = 'ReturnShippingFee'
                then 'return_shipping_fee'
            when sale_type = 'FeeSale' then 'unfinalized_fee'
            when sale_type = 'AdditionalFeeSale' then 'additional_fee'
            else 'unknown'
        end as sale_kind,
        case
            when action_type in ('ORDER', 'UPDATE') then 'charge'
            when action_type = 'RETURN' then 'return'
        end as action_group
    from joined
),

checked as (
    select
        *,
        case
            when not has_order then 'unresolved_missing_order'
            when not has_agreement then 'unresolved_missing_agreement'
            when sale_kind = 'unknown' then 'unresolved_sale_type'
            when line_type = 'UNKNOWN' then 'unresolved_line_type'
            when action_group is null then 'unresolved_action_type'
            when
                coalesce(agreement_reason, '') not in (
                    'ORDER', 'ORDER_EDIT', 'RETURN', 'REFUND'
                )
                then 'unresolved_agreement_reason'
            when
                sale_kind in ('merchandise', 'gift_card')
                and line_item_id is null
                then 'unresolved_missing_line_identity'
            when
                sale_kind in ('merchandise', 'gift_card') and not has_order_line
                then 'unresolved_missing_order_line'
            when
                sale_kind in ('merchandise', 'gift_card')
                and is_gift_card_line is null
                then 'unresolved_gift_card_evidence'
            when sale_type = 'GiftCardSale' and not is_gift_card_line
                then 'unresolved_gift_card_evidence'
            when sale_total_shop_amount is null or sale_tax_shop_amount is null
                then 'unresolved_missing_money'
            when
                sale_kind = 'merchandise' and action_group = 'charge'
                and source_discount_before_taxes_shop_amount is null
                then 'unresolved_missing_money'
            when
                coalesce(total_amount_shop_currency_code, '') <> 'USD'
                or coalesce(total_tax_amount_shop_currency_code, '') <> 'USD'
                then 'unresolved_currency'
            when
                source_discount_before_taxes_shop_amount is not null
                and coalesce(source_discount_currency_code, '') <> 'USD'
                then 'unresolved_currency'
            when action_type = 'ORDER' and (
                sale_total_shop_amount < 0
                or source_discount_before_taxes_shop_amount < 0
            ) then 'unresolved_sign_conflict'
            when
                action_group = 'return' and sale_kind in (
                    'merchandise', 'gift_card', 'shipping', 'duty'
                ) and (sale_total_shop_amount > 0 or sale_tax_shop_amount > 0)
                then 'unresolved_sign_conflict'
            when
                (sale_tax_shop_amount > 0 and sale_total_shop_amount < 0)
                or (sale_tax_shop_amount < 0 and sale_total_shop_amount > 0)
                then 'unresolved_sign_conflict'
            else 'resolved'
        end as resolution_status
    from classified
),

-- The tax-exclusive net is the source total minus the source total tax.
netted as (
    select
        *,
        sale_total_shop_amount - sale_tax_shop_amount as sale_net_shop_amount
    from checked
)

select
    shop_id,
    order_id,
    agreement_id,
    sale_id,
    agreement_type,
    agreement_reason,
    agreement_refund_id,
    agreement_happened_at,
    sale_type,
    action_type,
    line_type,
    signed_quantity,
    line_item_id,
    shipping_line_id,
    duty_id,
    fee_id,
    fee_type,
    additional_fee_id,
    is_gift_card_line,
    sale_kind,
    resolution_status,
    sale_total_shop_amount,
    sale_tax_shop_amount,
    source_discount_before_taxes_shop_amount,
    'USD' as reporting_currency_code,
    resolution_status = 'resolved' as is_resolved,
    case
        when resolution_status = 'resolved' then sale_net_shop_amount
    end as sale_net_shop_amount,
    case
        when resolution_status = 'resolved'
            then case
                when sale_kind = 'merchandise' and action_group = 'charge'
                    then
                        sale_net_shop_amount
                        + source_discount_before_taxes_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as gross_merchandise_shop_amount,
    case
        when resolution_status = 'resolved'
            then case
                when sale_kind = 'merchandise' and action_group = 'charge'
                    then -source_discount_before_taxes_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as merchandise_discount_shop_amount,
    case
        when resolution_status = 'resolved'
            then case
                when sale_kind = 'merchandise' and action_group = 'return'
                    then sale_net_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as merchandise_refund_shop_amount,
    case
        when resolution_status = 'resolved'
            then case
                when sale_kind = 'gift_card' then sale_net_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as gift_card_liability_shop_amount,
    case
        when resolution_status = 'resolved'
            then case
                when sale_kind = 'shipping' and action_group = 'charge'
                    then sale_net_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as shipping_shop_amount,
    case
        when resolution_status = 'resolved'
            then case
                when sale_kind = 'shipping' and action_group = 'return'
                    then sale_net_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as shipping_refund_shop_amount,
    case
        when resolution_status = 'resolved'
            then case
                when sale_kind = 'duty' then sale_net_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as duty_shop_amount,
    case
        when resolution_status = 'resolved'
            then case
                when sale_kind = 'return_shipping_fee' then sale_net_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as return_shipping_fee_shop_amount,
    case
        when resolution_status = 'resolved'
            then case
                when sale_kind = 'unfinalized_fee' then sale_net_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as unfinalized_fee_shop_amount,
    case
        when resolution_status = 'resolved'
            then case
                when sale_kind = 'additional_fee' then sale_net_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as additional_fee_shop_amount,
    case
        when resolution_status = 'resolved' then
            case
                when sale_kind = 'handling_fee' then sale_net_shop_amount
                else cast(0 as decimal(38, 9))
            end
    end as handling_fee_shop_amount
from netted
