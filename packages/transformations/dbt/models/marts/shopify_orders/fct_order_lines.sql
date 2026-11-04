with lines as (
    select * from {{ ref('stg_shopify_orders__order_lines') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

contacts as (
    select * from {{ ref('dim_order_contacts') }}
),

net_units as (
    select * from {{ ref('int_shopify_order_lines__net_units') }}
),

allocations as (
    select * from {{ ref('int_shopify_order_lines__allocations') }}
),

asp_eligibility as (
    select * from {{ ref('int_shopify_order_lines__asp_eligibility') }}
),

commercial as (
    select * from {{ ref('int_shopify_order_lines__commercial_components') }}
),

joined as (
    select
        lines.shop_id as shopify_shop_id,
        lines.order_id as shopify_order_id,
        lines.line_item_id as shopify_line_item_id,
        lines.sku,
        lines.line_name,
        lines.title,
        lines.variant_title,
        lines.product_id as shopify_product_id,
        lines.variant_id as shopify_variant_id,
        lines.is_gift_card as is_gift_card_item,
        lines.order_line_quantity as gross_ordered_quantity,
        lines.current_quantity,
        lines.original_unit_price_shop_amount,
        lines.original_unit_price_presentment_amount,
        lines.discounted_total_shop_amount,
        lines.discounted_total_presentment_amount,
        cast(
            lines.order_line_quantity * lines.original_unit_price_shop_amount
            as decimal(38, 9)
        ) as original_line_value_shop_amount,
        cast(
            lines.order_line_quantity
            * lines.original_unit_price_presentment_amount
            as decimal(38, 9)
        ) as original_line_value_presentment_amount,
        orders.order_name,
        orders.shopify_customer_id,
        orders.created_at,
        orders.processed_at,
        orders.updated_at,
        orders.cancelled_at,
        orders.closed_at,
        orders.ordered_at,
        orders.order_date_source,
        orders.order_date,
        orders.cancel_reason,
        orders.is_test,
        orders.source_name,
        orders.financial_status,
        orders.fulfillment_status,
        orders.is_tax_included,
        orders.shop_currency_code,
        orders.presentment_currency_code,
        orders.source_observed_at,
        orders.is_cancelled,
        orders.is_revenue_eligible,
        orders.revenue_exclusion_reasons,
        orders.revenue_unresolved_requirements,
        orders.is_aov_eligible,
        orders.aov_exclusion_reasons,
        orders.aov_unresolved_requirements,
        asp_eligibility.is_asp_eligible,
        asp_eligibility.asp_exclusion_reasons,
        asp_eligibility.asp_unresolved_requirements,
        contacts.billing_address_city,
        contacts.billing_address_province,
        contacts.billing_address_province_code,
        contacts.billing_address_zip,
        contacts.billing_address_country,
        contacts.billing_address_country_code,
        contacts.shipping_address_city,
        contacts.shipping_address_province,
        contacts.shipping_address_province_code,
        contacts.shipping_address_zip,
        contacts.shipping_address_country,
        contacts.shipping_address_country_code,
        net_units.net_ordered_quantity,
        net_units.net_quantity_status,
        allocations.merchandise_discount_direct_shop_amount,
        allocations.merchandise_discount_allocated_shop_amount,
        allocations.merchandise_discount_shop_amount,
        allocations.merchandise_discount_allocation_method,
        allocations.merchandise_refund_direct_shop_amount,
        allocations.merchandise_refund_allocated_shop_amount,
        allocations.merchandise_refund_shop_amount,
        allocations.merchandise_refund_allocation_method,
        allocations.shipping_shop_amount,
        allocations.shipping_allocation_method,
        allocations.shipping_refund_shop_amount,
        allocations.shipping_refund_allocation_method,
        allocations.reporting_currency_code,
        commercial.unresolved_line_sale_count,
        commercial.line_commercial_status,
        commercial.gross_merchandise_shop_amount,
        commercial.gift_card_liability_shop_amount,
        commercial.merchandise_tax_shop_amount,
        orders.commercial_resolution_status,
        orders.allocation_status,
        case when orders.is_commercial_resolved then cast(
            commercial.gross_merchandise_shop_amount
            + allocations.merchandise_discount_shop_amount
            + allocations.merchandise_refund_shop_amount
            as decimal(38, 9)
        ) end as net_merchandise_shop_amount
    from lines
    left join asp_eligibility
        on
            lines.shop_id = asp_eligibility.shop_id
            and lines.order_id = asp_eligibility.order_id
            and lines.line_item_id = asp_eligibility.line_item_id
    left join allocations
        on
            lines.shop_id = allocations.shop_id
            and lines.order_id = allocations.order_id
            and lines.line_item_id = allocations.line_item_id
    left join commercial
        on
            lines.shop_id = commercial.shop_id
            and lines.order_id = commercial.order_id
            and lines.line_item_id = commercial.line_item_id
    left join orders
        on
            lines.shop_id = orders.shopify_shop_id
            and lines.order_id = orders.shopify_order_id
    left join contacts
        on
            lines.shop_id = contacts.shopify_shop_id
            and lines.order_id = contacts.shopify_order_id
    left join net_units
        on
            lines.shop_id = net_units.shop_id
            and lines.order_id = net_units.order_id
            and lines.line_item_id = net_units.line_item_id
)

select * from joined
