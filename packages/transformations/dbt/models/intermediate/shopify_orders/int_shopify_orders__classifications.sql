-- Channel and order-characteristic flags, one row per order. Rules follow the
-- order marts specification (Channel and order flags; Channel decision table).
--
-- Flags are nullable: true for an established match, false for a conclusively
-- evaluated non-match, and null when required evidence is unavailable.
-- Extraction revision 2 and later select tags, discount codes and order email,
-- so an order with no tag or code rows at those revisions has empty lists.
-- Earlier observations never selected them: their tag and code rules are
-- unknown, and POS still establishes retail.
{%- set employee_email_patterns = [] -%}
{%- for domain in var('employee_email_domains') -%}
    {%- do employee_email_patterns.append('%@' ~ domain) -%}
{%- endfor %}
with orders as (
    select * from {{ ref('stg_shopify_orders__orders') }}
),

tags as (
    select
        shop_id,
        order_id,
        normalized_tag as tag
    from {{ ref('stg_shopify_orders__order_tags') }}
),

discount_codes as (
    select
        shop_id,
        order_id,
        normalized_discount_code as code
    from {{ ref('stg_shopify_orders__order_discount_codes') }}
),

-- Tag and code rules are project vars; see dbt_project.yml.
tag_matches as (
    select
        shop_id,
        order_id,
        max(case
            when {{ matches_var('tag', 'specialty_order_tags') }}
                then 1
            else 0
        end) = 1 as has_specialty_tag,
        max(case
            when {{ matches_var('tag', 'bulk_order_tags') }}
                then 1
            else 0
        end) = 1 as has_bulk_tag,
        max(case
            when {{ matches_var('tag', 'replacement_order_tags') }}
                then 1
            else 0
        end) = 1 as has_replacement_tag,
        max(case
            when {{ matches_var('tag', 'exchange_order_tags') }}
                then 1
            else 0
        end) = 1 as has_exchange_tag,
        max(case
            when {{ matches_var('tag', 'gifting_order_tags') }}
                then 1
            else 0
        end) = 1 as has_gifting_tag,
        max(case
            when {{ matches_var('tag', 'shopify_collective_order_tags') }}
                then 1
            else 0
        end) = 1 as has_collective_tag,
        max(case
            when {{ matches_var('tag', 'broadcast_order_tags') }}
                then 1
            else 0
        end) = 1 as has_broadcast_tag
    from tags
    group by shop_id, order_id
),

code_matches as (
    select
        shop_id,
        order_id,
        max(case
            when {{ matches_var('code', 'specialty_discount_codes') }}
                then 1
            else 0
        end) = 1 as has_specialty_code,
        max(case
            when {{ matches_var('code', 'replacement_discount_codes') }}
                then 1
            else 0
        end) = 1 as has_replacement_code,
        max(case
            when {{ matches_var('code', 'exchange_discount_codes') }}
                then 1
            else 0
        end) = 1 as has_exchange_code
    from discount_codes
    group by shop_id, order_id
),

-- Rule matches. With tag and code evidence selected, a missing row means an
-- empty list (false); without it the rule is unknown (null).
evidence as (
    select
        orders.shop_id,
        orders.order_id,
        orders.is_test,
        case
            when
                orders.extraction_revision >= 2
                and orders.original_total_price_shop_amount >= 0
                then orders.original_total_price_shop_amount = 0
        end as is_free_at_placement,
        orders.extraction_revision >= 2 as has_classification_evidence,
        coalesce(lower(trim(orders.source_name)) = 'pos', false)
            as has_retail_evidence,
        case
            when orders.extraction_revision >= 2
                then
                    coalesce(tag_matches.has_specialty_tag, false)
                    or coalesce(code_matches.has_specialty_code, false)
        end as has_specialty_evidence,
        case
            when orders.extraction_revision >= 2
                then coalesce(tag_matches.has_bulk_tag, false)
        end as has_bulk_evidence,
        case
            when orders.extraction_revision >= 2
                then coalesce(tag_matches.has_gifting_tag, false)
        end as has_gifting_tag_evidence,
        case
            when orders.extraction_revision >= 2
                then
                    coalesce(tag_matches.has_replacement_tag, false)
                    or coalesce(code_matches.has_replacement_code, false)
        end as is_replacement,
        case
            when orders.extraction_revision >= 2
                then
                    coalesce(tag_matches.has_exchange_tag, false)
                    or coalesce(code_matches.has_exchange_code, false)
        end as is_exchange,
        case
            when orders.extraction_revision >= 2
                then coalesce(tag_matches.has_collective_tag, false)
        end as is_shopify_collective,
        case
            when orders.extraction_revision >= 2
                then coalesce(tag_matches.has_broadcast_tag, false)
        end as is_broadcast,
        -- The order email at its latest observation, never the customer's
        -- current profile email, matched against employee_email_domains
        -- exactly. A missing email leaves the rule unknown.
        case
            when
                orders.extraction_revision >= 2
                and orders.order_email is not null
                then {{ matches_any(
                    'lower(trim(orders.order_email))', employee_email_patterns
                ) }}
        end as is_employee_order
    from orders
    left join tag_matches
        on
            orders.shop_id = tag_matches.shop_id
            and orders.order_id = tag_matches.order_id
    left join code_matches
        on
            orders.shop_id = code_matches.shop_id
            and orders.order_id = code_matches.order_id
),

-- Commercial precedence: retail, then specialty, then bulk. Unknown evidence
-- for a higher-priority channel leaves lower channels unknown rather than
-- letting them win.
channels as (
    select
        *,
        has_retail_evidence as is_retail,
        case
            when has_retail_evidence then false
            else has_specialty_evidence
        end as is_specialty,
        case
            when has_retail_evidence then false
            when has_specialty_evidence then false
            when has_specialty_evidence is null then null
            else has_bulk_evidence
        end as is_bulk
    from evidence
),

classified as (
    select
        shop_id,
        order_id,
        has_classification_evidence,
        has_retail_evidence,
        has_specialty_evidence,
        has_bulk_evidence,
        has_gifting_tag_evidence,
        is_retail,
        is_specialty,
        is_bulk,
        is_employee_order,
        is_replacement,
        is_exchange,
        is_shopify_collective,
        is_broadcast,
        case
            when is_retail or is_specialty or is_bulk then true
            when
                is_retail is null or is_specialty is null or is_bulk is null
                then null
            else false
        end as is_commercial,
        -- D2C is the residual channel: every commercial rule was evaluated
        -- and none matched.
        case
            when is_retail or is_specialty or is_bulk then false
            when
                is_retail is null or is_specialty is null or is_bulk is null
                then null
            else true
        end as is_d2c,
        -- Original total includes shipping and tax after discounts, before
        -- later edits/refunds. Missing or invalid evidence stays null.
        -- Three-valued logic preserves unknowns unless a decisive condition
        -- resolves the rule. Explicit gifting tags always establish gifting.
        has_gifting_tag_evidence or (
            is_employee_order
            and is_free_at_placement
            and not is_test
            and not is_replacement
        ) as is_gifted
    from channels
)

select * from classified
