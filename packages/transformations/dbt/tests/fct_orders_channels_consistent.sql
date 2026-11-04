-- Published channel flags obey the specification's exclusivity rules:
-- D2C is exactly the negation of commercial, including null; a commercial
-- order has exactly one true subchannel; a non-commercial order has none;
-- resolved channels have matching rule evidence; and orders without
-- classification evidence carry no tag, code or email verdicts. Gifting must
-- agree with explicit tags or the free-at-placement employee inference.
with orders as (
    select * from {{ ref('fct_orders') }}
),

counted as (
    select
        *,
        case when is_retail then 1 else 0 end
        + case when is_specialty then 1 else 0 end
        + case when is_bulk then 1 else 0 end as true_subchannel_count
    from orders
)

select
    shopify_shop_id,
    shopify_order_id
from counted
where
    is_gifted is distinct from (
        has_gifting_tag_evidence or (
            is_employee_order
            and not is_test
            and not is_replacement
            and case
                when
                    has_classification_evidence
                    and original_total_price_shop_amount >= 0
                    then original_total_price_shop_amount = 0
            end
        )
    )
    or is_d2c is distinct from (not is_commercial)
    or (is_commercial and true_subchannel_count <> 1)
    or (
        not is_commercial
        and (
            is_retail is distinct from false
            or is_specialty is distinct from false
            or is_bulk is distinct from false
        )
    )
    or (is_commercial is null and true_subchannel_count > 0)
    or is_retail is distinct from has_retail_evidence
    or (is_specialty and not coalesce(has_specialty_evidence, false))
    or (is_bulk and not coalesce(has_bulk_evidence, false))
    or (
        not has_classification_evidence
        and (
            has_specialty_evidence is not null
            or has_bulk_evidence is not null
            or has_gifting_tag_evidence is not null
            or is_employee_order is not null
            or is_replacement is not null
            or is_exchange is not null
            or is_shopify_collective is not null
            or is_broadcast is not null
        )
    )
