{#-
  Allocate one USD component amount across the eligible lines of an order component.

  input_sql: a query with one row per eligible line for each (shop_id, order_id, component).
    Columns: shop_id, order_id, line_item_id, component (varchar);
    component_shop_amount decimal(38, 9), repeated on every line of the component;
    value_weight decimal(38, 9) and quantity_weight bigint, both nonnegative when known.

  Returns a query with one row per input row: shop_id, order_id, line_item_id,
  component, allocated_shop_amount decimal(38, 9), allocation_method varchar.
  allocation_method is value_weight, quantity_weight or equal_share when resolved.
  Otherwise it names an unresolved_* reason and allocated_shop_amount is NULL.
  Input with no rows returns no rows.

  Method selection, per component:
    1. value_weight when a known value weight is nonzero, or when any value weight is missing.
    2. quantity_weight when a known quantity weight is nonzero, or when any quantity weight is missing.
    3. equal_share otherwise.
  A missing or negative weight in the selected weights leaves the whole component unresolved.
  Lines never absorb the share of a missing line.

  Rounding: the total is converted to whole cents. Each line receives the floor of its
  exact share. The leftover cents go to the lines with the largest remainders; ties go to
  the lower line_item_id. A signed total is allocated by its magnitude and then negated,
  so the allocated amounts always sum exactly to the component total.
  A total that is not a whole number of cents is unresolved.
  Arithmetic is exact integer arithmetic. Overflow fails the query. The macro never approximates.
-#}
{% macro shopify_allocate_usd(input_sql) %}
with allocation_input as (
    select
        cast(shop_id as varchar) as shop_id,
        cast(order_id as varchar) as order_id,
        cast(line_item_id as varchar) as line_item_id,
        cast(component as varchar) as component,
        cast(component_shop_amount as decimal(38, 9)) as component_shop_amount,
        cast(value_weight as decimal(38, 9)) as value_weight,
        cast(quantity_weight as bigint) as quantity_weight
    from ({{ input_sql }}) as allocation_source
),

allocation_groups as (
    select
        shop_id,
        order_id,
        component,
        count(*) as line_count,
        count(distinct line_item_id) as distinct_line_count,
        sum(
            case
                when shop_id is null or order_id is null or component is null or line_item_id is null
                    then 1
                else 0
            end
        ) as missing_key_count,
        count(component_shop_amount) as amount_count,
        min(component_shop_amount) as amount_min,
        max(component_shop_amount) as amount_max,
        count(value_weight) as value_known_count,
        count(case when value_weight <> 0 then 1 end) as value_nonzero_count,
        count(case when value_weight < 0 then 1 end) as value_negative_count,
        count(quantity_weight) as quantity_known_count,
        count(case when quantity_weight <> 0 then 1 end) as quantity_nonzero_count,
        count(case when quantity_weight < 0 then 1 end) as quantity_negative_count
    from allocation_input
    group by shop_id, order_id, component
),

allocation_methods as (
    select
        *,
        case
            when value_nonzero_count > 0 or value_known_count < line_count then 'value_weight'
            when quantity_nonzero_count > 0 or quantity_known_count < line_count then 'quantity_weight'
            else 'equal_share'
        end as selected_method
    from allocation_groups
),

allocation_resolutions as (
    select
        *,
        case
            when missing_key_count > 0 then 'unresolved_missing_key'
            when distinct_line_count < line_count then 'unresolved_duplicate_line'
            when amount_count < line_count then 'unresolved_missing_amount'
            when amount_min <> amount_max then 'unresolved_inconsistent_amount'
            when cast(amount_max * 100 as decimal(38, 0)) <> amount_max * 100 then 'unresolved_fractional_cents'
            when selected_method = 'value_weight' and value_known_count < line_count then 'unresolved_missing_weight'
            when selected_method = 'value_weight' and value_negative_count > 0 then 'unresolved_negative_weight'
            when selected_method = 'quantity_weight' and quantity_known_count < line_count then 'unresolved_missing_weight'
            when selected_method = 'quantity_weight' and quantity_negative_count > 0 then 'unresolved_negative_weight'
        end as unresolved_reason,
        cast(amount_max * 100 as decimal(38, 0)) as total_cents
    from allocation_methods
),

-- Each input row carries its component's resolution and integer weight.
-- Weights are scaled to integers: value weights by 10^9 and quantities as-is.
-- Equal shares use weight 1. Unresolved components use weight 0 and stay NULL.
allocation_lines as (
    select
        line.shop_id,
        line.order_id,
        line.line_item_id,
        line.component,
        coalesce(resolution.unresolved_reason, resolution.selected_method) as allocation_method,
        resolution.unresolved_reason is null as is_resolved,
        resolution.total_cents < 0 as is_negative_total,
        abs(resolution.total_cents) as magnitude_cents,
        case
            when resolution.unresolved_reason is not null
                then cast(0 as decimal(38, 0))
            when resolution.selected_method = 'value_weight'
                then cast(line.value_weight * 1000000000 as decimal(38, 0))
            when resolution.selected_method = 'quantity_weight'
                then cast(line.quantity_weight as decimal(38, 0))
            else cast(1 as decimal(38, 0))
        end as weight_units
    from allocation_input as line
    inner join allocation_resolutions as resolution
        on line.shop_id is not distinct from resolution.shop_id
        and line.order_id is not distinct from resolution.order_id
        and line.component is not distinct from resolution.component
),

allocation_totals as (
    select
        *,
        sum(weight_units) over (partition by shop_id, order_id, component) as weight_total
    from allocation_lines
),

-- Numerator and denominator stay nonnegative. An unresolved component has
-- weight 0, so its numerator is 0; its denominator is raised to 1. A resolved
-- component has a weight total of at least 1.
allocation_shares as (
    select
        *,
        magnitude_cents * weight_units as share_numerator,
        greatest(weight_total, 1) as share_denominator
    from allocation_totals
),

allocation_floors as (
    select
        *,
        {{ shopify_allocation_quotient('share_numerator', 'share_denominator') }} as floor_cents,
        mod(share_numerator, share_denominator) as remainder_numerator
    from allocation_shares
),

allocation_remainders as (
    select
        *,
        sum(floor_cents) over (partition by shop_id, order_id, component) as floor_cents_total,
        row_number() over (
            partition by shop_id, order_id, component
            order by remainder_numerator desc, line_item_id asc
        ) as remainder_rank
    from allocation_floors
),

allocation_cents as (
    select
        shop_id,
        order_id,
        line_item_id,
        component,
        allocation_method,
        is_resolved,
        is_negative_total,
        magnitude_cents - floor_cents_total as leftover_cents,
        remainder_rank,
        floor_cents
    from allocation_remainders
),

allocation_output as (
    select
        shop_id,
        order_id,
        line_item_id,
        component,
        allocation_method,
        is_resolved,
        is_negative_total,
        floor_cents + case when remainder_rank <= leftover_cents then 1 else 0 end as magnitude_allocated_cents
    from allocation_cents
)

select
    shop_id,
    order_id,
    line_item_id,
    component,
    case
        when is_resolved then
            cast(
                (case when is_negative_total then -magnitude_allocated_cents else magnitude_allocated_cents end)
                * 0.01
                as decimal(38, 9)
            )
    end as allocated_shop_amount,
    allocation_method
from allocation_output
{% endmacro %}

{#- Exact floor of numerator / denominator for nonnegative whole numbers. -#}
{% macro shopify_allocation_quotient(numerator, denominator) %}
    {{ return(adapter.dispatch('shopify_allocation_quotient')(numerator, denominator)) }}
{% endmacro %}

{% macro duckdb__shopify_allocation_quotient(numerator, denominator) %}
    cast((cast({{ numerator }} as hugeint) // cast({{ denominator }} as hugeint)) as decimal(38, 0))
{% endmacro %}

{# DuckDB decimal / and // return DOUBLE, which drops digits. Integer // on HUGEINT is exact. #}

{% macro snowflake__shopify_allocation_quotient(numerator, denominator) %}
    cast(({{ numerator }} - mod({{ numerator }}, {{ denominator }})) / {{ denominator }} as number(38, 0))
{% endmacro %}

{#- Removing the remainder first makes the quotient an exact integer. A plain a / b can round up to the next integer when the true quotient is just below it. -#}

{% macro default__shopify_allocation_quotient(numerator, denominator) %}
    {{ exceptions.raise_compiler_error('shopify_allocation_quotient has no implementation for ' ~ adapter.type()) }}
{% endmacro %}
