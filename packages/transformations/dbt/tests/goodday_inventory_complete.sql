{{ config(enabled=env_var('GOODDAY_INVENTORY_ENABLED', 'false') == 'true') }}

with captures as (
    select * from {{ ref('stg_goodday__inventory_captures') }}
),

latest as (
    select inventory_row_count
    from captures
    order by
        observed_at desc, captured_at desc,
        inventory_capture_id desc, inventory_load_id desc
    limit 1
),

item_counts as (
    select
        inventory_capture_id,
        inventory_load_id,
        count(*) as actual_row_count
    from {{ ref('stg_goodday__inventory_items') }}
    group by inventory_capture_id, inventory_load_id
)

select c.inventory_capture_id
from captures as c
left join item_counts as r
    on
        c.inventory_capture_id = r.inventory_capture_id
        and c.inventory_load_id = r.inventory_load_id
where
    c.inventory_row_count is null or c.inventory_row_count < 0
    or c.inventory_row_count != coalesce(r.actual_row_count, 0)
    or c.observed_at is null or c.captured_at is null
    or c.source_relation is null
union all
select 'missing_successful_capture'
where not exists (select 1 from captures)
union all
select 'published_count_mismatch'
from latest
where inventory_row_count != (
    select count(*) from {{ ref('fct_inventory__current') }}
)
