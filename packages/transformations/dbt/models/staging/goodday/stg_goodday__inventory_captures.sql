select
    c.capture_id as inventory_capture_id,
    c._dlt_load_id as inventory_load_id,
    c.source_relation,
    cast(c.row_count as bigint) as inventory_row_count,
    {{ utc_timestamp('c.captured_at') }} as captured_at,
    {{ utc_timestamp('c.observed_at') }} as observed_at
from {{ source('goodday', 'inventory_captures') }} as c
inner join {{ source('goodday', '_dlt_loads') }} as l
    on c._dlt_load_id = l.load_id
where l.status = 0
