-- Snapshot evidence is independent of return rows, so an empty load is visible.
{% if env_var('LOOP_RETURNS_ENABLED', 'false') == 'true' %}
with headers as (
    select
        shop_id,
        count(*) as current_return_count
    from {{ source('loop_return_headers', 'returns') }}
    group by shop_id
),

load_rows as (
    select
        shop_id,
        _dlt_load_id as load_id,
        count(*) as loaded_return_count
    from {{ source('loop_return_headers', 'returns') }}
    group by shop_id, _dlt_load_id
)

select
    snapshots.shop_id,
    snapshots.snapshot_id,
    snapshots._dlt_load_id as snapshot_load_id,
    {{ utc_timestamp('snapshots.snapshot_end') }} as snapshot_ended_at,
    {{ utc_timestamp('snapshots.completed_at') }} as extraction_completed_at,
    cast(snapshots.row_count as bigint) as expected_return_count,
    cast(coalesce(headers.current_return_count, 0) as bigint)
        as current_return_count,
    coalesce(
        loads.status = 0
        and snapshots.row_count >= 0
        and snapshots.snapshot_end <= snapshots.completed_at
        and snapshots.row_count = coalesce(headers.current_return_count, 0)
        and snapshots.row_count = coalesce(load_rows.loaded_return_count, 0),
        false
    ) as is_snapshot_complete
from {{ source('loop_return_headers', 'return_snapshots') }} as snapshots
left join {{ source('loop_return_headers', 'loads') }} as loads
    on snapshots._dlt_load_id = loads.load_id
left join headers on snapshots.shop_id = headers.shop_id
left join load_rows
    on
        snapshots.shop_id = load_rows.shop_id
        and snapshots._dlt_load_id = load_rows.load_id
{% else %}
    select
        cast(null as varchar) as shop_id,
        cast(null as varchar) as snapshot_id,
        cast(null as varchar) as snapshot_load_id,
        cast(null as timestamp) as snapshot_ended_at,
        cast(null as timestamp) as extraction_completed_at,
        cast(null as bigint) as expected_return_count,
        cast(null as bigint) as current_return_count,
        cast(false as boolean) as is_snapshot_complete
    where false
{% endif %}
