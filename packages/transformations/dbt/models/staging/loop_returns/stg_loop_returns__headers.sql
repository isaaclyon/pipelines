-- Enabled when client.yaml lists loop_returns.
-- No legacy table fallback is allowed. A missing feed is not a zero fee.
with headers as (
    select * from {{ source('loop_return_headers', 'returns') }}
),

snapshots as (
    select * from {{ source('loop_return_headers', 'return_snapshots') }}
),

loads as (
    select * from {{ source('loop_return_headers', 'loads') }}
)

select
    headers.shop_id,
    headers.return_id,
    'gid://shopify/Order/' || headers.shopify_order_id as order_id,
    headers.return_state,
    {{ utc_timestamp('headers.created_at') }} as created_at,
    {{ utc_timestamp('headers.updated_at') }} as updated_at,
    headers.currency as currency_code,
    headers.return_total as return_total_amount,
    headers.refund as refund_amount,
    headers.handling_fee as handling_fee_amount,
    headers.gift_card as gift_card_amount,
    headers.exchange_total as exchange_total_amount,
    headers.upsell as upsell_amount,
    snapshots.snapshot_id,
    {{ utc_timestamp('snapshots.snapshot_end') }} as snapshot_ended_at,
    cast(
        snapshots.snapshot_id is not null
        and coalesce(loads.status = 0, false) as boolean
    ) as is_snapshot_complete
from headers
left join snapshots
    on
        headers._dlt_load_id = snapshots._dlt_load_id
        and headers.shop_id = snapshots.shop_id
left join loads on headers._dlt_load_id = loads.load_id
