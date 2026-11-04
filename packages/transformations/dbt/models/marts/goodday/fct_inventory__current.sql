with ranked_captures as (
    select
        *,
        row_number() over (
            order by
                observed_at desc, captured_at desc,
                inventory_capture_id desc, inventory_load_id desc
        ) as capture_rank
    from {{ ref('stg_goodday__inventory_captures') }}
)

select
    i.inventory_capture_id,
    i.inventory_load_id,
    i.inventory_row_number,
    i.merchant_id as goodday_merchant_id,
    i.item_id as goodday_item_id,
    i.channel_id as goodday_channel_id,
    i.location_id as goodday_location_id,
    i.parent_item_id as goodday_parent_item_id,
    i.merchant_name,
    i.item_name,
    i.item_status,
    i.sku,
    i.channel_name,
    i.location_name,
    i.on_hand_total_quantity,
    i.on_hand_sellable_quantity,
    i.on_hand_unsellable_quantity,
    i.on_hand_reserved_quantity,
    i.on_hand_committed_quantity,
    i.on_hand_available_quantity,
    i.available_quantity,
    i.ats_quantity,
    i.inbound_total_quantity,
    i.inbound_available_quantity,
    i.fulfilled_total_quantity,
    i.transfer_in_transit_quantity,
    i.return_total_quantity,
    i.source_updated_at,
    c.captured_at,
    c.observed_at,
    c.source_relation
from {{ ref('stg_goodday__inventory_items') }} as i
inner join ranked_captures as c
    on
        i.inventory_capture_id = c.inventory_capture_id
        and i.inventory_load_id = c.inventory_load_id
where c.capture_rank = 1
