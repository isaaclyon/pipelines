{{ config(enabled=env_var('GOODDAY_INVENTORY_ENABLED', 'false') == 'true') }}

with latest as (
    select max(observed_at) as observed_at
    from {{ ref('stg_goodday__inventory_captures') }}
),

ranked as (
    select
        c.*,
        row_number() over (
            order by
                c.captured_at desc, c.inventory_capture_id desc,
                c.inventory_load_id desc
        ) as tie_rank
    from {{ ref('stg_goodday__inventory_captures') }} as c
    inner join latest as l on c.observed_at = l.observed_at
),

expected as (
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
        c.source_relation,
        c.captured_at,
        c.observed_at
    from {{ ref('stg_goodday__inventory_items') }} as i
    inner join ranked as c
        on
            i.inventory_capture_id = c.inventory_capture_id
            and i.inventory_load_id = c.inventory_load_id
    where c.tie_rank = 1
),

actual as (
    select
        inventory_capture_id,
        inventory_load_id,
        inventory_row_number,
        goodday_merchant_id,
        goodday_item_id,
        goodday_channel_id,
        goodday_location_id,
        goodday_parent_item_id,
        merchant_name,
        item_name,
        item_status,
        sku,
        channel_name,
        location_name,
        on_hand_total_quantity,
        on_hand_sellable_quantity,
        on_hand_unsellable_quantity,
        on_hand_reserved_quantity,
        on_hand_committed_quantity,
        on_hand_available_quantity,
        available_quantity,
        ats_quantity,
        inbound_total_quantity,
        inbound_available_quantity,
        fulfilled_total_quantity,
        transfer_in_transit_quantity,
        return_total_quantity,
        source_updated_at,
        source_relation,
        captured_at,
        observed_at
    from {{ ref('fct_inventory__current') }}
),

missing as (
    select * from expected
    except
    select * from actual
),

unexpected as (
    select * from actual
    except
    select * from expected
)

select inventory_capture_id from missing
union all
select inventory_capture_id from unexpected
