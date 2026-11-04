{{ config(enabled=env_var('GOODDAY_INVENTORY_ENABLED', 'false') == 'true') }}

select
    goodday_merchant_id,
    goodday_item_id,
    goodday_channel_id,
    goodday_location_id
from {{ ref('fct_inventory__current') }}
group by
    goodday_merchant_id,
    goodday_item_id,
    goodday_channel_id,
    goodday_location_id
having count(*) > 1 or goodday_merchant_id is null or goodday_item_id is null
