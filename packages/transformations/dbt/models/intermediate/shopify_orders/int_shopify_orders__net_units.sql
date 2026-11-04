select
    shop_id,
    order_id,
    cast(case
        when count(*) = count(net_ordered_quantity)
            then sum(net_ordered_quantity)
    end as bigint) as net_ordered_quantity,
    cast(
        sum(case when net_ordered_quantity is null then 1 else 0 end) as bigint
    ) as unresolved_quantity_line_count
from {{ ref('int_shopify_order_lines__net_units') }}
group by shop_id, order_id
