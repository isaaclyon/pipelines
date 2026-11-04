{{ config(meta={'dagster': {'ref': {'name': 'dim_order_contacts'}}}) }}

-- Order contacts cover exactly the orders in fct_orders, so the one-to-one join
-- in Cube can neither drop orders nor add rows.
with contacts as (
    select shopify_shop_id, shopify_order_id from {{ ref('dim_order_contacts') }}
),

orders as (
    select shopify_shop_id, shopify_order_id from {{ ref('fct_orders') }}
),

missing as (
    select * from orders
    except
    select * from contacts
),

extra as (
    select * from contacts
    except
    select * from orders
)

select 'missing' as problem, * from missing
union all
select 'extra' as problem, * from extra
