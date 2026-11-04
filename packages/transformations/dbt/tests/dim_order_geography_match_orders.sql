{{ config(enabled=env_var('SHOPIFY_CUSTOMERS_ENABLED', 'false') == 'true') }}

-- Order geography covers exactly the orders in fct_orders, so the one-to-one
-- join in Cube can neither drop orders nor add rows.
with geography as (
    select shopify_shop_id, shopify_order_id from {{ ref('dim_order_geography') }}
),

orders as (
    select shopify_shop_id, shopify_order_id from {{ ref('fct_orders') }}
),

missing as (
    select * from orders
    except
    select * from geography
),

extra as (
    select * from geography
    except
    select * from orders
)

select 'missing' as problem, * from missing
union all
select 'extra' as problem, * from extra
