{{ config(meta={'dagster': {'ref': {'name': 'dim_customers'}}}) }}

{{ config(enabled=env_var('SHOPIFY_CUSTOMERS_ENABLED', 'false') == 'true') }}

with source as (
    select shop_id, customer_id from {{ ref('stg_shopify_customers__customers') }}
),

analytical as (
    select shopify_shop_id as shop_id, shopify_customer_id as customer_id
    from {{ ref('dim_customers') }}
),

restricted as (
    select shopify_shop_id as shop_id, shopify_customer_id as customer_id
    from {{ ref('dim_customer_details') }}
)

select * from (select * from source except select * from analytical) as missing_analytical
union all
select * from (select * from analytical except select * from source) as extra_analytical
union all
select * from (select * from source except select * from restricted) as missing_restricted
union all
select * from (select * from restricted except select * from source) as extra_restricted
