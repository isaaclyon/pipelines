with customers as (
    select * from {{ source('shop', 'customers') }}
),

countries as (
    select * from {{ ref('countries') }}
)

select customers.customer_id, countries.country_name
from customers
left join countries on customers.country_code = countries.country_code
