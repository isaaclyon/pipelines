{{ config(severity='error') }}
with source_total as (
    select sum(observed_amount) as amount from {{ ref('stg_fixture__observations') }}
), mart_total as (
    select sum(observed_amount) as amount from {{ ref('fct_observations') }}
)
select source_total.amount
from source_total cross join mart_total
where source_total.amount <> mart_total.amount or mart_total.amount < 0
