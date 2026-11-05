select cast(1 as bigint) as observation_id,
       cast({{ var('amount', 100) }} as decimal(18, 2)) as observed_amount,
       cast('USD' as varchar) as currency_code
