select observation_id, observed_amount, currency_code
from {{ ref('stg_fixture__observations') }}
