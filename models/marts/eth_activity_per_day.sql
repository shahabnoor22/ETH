select
date,
transaction_category,
count(*) as tx_count,
sum(value)/1e18 as sum_eth_value

from {{ ref('stg_transactions_enriched') }}

group by 
date,
transaction_category