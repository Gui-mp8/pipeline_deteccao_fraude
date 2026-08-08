{{ config(unique_key='transaction_id', partition_by={"field": "transaction_date", "data_type": "date"}) }}

select
  transaction_id,
  player_id,
  transaction_type,
  amount,
  transaction_at,
  date(transaction_at) as transaction_date,
  case when transaction_type = 'deposit' then amount else 0 end as deposit_amount,
  case when transaction_type = 'withdraw' then amount else 0 end as withdraw_amount,
  case when transaction_type = 'bet' then amount else 0 end as bet_amount
from {{ ref('brz_transactions') }}
where transaction_id is not null
  and player_id is not null
  and transaction_at is not null
  and transaction_type in ('deposit', 'withdraw', 'bet')
  and amount >= 0

{% if is_incremental() %}
  and transaction_at >= (select coalesce(max(transaction_at), timestamp('1900-01-01')) from {{ this }})
{% endif %}
