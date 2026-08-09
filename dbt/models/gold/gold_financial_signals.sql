with tx as (
  select * from {{ ref('slv_transactions') }}
),

agg as (
  select
    player_id,
    count(*) as transaction_count,
    countif(transaction_type = 'deposit') as deposit_count,
    countif(transaction_type = 'withdraw') as withdraw_count,
    countif(transaction_type = 'bet') as bet_count,
    sum(deposit_amount) as total_deposit_amount,
    sum(withdraw_amount) as total_withdraw_amount,
    sum(bet_amount) as total_bet_amount,
    max(transaction_at) as last_transaction_at
  from tx
  group by 1
)

select
  player_id,
  transaction_count,
  deposit_count,
  withdraw_count,
  bet_count,
  total_deposit_amount,
  total_withdraw_amount,
  total_bet_amount,
  safe_divide(total_withdraw_amount, nullif(total_deposit_amount, 0)) as withdraw_to_deposit_ratio,
  safe_divide(total_bet_amount, nullif(total_deposit_amount, 0)) as bet_to_deposit_ratio,
  total_withdraw_amount > total_deposit_amount * 1.5 and total_withdraw_amount >= 500 as has_high_withdraw_signal,
  total_bet_amount >= total_deposit_amount * 5 and total_bet_amount >= 1000 as has_high_bet_velocity_signal,
  last_transaction_at
from agg
