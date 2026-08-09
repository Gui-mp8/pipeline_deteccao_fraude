select *
from {{ ref('gold_financial_signals') }}
where transaction_count < 0
   or deposit_count < 0
   or withdraw_count < 0
   or bet_count < 0
   or total_deposit_amount < 0
   or total_withdraw_amount < 0
   or total_bet_amount < 0
   or withdraw_to_deposit_ratio < 0
   or bet_to_deposit_ratio < 0
