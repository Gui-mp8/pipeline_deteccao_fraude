WITH tx AS (
  SELECT * FROM {{ ref('slv_transactions') }}
),

agg AS (
  SELECT
    player_id,
    COUNT(*) AS transaction_count,
    COUNTIF(transaction_type = 'deposit') AS deposit_count,
    COUNTIF(transaction_type = 'withdraw') AS withdraw_count,
    COUNTIF(transaction_type = 'bet') AS bet_count,
    SUM(deposit_amount) AS total_deposit_amount,
    SUM(withdraw_amount) AS total_withdraw_amount,
    SUM(bet_amount) AS total_bet_amount,
    MAX(transaction_at) AS last_transaction_at
  FROM tx
  GROUP BY 1
)

SELECT
  player_id,
  transaction_count,
  deposit_count,
  withdraw_count,
  bet_count,
  total_deposit_amount,
  total_withdraw_amount,
  total_bet_amount,
  SAFE_DIVIDE(total_withdraw_amount, NULLIF(total_deposit_amount, 0)) AS withdraw_to_deposit_ratio,
  SAFE_DIVIDE(total_bet_amount, NULLIF(total_deposit_amount, 0)) AS bet_to_deposit_ratio,
  total_withdraw_amount > total_deposit_amount * 1.5 AND total_withdraw_amount >= 500 AS has_high_withdraw_signal,
  total_bet_amount >= total_deposit_amount * 5 AND total_bet_amount >= 1000 AS has_high_bet_velocity_signal,
  last_transaction_at
FROM agg
