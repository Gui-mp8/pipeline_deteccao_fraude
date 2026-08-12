{{
  config(
    unique_key='transaction_id',
    partition_by={
      "field": "transaction_date",
      "data_type": "date"
    }
  )
}}

SELECT
  CAST(transaction_id AS STRING) AS transaction_id,
  CAST(player_id AS STRING) AS player_id,
  LOWER(CAST(type AS STRING)) AS transaction_type,
  CAST(amount AS NUMERIC) AS amount,
  CAST(timestamp AS TIMESTAMP) AS transaction_at,
  DATE(CAST(timestamp AS TIMESTAMP)) AS transaction_date,
  CASE
    WHEN LOWER(CAST(type AS STRING)) = 'deposit' THEN CAST(amount AS NUMERIC)
    ELSE 0
  END AS deposit_amount,
  CASE
    WHEN LOWER(CAST(type AS STRING)) = 'withdraw' THEN CAST(amount AS NUMERIC)
    ELSE 0
  END AS withdraw_amount,
  CASE
    WHEN LOWER(CAST(type AS STRING)) = 'bet' THEN CAST(amount AS NUMERIC)
    ELSE 0
  END AS bet_amount
FROM {{ source('raw_fraud', 'transactions') }}
WHERE transaction_id IS NOT NULL
  AND player_id IS NOT NULL
  AND timestamp IS NOT NULL
  AND LOWER(CAST(type AS STRING)) IN ('deposit', 'withdraw', 'bet')
  AND CAST(amount AS NUMERIC) >= 0
  AND {{ bronze_partition_filter('dt') }}

{% if is_incremental() %}
  AND CAST(timestamp AS TIMESTAMP) >= (
    SELECT COALESCE(MAX(transaction_at), TIMESTAMP('1900-01-01'))
    FROM {{ this }}
  )
{% endif %}
