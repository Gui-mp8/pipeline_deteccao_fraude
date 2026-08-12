{{
  config(
    incremental_strategy='merge',
    unique_key='transaction_id',
    partition_by={
      "field": "transaction_date",
      "data_type": "date"
    },
    incremental_predicates=[
      "DBT_INTERNAL_DEST.transaction_date = DBT_INTERNAL_SOURCE.transaction_date"
    ]
  )
}}

WITH source_data AS (
  SELECT
    CAST(transaction_id AS STRING) AS transaction_id,
    CAST(player_id AS STRING) AS player_id,
    LOWER(CAST(type AS STRING)) AS transaction_type,
    CAST(amount AS NUMERIC) AS amount,
    CAST(timestamp AS TIMESTAMP) AS transaction_at,
    DATE(CAST(timestamp AS TIMESTAMP)) AS transaction_date,
    dt
  FROM {{ source('case_bronze', 'transactions') }}
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
),

deduplicated AS (
  SELECT
    transaction_id,
    player_id,
    transaction_type,
    amount,
    transaction_at,
    transaction_date,
    CASE
      WHEN transaction_type = 'deposit' THEN amount
      ELSE 0
    END AS deposit_amount,
    CASE
      WHEN transaction_type = 'withdraw' THEN amount
      ELSE 0
    END AS withdraw_amount,
    CASE
      WHEN transaction_type = 'bet' THEN amount
      ELSE 0
    END AS bet_amount
  FROM source_data
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY transaction_id
    ORDER BY transaction_at DESC, dt DESC
  ) = 1
)

SELECT *
FROM deduplicated
