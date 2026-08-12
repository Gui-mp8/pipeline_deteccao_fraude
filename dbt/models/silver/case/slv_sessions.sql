{{
  config(
    incremental_strategy='merge',
    unique_key='session_id',
    partition_by={
      "field": "session_date",
      "data_type": "date"
    },
    incremental_predicates=[
      "DBT_INTERNAL_DEST.session_date = DBT_INTERNAL_SOURCE.session_date"
    ]
  )
}}

WITH source_data AS (
  SELECT
    CAST(session_id AS STRING) AS session_id,
    CAST(player_id AS STRING) AS player_id,
    CAST(ip AS STRING) AS ip,
    LOWER(TRIM(CAST(device AS STRING))) AS device,
    CAST(timestamp AS TIMESTAMP) AS session_at,
    DATE(CAST(timestamp AS TIMESTAMP)) AS session_date,
    dt
  FROM {{ source('case_bronze', 'sessions') }}
  WHERE session_id IS NOT NULL
    AND player_id IS NOT NULL
    AND timestamp IS NOT NULL
    AND {{ bronze_partition_filter('dt') }}
    {% if is_incremental() %}
      AND CAST(timestamp AS TIMESTAMP) >= (
        SELECT COALESCE(MAX(session_at), TIMESTAMP('1900-01-01'))
        FROM {{ this }}
      )
    {% endif %}
),

deduplicated AS (
  SELECT * EXCEPT(dt)
  FROM source_data
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY session_id
    ORDER BY session_at DESC, dt DESC
  ) = 1
)

SELECT *
FROM deduplicated
