{{
  config(
    incremental_strategy='merge',
    unique_key='player_id',
    partition_by={
      "field": "created_at",
      "data_type": "date"
    },
    incremental_predicates=[
      "DBT_INTERNAL_DEST.created_at = DBT_INTERNAL_SOURCE.created_at"
    ]
  )
}}

WITH source_data AS (
  SELECT
    CAST(player_id AS STRING) AS player_id,
    LOWER(TRIM(CAST(email AS STRING))) AS email,
    REGEXP_EXTRACT(LOWER(TRIM(CAST(email AS STRING))), r'@(.+)$') AS email_domain,
    INITCAP(TRIM(CAST(city AS STRING))) AS city,
    CAST(created_at AS DATE) AS created_at,
    dt
  FROM {{ source('case_bronze', 'players') }}
  WHERE player_id IS NOT NULL
    {% if is_incremental() %}
      AND dt >= (
        SELECT COALESCE(MAX(created_at), DATE('1900-01-01'))
        FROM {{ this }}
      )
    {% else %}
      AND {{ bronze_partition_filter('dt') }}
    {% endif %}
),

deduplicated AS (
  SELECT * EXCEPT(dt)
  FROM source_data
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY player_id
    ORDER BY created_at DESC, dt DESC
  ) = 1
)

SELECT *
FROM deduplicated
