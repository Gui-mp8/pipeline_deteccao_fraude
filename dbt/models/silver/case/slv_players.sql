{{
  config(
    unique_key='player_id'
  )
}}

SELECT
  CAST(player_id AS STRING) AS player_id,
  LOWER(TRIM(CAST(email AS STRING))) AS email,
  REGEXP_EXTRACT(LOWER(TRIM(CAST(email AS STRING))), r'@(.+)$') AS email_domain,
  INITCAP(TRIM(CAST(city AS STRING))) AS city,
  CAST(created_at AS DATE) AS created_at
FROM {{ source('raw_fraud', 'players') }}
WHERE player_id IS NOT NULL
  AND {{ bronze_partition_filter('dt') }}

{% if is_incremental() %}
  AND CAST(created_at AS DATE) >= (
    SELECT COALESCE(MAX(created_at), DATE('1900-01-01'))
    FROM {{ this }}
  )
{% endif %}
