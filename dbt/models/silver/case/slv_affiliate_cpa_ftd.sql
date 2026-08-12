{{
  config(
    materialized='table'
  )
}}

SELECT
  {{ dbt_utils.generate_surrogate_key(['affiliate_id', 'player_id', 'country']) }} AS affiliate_player_country_key,
  CAST(affiliate_id AS STRING) AS affiliate_id,
  CAST(player_id AS STRING) AS player_id,
  UPPER(CAST(country AS STRING)) AS country,
  SUM(CAST(clicks AS INT64)) AS clicks,
  SUM(CAST(registrations AS INT64)) AS registrations,
  SUM(CAST(ftd AS INT64)) AS ftd,
  MAX(CAST(cpa_value AS NUMERIC)) AS cpa_value,
  SUM(CAST(ftd AS INT64) * CAST(cpa_value AS NUMERIC)) AS estimated_cpa_cost,
  SAFE_DIVIDE(
    SUM(CAST(registrations AS INT64)),
    NULLIF(SUM(CAST(clicks AS INT64)), 0)
  ) AS registration_rate,
  SAFE_DIVIDE(
    SUM(CAST(ftd AS INT64)),
    NULLIF(SUM(CAST(registrations AS INT64)), 0)
  ) AS ftd_rate,
  SUM(CAST(registrations AS INT64)) > SUM(CAST(clicks AS INT64)) AS has_registration_over_click_anomaly,
  SUM(CAST(ftd AS INT64)) > SUM(CAST(registrations AS INT64)) AS has_ftd_over_registration_anomaly
FROM {{ source('case_bronze', 'affiliate_cpa_ftd') }}
WHERE affiliate_id IS NOT NULL
  AND player_id IS NOT NULL
  AND country IS NOT NULL
  AND {{ bronze_partition_filter('ingest_date') }}
GROUP BY 1, 2, 3, 4
