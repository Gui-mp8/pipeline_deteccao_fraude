SELECT
  affiliate_id,
  country,
  COUNT(DISTINCT player_id) AS attributed_players,
  SUM(clicks) AS clicks,
  SUM(registrations) AS registrations,
  SUM(ftd) AS ftd,
  SUM(estimated_cpa_cost) AS estimated_cpa_cost,
  SAFE_DIVIDE(SUM(registrations), NULLIF(SUM(clicks), 0)) AS registration_rate,
  SAFE_DIVIDE(SUM(ftd), NULLIF(SUM(registrations), 0)) AS ftd_rate,
  COUNTIF(has_registration_over_click_anomaly) AS registration_over_click_rows,
  COUNTIF(has_ftd_over_registration_anomaly) AS ftd_over_registration_rows,
  SUM(CASE WHEN has_registration_over_click_anomaly OR has_ftd_over_registration_anomaly THEN 1 ELSE 0 END) > 0 AS has_funnel_anomaly
FROM {{ ref('slv_affiliate_cpa_ftd') }}
GROUP BY 1, 2
