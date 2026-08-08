select
  affiliate_id,
  country,
  count(distinct player_id) as attributed_players,
  sum(clicks) as clicks,
  sum(registrations) as registrations,
  sum(ftd) as ftd,
  sum(estimated_cpa_cost) as estimated_cpa_cost,
  safe_divide(sum(registrations), nullif(sum(clicks), 0)) as registration_rate,
  safe_divide(sum(ftd), nullif(sum(registrations), 0)) as ftd_rate,
  countif(has_registration_over_click_anomaly) as registration_over_click_rows,
  countif(has_ftd_over_registration_anomaly) as ftd_over_registration_rows,
  sum(case when has_registration_over_click_anomaly or has_ftd_over_registration_anomaly then 1 else 0 end) > 0 as has_funnel_anomaly
from {{ ref('slv_affiliate_cpa_ftd') }}
group by 1, 2
