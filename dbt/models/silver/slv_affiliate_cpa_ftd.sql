{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key(['affiliate_id', 'player_id', 'country']) }} as affiliate_player_country_key,
  affiliate_id,
  player_id,
  country,
  sum(clicks) as clicks,
  sum(registrations) as registrations,
  sum(ftd) as ftd,
  max(cpa_value) as cpa_value,
  sum(ftd * cpa_value) as estimated_cpa_cost,
  safe_divide(sum(registrations), nullif(sum(clicks), 0)) as registration_rate,
  safe_divide(sum(ftd), nullif(sum(registrations), 0)) as ftd_rate,
  sum(registrations) > sum(clicks) as has_registration_over_click_anomaly,
  sum(ftd) > sum(registrations) as has_ftd_over_registration_anomaly
from {{ ref('brz_affiliate_cpa_ftd') }}
where affiliate_id is not null
  and player_id is not null
  and country is not null
group by 1, 2, 3, 4
