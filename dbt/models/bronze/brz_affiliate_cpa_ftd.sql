select
  cast(affiliate_id as string) as affiliate_id,
  cast(player_id as string) as player_id,
  upper(cast(country as string)) as country,
  safe_cast(clicks as int64) as clicks,
  safe_cast(registrations as int64) as registrations,
  safe_cast(ftd as int64) as ftd,
  safe_cast(cpa_value as numeric) as cpa_value
from {{ source('raw_fraud', 'affiliate_cpa_ftd') }}
