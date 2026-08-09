select
  cast(player_id as string) as player_id,
  cast(email as string) as email,
  cast(city as string) as city,
  safe_cast(created_at as date) as created_at
from {{ source('raw_fraud', 'players') }}
