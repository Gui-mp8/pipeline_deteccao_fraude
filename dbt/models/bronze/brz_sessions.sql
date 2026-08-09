select
  cast(session_id as string) as session_id,
  cast(player_id as string) as player_id,
  cast(ip as string) as ip,
  cast(device as string) as device,
  safe_cast(timestamp as timestamp) as session_at
from {{ source('raw_fraud', 'sessions') }}
