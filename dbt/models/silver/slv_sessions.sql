{{ config(unique_key='session_id', partition_by={"field": "session_date", "data_type": "date"}) }}

select
  session_id,
  player_id,
  ip,
  lower(trim(device)) as device,
  session_at,
  date(session_at) as session_date
from {{ ref('brz_sessions') }}
where session_id is not null
  and player_id is not null
  and session_at is not null

{% if is_incremental() %}
  and session_at >= (select coalesce(max(session_at), timestamp('1900-01-01')) from {{ this }})
{% endif %}
