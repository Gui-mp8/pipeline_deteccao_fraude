{{ config(unique_key='player_id') }}

select
  player_id,
  lower(trim(email)) as email,
  regexp_extract(lower(trim(email)), r'@(.+)$') as email_domain,
  initcap(trim(city)) as city,
  created_at
from {{ ref('brz_players') }}
where player_id is not null

{% if is_incremental() %}
  and created_at >= (select coalesce(max(created_at), date('1900-01-01')) from {{ this }})
{% endif %}
