with players as (
  select * from {{ ref('slv_players') }}
),

session_features as (
  select
    player_id,
    count(*) as session_count,
    count(distinct ip) as distinct_ip_count,
    count(distinct device) as distinct_device_count,
    max(session_at) as last_session_at
  from {{ ref('slv_sessions') }}
  group by 1
),

ip_risk as (
  select
    s.player_id,
    max(ip_players.player_count) as max_players_on_same_ip
  from {{ ref('slv_sessions') }} s
  join (
    select ip, count(distinct player_id) as player_count
    from {{ ref('slv_sessions') }}
    group by 1
  ) ip_players using (ip)
  group by 1
),

affiliate_risk as (
  select
    player_id,
    max(cast(has_registration_over_click_anomaly or has_ftd_over_registration_anomaly as int64)) = 1 as has_affiliate_funnel_anomaly
  from {{ ref('slv_affiliate_cpa_ftd') }}
  group by 1
),

financial as (
  select * from {{ ref('gold_financial_signals') }}
)

select
  p.player_id,
  p.email_domain,
  p.city,
  p.created_at,
  coalesce(sf.session_count, 0) as session_count,
  coalesce(sf.distinct_ip_count, 0) as distinct_ip_count,
  coalesce(sf.distinct_device_count, 0) as distinct_device_count,
  coalesce(ir.max_players_on_same_ip, 0) as max_players_on_same_ip,
  coalesce(f.transaction_count, 0) as transaction_count,
  coalesce(f.total_deposit_amount, 0) as total_deposit_amount,
  coalesce(f.total_withdraw_amount, 0) as total_withdraw_amount,
  coalesce(f.total_bet_amount, 0) as total_bet_amount,
  coalesce(f.withdraw_to_deposit_ratio, 0) as withdraw_to_deposit_ratio,
  coalesce(f.bet_to_deposit_ratio, 0) as bet_to_deposit_ratio,
  coalesce(ir.max_players_on_same_ip, 0) >= 5 as has_shared_ip_signal,
  coalesce(sf.distinct_device_count, 0) >= 4 as has_many_devices_signal,
  coalesce(f.has_high_withdraw_signal, false) as has_high_withdraw_signal,
  coalesce(f.has_high_bet_velocity_signal, false) as has_high_bet_velocity_signal,
  coalesce(ar.has_affiliate_funnel_anomaly, false) as has_affiliate_funnel_anomaly,
  (
    cast(coalesce(ir.max_players_on_same_ip, 0) >= 5 as int64)
    + cast(coalesce(sf.distinct_device_count, 0) >= 4 as int64)
    + cast(coalesce(f.has_high_withdraw_signal, false) as int64)
    + cast(coalesce(f.has_high_bet_velocity_signal, false) as int64)
    + cast(coalesce(ar.has_affiliate_funnel_anomaly, false) as int64)
  ) as fraud_signal_count,
  sf.last_session_at,
  f.last_transaction_at
from players p
left join session_features sf using (player_id)
left join ip_risk ir using (player_id)
left join financial f using (player_id)
left join affiliate_risk ar using (player_id)
