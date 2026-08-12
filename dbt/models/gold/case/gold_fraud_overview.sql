WITH players AS (
  SELECT * FROM {{ ref('slv_players') }}
),

session_features AS (
  SELECT
    player_id,
    COUNT(*) AS session_count,
    COUNT(DISTINCT ip) AS distinct_ip_count,
    COUNT(DISTINCT device) AS distinct_device_count,
    MAX(session_at) AS last_session_at
  FROM {{ ref('slv_sessions') }}
  GROUP BY 1
),

ip_risk AS (
  SELECT
    s.player_id,
    MAX(ip_players.player_count) AS max_players_on_same_ip
  FROM {{ ref('slv_sessions') }} s
  JOIN (
    SELECT ip, COUNT(DISTINCT player_id) AS player_count
    FROM {{ ref('slv_sessions') }}
    GROUP BY 1
  ) ip_players USING (ip)
  GROUP BY 1
),

affiliate_risk AS (
  SELECT
    player_id,
    MAX(CAST(has_registration_over_click_anomaly OR has_ftd_over_registration_anomaly AS INT64)) = 1 AS has_affiliate_funnel_anomaly
  FROM {{ ref('slv_affiliate_cpa_ftd') }}
  GROUP BY 1
),

financial AS (
  SELECT * FROM {{ ref('gold_financial_signals') }}
)

SELECT
  p.player_id,
  p.email_domain,
  p.city,
  p.created_at,
  COALESCE(sf.session_count, 0) AS session_count,
  COALESCE(sf.distinct_ip_count, 0) AS distinct_ip_count,
  COALESCE(sf.distinct_device_count, 0) AS distinct_device_count,
  COALESCE(ir.max_players_on_same_ip, 0) AS max_players_on_same_ip,
  COALESCE(f.transaction_count, 0) AS transaction_count,
  COALESCE(f.total_deposit_amount, 0) AS total_deposit_amount,
  COALESCE(f.total_withdraw_amount, 0) AS total_withdraw_amount,
  COALESCE(f.total_bet_amount, 0) AS total_bet_amount,
  COALESCE(f.withdraw_to_deposit_ratio, 0) AS withdraw_to_deposit_ratio,
  COALESCE(f.bet_to_deposit_ratio, 0) AS bet_to_deposit_ratio,
  COALESCE(ir.max_players_on_same_ip, 0) >= 5 AS has_shared_ip_signal,
  COALESCE(sf.distinct_device_count, 0) >= 4 AS has_many_devices_signal,
  COALESCE(f.has_high_withdraw_signal, FALSE) AS has_high_withdraw_signal,
  COALESCE(f.has_high_bet_velocity_signal, FALSE) AS has_high_bet_velocity_signal,
  COALESCE(ar.has_affiliate_funnel_anomaly, FALSE) AS has_affiliate_funnel_anomaly,
  (
    CAST(COALESCE(ir.max_players_on_same_ip, 0) >= 5 AS INT64)
    + CAST(COALESCE(sf.distinct_device_count, 0) >= 4 AS INT64)
    + CAST(COALESCE(f.has_high_withdraw_signal, FALSE) AS INT64)
    + CAST(COALESCE(f.has_high_bet_velocity_signal, FALSE) AS INT64)
    + CAST(COALESCE(ar.has_affiliate_funnel_anomaly, FALSE) AS INT64)
  ) AS fraud_signal_count,
  sf.last_session_at,
  f.last_transaction_at
FROM players p
LEFT JOIN session_features sf USING (player_id)
LEFT JOIN ip_risk ir USING (player_id)
LEFT JOIN financial f USING (player_id)
LEFT JOIN affiliate_risk ar USING (player_id)
