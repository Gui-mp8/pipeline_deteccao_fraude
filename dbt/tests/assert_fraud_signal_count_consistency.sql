select *
from {{ ref('gold_fraud_overview') }}
where fraud_signal_count not between 0 and 5
   or fraud_signal_count != (
      cast(has_shared_ip_signal as int64)
      + cast(has_many_devices_signal as int64)
      + cast(has_high_withdraw_signal as int64)
      + cast(has_high_bet_velocity_signal as int64)
      + cast(has_affiliate_funnel_anomaly as int64)
   )
