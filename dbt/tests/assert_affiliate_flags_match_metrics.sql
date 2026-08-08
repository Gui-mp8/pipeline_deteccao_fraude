select *
from {{ ref('slv_affiliate_cpa_ftd') }}
where has_registration_over_click_anomaly != (registrations > clicks)
   or has_ftd_over_registration_anomaly != (ftd > registrations)
