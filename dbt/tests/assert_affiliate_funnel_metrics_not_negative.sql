select *
from {{ ref('slv_affiliate_cpa_ftd') }}
where clicks < 0
   or registrations < 0
   or ftd < 0
   or cpa_value < 0
