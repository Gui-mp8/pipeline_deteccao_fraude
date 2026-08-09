select *
from {{ ref('slv_transactions') }}
where amount < 0
