select
  cast(transaction_id as string) as transaction_id,
  cast(player_id as string) as player_id,
  lower(cast(type as string)) as transaction_type,
  safe_cast(amount as numeric) as amount,
  safe_cast(timestamp as timestamp) as transaction_at
from {{ source('raw_fraud', 'transactions') }}
where {{ bronze_partition_filter('dt') }}
