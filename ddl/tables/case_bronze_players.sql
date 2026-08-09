create or replace external table `case-grupo-otg1.case_bronze.players`
options (
  format = 'PARQUET',
  uris = ['gs://case-grupo-otg1/staging/players/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://case-grupo-otg1/staging/players',
  require_hive_partition_filter = false
);
