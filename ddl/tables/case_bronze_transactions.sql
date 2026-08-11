create or replace external table `case-grupo-otg1.case_bronze.transactions`
options (
  format = 'PARQUET',
  uris = ['gs://case-grupo-otg1/staging/transactions/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://case-grupo-otg1/staging/transactions',
  require_hive_partition_filter = false
);
