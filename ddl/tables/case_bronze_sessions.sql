create or replace external table `case-grupo-otg1.case_bronze.sessions`
options (
  format = 'PARQUET',
  uris = ['gs://case-grupo-otg1/staging/sessions/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://case-grupo-otg1/staging/sessions',
  require_hive_partition_filter = false
);
