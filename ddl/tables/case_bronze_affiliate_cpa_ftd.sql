create or replace external table `case-grupo-otg1.case_bronze.affiliate_cpa_ftd`
options (
  format = 'PARQUET',
  uris = ['gs://case-grupo-otg1/staging/affiliate_cpa_ftd/ingest_date=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://case-grupo-otg1/staging/affiliate_cpa_ftd',
  require_hive_partition_filter = false
);
