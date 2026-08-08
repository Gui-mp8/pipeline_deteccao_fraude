create schema if not exists `{{ params.project_id }}.fraude_bronze`
options(location = 'southamerica-east1');

create or replace external table `{{ params.project_id }}.fraude_bronze.affiliate_cpa_ftd`
options (
  format = 'PARQUET',
  uris = ['gs://{{ params.landing_bucket }}/landing/affiliate_cpa_ftd/ingest_date=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://{{ params.landing_bucket }}/landing/affiliate_cpa_ftd',
  require_hive_partition_filter = false
);
