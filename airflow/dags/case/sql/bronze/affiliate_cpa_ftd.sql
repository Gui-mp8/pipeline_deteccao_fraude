create schema if not exists `{{ params.project_id }}.{{ params.bronze_dataset }}`
options(location = '{{ params.bigquery_location }}');

create or replace external table `{{ params.project_id }}.{{ params.bronze_dataset }}.affiliate_cpa_ftd`
options (
  format = 'PARQUET',
  uris = ['gs://{{ params.gcs_bucket }}/staging/affiliate_cpa_ftd/ingest_date=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://{{ params.gcs_bucket }}/staging/affiliate_cpa_ftd',
  require_hive_partition_filter = false
);
