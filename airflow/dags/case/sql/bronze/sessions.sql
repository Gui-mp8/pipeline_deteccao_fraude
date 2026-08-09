create schema if not exists `{{ params.project_id }}.{{ params.bronze_dataset }}`
options(location = '{{ params.bigquery_location }}');

create or replace external table `{{ params.project_id }}.{{ params.bronze_dataset }}.sessions`
options (
  format = 'PARQUET',
  uris = ['gs://{{ params.gcs_bucket }}/staging/sessions/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://{{ params.gcs_bucket }}/staging/sessions',
  require_hive_partition_filter = false
);
