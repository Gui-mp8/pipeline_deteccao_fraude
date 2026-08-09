create schema if not exists `{{ params.project_id }}.fraude_bronze`
options(location = 'southamerica-east1');

create or replace external table `{{ params.project_id }}.fraude_bronze.transactions`
options (
  format = 'PARQUET',
  uris = ['gs://{{ params.gcs_bucket }}/staging/transactions/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://{{ params.gcs_bucket }}/staging/transactions',
  require_hive_partition_filter = false
);
