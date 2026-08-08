create schema if not exists `{{ params.project_id }}.fraude_bronze`
options(location = 'southamerica-east1');

create or replace external table `{{ params.project_id }}.fraude_bronze.sessions`
options (
  format = 'PARQUET',
  uris = ['gs://{{ params.landing_bucket }}/landing/sessions/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://{{ params.landing_bucket }}/landing/sessions',
  require_hive_partition_filter = false
);
