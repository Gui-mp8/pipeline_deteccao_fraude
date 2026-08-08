-- Template standalone para criacao da Bronze.
-- Renderize antes de executar, por exemplo:
-- envsubst < ddl/bigquery/bronze_external_tables.sql | bq query --use_legacy_sql=false
--
-- Variaveis esperadas:
--   GCP_PROJECT_ID
--   LANDING_BUCKET

declare landing_bucket string default '${LANDING_BUCKET}';

execute immediate format("""
create schema if not exists `%s.fraude_bronze`
options(location = 'southamerica-east1')
""", '${GCP_PROJECT_ID}');

execute immediate format("""
create or replace external table `%s.fraude_bronze.players`
options (
  format = 'PARQUET',
  uris = ['gs://%s/landing/players/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://%s/landing/players',
  require_hive_partition_filter = false
)
""", '${GCP_PROJECT_ID}', landing_bucket, landing_bucket);

execute immediate format("""
create or replace external table `%s.fraude_bronze.sessions`
options (
  format = 'PARQUET',
  uris = ['gs://%s/landing/sessions/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://%s/landing/sessions',
  require_hive_partition_filter = false
)
""", '${GCP_PROJECT_ID}', landing_bucket, landing_bucket);

execute immediate format("""
create or replace external table `%s.fraude_bronze.transactions`
options (
  format = 'PARQUET',
  uris = ['gs://%s/landing/transactions/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://%s/landing/transactions',
  require_hive_partition_filter = false
)
""", '${GCP_PROJECT_ID}', landing_bucket, landing_bucket);

execute immediate format("""
create or replace external table `%s.fraude_bronze.affiliate_cpa_ftd`
options (
  format = 'PARQUET',
  uris = ['gs://%s/landing/affiliate_cpa_ftd/ingest_date=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://%s/landing/affiliate_cpa_ftd',
  require_hive_partition_filter = false
)
""", '${GCP_PROJECT_ID}', landing_bucket, landing_bucket);
