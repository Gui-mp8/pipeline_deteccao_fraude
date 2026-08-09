-- Template standalone para criacao da Bronze.
-- Renderize antes de executar, por exemplo:
-- envsubst < ddl/bigquery/bronze_external_tables.sql | bq query --use_legacy_sql=false
--
-- Variaveis esperadas:
--   GCP_PROJECT_ID
--   GCS_BUCKET

declare gcs_bucket string default '${GCS_BUCKET}';

execute immediate format("""
create schema if not exists `%s.fraude_bronze`
options(location = 'southamerica-east1')
""", '${GCP_PROJECT_ID}');

execute immediate format("""
create or replace external table `%s.fraude_bronze.players`
options (
  format = 'PARQUET',
  uris = ['gs://%s/staging/players/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://%s/staging/players',
  require_hive_partition_filter = false
)
""", '${GCP_PROJECT_ID}', gcs_bucket, gcs_bucket);

execute immediate format("""
create or replace external table `%s.fraude_bronze.sessions`
options (
  format = 'PARQUET',
  uris = ['gs://%s/staging/sessions/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://%s/staging/sessions',
  require_hive_partition_filter = false
)
""", '${GCP_PROJECT_ID}', gcs_bucket, gcs_bucket);

execute immediate format("""
create or replace external table `%s.fraude_bronze.transactions`
options (
  format = 'PARQUET',
  uris = ['gs://%s/staging/transactions/dt=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://%s/staging/transactions',
  require_hive_partition_filter = false
)
""", '${GCP_PROJECT_ID}', gcs_bucket, gcs_bucket);

execute immediate format("""
create or replace external table `%s.fraude_bronze.affiliate_cpa_ftd`
options (
  format = 'PARQUET',
  uris = ['gs://%s/staging/affiliate_cpa_ftd/ingest_date=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://%s/staging/affiliate_cpa_ftd',
  require_hive_partition_filter = false
)
""", '${GCP_PROJECT_ID}', gcs_bucket, gcs_bucket);
