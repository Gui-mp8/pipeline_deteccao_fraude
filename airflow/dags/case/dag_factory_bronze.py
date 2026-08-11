from __future__ import annotations

from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import dag, task

from case.config import (
    BIGQUERY_LOCATION,
    BRONZE_DATASET,
    DEFAULT_ARGS,
    GCP_CONN_ID,
    LANDING_JOB_NAME,
    LOCAL_TZ,
    PROJECT_ID,
    REGION,
    TABLES,
)
from case.datasets import bronze_asset, staging_asset


def build_bronze_dag(table):
    @dag(
        dag_id=f"case_bronze_{table.name}",
        default_args=DEFAULT_ARGS,
        start_date=LOCAL_TZ.datetime(2026, 1, 1),
        schedule=table.schedule,
        catchup=False,
        max_active_runs=1,
        tags=["case", "bronze", table.name],
    )
    def _bronze_dag():
        run_landing_to_staging = CloudRunExecuteJobOperator(
            task_id="transform_landing_to_staging_parquet",
            project_id=PROJECT_ID,
            region=REGION,
            job_name=LANDING_JOB_NAME,
            gcp_conn_id=GCP_CONN_ID,
            overrides={
                "container_overrides": [
                    {
                        "env": [
                            {"name": "TABLE_NAME", "value": table.name},
                            {"name": "SOURCE_FORMAT", "value": table.source_format},
                            {"name": "SOURCE_URI", "value": table.source_uri},
                            {"name": "DESTINATION_URI", "value": table.staging_prefix},
                            {"name": "BATCH_SIZE", "value": "{{ var.value.get('case_landing_batch_size', '1000') }}"},
                            {"name": "RUN_ID", "value": "{{ run_id | replace(':', '_') | replace('+', '_') }}"},
                        ]
                    }
                ]
            },
        )

        @task(outlets=[staging_asset(table.name)])
        def publish_staging_dataset() -> str:
            return table.name

        validate_external_table = BigQueryInsertJobOperator(
            task_id="validate_external_table",
            gcp_conn_id=GCP_CONN_ID,
            location=BIGQUERY_LOCATION,
            configuration={
                "query": {
                    "query": """
                    assert (
                      select count(*)
                      from `{{ params.project_id }}.{{ params.bronze_dataset }}.INFORMATION_SCHEMA.TABLES`
                      where table_name = '{{ params.table_name }}'
                    ) = 1 as 'External table {{ params.bronze_dataset }}.{{ params.table_name }} not found';
                    """,
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": PROJECT_ID,
                "bronze_dataset": BRONZE_DATASET,
                "bigquery_location": BIGQUERY_LOCATION,
                "table_name": table.name,
            },
        )

        @task(outlets=[bronze_asset(table.name)])
        def publish_bronze_dataset() -> str:
            return table.name

        run_landing_to_staging >> publish_staging_dataset() >> validate_external_table >> publish_bronze_dataset()

    return _bronze_dag()


for table_config in TABLES:
    globals()[f"case_bronze_{table_config.name}"] = build_bronze_dag(table_config)
