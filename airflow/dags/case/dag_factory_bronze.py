from __future__ import annotations

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import dag, task

from case.config import (
    BIGQUERY_LOCATION,
    BRONZE_DATASET,
    DEFAULT_ARGS,
    GCP_CONN_ID,
    LOCAL_TZ,
    PROJECT_ID,
    TABLES,
)
from case.datasets import bronze_asset, staging_asset


def build_bronze_dag(table):
    @dag(
        dag_id=f"case_bronze_{table.name}",
        default_args=DEFAULT_ARGS,
        start_date=LOCAL_TZ.datetime(2026, 1, 1),
        schedule=[staging_asset(table.name)],
        catchup=False,
        max_active_runs=1,
        tags=["case", "bronze", table.name],
    )
    def _bronze_dag():
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
        def publish_dataset() -> str:
            return table.name

        validate_external_table >> publish_dataset()

    return _bronze_dag()


for table_config in TABLES:
    globals()[f"case_bronze_{table_config.name}"] = build_bronze_dag(table_config)
