from __future__ import annotations

from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import dag, task

from case.config import (
    BIGQUERY_LOCATION,
    BRONZE_DATASET,
    DEFAULT_ARGS,
    DBT_JOB_NAME,
    GCP_CONN_ID,
    LOCAL_TZ,
    PROJECT_ID,
    REGION,
    TABLES,
)
from case.datasets import bronze_asset, silver_asset


def build_silver_dag(table):
    @dag(
        dag_id=f"case_silver_{table.name}",
        default_args=DEFAULT_ARGS,
        start_date=LOCAL_TZ.datetime(2026, 1, 1),
        schedule=[bronze_asset(table.name)],
        catchup=False,
        max_active_runs=1,
        tags=["case", "silver", table.name],
    )
    def _silver_dag():
        validate_external_table = BigQueryInsertJobOperator(
            task_id="validate_bronze_external_table",
            gcp_conn_id=GCP_CONN_ID,
            location=BIGQUERY_LOCATION,
            configuration={
                "query": {
                    "query": """
                    ASSERT (
                      SELECT COUNT(*)
                      FROM `{{ params.project_id }}.{{ params.bronze_dataset }}.INFORMATION_SCHEMA.TABLES`
                      WHERE table_name = '{{ params.table_name }}'
                    ) = 1 AS 'External table {{ params.bronze_dataset }}.{{ params.table_name }} not found';
                    """,
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": PROJECT_ID,
                "bronze_dataset": BRONZE_DATASET,
                "table_name": table.name,
            },
        )

        build_model = CloudRunExecuteJobOperator(
            task_id="dbt_build_silver_model",
            project_id=PROJECT_ID,
            region=REGION,
            job_name=DBT_JOB_NAME,
            gcp_conn_id=GCP_CONN_ID,
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "build",
                            "--select",
                            table.silver_model,
                        ]
                    }
                ]
            },
        )

        @task(outlets=[silver_asset(table.name)])
        def publish_dataset() -> str:
            return table.name

        validate_external_table >> build_model >> publish_dataset()

    return _silver_dag()


for table_config in TABLES:
    globals()[f"case_silver_{table_config.name}"] = build_silver_dag(table_config)
