from __future__ import annotations

from pathlib import Path

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import dag, task

from igaming_case.config import DEFAULT_ARGS, GCP_CONN_ID, LANDING_BUCKET, PROJECT_ID, TABLES, LOCAL_TZ
from igaming_case.datasets import bronze_asset, landing_asset


SQL_DIR = Path(__file__).parent / "sql" / "bronze"


def build_bronze_dag(table):
    sql = (SQL_DIR / f"{table.name}.sql").read_text(encoding="utf-8")
    @dag(
        dag_id=f"igaming_bronze_{table.name}",
        default_args=DEFAULT_ARGS,
        start_date=LOCAL_TZ.datetime(2026, 1, 1),
        schedule=[landing_asset(table.name)],
        catchup=False,
        max_active_runs=1,
        tags=["igaming", "bronze", table.name],
    )
    def _bronze_dag():
        create_external_table = BigQueryInsertJobOperator(
            task_id="create_or_replace_external_table",
            gcp_conn_id=GCP_CONN_ID,
            configuration={
                "query": {
                    "query": sql,
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": PROJECT_ID,
                "landing_bucket": LANDING_BUCKET,
            },
        )

        @task(outlets=[bronze_asset(table.name)])
        def publish_dataset() -> str:
            return table.name

        create_external_table >> publish_dataset()

    return _bronze_dag()


for table_config in TABLES:
    globals()[f"igaming_bronze_{table_config.name}"] = build_bronze_dag(table_config)
