from __future__ import annotations

from airflow.sdk import dag

from include.task_groups.dbt_checks_cloud_run import TaskStrategyDbtChecksCloudRunTG
from include.task_groups.dbt_cloud_run import TaskStrategyDbtCloudRunTG
from include.utils.config import (
    CONFIG,
    DEFAULT_ARGS,
    LOCAL_TZ,
    TABLES,
)
from include.utils.datasets import bronze_dataset


def build_silver_dag(table):
    @dag(
        dag_id=f"case_silver_{table.name}",
        default_args=DEFAULT_ARGS,
        start_date=LOCAL_TZ.datetime(2026, 1, 1),
        schedule=[bronze_dataset(CONFIG["project_directory"], table.name)],
        catchup=False,
        max_active_runs=1,
        tags=["case", "silver", table.name],
    )
    def _silver_dag():
        bronze_checks = TaskStrategyDbtChecksCloudRunTG(
            group_id="bronze_source_checks",
            config=CONFIG,
            dbt_select=f"source:raw_fraud.{table.name}",
            layer="bronze",
            schema_key=table.name,
            deps=[bronze_dataset(CONFIG["project_directory"], table.name)],
        )

        dbt_build = TaskStrategyDbtCloudRunTG(
            group_id="dbt_build_silver",
            config=CONFIG,
            schema_key=table.name,
            dbt_select=table.silver_model,
            layer="prata",
            deps=[bronze_dataset(CONFIG["project_directory"], table.name)],
        )

        bronze_checks >> dbt_build

    return _silver_dag()


for table_config in TABLES:
    globals()[f"case_silver_{table_config.name}"] = build_silver_dag(table_config)
