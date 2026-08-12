from __future__ import annotations

from airflow.sdk import dag

from include.task_groups.landing_to_bronze_cloud_run import TaskStrategyLandingToBronzeCloudRunTG
from include.utils.config import (
    CONFIG,
    DEFAULT_ARGS,
    LOCAL_TZ,
    TABLES,
)


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
        TaskStrategyLandingToBronzeCloudRunTG(
            group_id="landing_to_bronze",
            config=CONFIG,
            table=table,
        )

    return _bronze_dag()


for table_config in TABLES:
    globals()[f"case_bronze_{table_config.name}"] = build_bronze_dag(table_config)
