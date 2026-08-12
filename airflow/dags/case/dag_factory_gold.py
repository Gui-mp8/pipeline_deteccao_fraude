from __future__ import annotations

from airflow.sdk import dag

from include.task_groups.dbt_cloud_run import TaskStrategyDbtCloudRunTG
from include.utils.config import CONFIG, DEFAULT_ARGS, GOLD_MODELS, LOCAL_TZ
from include.utils.datasets import silver_dataset


def build_gold_dag(model_name: str, upstream_tables: tuple[str, ...]):
    @dag(
        dag_id=f"case_gold_{model_name.removeprefix('gold_')}",
        default_args=DEFAULT_ARGS,
        start_date=LOCAL_TZ.datetime(2026, 1, 1),
        schedule=[silver_dataset(CONFIG["project_directory"], table) for table in upstream_tables],
        catchup=False,
        max_active_runs=1,
        tags=["case", "gold", model_name],
    )
    def _gold_dag():
        TaskStrategyDbtCloudRunTG(
            group_id="dbt_build_gold",
            config=CONFIG,
            schema_key=model_name,
            dbt_select=model_name,
            layer="ouro",
            deps=[silver_dataset(CONFIG["project_directory"], table) for table in upstream_tables],
        )

    return _gold_dag()


for gold_model, dependencies in GOLD_MODELS.items():
    globals()[f"case_gold_{gold_model}"] = build_gold_dag(gold_model, dependencies)
