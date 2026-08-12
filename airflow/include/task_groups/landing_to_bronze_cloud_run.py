from __future__ import annotations

from typing import Optional

from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.sdk import TaskGroup

from include.task_groups.common import build_dataset, build_env_overrides, build_operator_kwargs
from include.utils.config import TableConfig


class TaskStrategyLandingToBronzeCloudRunTG(TaskGroup):
    def __init__(
        self,
        group_id: str,
        config: dict,
        table: TableConfig,
        tooltip: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            group_id=group_id,
            tooltip=tooltip or f"landing to bronze {table.name}",
            **kwargs,
        )

        landing_config = config["service"]["cloud_run"]["landing"]
        env = {
            "TABLE_NAME": table.name,
            "SOURCE_FORMAT": table.source_format,
            "SOURCE_URI": table.source_uri,
            "DESTINATION_URI": table.staging_prefix,
            "BATCH_SIZE": "{{ var.value.get('case_landing_batch_size', '1000') }}",
            "RUN_ID": "{{ run_id | replace(':', '_') | replace('+', '_') }}",
        }

        CloudRunExecuteJobOperator(
            task_id="transform_landing_to_staging_parquet",
            overrides=build_env_overrides(landing_config, env),
            outlets=[build_dataset(config, "bronze", table.name)],
            **build_operator_kwargs(landing_config, self),
        )
