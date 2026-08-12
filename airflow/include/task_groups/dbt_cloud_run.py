from __future__ import annotations

import shlex
from typing import Optional

from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.sdk import TaskGroup

from include.task_groups.common import build_args_overrides, build_dataset, build_operator_kwargs


class TaskStrategyDbtCloudRunTG(TaskGroup):
    def __init__(
        self,
        group_id: str,
        config: dict,
        schema_key: str,
        dbt_select: str,
        layer: str,
        deps: Optional[list] = None,
        dbt_args: Optional[list[str]] = None,
        dbt_command: Optional[str] = None,
        tooltip: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            group_id=group_id,
            tooltip=tooltip or f"dbt Cloud Run {schema_key}",
            **kwargs,
        )

        dbt_config = config["service"]["cloud_run"]["dbt"]
        resolved_dbt_args = dbt_args
        if resolved_dbt_args is None:
            resolved_dbt_args = shlex.split(dbt_command) if dbt_command else ["build", "--select", dbt_select]

        operator_kwargs = build_operator_kwargs(dbt_config, self)

        CloudRunExecuteJobOperator(
            task_id="dbt_run",
            overrides=build_args_overrides(dbt_config, resolved_dbt_args),
            inlets=deps or [],
            outlets=[build_dataset(config, layer, schema_key)],
            **operator_kwargs,
        )
