from __future__ import annotations

from typing import Optional

from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.sdk import TaskGroup

from include.task_groups.common import build_args_overrides, build_dataset, build_operator_kwargs


class TaskStrategyDbtChecksCloudRunTG(TaskGroup):
    def __init__(
        self,
        group_id: str,
        config: dict,
        dbt_select: str,
        layer: str,
        schema_key: str,
        deps: Optional[list] = None,
        test_args: Optional[list[str]] = None,
        run_freshness: bool = False,
        freshness_args: Optional[list[str]] = None,
        publish_outlet: bool = False,
        tooltip: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            group_id=group_id,
            tooltip=tooltip or f"dbt checks {dbt_select}",
            **kwargs,
        )

        dbt_config = config["service"]["cloud_run"]["dbt"]
        operator_kwargs = build_operator_kwargs(dbt_config, self)
        outlets = [build_dataset(config, layer, schema_key)] if publish_outlet else None

        resolved_test_args = test_args or ["test", "--select", dbt_select]
        dbt_tests = CloudRunExecuteJobOperator(
            task_id="dbt_tests",
            overrides=build_args_overrides(dbt_config, resolved_test_args),
            inlets=deps or [],
            outlets=outlets,
            **operator_kwargs,
        )

        if run_freshness:
            resolved_freshness_args = freshness_args or ["source", "freshness", "--select", dbt_select]
            dbt_freshness = CloudRunExecuteJobOperator(
                task_id="dbt_freshness",
                overrides=build_args_overrides(dbt_config, resolved_freshness_args),
                inlets=deps or [],
                **operator_kwargs,
            )

            dbt_freshness >> dbt_tests
