from __future__ import annotations

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

from igaming_case.config import DEFAULT_ARGS, DBT_JOB_NAME, GCP_CONN_ID, GOLD_MODELS, PROJECT_ID, REGION, LOCAL_TZ
from igaming_case.datasets import gold_dataset, silver_dataset


def build_gold_dag(model_name: str, upstream_tables: tuple[str, ...]):
    with DAG(
        dag_id=f"igaming_gold_{model_name.removeprefix('gold_')}",
        default_args=DEFAULT_ARGS,
        start_date=LOCAL_TZ.datetime(2026, 1, 1),
        schedule=[silver_dataset(table) for table in upstream_tables],
        catchup=False,
        max_active_runs=1,
        tags=["igaming", "gold", model_name],
    ) as dag:
        build_model = CloudRunExecuteJobOperator(
            task_id="dbt_build_gold_model",
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
                            model_name,
                        ]
                    }
                ]
            },
        )

        @task(outlets=[gold_dataset(model_name)])
        def publish_dataset() -> str:
            return model_name

        build_model >> publish_dataset()

    return dag


for gold_model, dependencies in GOLD_MODELS.items():
    globals()[f"igaming_gold_{gold_model}"] = build_gold_dag(gold_model, dependencies)
