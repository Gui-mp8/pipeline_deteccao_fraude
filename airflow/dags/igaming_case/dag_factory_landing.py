from __future__ import annotations

from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.sdk import dag, task

from igaming_case.config import DEFAULT_ARGS, GCP_CONN_ID, LANDING_JOB_NAME, PROJECT_ID, REGION, TABLES, LOCAL_TZ
from igaming_case.datasets import landing_asset


def build_landing_dag(table):
    @dag(
        dag_id=f"igaming_landing_{table.name}",
        default_args=DEFAULT_ARGS,
        start_date=LOCAL_TZ.datetime(2026, 1, 1),
        schedule=table.schedule,
        catchup=False,
        max_active_runs=1,
        tags=["igaming", "landing", table.name],
    )
    def _landing_dag():
        run_landing = CloudRunExecuteJobOperator(
            task_id="run_file_to_landing_parquet",
            project_id=PROJECT_ID,
            region=REGION,
            job_name=LANDING_JOB_NAME,
            gcp_conn_id=GCP_CONN_ID,
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            "--source-uri",
                            table.source_uri,
                            "--destination-uri",
                            table.landing_prefix,
                            "--table",
                            table.name,
                            "--format",
                            table.source_format,
                            "--batch-size",
                            "{{ var.value.get('igaming_landing_batch_size', '1000') }}",
                            "--run-id",
                            "{{ run_id | replace(':', '_') | replace('+', '_') }}",
                        ]
                    }
                ]
            },
        )

        @task(outlets=[landing_asset(table.name)])
        def publish_dataset() -> str:
            return table.name

        run_landing >> publish_dataset()

    return _landing_dag()


for table_config in TABLES:
    globals()[f"igaming_landing_{table_config.name}"] = build_landing_dag(table_config)
