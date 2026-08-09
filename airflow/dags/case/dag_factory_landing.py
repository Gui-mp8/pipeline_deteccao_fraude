from __future__ import annotations

from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.sdk import dag, task

from case.config import DEFAULT_ARGS, GCP_CONN_ID, LANDING_JOB_NAME, PROJECT_ID, REGION, TABLES, LOCAL_TZ
from case.datasets import staging_asset


def build_landing_dag(table):
    @dag(
        dag_id=f"case_landing_{table.name}",
        default_args=DEFAULT_ARGS,
        start_date=LOCAL_TZ.datetime(2026, 1, 1),
        schedule=table.schedule,
        catchup=False,
        max_active_runs=1,
        tags=["case", "landing", table.name],
    )
    def _landing_dag():
        run_landing = CloudRunExecuteJobOperator(
            task_id="transform_landing_to_staging_parquet",
            project_id=PROJECT_ID,
            region=REGION,
            job_name=LANDING_JOB_NAME,
            gcp_conn_id=GCP_CONN_ID,
            overrides={
                "container_overrides": [
                    {
                        "env": [
                            {"name": "TABLE_NAME", "value": table.name},
                            {"name": "SOURCE_FORMAT", "value": table.source_format},
                            {"name": "SOURCE_URI", "value": table.source_uri},
                            {"name": "DESTINATION_URI", "value": table.staging_prefix},
                            {"name": "BATCH_SIZE", "value": "{{ var.value.get('case_landing_batch_size', '1000') }}"},
                            {"name": "RUN_ID", "value": "{{ run_id | replace(':', '_') | replace('+', '_') }}"},
                        ]
                    }
                ]
            },
        )

        @task(outlets=[staging_asset(table.name)])
        def publish_dataset() -> str:
            return table.name

        run_landing >> publish_dataset()

    return _landing_dag()


for table_config in TABLES:
    globals()[f"case_landing_{table_config.name}"] = build_landing_dag(table_config)
