from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import pendulum


LOCAL_TZ = pendulum.timezone("America/Sao_Paulo")


@dataclass(frozen=True)
class TableConfig:
    name: str
    source_uri: str
    source_format: str
    staging_prefix: str
    schedule: str
    silver_model: str


def airflow_var(name: str, default: str) -> str:
    return f"{{{{ var.value.get('{name}', '{default}') }}}}"


PROJECT_ID = airflow_var("case_gcp_project_id", "case-grupo-otg1")
REGION = airflow_var("case_gcp_region", "us-central1")
GCS_BUCKET = airflow_var("case_gcs_bucket", "case-grupo-otg1")
GCP_CONN_ID = airflow_var("case_gcp_conn_id", "google_cloud_default")
LANDING_JOB_NAME = airflow_var("case_landing_job_name", "fraud-landing-to-staging-parquet")
DBT_JOB_NAME = airflow_var("case_dbt_job_name", "fraud-dbt")

PROJECT_DIRECTORY = "case"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

LANDING_CLOUD_RUN_CONFIG = {
    "project_id": PROJECT_ID,
    "region": REGION,
    "job_name": LANDING_JOB_NAME,
    "gcp_conn_id": GCP_CONN_ID,
    "deferrable": True,
}

DBT_CLOUD_RUN_CONFIG = {
    "project_id": PROJECT_ID,
    "region": REGION,
    "job_name": DBT_JOB_NAME,
    "gcp_conn_id": GCP_CONN_ID,
    "deferrable": True,
}

CONFIG = {
    "project_directory": PROJECT_DIRECTORY,
    "service": {
        "cloud_run": {
            "landing": LANDING_CLOUD_RUN_CONFIG,
            "dbt": DBT_CLOUD_RUN_CONFIG,
        },
    },
}

TABLES: tuple[TableConfig, ...] = (
    TableConfig(
        name="players",
        source_uri=airflow_var("case_players_source_uri", "gs://case-grupo-otg1/landing/players.json"),
        source_format="json",
        staging_prefix=f"gs://{GCS_BUCKET}/staging/players",
        schedule="0 2 * * *",
        silver_model="slv_players",
    ),
    TableConfig(
        name="sessions",
        source_uri=airflow_var("case_sessions_source_uri", "gs://case-grupo-otg1/landing/sessions.json"),
        source_format="json",
        staging_prefix=f"gs://{GCS_BUCKET}/staging/sessions",
        schedule="0 * * * *",
        silver_model="slv_sessions",
    ),
    TableConfig(
        name="transactions",
        source_uri=airflow_var("case_transactions_source_uri", "gs://case-grupo-otg1/landing/transactions.csv"),
        source_format="csv",
        staging_prefix=f"gs://{GCS_BUCKET}/staging/transactions",
        schedule="15 * * * *",
        silver_model="slv_transactions",
    ),
    TableConfig(
        name="affiliate_cpa_ftd",
        source_uri=airflow_var(
            "case_affiliate_source_uri",
            "gs://case-grupo-otg1/landing/affiliate_cpa_ftd.csv",
        ),
        source_format="csv",
        staging_prefix=f"gs://{GCS_BUCKET}/staging/affiliate_cpa_ftd",
        schedule="30 2 * * *",
        silver_model="slv_affiliate_cpa_ftd",
    ),
)

GOLD_MODELS: dict[str, tuple[str, ...]] = {
    "gold_financial_signals": ("transactions",),
    "gold_affiliate_metrics": ("affiliate_cpa_ftd",),
    "gold_fraud_overview": ("players", "sessions", "transactions", "affiliate_cpa_ftd"),
}
