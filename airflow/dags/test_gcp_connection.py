from __future__ import annotations

import pendulum
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.sdk import dag, task
from google.auth.transport.requests import Request


GCP_CONN_ID = "{{ var.value.get('case_gcp_conn_id', 'google_cloud_default') }}"
PROJECT_ID = "{{ var.value.get('case_gcp_project_id', 'case-grupo-otg1') }}"
LOCATION = "{{ var.value.get('case_bigquery_location', 'us-central1') }}"


@dag(
    dag_id="test_gcp_connection",
    description="Valida a conexao Google Cloud configurada no Airflow.",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=["gcp", "connection-test"],
)
def test_gcp_connection():

    @task
    def check_google_credentials() -> str:
        hook = GoogleBaseHook(gcp_conn_id=GCP_CONN_ID)
        credentials = hook.get_credentials()
        credentials.refresh(Request())
        return "Credenciais Google Cloud carregadas e token atualizado com sucesso."

    test_bigquery = BigQueryInsertJobOperator(
        task_id="test_bigquery_select_1",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": "select 1 as ok, current_timestamp() as tested_at",
                "useLegacySql": False,
            }
        },
    )

    check_google_credentials() >> test_bigquery


test_gcp_connection_dag = test_gcp_connection()
