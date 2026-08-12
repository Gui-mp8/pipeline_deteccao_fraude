from __future__ import annotations

from airflow.sdk import Asset


PROJECT_ID = "case-grupo-otg1"


def bronze_dataset(project_directory: str, schema_key: str) -> Asset:
    return Asset(f"bq://{PROJECT_ID}/{project_directory}_bronze/{schema_key}")


def prata_dataset(project_directory: str, schema_key: str) -> Asset:
    return Asset(f"bq://{PROJECT_ID}/{project_directory}_silver/{schema_key}")


def ouro_dataset(project_directory: str, schema_key: str) -> Asset:
    return Asset(f"bq://{PROJECT_ID}/{project_directory}_gold/{schema_key}")


def silver_dataset(project_directory: str, schema_key: str) -> Asset:
    return prata_dataset(project_directory, schema_key)


def gold_dataset(project_directory: str, schema_key: str) -> Asset:
    return ouro_dataset(project_directory, schema_key)
