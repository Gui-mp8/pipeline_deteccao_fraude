from __future__ import annotations

from airflow.sdk import Asset


def staging_asset(table: str) -> Asset:
    return Asset(f"gcs://igaming-case/staging/{table}")


def bronze_asset(table: str) -> Asset:
    return Asset(f"bq://igaming-case/bronze/{table}")


def silver_asset(table: str) -> Asset:
    return Asset(f"bq://igaming-case/silver/{table}")


def gold_asset(model: str) -> Asset:
    return Asset(f"bq://igaming-case/gold/{model}")
