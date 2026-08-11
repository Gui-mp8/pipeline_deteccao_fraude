from __future__ import annotations

from airflow.sdk import Asset


def bronze_asset(table: str) -> Asset:
    return Asset(f"bq://case-grupo-otg1/case_bronze/{table}")


def silver_asset(table: str) -> Asset:
    return Asset(f"bq://case-grupo-otg1/case_silver/{table}")


def gold_asset(model: str) -> Asset:
    return Asset(f"bq://case-grupo-otg1/case_gold/{model}")
