from __future__ import annotations

from airflow.datasets import Dataset


def landing_dataset(table: str) -> Dataset:
    return Dataset(f"gcs://igaming-case/landing/{table}")


def bronze_dataset(table: str) -> Dataset:
    return Dataset(f"bq://igaming-case/bronze/{table}")


def silver_dataset(table: str) -> Dataset:
    return Dataset(f"bq://igaming-case/silver/{table}")


def gold_dataset(model: str) -> Dataset:
    return Dataset(f"bq://igaming-case/gold/{model}")
