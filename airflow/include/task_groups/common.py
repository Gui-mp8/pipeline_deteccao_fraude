from __future__ import annotations

from airflow.sdk import TaskGroup

from include.utils.datasets import bronze_dataset, ouro_dataset, prata_dataset


def build_args_overrides(cloud_run_config: dict, args: list[str]) -> dict:
    container_override = {"args": [str(arg) for arg in args]}
    container_name = cloud_run_config.get("container_name")
    if container_name:
        container_override["name"] = container_name
    return {"container_overrides": [container_override]}


def build_env_overrides(cloud_run_config: dict, env: dict[str, str]) -> dict:
    container_override = {
        "env": [{"name": name, "value": str(value)} for name, value in env.items()],
    }
    container_name = cloud_run_config.get("container_name")
    if container_name:
        container_override["name"] = container_name
    return {"container_overrides": [container_override]}


def build_dataset(config: dict, layer: str, schema_key: str):
    if layer == "bronze":
        return bronze_dataset(config["project_directory"], schema_key)
    if layer == "prata":
        return prata_dataset(config["project_directory"], schema_key)
    if layer == "ouro":
        return ouro_dataset(config["project_directory"], schema_key)
    raise ValueError(f"layer invalida: {layer}")


def build_operator_kwargs(cloud_run_config: dict, task_group: TaskGroup) -> dict:
    return {
        "project_id": cloud_run_config["project_id"],
        "region": cloud_run_config.get("region", "us-central1"),
        "job_name": cloud_run_config["job_name"],
        "gcp_conn_id": cloud_run_config.get("gcp_conn_id", "google_cloud_default"),
        "deferrable": cloud_run_config.get("deferrable", True),
        "task_group": task_group,
    }
