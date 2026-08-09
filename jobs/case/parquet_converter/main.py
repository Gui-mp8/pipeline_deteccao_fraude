from __future__ import annotations

import json
import os

from src.adapters.csv_adapter import CsvAdapter
from src.adapters.json_adapter import JsonAdapter
from src.config import TABLE_CONFIGS
from src.interfaces.parquet_transformer_interface import ParquetTransformer
from src.repository.gcs_repository import GCSRepository


BUCKET = "case-grupo-otg1"
LANDING_PREFIX = os.getenv("LANDING_PREFIX", f"gs://{BUCKET}/landing")
STAGING_PREFIX = os.getenv("STAGING_PREFIX", f"gs://{BUCKET}/staging")

TABLE_FILE_CONFIGS = {
    "players": {"format": "json", "file_name": "players.json"},
    "sessions": {"format": "json", "file_name": "sessions.json"},
    "transactions": {"format": "csv", "file_name": "transactions.csv"},
    "affiliate_cpa_ftd": {"format": "csv", "file_name": "affiliate_cpa_ftd.csv"},
}


def build_landing_uri(table_name: str) -> str:
    file_name = os.getenv("SOURCE_FILE_NAME", TABLE_FILE_CONFIGS[table_name]["file_name"])
    return os.getenv("SOURCE_URI", f"{LANDING_PREFIX.rstrip('/')}/{file_name}")


def build_staging_uri(table_name: str) -> str:
    return os.getenv("DESTINATION_URI", f"{STAGING_PREFIX.rstrip('/')}/{table_name}")


def build_adapter(table_name: str, source_format: str, batch_size: int) -> ParquetTransformer:
    table_config = TABLE_CONFIGS[table_name]
    normalized_format = source_format.lower().replace("_", "-")
    if normalized_format == "csv":
        return CsvAdapter(table_config=table_config, batch_size=batch_size)
    if normalized_format in {"json", "json-array"}:
        return JsonAdapter(table_config=table_config, batch_size=batch_size)
    raise ValueError(f"Unsupported source format: {source_format}")


def main() -> None:
    table_name = os.environ["TABLE_NAME"]
    source_format = os.getenv("SOURCE_FORMAT", TABLE_FILE_CONFIGS[table_name]["format"])
    batch_size = int(os.getenv("BATCH_SIZE", "1000"))
    run_id = os.getenv("RUN_ID")

    source_uri = build_landing_uri(table_name)
    destination_uri = build_staging_uri(table_name)
    adapter = build_adapter(table_name=table_name, source_format=source_format, batch_size=batch_size)
    repository = GCSRepository(destination_uri=destination_uri, run_id=run_id) if run_id else GCSRepository(destination_uri=destination_uri)

    rows_written = 0
    written_paths: list[str] = []
    for batch in adapter.transform_to_parquet(source_uri):
        written_paths.append(repository.save(batch))
        rows_written += batch.row_count

    print(
        json.dumps(
            {
                "rows_written": rows_written,
                "files_written": len(written_paths),
                "source_uri": source_uri,
                "destination_uri": destination_uri,
                "table": table_name,
                "written_paths": written_paths,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

