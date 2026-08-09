from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from datetime import date, datetime, timezone
from itertools import count

import ijson
import pyarrow as pa
import pyarrow.fs as pafs
from pydantic import ValidationError

from src.config import TableConfig
from src.interfaces.parquet_transformer_interface import ParquetBatch, ParquetTransformer


class JsonAdapter(ParquetTransformer):
    """Adapter that reads JSON from landing storage and transforms it to Parquet."""

    def __init__(self, table_config: TableConfig, batch_size: int = 1000, ingest_date: date | None = None) -> None:
        self.table_config = table_config
        self.batch_size = batch_size
        self.ingest_date = ingest_date
        self._part_numbers: defaultdict[str, count] = defaultdict(lambda: count(1))

    def read_from_storage(self, source_uri: str) -> Iterator[dict[str, object]]:
        if not source_uri.startswith("gs://"):
            raise ValueError("JSON source_uri must be a Cloud Storage URI starting with gs://")
        yield from self._iter_storage_rows(source_uri)

    def transform_to_parquet(self, source_uri: str) -> Iterator[ParquetBatch]:
        buffers: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
        for raw_row in self.read_from_storage(source_uri):
            self._validate_required_columns(raw_row)
            row = self._stringify_row(raw_row)
            partition = self._resolve_partition(row)
            buffers[partition].append(row)
            if len(buffers[partition]) >= self.batch_size:
                yield self._build_batch(partition, buffers[partition])
                buffers[partition].clear()

        for partition, rows in buffers.items():
            if rows:
                yield self._build_batch(partition, rows)

    def _iter_storage_rows(self, source_uri: str) -> Iterator[dict[str, object]]:
        fs, path = pafs.FileSystem.from_uri(source_uri)
        with fs.open_input_file(path) as file:
            for item in ijson.items(file, "item"):
                yield dict(item)

    def _validate_required_columns(self, row: dict[str, object]) -> None:
        try:
            self.table_config.contract.model_validate(row)
        except ValidationError as exc:
            missing = [str(error["loc"][0]) for error in exc.errors() if error["type"] == "missing"]
            if missing:
                raise ValueError(f"Missing required columns: {', '.join(missing)}") from exc
            raise

    def _stringify_row(self, row: dict[str, object]) -> dict[str, object]:
        return {
            field.name: str(row[field.name]) if row.get(field.name) is not None else None
            for field in self.table_config.schema
        }

    def _resolve_partition(self, row: dict[str, object]) -> str:
        if self.table_config.partition_field:
            value = row.get(self.table_config.partition_field)
            partition_date = self._extract_partition_date(value)
            if partition_date:
                return f"dt={partition_date}"
            raise ValueError(f"Partition field {self.table_config.partition_field} is missing or invalid")

        partition_date = self.ingest_date or datetime.now(timezone.utc).date()
        return f"{self.table_config.fallback_partition_field}={partition_date.isoformat()}"

    def _extract_partition_date(self, value: object) -> str | None:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if value is None:
            return None

        text = str(value).strip()
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        return None

    def _build_batch(self, partition: str, rows: list[dict[str, object]]) -> ParquetBatch:
        table = pa.Table.from_pylist(rows, schema=self.table_config.schema)
        return ParquetBatch(
            partition=partition,
            table=table,
            row_count=table.num_rows,
            part_number=next(self._part_numbers[partition]),
        )
