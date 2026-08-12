from __future__ import annotations

from uuid import uuid4

import pyarrow.fs as pafs
import pyarrow.parquet as pq

from src.interfaces.parquet_transformer_interface import ParquetBatch


class GCSRepository:
    """Repository responsible only for saving Parquet batches in staging storage."""

    def __init__(self, destination_uri: str, run_id: str | None = None) -> None:
        self.destination_uri = destination_uri
        self.run_id = run_id or uuid4().hex
        self.fs, self.base_path = pafs.FileSystem.from_uri(self.destination_uri)
        self._destination_prepared = False

    def save(self, batch: ParquetBatch) -> str:
        self._prepare_destination()
        output_path = (
            f"{self.base_path.rstrip('/')}/"
            f"{batch.partition}/"
            f"part-{self.run_id}-{batch.part_number:05d}.parquet"
        )
        parent = output_path.rsplit("/", 1)[0]
        self.fs.create_dir(parent, recursive=True)
        with self.fs.open_output_stream(output_path) as stream:
            pq.write_table(batch.table, stream, compression="snappy")
        return output_path

    def _prepare_destination(self) -> None:
        if self._destination_prepared:
            return

        self.fs.delete_dir_contents(self.base_path, missing_dir_ok=True)
        self._destination_prepared = True
