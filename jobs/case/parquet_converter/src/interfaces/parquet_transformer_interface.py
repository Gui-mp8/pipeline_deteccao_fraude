from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator

import pyarrow as pa


class ParquetBatch:
    def __init__(self, partition: str, table: pa.Table, row_count: int, part_number: int) -> None:
        self.partition = partition
        self.table = table
        self.row_count = row_count
        self.part_number = part_number


class ParquetTransformer(ABC):
    @abstractmethod
    def read_from_storage(self, source_uri: str) -> Iterator[dict[str, object]]:
        """Read source rows from Cloud Storage."""

    @abstractmethod
    def transform_to_parquet(self, source_uri: str) -> Iterator[ParquetBatch]:
        """Transform source rows into partitioned Parquet batches."""
