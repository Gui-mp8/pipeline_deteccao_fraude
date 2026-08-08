from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from .config import TableConfig
from .partitioning import PartitionResolver
from .readers import RowReader
from .writers import ParquetBatchWriter


@dataclass(frozen=True)
class PipelineResult:
    rows_read: int
    files_written: int
    written_paths: tuple[str, ...]


class RequiredColumnsValidator:
    def __init__(self, required_columns: Iterable[str]) -> None:
        self.required_columns = tuple(required_columns)

    def validate(self, row: dict[str, object]) -> None:
        missing = [column for column in self.required_columns if column not in row]
        if missing:
            raise ValueError(f"Missing required columns: {', '.join(missing)}")


class LandingIngestionPipeline:
    def __init__(
        self,
        reader: RowReader,
        writer: ParquetBatchWriter,
        table_config: TableConfig,
        partition_resolver: PartitionResolver,
        batch_size: int = 1000,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be greater than zero")
        self.reader = reader
        self.writer = writer
        self.table_config = table_config
        self.partition_resolver = partition_resolver
        self.batch_size = batch_size
        self.validator = RequiredColumnsValidator(table_config.required_columns)

    def run(self) -> PipelineResult:
        buffers: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
        rows_read = 0
        written_paths: list[str] = []

        for raw_row in self.reader.iter_rows():
            rows_read += 1
            self.validator.validate(raw_row)
            row = self.table_config.normalizer(raw_row) if self.table_config.normalizer else raw_row
            partition = self.partition_resolver.resolve(row)
            buffers[partition].append(row)
            if len(buffers[partition]) >= self.batch_size:
                path = self.writer.write_batch(partition, buffers[partition])
                if path:
                    written_paths.append(path)
                buffers[partition].clear()

        for partition, rows in buffers.items():
            path = self.writer.write_batch(partition, rows)
            if path:
                written_paths.append(path)

        return PipelineResult(rows_read=rows_read, files_written=len(written_paths), written_paths=tuple(written_paths))
