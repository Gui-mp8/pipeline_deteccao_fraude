from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from itertools import count
from uuid import uuid4

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq


@dataclass
class ParquetBatchWriter:
    destination_uri: str
    schema: pa.Schema
    run_id: str = field(default_factory=lambda: uuid4().hex)

    def __post_init__(self) -> None:
        self.fs, self.base_path = pafs.FileSystem.from_uri(self.destination_uri)
        self._counters: defaultdict[str, count] = defaultdict(lambda: count(1))

    def write_batch(self, partition: str, rows: list[dict[str, object]]) -> str | None:
        if not rows:
            return None
        table = pa.Table.from_pylist(rows, schema=self.schema)
        part_number = next(self._counters[partition])
        output_path = f"{self.base_path.rstrip('/')}/{partition}/part-{self.run_id}-{part_number:05d}.parquet"
        parent = output_path.rsplit("/", 1)[0]
        self.fs.create_dir(parent, recursive=True)
        with self.fs.open_output_stream(output_path) as stream:
            pq.write_table(table, stream, compression="snappy")
        return output_path
