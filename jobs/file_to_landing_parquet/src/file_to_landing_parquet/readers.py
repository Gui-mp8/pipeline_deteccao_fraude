from __future__ import annotations

import csv
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator

import ijson
import pyarrow.fs as pafs


class RowReader(ABC):
    @abstractmethod
    def iter_rows(self) -> Iterator[dict[str, object]]:
        """Yield one record at a time from the source."""


class LocalCsvRowReader(RowReader):
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def iter_rows(self) -> Iterator[dict[str, object]]:
        with self.path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                yield dict(row)


class LocalJsonArrayRowReader(RowReader):
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def iter_rows(self) -> Iterator[dict[str, object]]:
        with self.path.open("rb") as file:
            for item in ijson.items(file, "item"):
                yield dict(item)


class GcsJsonArrayRowReader(RowReader):
    def __init__(self, uri: str) -> None:
        self.uri = uri

    def iter_rows(self) -> Iterator[dict[str, object]]:
        fs, path = pafs.FileSystem.from_uri(self.uri)
        with fs.open_input_file(path) as file:
            for item in ijson.items(file, "item"):
                yield dict(item)


class GcsCsvRowReader(RowReader):
    def __init__(self, uri: str) -> None:
        self.uri = uri

    def iter_rows(self) -> Iterator[dict[str, object]]:
        fs, path = pafs.FileSystem.from_uri(self.uri)
        with fs.open_input_file(path) as file:
            text = (line.decode("utf-8") for line in file)
            reader = csv.DictReader(text)
            for row in reader:
                yield dict(row)


def build_reader(source_uri: str, source_format: str) -> RowReader:
    is_gcs = source_uri.startswith("gs://")
    normalized_format = source_format.lower().replace("_", "-")
    if normalized_format == "csv":
        return GcsCsvRowReader(source_uri) if is_gcs else LocalCsvRowReader(source_uri)
    if normalized_format in {"json", "json-array"}:
        return GcsJsonArrayRowReader(source_uri) if is_gcs else LocalJsonArrayRowReader(source_uri)
    raise ValueError(f"Unsupported source format: {source_format}")
