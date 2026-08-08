from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone


@dataclass(frozen=True)
class PartitionResolver:
    partition_field: str | None
    fallback_partition_field: str = "ingest_date"
    ingest_date: date | None = None

    def resolve(self, row: dict[str, object]) -> str:
        if self.partition_field:
            value = row.get(self.partition_field)
            if isinstance(value, datetime):
                return f"dt={value.date().isoformat()}"
            if isinstance(value, date):
                return f"dt={value.isoformat()}"
            raise ValueError(f"Partition field {self.partition_field} is missing or invalid")

        partition_date = self.ingest_date or datetime.now(timezone.utc).date()
        return f"{self.fallback_partition_field}={partition_date.isoformat()}"
