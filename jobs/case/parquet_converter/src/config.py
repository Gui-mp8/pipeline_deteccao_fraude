from __future__ import annotations

from typing import Type

import pyarrow as pa
from pydantic import BaseModel

from src.contracts import AffiliateCpaFtdContract, PlayerContract, SessionContract, TransactionContract


class TableConfig:
    def __init__(
        self,
        name: str,
        schema: pa.Schema,
        contract: Type[BaseModel],
        partition_field: str | None,
        fallback_partition_field: str = "ingest_date",
    ) -> None:
        self.name = name
        self.schema = schema
        self.contract = contract
        self.partition_field = partition_field
        self.fallback_partition_field = fallback_partition_field


TABLE_CONFIGS: dict[str, TableConfig] = {
    "players": TableConfig(
        name="players",
        schema=pa.schema(
            [
                ("player_id", pa.string()),
                ("email", pa.string()),
                ("city", pa.string()),
                ("created_at", pa.string()),
            ]
        ),
        contract=PlayerContract,
        partition_field="created_at",
    ),
    "sessions": TableConfig(
        name="sessions",
        schema=pa.schema(
            [
                ("session_id", pa.string()),
                ("player_id", pa.string()),
                ("ip", pa.string()),
                ("device", pa.string()),
                ("timestamp", pa.string()),
            ]
        ),
        contract=SessionContract,
        partition_field="timestamp",
    ),
    "transactions": TableConfig(
        name="transactions",
        schema=pa.schema(
            [
                ("transaction_id", pa.string()),
                ("player_id", pa.string()),
                ("type", pa.string()),
                ("amount", pa.string()),
                ("timestamp", pa.string()),
            ]
        ),
        contract=TransactionContract,
        partition_field="timestamp",
    ),
    "affiliate_cpa_ftd": TableConfig(
        name="affiliate_cpa_ftd",
        schema=pa.schema(
            [
                ("affiliate_id", pa.string()),
                ("player_id", pa.string()),
                ("country", pa.string()),
                ("clicks", pa.string()),
                ("registrations", pa.string()),
                ("ftd", pa.string()),
                ("cpa_value", pa.string()),
            ]
        ),
        contract=AffiliateCpaFtdContract,
        partition_field=None,
    ),
}
