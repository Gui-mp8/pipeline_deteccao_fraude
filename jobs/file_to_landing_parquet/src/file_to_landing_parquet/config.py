from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Callable

import pyarrow as pa
from dateutil import parser as date_parser


Normalizer = Callable[[dict[str, object]], dict[str, object]]


@dataclass(frozen=True)
class TableConfig:
    name: str
    schema: pa.Schema
    required_columns: tuple[str, ...]
    partition_field: str | None
    fallback_partition_field: str = "ingest_date"
    normalizer: Normalizer | None = None


def _clean(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _lower(value: object) -> str | None:
    cleaned = _clean(value)
    return cleaned.lower() if cleaned is not None else None


def _upper(value: object) -> str | None:
    cleaned = _clean(value)
    return cleaned.upper() if cleaned is not None else None


def _date(value: object) -> date | None:
    cleaned = _clean(value)
    if cleaned is None:
        return None
    return date_parser.parse(cleaned).date()


def _timestamp(value: object):
    cleaned = _clean(value)
    if cleaned is None:
        return None
    return date_parser.parse(cleaned)


def _decimal(value: object) -> Decimal | None:
    cleaned = _clean(value)
    if cleaned is None:
        return None
    return Decimal(cleaned)


def _int(value: object) -> int | None:
    cleaned = _clean(value)
    if cleaned is None:
        return None
    return int(cleaned)


def normalize_players(row: dict[str, object]) -> dict[str, object]:
    return {
        "player_id": _clean(row.get("player_id")),
        "email": _lower(row.get("email")),
        "city": _clean(row.get("city")),
        "created_at": _date(row.get("created_at")),
    }


def normalize_sessions(row: dict[str, object]) -> dict[str, object]:
    return {
        "session_id": _clean(row.get("session_id")),
        "player_id": _clean(row.get("player_id")),
        "ip": _clean(row.get("ip")),
        "device": _lower(row.get("device")),
        "timestamp": _timestamp(row.get("timestamp")),
    }


def normalize_transactions(row: dict[str, object]) -> dict[str, object]:
    return {
        "transaction_id": _clean(row.get("transaction_id")),
        "player_id": _clean(row.get("player_id")),
        "type": _lower(row.get("type")),
        "amount": _decimal(row.get("amount")),
        "timestamp": _timestamp(row.get("timestamp")),
    }


def normalize_affiliate(row: dict[str, object]) -> dict[str, object]:
    return {
        "affiliate_id": _clean(row.get("affiliate_id")),
        "player_id": _clean(row.get("player_id")),
        "country": _upper(row.get("country")),
        "clicks": _int(row.get("clicks")),
        "registrations": _int(row.get("registrations")),
        "ftd": _int(row.get("ftd")),
        "cpa_value": _decimal(row.get("cpa_value")),
    }


TABLE_CONFIGS: dict[str, TableConfig] = {
    "players": TableConfig(
        name="players",
        schema=pa.schema(
            [
                ("player_id", pa.string()),
                ("email", pa.string()),
                ("city", pa.string()),
                ("created_at", pa.date32()),
            ]
        ),
        required_columns=("player_id", "email", "city", "created_at"),
        partition_field="created_at",
        normalizer=normalize_players,
    ),
    "sessions": TableConfig(
        name="sessions",
        schema=pa.schema(
            [
                ("session_id", pa.string()),
                ("player_id", pa.string()),
                ("ip", pa.string()),
                ("device", pa.string()),
                ("timestamp", pa.timestamp("us")),
            ]
        ),
        required_columns=("session_id", "player_id", "ip", "device", "timestamp"),
        partition_field="timestamp",
        normalizer=normalize_sessions,
    ),
    "transactions": TableConfig(
        name="transactions",
        schema=pa.schema(
            [
                ("transaction_id", pa.string()),
                ("player_id", pa.string()),
                ("type", pa.string()),
                ("amount", pa.decimal128(18, 2)),
                ("timestamp", pa.timestamp("us")),
            ]
        ),
        required_columns=("transaction_id", "player_id", "type", "amount", "timestamp"),
        partition_field="timestamp",
        normalizer=normalize_transactions,
    ),
    "affiliate_cpa_ftd": TableConfig(
        name="affiliate_cpa_ftd",
        schema=pa.schema(
            [
                ("affiliate_id", pa.string()),
                ("player_id", pa.string()),
                ("country", pa.string()),
                ("clicks", pa.int64()),
                ("registrations", pa.int64()),
                ("ftd", pa.int64()),
                ("cpa_value", pa.decimal128(18, 2)),
            ]
        ),
        required_columns=("affiliate_id", "player_id", "country", "clicks", "registrations", "ftd", "cpa_value"),
        partition_field=None,
        normalizer=normalize_affiliate,
    ),
}
