from __future__ import annotations

import io
import json

import pyarrow as pa
import pyarrow.parquet as pq

import src.adapters.csv_adapter as csv_adapter_module
import src.adapters.json_adapter as json_adapter_module
from src.adapters.csv_adapter import CsvAdapter
from src.adapters.json_adapter import JsonAdapter
from src.config import TABLE_CONFIGS
from src.interfaces.parquet_transformer_interface import ParquetBatch
from src.repository.gcs_repository import GCSRepository


def mock_storage(monkeypatch, adapter_module, content: str) -> None:
    class FakeFileSystem:
        def open_input_file(self, path):
            return io.BytesIO(content.encode("utf-8"))

    class FakeFileSystemFactory:
        @staticmethod
        def from_uri(source_uri):
            return FakeFileSystem(), source_uri.removeprefix("gs://case-grupo-otg1/")

    monkeypatch.setattr(adapter_module.pafs, "FileSystem", FakeFileSystemFactory)


def test_csv_adapter_reads_from_storage_and_transforms_to_parquet(monkeypatch):
    mock_storage(
        monkeypatch,
        csv_adapter_module,
        "transaction_id,player_id,type,amount,timestamp\n"
        "tx_1,pl_1,deposit,10.25,2026-01-20 21:46:20\n"
        "tx_2,pl_1,bet,5.00,2026-01-20 22:00:00\n",
    )
    source_uri = "gs://case-grupo-otg1/landing/transactions.csv"

    adapter = CsvAdapter(TABLE_CONFIGS["transactions"], batch_size=1)
    rows = adapter.read_from_storage(source_uri)

    assert iter(rows) is rows
    assert next(rows)["transaction_id"] == "tx_1"
    assert next(rows)["type"] == "bet"

    batches = list(adapter.transform_to_parquet(source_uri))
    assert len(batches) == 2
    assert batches[0].partition == "dt=2026-01-20"
    assert batches[0].table.schema.field("amount").type == pa.string()
    assert batches[0].table.column("amount").to_pylist() == ["10.25"]


def test_json_adapter_reads_from_storage_and_transforms_to_parquet(monkeypatch):
    mock_storage(
        monkeypatch,
        json_adapter_module,
        json.dumps(
            [
                {"player_id": "pl_1", "email": "USER1@MAIL.COM", "city": "Recife", "created_at": "2026-01-20"},
                {"player_id": "pl_2", "email": "user2@mail.com", "city": "Salvador", "created_at": "2026-01-21"},
            ]
        ),
    )
    source_uri = "gs://case-grupo-otg1/landing/players.json"

    adapter = JsonAdapter(TABLE_CONFIGS["players"], batch_size=1)
    rows = adapter.read_from_storage(source_uri)

    assert iter(rows) is rows
    assert next(rows)["player_id"] == "pl_1"
    assert next(rows)["city"] == "Salvador"

    batches = list(adapter.transform_to_parquet(source_uri))
    assert len(batches) == 2
    assert batches[0].partition == "dt=2026-01-20"
    assert batches[0].table.column("email").to_pylist() == ["USER1@MAIL.COM"]


def test_csv_adapter_batches_and_partitions_rows(monkeypatch):
    mock_storage(
        monkeypatch,
        csv_adapter_module,
        (
            "transaction_id,player_id,type,amount,timestamp\n"
            "tx_1,pl_1,deposit,10.25,2026-01-20 21:46:20\n"
            "tx_2,pl_1,bet,5.00,2026-01-20 22:00:00\n"
            "tx_3,pl_2,withdraw,7.00,2026-01-21 10:00:00\n"
        ),
    )
    adapter = CsvAdapter(TABLE_CONFIGS["transactions"], batch_size=1)

    batches = list(adapter.transform_to_parquet("gs://case-grupo-otg1/landing/transactions.csv"))

    assert len(batches) == 3
    assert batches[0].partition == "dt=2026-01-20"
    assert batches[0].table.schema.field("timestamp").type == pa.string()
    assert batches[2].partition == "dt=2026-01-21"


def test_repository_saves_parquet_batch_locally(tmp_path):
    old_partition_path = tmp_path / "staging" / "affiliate_cpa_ftd" / "ingest_date=2026-01-20"
    old_partition_path.mkdir(parents=True)
    old_file = old_partition_path / "old.parquet"
    old_file.write_text("old")

    table = pa.Table.from_pylist(
        [
            {
                "affiliate_id": "aff_1",
                "player_id": "pl_1",
                "country": "br",
                "clicks": "10",
                "registrations": "2",
                "ftd": "1",
                "cpa_value": "40",
            }
        ],
        schema=TABLE_CONFIGS["affiliate_cpa_ftd"].schema,
    )
    batch = ParquetBatch(
        partition="ingest_date=2026-01-20",
        table=table,
        row_count=table.num_rows,
        part_number=1,
    )
    repository = GCSRepository(str(tmp_path / "staging" / "affiliate_cpa_ftd"), run_id="test-run")

    output_path = repository.save(batch)

    assert not old_file.exists()
    assert "ingest_date=" in output_path
    table = pq.read_table(output_path)
    assert table.num_rows == 1
    assert table.column("country").to_pylist() == ["br"]
    assert table.schema.field("clicks").type == pa.string()


def test_missing_required_column_fails_fast(monkeypatch):
    mock_storage(monkeypatch, csv_adapter_module, "player_id,email,city\npl_1,u@mail.com,Recife\n")
    adapter = CsvAdapter(TABLE_CONFIGS["players"], batch_size=10)

    try:
        next(adapter.transform_to_parquet("gs://case-grupo-otg1/landing/players.csv"))
    except ValueError as exc:
        assert "created_at" in str(exc)
    else:
        raise AssertionError("Expected missing required column validation to fail")


def test_adapter_rejects_non_gcs_source_uri():
    adapter = CsvAdapter(TABLE_CONFIGS["transactions"], batch_size=10)

    try:
        next(adapter.read_from_storage("/tmp/transactions.csv"))
    except ValueError as exc:
        assert "gs://" in str(exc)
    else:
        raise AssertionError("Expected local source URI to fail")
