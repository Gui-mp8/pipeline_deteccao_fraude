from __future__ import annotations

import json

import pyarrow.parquet as pq

from file_to_landing_parquet.config import TABLE_CONFIGS
from file_to_landing_parquet.partitioning import PartitionResolver
from file_to_landing_parquet.pipeline import LandingIngestionPipeline
from file_to_landing_parquet.readers import LocalCsvRowReader, LocalJsonArrayRowReader
from file_to_landing_parquet.writers import ParquetBatchWriter


def test_csv_reader_yields_rows_without_materializing(tmp_path):
    source = tmp_path / "transactions.csv"
    source.write_text(
        "transaction_id,player_id,type,amount,timestamp\n"
        "tx_1,pl_1,deposit,10.25,2026-01-20 21:46:20\n"
        "tx_2,pl_1,bet,5.00,2026-01-20 22:00:00\n",
        encoding="utf-8",
    )

    rows = LocalCsvRowReader(str(source)).iter_rows()

    assert iter(rows) is rows
    assert next(rows)["transaction_id"] == "tx_1"
    assert next(rows)["type"] == "bet"


def test_json_array_reader_streams_items(tmp_path):
    source = tmp_path / "players.json"
    source.write_text(
        json.dumps(
            [
                {"player_id": "pl_1", "email": "USER1@MAIL.COM", "city": "Recife", "created_at": "2026-01-20"},
                {"player_id": "pl_2", "email": "user2@mail.com", "city": "Salvador", "created_at": "2026-01-21"},
            ]
        ),
        encoding="utf-8",
    )

    rows = LocalJsonArrayRowReader(str(source)).iter_rows()

    assert iter(rows) is rows
    assert next(rows)["player_id"] == "pl_1"
    assert next(rows)["city"] == "Salvador"


def test_pipeline_writes_partitioned_parquet_in_small_batches(tmp_path):
    source = tmp_path / "transactions.csv"
    source.write_text(
        "transaction_id,player_id,type,amount,timestamp\n"
        "tx_1,pl_1,deposit,10.25,2026-01-20 21:46:20\n"
        "tx_2,pl_1,bet,5.00,2026-01-20 22:00:00\n"
        "tx_3,pl_2,withdraw,7.00,2026-01-21 10:00:00\n",
        encoding="utf-8",
    )
    destination = tmp_path / "landing" / "transactions"
    config = TABLE_CONFIGS["transactions"]

    result = LandingIngestionPipeline(
        reader=LocalCsvRowReader(str(source)),
        writer=ParquetBatchWriter(str(destination), config.schema, run_id="test-run"),
        table_config=config,
        partition_resolver=PartitionResolver(config.partition_field),
        batch_size=1,
    ).run()

    assert result.rows_read == 3
    assert result.files_written == 3
    assert (destination / "dt=2026-01-20").exists()
    assert (destination / "dt=2026-01-21").exists()

    table = pq.read_table(result.written_paths[0])
    assert table.schema.field("amount").type.precision == 18
    assert table.num_rows == 1


def test_affiliate_without_event_date_uses_ingest_partition(tmp_path):
    source = tmp_path / "affiliate_cpa_ftd.csv"
    source.write_text(
        "affiliate_id,player_id,country,clicks,registrations,ftd,cpa_value\n"
        "aff_1,pl_1,br,10,2,1,40\n",
        encoding="utf-8",
    )
    destination = tmp_path / "landing" / "affiliate_cpa_ftd"
    config = TABLE_CONFIGS["affiliate_cpa_ftd"]

    result = LandingIngestionPipeline(
        reader=LocalCsvRowReader(str(source)),
        writer=ParquetBatchWriter(str(destination), config.schema, run_id="test-run"),
        table_config=config,
        partition_resolver=PartitionResolver(config.partition_field),
        batch_size=10,
    ).run()

    assert result.rows_read == 1
    assert "ingest_date=" in result.written_paths[0]


def test_missing_required_column_fails_fast(tmp_path):
    source = tmp_path / "players.csv"
    source.write_text("player_id,email,city\npl_1,u@mail.com,Recife\n", encoding="utf-8")
    config = TABLE_CONFIGS["players"]

    pipeline = LandingIngestionPipeline(
        reader=LocalCsvRowReader(str(source)),
        writer=ParquetBatchWriter(str(tmp_path / "landing" / "players"), config.schema),
        table_config=config,
        partition_resolver=PartitionResolver(config.partition_field),
        batch_size=10,
    )

    try:
        pipeline.run()
    except ValueError as exc:
        assert "created_at" in str(exc)
    else:
        raise AssertionError("Expected missing required column validation to fail")
