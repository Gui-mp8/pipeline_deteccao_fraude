from __future__ import annotations

import argparse
import json

from .config import TABLE_CONFIGS
from .partitioning import PartitionResolver
from .pipeline import LandingIngestionPipeline
from .readers import build_reader
from .writers import ParquetBatchWriter


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert CSV/JSON source files to partitioned Parquet landing.")
    parser.add_argument("--source-uri", required=True, help="Local path or gs:// URI for the source file.")
    parser.add_argument("--destination-uri", required=True, help="Local path or gs:// URI for the table landing prefix.")
    parser.add_argument("--table", required=True, choices=sorted(TABLE_CONFIGS), help="Known table configuration.")
    parser.add_argument("--format", required=True, choices=["csv", "json", "json-array"], help="Source file format.")
    parser.add_argument("--batch-size", type=int, default=1000, help="Maximum records held per partition before writing.")
    parser.add_argument("--run-id", default=None, help="Optional run id used in destination path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    table_config = TABLE_CONFIGS[args.table]
    reader = build_reader(args.source_uri, args.format)
    writer = ParquetBatchWriter(args.destination_uri, table_config.schema, run_id=args.run_id) if args.run_id else ParquetBatchWriter(args.destination_uri, table_config.schema)
    partition_resolver = PartitionResolver(
        partition_field=table_config.partition_field,
        fallback_partition_field=table_config.fallback_partition_field,
    )
    result = LandingIngestionPipeline(
        reader=reader,
        writer=writer,
        table_config=table_config,
        partition_resolver=partition_resolver,
        batch_size=args.batch_size,
    ).run()
    print(json.dumps(result.__dict__, default=list, sort_keys=True))


if __name__ == "__main__":
    main()
