"""Integration tests for partitioned write behavior."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from transport_etl.publish.hive_writer import write_partitioned_table
from transport_etl.publish.partitions import required_partition_columns


def test_partition_contract() -> None:
    """Partition columns should match curated table contract."""
    assert required_partition_columns() == ["p_date", "region_code", "carrier_id"]


def test_write_partitioned_table_creates_partition_paths(spark, tmp_path: Path) -> None:
    """Partition writer should materialize Hive-style partition directories."""
    from pyspark.sql import functions as F

    output_path = tmp_path / "partitioned_output"
    input_df = spark.createDataFrame(
        [
            ("SHP1", "CAR1", "CA", "2026-01-01T08:00:00Z"),
            ("SHP2", None, "TX", "2026-01-01T09:00:00Z"),
        ],
        schema="shipment_id string, carrier_id string, region_code string, pickup_ts string",
    ).withColumn("pickup_ts", F.to_timestamp("pickup_ts", "yyyy-MM-dd'T'HH:mm:ssX"))

    written_path = write_partitioned_table(
        df=input_df,
        table_name="test_partitioned_output",
        output_path=str(output_path),
        partitions=required_partition_columns(),
        mode="overwrite",
        spark=spark,
        register_hive_table=False,
    )

    written_root = Path(written_path)
    partition_dirs = list(written_root.glob("p_date=*/region_code=*/carrier_id=*"))
    assert partition_dirs
    assert any(path.is_dir() for path in partition_dirs)

    parquet_files = list(written_root.rglob("*.parquet"))
    json_files = list(written_root.rglob("*.jsonl"))
    csv_files = list(written_root.rglob("*.csv"))
    assert parquet_files or json_files or csv_files

    if parquet_files:
        written_df = spark.read.parquet(str(written_root))
        assert written_df.filter(F.col("p_date").isNull()).count() == 0
        assert written_df.filter(F.col("region_code").isNull()).count() == 0
        assert written_df.filter(F.col("carrier_id").isNull()).count() == 0
    elif json_files:
        sampled_records: list[dict[str, object]] = []
        for path in json_files:
            for raw_line in path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                sampled_records.append(json.loads(line))
                if len(sampled_records) >= 5:
                    break
            if len(sampled_records) >= 5:
                break

        assert sampled_records
        assert all(record.get("p_date") not in (None, "") for record in sampled_records)
        assert all(record.get("region_code") not in (None, "") for record in sampled_records)
        assert all(record.get("carrier_id") not in (None, "") for record in sampled_records)
    elif csv_files:
        sampled_records: list[dict[str, object]] = []
        for path in csv_files:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    sampled_records.append(dict(row))
                    if len(sampled_records) >= 5:
                        break
            if len(sampled_records) >= 5:
                break

        assert sampled_records
        assert all(record.get("p_date") not in (None, "") for record in sampled_records)
        assert all(record.get("region_code") not in (None, "") for record in sampled_records)
        assert all(record.get("carrier_id") not in (None, "") for record in sampled_records)
    else:
        raise AssertionError(f"No data files written under {written_root}")
