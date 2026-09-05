"""Integration tests for local daily job entry point."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from transport_etl.jobs.run_daily_batch import run_daily_batch


def _has_any_data_files(root: Path) -> bool:
    """Return True when any parquet/jsonl/csv files exist under ``root``."""
    return bool(
        list(root.rglob("*.parquet")) or list(root.rglob("*.jsonl")) or list(root.rglob("*.csv"))
    )


def _read_jsonl_records(root: Path) -> list[dict[str, object]]:
    """Read every JSONL record produced under ``root`` (Windows fallback)."""
    records: list[dict[str, object]] = []
    for path in root.rglob("*.jsonl"):
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _read_csv_records(root: Path) -> list[dict[str, object]]:
    """Read every CSV record produced under ``root`` (Windows fallback)."""
    import csv as csv_mod

    records: list[dict[str, object]] = []
    for path in root.rglob("*.csv"):
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv_mod.DictReader(handle)
            for row in reader:
                records.append(dict(row))
    return records


def _load_bronze_records(spark, root: Path) -> list[dict[str, object]]:
    """Load Bronze records from the Windows fallback directories."""
    parquet_files = list(root.rglob("*.parquet"))
    if parquet_files:
        return [row.asDict(recursive=True) for row in spark.read.parquet(str(root)).collect()]

    json_records = _read_jsonl_records(root)
    if json_records:
        return json_records

    return _read_csv_records(root)


def test_daily_job_local_end_to_end(
    project_root: Path,
    sample_run_date: str,
    tmp_path: Path,
) -> None:
    """Daily batch should run locally and write curated partitioned outputs."""
    pytest.importorskip("pyspark")

    staging_base = tmp_path / "staging"
    curated_base = tmp_path / "curated"
    audit_base = tmp_path / "logs"

    status = run_daily_batch(
        config_path="config/dev.yaml",
        run_date=sample_run_date,
        overrides={
            "spark.enable_hive_support": False,
            "hive.register_tables": False,
            "paths.raw_base_path": str(project_root / "data" / "sample" / "raw"),
            "paths.reference_base_path": str(project_root / "data" / "sample" / "reference"),
            "paths.staging_base_path": str(staging_base),
            "paths.curated_base_path": str(curated_base),
            "paths.audit_base_path": str(audit_base),
        },
    )

    assert status == 0

    expected_tables = [
        "dim_carrier",
        "fct_shipment",
        "fct_delivery_event",
        "agg_shipment_daily",
        "kpi_delivery_daily",
    ]
    for table in expected_tables:
        table_path = curated_base / table
        assert table_path.exists()
        has_parquet = any(table_path.rglob("*.parquet"))
        has_json_fallback = any((table_path / "_fallback_json").rglob("*.jsonl"))
        has_csv_fallback = any((table_path / "_fallback_csv").rglob("*.csv"))

        assert has_parquet or has_json_fallback or has_csv_fallback


def test_daily_job_local_writes_bronze_layer(
    project_root: Path,
    sample_run_date: str,
    tmp_path: Path,
) -> None:
    """Daily batch should additionally write Bronze layer outputs
    (``raw_shipments``, ``raw_carriers``, ``raw_delivery_events``)
    containing operational ingestion metadata columns.
    """
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    staging_base = tmp_path / "staging"
    curated_base = tmp_path / "curated"
    audit_base = tmp_path / "logs"

    status = run_daily_batch(
        config_path="config/dev.yaml",
        run_date=sample_run_date,
        overrides={
            "spark.enable_hive_support": False,
            "hive.register_tables": False,
            "paths.raw_base_path": str(project_root / "data" / "sample" / "raw"),
            "paths.reference_base_path": str(project_root / "data" / "sample" / "reference"),
            "paths.staging_base_path": str(staging_base),
            "paths.curated_base_path": str(curated_base),
            "paths.audit_base_path": str(audit_base),
        },
    )

    assert status == 0

    expected_bronze = ["raw_shipments", "raw_carriers", "raw_delivery_events"]
    spark = SparkSession.builder.appName("bronze-verify").master("local[2]").getOrCreate()
    try:
        for table in expected_bronze:
            bronze_path = staging_base / table
            assert bronze_path.exists(), f"Expected Bronze path for {table} at {bronze_path}"
            assert _has_any_data_files(bronze_path), f"No data files written for {table}"

            records = _load_bronze_records(spark, bronze_path)
            assert records, f"Bronze table {table} is empty"
            sample = records[0]
            for meta_col in ("_ingested_at", "_source_file", "_batch_id", "_run_date"):
                assert (
                    meta_col in sample
                ), f"Bronze table {table} missing metadata column {meta_col}"
            assert sample["_run_date"] == sample_run_date
            assert sample["_batch_id"] == f"daily_{sample_run_date}"
    finally:
        try:
            spark.stop()
        except Exception:
            pass
