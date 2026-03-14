"""Integration tests for local daily job entry point."""

from __future__ import annotations

from pathlib import Path

import pytest

from transport_etl.jobs.run_daily_batch import run_daily_batch


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
