"""Benchmark JSON reports observed pipeline counts, not fixture guesses."""

from __future__ import annotations

import json

import pytest

from transport_etl import benchmark
from transport_etl.benchmark import build_benchmark_result, write_benchmark_result


def test_benchmark_result_schema_and_arithmetic(tmp_path):
    audit = {
        "run_id": "daily_2026-01-01_test",
        "batch_date": "2026-01-01",
        "environment": "dev",
        "status": "success",
        "source_rows": {"shipments": 100, "carriers": 4, "delivery_events": 180},
        "curated_rows": {"fct_shipment": 100, "fct_delivery_event": 180},
        "rejected_rows": {"shipments": 2, "delivery_events": 3},
    }
    result = build_benchmark_result(
        audit,
        elapsed_seconds=10.0,
        runtime_profile="local",
        spark_version="3.5.2",
        spark_configuration={"spark.sql.shuffle.partitions": "4"},
        output_format="parquet",
    )
    assert result["schema_version"] == 1
    assert result["input_rows"] == 284
    assert result["output_rows"] == 280
    assert result["rejected_rows"] == 5
    assert result["input_rows_per_second"] == 28.4
    assert result["source_rows"] == audit["source_rows"]
    assert result["spark_configuration"]["spark.sql.shuffle.partitions"] == "4"
    assert result["output_row_scope"] == "five core Gold audit counts"

    path = tmp_path / "result.json"
    write_benchmark_result(path, result)
    assert json.loads(path.read_text(encoding="utf-8")) == result


@pytest.mark.parametrize("elapsed", [0.0, -1.0])
def test_benchmark_result_rejects_nonpositive_elapsed(elapsed):
    with pytest.raises(ValueError):
        build_benchmark_result(
            {"status": "success", "source_rows": {}},
            elapsed_seconds=elapsed,
            runtime_profile="local",
            spark_version="3.5.2",
            spark_configuration={},
            output_format="parquet",
        )


def test_benchmark_cli_rejects_unmanaged_glue_profile(monkeypatch):
    monkeypatch.setattr(benchmark, "load_config", lambda name: {"spark": {"profile": "glue"}})
    with pytest.raises(SystemExit):
        benchmark.main(["--config", "glue", "--run-date", "2026-01-01"])
