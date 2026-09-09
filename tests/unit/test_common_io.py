"""Unit tests for shared Spark I/O option handling."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import pytest

from transport_etl.common.io import (
    _write_python_csv_fallback,
    _write_python_json_fallback,
    read_csv,
)


class _Reader:
    def __init__(self) -> None:
        self.options_received: dict[str, str] = {}
        self.schema_received = None
        self.path_received: str | None = None

    def options(self, **options: str) -> "_Reader":
        self.options_received = options
        return self

    def schema(self, schema: object) -> "_Reader":
        self.schema_received = schema
        return self

    def csv(self, path: str) -> "_Reader":
        self.path_received = path
        return self


class _Spark:
    def __init__(self) -> None:
        self.read = _Reader()


class _Row(dict[str, object]):
    def asDict(self, recursive: bool = True) -> dict[str, object]:
        return dict(self)


class _Frame:
    columns = ["shipment_id", "p_date"]

    def __init__(self, shipment_id: str, p_date: str) -> None:
        self._rows = [_Row(shipment_id=shipment_id, p_date=p_date)]

    def collect(self) -> list[_Row]:
        return self._rows


def test_csv_bad_records_path_does_not_set_conflicting_mode(tmp_path) -> None:
    """Spark serverless forbids specifying mode with badRecordsPath."""
    source = tmp_path / "input.csv"
    source.touch()
    spark = _Spark()

    read_csv(
        spark,
        str(source),
        options={"mode": "PERMISSIVE", "badRecordsPath": "dbfs:/tmp/bad"},
    )

    assert spark.read.options_received["badRecordsPath"] == "dbfs:/tmp/bad"
    assert "mode" not in spark.read.options_received
    assert spark.read.options_received["header"] == "true"


def test_csv_without_bad_records_path_keeps_configured_mode(tmp_path) -> None:
    """Normal local and EMR reads retain the configured CSV mode."""
    source = tmp_path / "input.csv"
    source.touch()
    spark = _Spark()

    read_csv(spark, str(source), options={"mode": "FAILFAST"})

    assert spark.read.options_received["mode"] == "FAILFAST"


@pytest.mark.parametrize(
    ("writer", "extension"),
    [
        (_write_python_json_fallback, "jsonl"),
        (_write_python_csv_fallback, "csv"),
    ],
)
def test_partitioned_fallback_overwrite_preserves_other_dates(
    tmp_path: Path,
    writer: Callable[..., Any],
    extension: str,
) -> None:
    destination = tmp_path / "table"

    writer(_Frame("SHP1", "2026-01-01"), str(destination), "overwrite", ["p_date"])
    writer(_Frame("SHP2", "2026-01-02"), str(destination), "overwrite", ["p_date"])

    assert list((destination / "p_date=2026-01-01").glob(f"*.{extension}"))
    assert list((destination / "p_date=2026-01-02").glob(f"*.{extension}"))
