"""Tests for local and Databricks runtime resource resolution."""

from __future__ import annotations

from pathlib import Path

from transport_etl.common.constants import PROJECT_ROOT
from transport_etl.jobs.run_daily_batch import _resolve_runtime_resource_paths


def test_runtime_resource_paths_default_to_project_root() -> None:
    """Local and EMR callers should retain repository-relative assets."""
    schema_dir, sql_dir = _resolve_runtime_resource_paths({})

    assert schema_dir == PROJECT_ROOT / "config" / "schemas"
    assert sql_dir == PROJECT_ROOT / "sql" / "staging"


def test_runtime_resource_paths_honor_databricks_sync_root(tmp_path: Path) -> None:
    """Databricks should resolve assets from the explicit synced bundle root."""
    schema_dir, sql_dir = _resolve_runtime_resource_paths(
        {"runtime": {"resource_base_path": str(tmp_path)}}
    )

    assert schema_dir == tmp_path / "config" / "schemas"
    assert sql_dir == tmp_path / "sql" / "staging"
