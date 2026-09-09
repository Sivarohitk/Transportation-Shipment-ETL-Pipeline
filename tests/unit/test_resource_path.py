"""Tests for configuration-driven resource paths."""

from __future__ import annotations

from pathlib import Path

import pytest

from transport_etl.common.constants import (
    PROJECT_ROOT,
    RESOURCE_BASE_PATH_ENV,
    resolve_resource_path,
)
from transport_etl.jobs.run_daily_batch import _resolve_entity_source_path


def test_resource_path_defaults_to_project_root(monkeypatch: pytest.MonkeyPatch) -> None:
    """Existing local and EMR resolution should remain unchanged."""
    monkeypatch.delenv(RESOURCE_BASE_PATH_ENV, raising=False)

    assert resolve_resource_path("config", "base.yaml") == PROJECT_ROOT / "config" / "base.yaml"


def test_resource_path_honors_runtime_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Synced Databricks assets should resolve outside the installed wheel."""
    monkeypatch.setenv(RESOURCE_BASE_PATH_ENV, str(tmp_path))

    assert resolve_resource_path("sql", "staging") == tmp_path / "sql" / "staging"


def test_explicit_source_date_never_substitutes_another_dated_file(tmp_path: Path) -> None:
    (tmp_path / "shipments_2026-01-02.csv").touch()

    with pytest.raises(FileNotFoundError, match="2026-01-01"):
        _resolve_entity_source_path(str(tmp_path), "shipments", "2026-01-01")


def test_explicit_source_date_allows_canonical_undated_file(tmp_path: Path) -> None:
    canonical = tmp_path / "shipments.csv"
    canonical.touch()

    assert _resolve_entity_source_path(str(tmp_path), "shipments", "2026-01-01") == str(canonical)
