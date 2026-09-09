"""Unit tests for CLI dispatch and override wiring."""

from __future__ import annotations

import argparse

import pytest

from transport_etl import main as cli


def test_build_overrides_maps_cli_flags() -> None:
    """CLI overrides should map supported flags to config dotted paths."""
    args = argparse.Namespace(
        raw_base_path="raw-path",
        reference_base_path="ref-path",
        staging_base_path="stage-path",
        curated_base_path="curated-path",
        resource_base_path="workspace-files",
        spark_profile="databricks",
        hive_database="curated_dev",
        catalog="workspace",
        bronze_schema="bronze_dev",
        silver_schema="silver_dev",
        gold_schema="gold_dev",
        fail_fast=False,
        register_hive=False,
    )

    overrides = cli._build_overrides(args)

    assert overrides == {
        "paths.raw_base_path": "raw-path",
        "paths.reference_base_path": "ref-path",
        "paths.staging_base_path": "stage-path",
        "paths.curated_base_path": "curated-path",
        "runtime.resource_base_path": "workspace-files",
        "spark.profile": "databricks",
        "hive.database": "curated_dev",
        "unity_catalog.catalog": "workspace",
        "unity_catalog.bronze_schema": "bronze_dev",
        "unity_catalog.silver_schema": "silver_dev",
        "unity_catalog.gold_schema": "gold_dev",
        "runtime.fail_fast": False,
        "hive.register_tables": False,
    }


def test_main_dispatches_daily_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI should dispatch daily job arguments to the daily runner."""
    recorded: dict[str, object] = {}

    def fake_run_daily_batch(
        config_path: str,
        run_date: str | None = None,
        overrides: dict[str, object] | None = None,
        config_dir: object | None = None,
    ) -> int:
        recorded["config_path"] = config_path
        recorded["run_date"] = run_date
        recorded["overrides"] = overrides
        recorded["config_dir"] = config_dir
        return 0

    monkeypatch.setattr(cli, "run_daily_batch", fake_run_daily_batch)

    status = cli.main(
        [
            "--job",
            "daily",
            "--config",
            "config/dev.yaml",
            "--config-dir",
            "config",
            "--run-date",
            "2026-01-01",
            "--no-register-hive",
        ]
    )

    assert status == 0
    assert recorded["config_path"] == "config/dev.yaml"
    assert recorded["run_date"] == "2026-01-01"
    assert recorded["overrides"] == {"hive.register_tables": False}
    assert str(recorded["config_dir"]) == "config"


def test_main_dispatches_backfill_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI should dispatch backfill job arguments to the backfill runner."""
    recorded: dict[str, object] = {}

    def fake_run_backfill_batch(
        config_path: str,
        start_date: str | None,
        end_date: str | None,
        overrides: dict[str, object] | None = None,
        config_dir: object | None = None,
    ) -> int:
        recorded["config_path"] = config_path
        recorded["start_date"] = start_date
        recorded["end_date"] = end_date
        recorded["overrides"] = overrides
        recorded["config_dir"] = config_dir
        return 0

    monkeypatch.setattr(cli, "run_backfill_batch", fake_run_backfill_batch)

    status = cli.main(
        [
            "--job",
            "backfill",
            "--config",
            "config/dev.yaml",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
            "--spark-profile",
            "local",
        ]
    )

    assert status == 0
    assert recorded["config_path"] == "config/dev.yaml"
    assert recorded["start_date"] == "2026-01-01"
    assert recorded["end_date"] == "2026-01-02"
    assert recorded["overrides"] == {"spark.profile": "local"}
    assert recorded["config_dir"] is None


def test_main_raises_for_nonzero_status_when_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Databricks wheel tasks must surface returned failures as exceptions."""
    monkeypatch.setattr(cli, "run_daily_batch", lambda **kwargs: 1)

    with pytest.raises(RuntimeError, match="status 1"):
        cli.main(["--job", "daily", "--raise-on-error"])
