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
        spark_profile="local",
        hive_database="curated_dev",
        fail_fast=False,
        register_hive=False,
    )

    overrides = cli._build_overrides(args)

    assert overrides == {
        "paths.raw_base_path": "raw-path",
        "paths.reference_base_path": "ref-path",
        "paths.staging_base_path": "stage-path",
        "paths.curated_base_path": "curated-path",
        "spark.profile": "local",
        "hive.database": "curated_dev",
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
    ) -> int:
        recorded["config_path"] = config_path
        recorded["run_date"] = run_date
        recorded["overrides"] = overrides
        return 0

    monkeypatch.setattr(cli, "run_daily_batch", fake_run_daily_batch)

    status = cli.main(
        [
            "--job",
            "daily",
            "--config",
            "config/dev.yaml",
            "--run-date",
            "2026-01-01",
            "--no-register-hive",
        ]
    )

    assert status == 0
    assert recorded["config_path"] == "config/dev.yaml"
    assert recorded["run_date"] == "2026-01-01"
    assert recorded["overrides"] == {"hive.register_tables": False}


def test_main_dispatches_backfill_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI should dispatch backfill job arguments to the backfill runner."""
    recorded: dict[str, object] = {}

    def fake_run_backfill_batch(
        config_path: str,
        start_date: str | None,
        end_date: str | None,
        overrides: dict[str, object] | None = None,
    ) -> int:
        recorded["config_path"] = config_path
        recorded["start_date"] = start_date
        recorded["end_date"] = end_date
        recorded["overrides"] = overrides
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
