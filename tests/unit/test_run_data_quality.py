"""Tests for the data-quality console-script wrapper."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from transport_etl.jobs import run_data_quality


def test_missing_test_directory_returns_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing synced test directory should fail without starting pytest."""
    called = False

    def fake_pytest_main(*args: object, **kwargs: object) -> int:
        nonlocal called
        called = True
        return 0

    pytest_main = Mock(side_effect=fake_pytest_main)
    monkeypatch.setattr(pytest, "main", pytest_main)

    status = run_data_quality.main(["--test-dir", str(tmp_path / "missing")])

    assert status == 2
    assert called is False


def test_existing_test_directory_propagates_pytest_exit_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The wrapper should invoke pytest and return its exact status."""
    test_dir = tmp_path / "data_quality"
    test_dir.mkdir()
    recorded_environment: dict[str, str | None] = {}

    def fake_pytest_main(arguments: list[str]) -> int:
        recorded_environment["resource"] = run_data_quality.os.environ.get(
            "TRANSPORT_ETL_RESOURCE_BASE_PATH"
        )
        recorded_environment["quarantine"] = run_data_quality.os.environ.get(
            "TRANSPORT_ETL_TEST_QUARANTINE_BASE_PATH"
        )
        return 7

    monkeypatch.delenv("TRANSPORT_ETL_RESOURCE_BASE_PATH", raising=False)
    monkeypatch.delenv("TRANSPORT_ETL_TEST_QUARANTINE_BASE_PATH", raising=False)
    pytest_main = Mock(side_effect=fake_pytest_main)
    monkeypatch.setattr(pytest, "main", pytest_main)

    status = run_data_quality.main(
        [
            "--target",
            "dev",
            "--test-dir",
            str(test_dir),
            "--resource-base-path",
            str(tmp_path),
            "--quarantine-base-path",
            "volume/quarantine",
        ]
    )

    assert status == 7
    assert pytest_main.call_args.args[0] == [
        "-q",
        "-p",
        "no:cacheprovider",
        "--rootdir",
        str(test_dir.parent),
        str(test_dir),
    ]
    assert recorded_environment == {
        "resource": str(tmp_path),
        "quarantine": "volume/quarantine",
    }
    assert "TRANSPORT_ETL_RESOURCE_BASE_PATH" not in run_data_quality.os.environ
    assert "TRANSPORT_ETL_TEST_QUARANTINE_BASE_PATH" not in run_data_quality.os.environ


def test_default_test_directory_is_used(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Omitting --test-dir should use the wrapper's default resolver."""
    monkeypatch.setattr(run_data_quality, "_default_test_dir", lambda: tmp_path)
    monkeypatch.setattr(pytest, "main", Mock(return_value=0))

    assert run_data_quality.main([]) == 0


def test_databricks_entry_point_raises_for_failed_pytest(monkeypatch: pytest.MonkeyPatch) -> None:
    """A wheel task must fail even though Databricks calls the entry point directly."""
    monkeypatch.setattr(run_data_quality, "main", Mock(return_value=4))

    with pytest.raises(SystemExit, match="4"):
        run_data_quality.databricks_main()


def test_databricks_entry_point_returns_cleanly_for_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful pytest run should leave the wheel task successful."""
    monkeypatch.setattr(run_data_quality, "main", Mock(return_value=0))

    assert run_data_quality.databricks_main() is None
