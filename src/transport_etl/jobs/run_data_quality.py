"""Run the repository data-quality suite as a console script."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from transport_etl.common.constants import (
    PROJECT_ROOT,
    RESOURCE_BASE_PATH_ENV,
    TEST_QUARANTINE_BASE_PATH_ENV,
)


def _default_test_dir() -> Path:
    """Return the path to the ``tests/data_quality`` directory.

    The script is packaged as a console-script entry point.  The
    ``tests/`` directory is **not** shipped inside the installed
    wheel, so when the bundle runs the wheel inside a cluster the
    test directory is not available.  In that case the bundle
    surfaces a clear error so the operator knows to either
    (a) ship the test directory in the bundle, or
    (b) drop the data-quality task from the Lakeflow job.

    For local development (and for the on-CI test suite that
    invokes this entry point directly), the test directory is
    resolved relative to the package install location.
    """
    return PROJECT_ROOT / "tests" / "data_quality"


def main(argv: list[str] | None = None) -> int:
    """Run the data-quality test suite via ``pytest``."""
    parser = argparse.ArgumentParser(
        prog="transport-etl-data-quality",
        description=(
            "Run the project's data-quality test suite.  This is a "
            "thin wrapper around ``pytest``; no assertions are "
            "duplicated here."
        ),
    )
    parser.add_argument(
        "--target",
        default="default",
        help="Free-form label recorded in the log output (e.g. dev, prod).",
    )
    parser.add_argument(
        "--test-dir",
        type=Path,
        default=None,
        help=(
            "Path to the data-quality test directory.  When omitted the "
            "script uses the directory shipped with the repository "
            "checkout."
        ),
    )
    parser.add_argument(
        "--resource-base-path",
        type=Path,
        default=None,
        help="Synced repository root used by tests that load schemas or SQL.",
    )
    parser.add_argument(
        "--quarantine-base-path",
        type=str,
        default=None,
        help="Writable base path for quarantine output created by Spark-backed tests.",
    )
    args = parser.parse_args(argv)

    test_dir = args.test_dir or _default_test_dir()
    if not test_dir.exists():
        print(
            f"ERROR: data-quality test directory not found at {test_dir}.",
            file=sys.stderr,
        )
        print(
            "The repository's tests/ tree is not shipped inside the "
            "wheel.  Either drop the data_quality_checks task from "
            "the Lakeflow job, or include the test directory in the "
            "bundle's sync.paths list.",
            file=sys.stderr,
        )
        return 2

    print(f"[transport-etl-data-quality] running pytest in {test_dir} " f"(target={args.target})")
    environment_updates: dict[str, str] = {}
    if args.resource_base_path is not None:
        environment_updates[RESOURCE_BASE_PATH_ENV] = str(args.resource_base_path)
    if args.quarantine_base_path is not None:
        environment_updates[TEST_QUARANTINE_BASE_PATH_ENV] = args.quarantine_base_path

    # Keep pytest in this process so Databricks' injected Spark Connect session
    # remains available to the shared fixtures.  Workspace Files are read-only,
    # so disable bytecode and pytest cache writes beside the synced tests.
    import pytest

    previous_environment = {name: os.environ.get(name) for name in environment_updates}
    previous_dont_write_bytecode = sys.dont_write_bytecode
    try:
        os.environ.update(environment_updates)
        sys.dont_write_bytecode = True
        return int(
            pytest.main(
                [
                    "-q",
                    "-p",
                    "no:cacheprovider",
                    "--rootdir",
                    str(test_dir.parent),
                    str(test_dir),
                ]
            )
        )
    finally:
        sys.dont_write_bytecode = previous_dont_write_bytecode
        for name, previous_value in previous_environment.items():
            if previous_value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous_value


def databricks_main() -> None:
    """Run data-quality checks and make a nonzero status fail a wheel task.

    Databricks invokes a Python wheel entry-point function directly instead of
    using the generated console-script shim, so merely returning pytest's exit
    code does not fail the task.  Raising ``SystemExit`` preserves normal CLI
    semantics and prevents collection or assertion failures from appearing
    successful in Lakeflow.
    """
    status = main()
    if status:
        raise SystemExit(status)


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
