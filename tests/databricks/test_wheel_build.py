"""Tests for the wheel build produced by the bundle.

The bundle's ``python.build_commands`` is supposed to build a
wheel locally.  We verify the configuration is consistent: the
``wheel_path`` variable in ``databricks.yml`` points at a path
that is reproducible by the build command.

We do not run ``pip wheel`` here — that would couple the test
suite to the build environment (PEP 517 build backend
dependencies, network access).  The actual build is a deploy-time
step run by ``databricks bundle deploy``.

These tests only assert that the configuration is **internally
consistent** (the wheel path that the bundle deploys matches the
artefact that the build command would produce).
"""

from __future__ import annotations

from pathlib import Path

import tomllib
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BUNDLE_ROOT = PROJECT_ROOT / "deploy" / "databricks"
PYPROJECT = PROJECT_ROOT / "pyproject.toml"


class TestWheelBuildConsistency:
    """The wheel the bundle deploys must be reproducible."""

    def test_pyproject_name_and_version(self) -> None:
        with PYPROJECT.open("rb") as handle:
            data = tomllib.load(handle)
        project = data["project"]
        assert project["name"] == "transport-shipment-etl"
        assert project["version"] == "0.1.0"

    def test_bundle_wheel_path_matches_pyproject(self) -> None:
        # The wheel name follows the
        # ``{distribution}-{version}-py3-none-any.whl`` convention.
        with PYPROJECT.open("rb") as handle:
            data = tomllib.load(handle)
        expected_name = (
            f"{data['project']['name'].replace('-', '_')}"
            f"-{data['project']['version']}-py3-none-any.whl"
        )
        with (BUNDLE_ROOT / "databricks.yml").open("r", encoding="utf-8") as h:
            bundle = yaml.safe_load(h)
        wheel_path = bundle["variables"]["wheel_path"]
        assert expected_name in wheel_path, (
            f"Bundle wheel_path {wheel_path!r} does not match the "
            f"expected artefact name {expected_name!r}"
        )

    def test_bundle_build_command_uses_pip_wheel(self) -> None:
        with (BUNDLE_ROOT / "databricks.yml").open("r", encoding="utf-8") as h:
            bundle = yaml.safe_load(h)
        build_commands = bundle["python"]["build_commands"]
        joined = "\n".join(build_commands)
        # ``pip wheel`` is the documented way to produce a
        # reproducible wheel without relying on ``setup.py``.
        assert "pip wheel" in joined
        # The wheel directory is ``dist`` (which we git-ignore).
        assert "dist" in joined
        # The wheel must not pull dependencies — the cluster
        # provides them from the runtime.
        assert "--no-deps" in joined
