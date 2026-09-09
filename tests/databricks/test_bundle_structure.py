"""Tests for the Databricks Declarative Automation Bundle.

These tests verify that:

- the YAML files in ``deploy/databricks/`` are syntactically
  valid and loadable;
- the bundle structure matches the current CLI schema;
- the Lakeflow job definition references only entry points that
  exist in the existing Python package — no notebook copies of
  the pipeline are introduced (AGENTS.md rule 1);
- the bundle includes the production code (no
  ``src/transport_etl/`` duplication);
- the Databricks CLI is **not** required to run the test suite
  (the test file never invokes ``databricks bundle validate``
  automatically — that command is documented as a manual step
  in ``docs/databricks_deployment.md``).

The tests do **not** mock Databricks.  AGENTS.md rule 9 is
explicit: tests that exercise Databricks APIs must skip when
no live workspace is available, **not** be silently mocked.
Bundle validation therefore happens by a human operator with the
Databricks CLI configured, not in this test file.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BUNDLE_ROOT = PROJECT_ROOT / "deploy" / "databricks"


# ---------------------------------------------------------------------------
# YAML structural tests
# ---------------------------------------------------------------------------


class TestBundleYamlStructure:
    """The top-level ``databricks.yml`` matches the documented schema."""

    @pytest.fixture
    def bundle_yaml(self):
        path = BUNDLE_ROOT / "databricks.yml"
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    def test_yaml_parses(self, bundle_yaml) -> None:
        assert isinstance(bundle_yaml, dict)

    def test_bundle_section_present(self, bundle_yaml) -> None:
        assert "bundle" in bundle_yaml
        assert "name" in bundle_yaml["bundle"]
        # The bundle name is the documented one.
        assert bundle_yaml["bundle"]["name"] == "transport_etl_bundle"

    def test_include_patterns_present(self, bundle_yaml) -> None:
        # Top-level include is only for bundle configuration fragments.
        assert "include" in bundle_yaml
        assert isinstance(bundle_yaml["include"], list)
        joined = "\n".join(bundle_yaml["include"])
        assert "resources" in joined
        assert "targets" in joined
        assert "transport_etl" not in joined
        assert "pyproject" not in joined

    def test_sync_covers_runtime_files(self, bundle_yaml) -> None:
        sync = bundle_yaml["sync"]
        paths = set(sync["paths"])
        assert "../../config" in paths
        assert "../../tests" in paths
        assert "../../data/sample" in paths
        assert "../../src" not in paths

    def test_variables_expose_catalog_and_schemas(self, bundle_yaml) -> None:
        # Catalog, schema, paths are exposed as variables per
        # AGENTS.md rule 6.
        variables = bundle_yaml.get("variables", {})
        # The runtime parameters the pipeline needs are exposed.
        assert "raw_base_path" in variables
        assert "staging_base_path" in variables
        assert "curated_base_path" in variables
        # The ML entry point is exposed as a variable so it can be
        # pinned in tests.
        assert "ml_entry_point" in variables
        assert variables["ml_entry_point"]["default"] == "transport-etl-ml"
        for name, value in variables.items():
            assert isinstance(value, dict), f"variable {name} must use current map syntax"

    def test_artifact_builds_from_repository_root(self, bundle_yaml) -> None:
        artifact = bundle_yaml["artifacts"]["python_wheel"]
        assert artifact["type"] == "whl"
        assert artifact["path"] == "../.."
        joined = artifact["build"]
        assert "pip wheel" in joined

    def test_targets_dev_and_prod(self, bundle_yaml) -> None:
        # Both targets are defined; dev is the default.
        targets = bundle_yaml.get("targets", {})
        assert "dev" in targets
        assert "prod" in targets
        assert targets["dev"].get("default", False) is True

    def test_no_real_credentials_in_yaml(self) -> None:
        # AGENTS.md rule 4 forbids hard-coded credentials.  The
        # bundle YAML must not contain tokens, account IDs, or
        # workspace URLs.  We strip comments first so that the
        # documentation itself (which talks about what *is* and
        # *isn't* allowed) does not fail the test.
        raw_text = (BUNDLE_ROOT / "databricks.yml").read_text(encoding="utf-8")
        # Remove YAML comments (lines starting with ``#``).
        non_comment = "\n".join(
            line for line in raw_text.splitlines() if not line.lstrip().startswith("#")
        )
        forbidden = ("token", "secret", "password", "client_secret", "account_id")
        for term in forbidden:
            assert term not in non_comment.lower(), (
                f"databricks.yml contains forbidden term {term!r} " "in a non-comment line"
            )

    def test_workspace_host_is_not_committed(self) -> None:
        text = (BUNDLE_ROOT / "databricks.yml").read_text(encoding="utf-8")
        assert "workspace:" not in text
        assert "cloud.databricks.com" not in text


class TestLakeflowJobYamlStructure:
    """The Lakeflow job definition matches the documented task DAG."""

    @pytest.fixture
    def job_yaml(self):
        path = BUNDLE_ROOT / "resources" / "transport_etl_job.yml"
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    def test_job_yaml_parses(self, job_yaml) -> None:
        assert isinstance(job_yaml, dict)
        assert "resources" in job_yaml
        assert "jobs" in job_yaml["resources"]

    def test_exactly_one_job(self, job_yaml) -> None:
        jobs = job_yaml["resources"]["jobs"]
        assert "transport_etl_job" in jobs
        # The bundle defines one job; multi-job support is out of
        # scope for Phase 9.
        assert len(jobs) == 1

    def test_task_dag_has_three_tasks(self, job_yaml) -> None:
        job = job_yaml["resources"]["jobs"]["transport_etl_job"]
        tasks = job["tasks"]
        keys = {task["task_key"] for task in tasks}
        assert keys == {
            "ingest_bronze_silver_gold",
            "score_late_risk",
            "data_quality_checks",
        }

    def test_task_dependencies_are_documented(self, job_yaml) -> None:
        job = job_yaml["resources"]["jobs"]["transport_etl_job"]
        by_key = {task["task_key"]: task for task in job["tasks"]}

        # The Bronze+Silver+Gold task has no predecessors.
        assert by_key["ingest_bronze_silver_gold"].get("depends_on", []) == []

        # The ML task depends on Bronze+Silver+Gold.
        ml_parents = {d["task_key"] for d in by_key["score_late_risk"].get("depends_on", [])}
        assert ml_parents == {"ingest_bronze_silver_gold"}

        # The data-quality task depends on both upstream tasks.
        dq_parents = {d["task_key"] for d in by_key["data_quality_checks"].get("depends_on", [])}
        assert dq_parents == {"ingest_bronze_silver_gold", "score_late_risk"}

    def test_tasks_use_python_wheel_task_not_notebook(self, job_yaml) -> None:
        # AGENTS.md rule 1: reuse the Python package; do not
        # duplicate into notebooks.  Every task must be a
        # ``python_wheel_task`` (or ``spark_python_task`` invoking
        # the wheel).
        job = job_yaml["resources"]["jobs"]["transport_etl_job"]
        for task in job["tasks"]:
            assert "python_wheel_task" in task, (
                f"Task {task['task_key']!r} does not use python_wheel_task; "
                "the bundle should invoke the existing Python "
                "package, not duplicate into a notebook."
            )

    def test_tasks_invoke_documented_entry_points(self, job_yaml) -> None:
        # The bundle uses ``${var.X}`` references which are resolved
        # at deploy time by the Databricks CLI.  We assert the
        # declared variable values rather than the resolved strings
        # so the test does not depend on the CLI's resolver.
        bundle_yaml = (BUNDLE_ROOT / "databricks.yml").read_text(encoding="utf-8")
        bundle = yaml.safe_load(bundle_yaml)
        variables = bundle["variables"]
        assert variables["etl_entry_point"]["default"] == "transport-etl"
        assert variables["ml_entry_point"]["default"] == "transport-etl-ml"

        job = job_yaml["resources"]["jobs"]["transport_etl_job"]
        # The Bronze+Silver+Gold task references the ETL entry-point
        # variable; the ML task references the ML entry-point variable.
        for task in job["tasks"]:
            entry_point = task["python_wheel_task"]["entry_point"]
            if task["task_key"] == "ingest_bronze_silver_gold":
                assert entry_point == "${var.etl_entry_point}"
            elif task["task_key"] == "score_late_risk":
                assert entry_point == "${var.ml_entry_point}"
            elif task["task_key"] == "data_quality_checks":
                assert entry_point == "${var.dq_entry_point}"
                params = task["python_wheel_task"]["parameters"]
                assert any(
                    "tests/data_quality" in str(p) for p in params
                ), "data-quality task must target the synced test suite"
                assert params[params.index("--resource-base-path") + 1] == (
                    "${workspace.file_path}"
                )
                assert params[params.index("--quarantine-base-path") + 1] == (
                    "${var.audit_base_path}/data_quality/quarantine"
                )
            else:  # pragma: no cover - defensive
                pytest.fail(f"unexpected task {task['task_key']!r}")

    def test_job_uses_serverless_environment(self, job_yaml) -> None:
        job = job_yaml["resources"]["jobs"]["transport_etl_job"]
        assert "job_clusters" not in job
        assert job["environments"][0]["environment_key"] == "serverless"
        dependencies = job["environments"][0]["spec"]["dependencies"]
        assert any(
            str(item).endswith("transport_shipment_etl-0.1.0-py3-none-any.whl")
            for item in dependencies
        )
        for task in job["tasks"]:
            assert task["environment_key"] == "serverless"
            assert "job_cluster_key" not in task
            assert "new_cluster" not in task
            assert "libraries" not in task

    def test_job_exposes_run_date_parameter(self, job_yaml) -> None:
        job = job_yaml["resources"]["jobs"]["transport_etl_job"]
        # ``run_date`` is the parameter operators set to
        # backfill a specific day.
        param_names = {p["name"] for p in job["parameters"]}
        assert "run_date" in param_names

    def test_etl_task_uses_synced_runtime_resources(self, job_yaml) -> None:
        """The installed wheel must resolve config, schemas, and SQL from sync."""
        job = job_yaml["resources"]["jobs"]["transport_etl_job"]
        task = next(
            item for item in job["tasks"] if item["task_key"] == "ingest_bronze_silver_gold"
        )
        params = task["python_wheel_task"]["parameters"]

        assert params[params.index("--config") + 1] == (
            "${workspace.file_path}/config/databricks.yaml"
        )
        assert params[params.index("--config-dir") + 1] == "${workspace.file_path}/config"
        assert params[params.index("--resource-base-path") + 1] == "${workspace.file_path}"
        assert "--raise-on-error" in params


class TestTargetOverrides:
    """The dev and prod target files contain only environment
    overrides — no credentials, no real workspace URLs."""

    @pytest.fixture(params=["dev", "prod"])
    def target_yaml(self, request):
        path = BUNDLE_ROOT / "targets" / f"{request.param}.yml"
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle), path

    def test_target_yaml_parses(self, target_yaml) -> None:
        data, _ = target_yaml
        assert isinstance(data, dict)

    def test_target_exposes_paths(self, target_yaml) -> None:
        # Per-target files override only the variables that differ
        # between dev and prod.  The storage paths are exposed at the
        # bundle level (``databricks.yml``) so a target that does
        # not override them inherits the default — that is the
        # documented behavior.
        data, path = target_yaml
        target_name = path.stem
        assert "targets" in data
        assert target_name in data["targets"]
        # The target exposes the catalog- and environment-related
        # overrides; the storage paths are inherited from
        # ``databricks.yml`` when not explicitly set.
        variables = data["targets"][target_name]["variables"]
        # The target exposes the catalog name and the environment
        # label (used for tagging).
        assert "catalog_name" in variables
        assert "environment" in variables
        # Bronze / silver / gold schema names are exposed.
        for key in ("bronze_schema", "silver_schema", "gold_schema"):
            assert key in variables
        for name, value in variables.items():
            assert isinstance(value, dict), f"target variable {name} must use map syntax"

    def test_target_has_no_real_credentials(self, target_yaml) -> None:
        _, path = target_yaml
        text = path.read_text(encoding="utf-8")
        # Comments are stripped so the documentation prose does not
        # fail the check.
        non_comment = "\n".join(
            line for line in text.splitlines() if not line.lstrip().startswith("#")
        )
        for term in ("token", "password", "secret", "account_id"):
            assert term not in non_comment.lower(), (
                f"{path.name} contains forbidden term {term!r} " "in a non-comment line"
            )


# ---------------------------------------------------------------------------
# Python entry-point contract
# ---------------------------------------------------------------------------


class TestEntryPointsExist:
    """The bundle's ``entry_point`` values resolve to real Python
    functions.  AGENTS.md rule 1: reuse the existing package; do
    not duplicate the pipeline into notebooks."""

    def test_transport_etl_entry_point_importable(self) -> None:
        from transport_etl.main import main as etl_main

        assert callable(etl_main)

    def test_transport_etl_ml_entry_point_importable(self) -> None:
        from transport_etl.ml.cli import main as ml_main

        assert callable(ml_main)

    def test_transport_etl_ml_cli_has_train_and_score_command(self) -> None:
        from transport_etl.ml.cli import _build_arg_parser

        parser = _build_arg_parser()
        sub_actions = [action for action in parser._actions if action.dest == "command"]
        assert sub_actions, "ML CLI parser is missing the command subparser"
        # The ML CLI exposes exactly one subcommand: ``train-and-score``.
        choices = list(sub_actions[0].choices)
        assert "train-and-score" in choices

    def test_pyproject_registers_both_entry_points(self) -> None:
        # The bundle relies on the wheel exporting the same entry
        # points; this test guards against drift between
        # ``pyproject.toml`` and the bundle YAML.
        pyproject = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        assert "transport-etl = " in pyproject
        assert "transport-etl-ml = " in pyproject
        assert "transport-etl-data-quality = " in pyproject


# ---------------------------------------------------------------------------
# Pipeline-not-duplicated contract
# ---------------------------------------------------------------------------


class TestNoNotebookDuplicate:
    """The bundle must not embed a duplicate of the pipeline in a
    notebook.  We assert this structurally: no ``.ipynb`` files in
    the bundle tree."""

    def test_no_ipynb_files_in_bundle(self) -> None:
        for path in BUNDLE_ROOT.rglob("*.ipynb"):
            pytest.fail(
                f"Databricks bundle contains a notebook {path}.  "
                "Notebooks would duplicate the pipeline; the bundle "
                "must invoke the existing transport-etl / "
                "transport-etl-ml entry points."
            )

    def test_no_extra_source_under_bundle(self) -> None:
        # The bundle must not contain an extra ``transport_etl``
        # source tree; the wheel is the deployment artefact.
        assert not (BUNDLE_ROOT / "src").exists(), (
            "Bundle contains a duplicate src/transport_etl tree; "
            "the wheel is the deployment artefact."
        )


# ---------------------------------------------------------------------------
# Databricks CLI: not required, not mocked
# ---------------------------------------------------------------------------


class TestDatabricksCliAvailability:
    """AGENTS.md rule 9 forbids mocking the Databricks CLI.  We
    document its absence here so the test runner is honest about
    what was and was not validated.
    """

    def test_databricks_cli_status_documented(self) -> None:
        if shutil.which("databricks") is None:
            # Document the absence; the test still passes so the
            # suite is green on machines without the CLI.
            pytest.skip(
                "databricks CLI not installed; bundle validation is "
                "a manual step.  See docs/databricks_deployment.md."
            )
        else:  # pragma: no cover - executed when CLI is present
            # When the CLI is installed we do *not* auto-run
            # ``databricks bundle validate``; an operator must run
            # it after configuring the workspace.  We still record
            # the availability.
            assert shutil.which("databricks") is not None
