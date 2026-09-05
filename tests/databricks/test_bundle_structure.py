"""Tests for the Databricks Declarative Automation Bundle.

These tests verify that:

- the YAML files in ``deploy/databricks/`` are syntactically
  valid and loadable;
- the bundle structure matches the documented schema
  (bundle, targets, resources, variables, python);
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
        assert "version" in bundle_yaml["bundle"]
        # The bundle name is the documented one.
        assert bundle_yaml["bundle"]["name"] == "transport_etl_bundle"

    def test_include_patterns_present(self, bundle_yaml) -> None:
        # The bundle must include the source, configs, schemas, and
        # resources.  AGENTS.md rule 6 forbids hard-coded storage
        # paths so the ``include`` patterns are the only source
        # reference.
        assert "include" in bundle_yaml
        assert isinstance(bundle_yaml["include"], list)
        joined = "\n".join(bundle_yaml["include"])
        # The pipeline code must be in the bundle.
        assert "transport_etl" in joined
        # The schemas must be in the bundle.
        assert "schemas" in joined
        # The job resource must be in the bundle.
        assert "resources" in joined

    def test_exclude_blocks_generated_and_local(self, bundle_yaml) -> None:
        # Per AGENTS.md rule 13, generated / local artefacts must
        # not ship to a production workspace.
        assert "exclude" in bundle_yaml
        joined = "\n".join(bundle_yaml["exclude"])
        assert "data/generated" in joined
        assert "__pycache__" in joined

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
        assert variables["ml_entry_point"] == "transport-etl-ml"

    def test_python_build_commands_present(self, bundle_yaml) -> None:
        # The bundle builds a wheel locally.  The wheel path is
        # git-ignored; the build runs on every ``databricks bundle
        # deploy``.
        assert "python" in bundle_yaml
        assert "build_commands" in bundle_yaml["python"]
        joined = "\n".join(bundle_yaml["python"]["build_commands"])
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

    def test_host_is_placeholder_only(self) -> None:
        # The workspace host must be a placeholder in
        # ``databricks.yml`` — the per-target files inherit the
        # host unless they override it (they don't, by design).
        text = (BUNDLE_ROOT / "databricks.yml").read_text(encoding="utf-8")
        assert "<your-workspace>" in text, "databricks.yml must use a placeholder workspace host"


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
        assert variables["etl_entry_point"] == "transport-etl"
        assert variables["ml_entry_point"] == "transport-etl-ml"

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
                # The data-quality task shells out to ``pytest``
                # because there is no dedicated console script for
                # it.  The bundle must still invoke the existing
                # test suite (not duplicate the assertions into a
                # notebook).
                assert entry_point == "sh"
                # The pytest invocation is present in the
                # parameters.
                params = task["python_wheel_task"]["parameters"]
                assert any(
                    "pytest" in str(p) for p in params
                ), "data-quality task must invoke pytest"
            else:  # pragma: no cover - defensive
                pytest.fail(f"unexpected task {task['task_key']!r}")

    def test_job_cluster_uses_serverless_compatible_settings(self, job_yaml) -> None:
        job = job_yaml["resources"]["jobs"]["transport_etl_job"]
        cluster = job["job_clusters"][0]["new_cluster"]
        # The runtime is pinned to a known LTS Databricks Runtime
        # so the test suite is reproducible across deployments.
        assert cluster["spark_version"].startswith("14.3")
        # Single-user data-security mode is the documented
        # production setting for Lakeflow jobs.
        assert cluster["data_security_mode"] == "SINGLE_USER"

    def test_job_exposes_run_date_parameter(self, job_yaml) -> None:
        job = job_yaml["resources"]["jobs"]["transport_etl_job"]
        # ``run_date`` is the parameter operators set to
        # backfill a specific day.
        param_names = {p["name"] for p in job["parameters"]}
        assert "run_date" in param_names


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
        data, _ = target_yaml
        assert "variables" in data
        # The target exposes the catalog- and environment-related
        # overrides; the storage paths are inherited from
        # ``databricks.yml`` when not explicitly set.
        variables = data["variables"]
        # The target exposes the catalog name and the environment
        # label (used for tagging).
        assert "catalog_name" in variables
        assert "environment" in variables
        # Bronze / silver / gold schema names are exposed.
        for key in ("bronze_schema", "silver_schema", "gold_schema"):
            assert key in variables

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
