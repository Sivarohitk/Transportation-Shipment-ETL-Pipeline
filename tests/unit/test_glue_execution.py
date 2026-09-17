"""Glue entrypoint and artifact contract without an AWS Glue installation."""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from deploy.glue import build_artifacts, job_entrypoint

ROOT = Path(__file__).resolve().parents[2]
PATH_ARGS = [
    "--raw_base_path",
    "s3://example-bucket/raw",
    "--reference_base_path",
    "s3://example-bucket/reference",
    "--staging_base_path",
    "s3://example-bucket/staging",
    "--curated_base_path",
    "s3://example-bucket/curated",
    "--audit_base_path",
    "s3://example-bucket/audit",
]


def test_argument_parser_rejects_missing_date_and_invalid_boolean() -> None:
    with pytest.raises(SystemExit):
        job_entrypoint.parse_job_arguments(["--JOB_NAME", "test"])
    with pytest.raises(SystemExit):
        job_entrypoint.parse_job_arguments(
            ["--JOB_NAME", "test", "--run_date", "2026-01-01", "--glue_enabled", "maybe"]
        )


def test_argument_parser_accepts_explicit_force_override() -> None:
    args = job_entrypoint.parse_job_arguments(
        ["--JOB_NAME", "test", "--run_date", "2026-01-01", "--force=true"]
    )
    assert args.force is True


def test_glue_job_uses_shared_daily_runner_and_commits_on_success() -> None:
    calls = []

    class Job:
        def commit(self):
            calls.append("commit")

    spark = object()

    def runtime_factory(name, arguments):
        calls.append((name, arguments.run_date))
        return spark, Job()

    def daily_runner(**kwargs):
        calls.append(kwargs)
        return 0

    assert (
        job_entrypoint.run_glue_job(
            [
                "--JOB_NAME",
                "test-job",
                "--run_date",
                "2026-01-01",
                *PATH_ARGS,
                "--glue_enabled",
                "true",
                "--glue_database",
                "curated",
                "--glue_region",
                "us-east-1",
            ],
            resource_root=ROOT,
            runtime_factory=runtime_factory,
            daily_runner=daily_runner,
        )
        == 0
    )
    assert calls[0] == ("test-job", "2026-01-01")
    assert calls[1]["spark_session"] is spark
    assert calls[1]["overrides"]["paths.raw_base_path"] == "s3://example-bucket/raw"
    assert calls[1]["overrides"]["glue.database"] == "curated"
    assert calls[2] == "commit"


def test_glue_job_failure_does_not_commit() -> None:
    class Job:
        committed = False

        def commit(self):
            self.committed = True

    job = Job()
    with pytest.raises(RuntimeError, match="status 1"):
        job_entrypoint.run_glue_job(
            ["--JOB_NAME", "test-job", "--run_date", "2026-01-01", *PATH_ARGS],
            resource_root=ROOT,
            runtime_factory=lambda name, arguments: (object(), job),
            daily_runner=lambda **kwargs: 1,
        )
    assert job.committed is False


def test_placeholder_s3_path_fails_before_glue_runtime_starts() -> None:
    with pytest.raises(ValueError, match="real S3"):
        job_entrypoint.run_glue_job(
            ["--JOB_NAME", "test-job", "--run_date", "2026-01-01"],
            resource_root=ROOT,
            runtime_factory=lambda *_: (_ for _ in ()).throw(AssertionError("started Glue")),
        )


def test_example_angle_bracket_bucket_fails_before_glue_runtime_starts() -> None:
    example_paths = [
        "s3://<DATA_BUCKET>/raw" if value == "s3://example-bucket/raw" else value
        for value in PATH_ARGS
    ]
    with pytest.raises(ValueError, match="real S3"):
        job_entrypoint.run_glue_job(
            ["--JOB_NAME", "test-job", "--run_date", "2026-01-01", *example_paths],
            resource_root=ROOT,
            runtime_factory=lambda *_: (_ for _ in ()).throw(AssertionError("started Glue")),
        )


def test_example_glue_region_fails_before_glue_runtime_starts() -> None:
    with pytest.raises(ValueError, match="real glue.region"):
        job_entrypoint.run_glue_job(
            [
                "--JOB_NAME",
                "test-job",
                "--run_date",
                "2026-01-01",
                *PATH_ARGS,
                "--glue_enabled",
                "true",
                "--glue_region",
                "<AWS_REGION>",
                "--glue_database",
                "curated",
            ],
            resource_root=ROOT,
            runtime_factory=lambda *_: (_ for _ in ()).throw(AssertionError("started Glue")),
        )


def test_resource_archive_rejects_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "resources.zip"
    with zipfile.ZipFile(archive, "w") as target:
        target.writestr("../escape", "bad")
    with pytest.raises(ValueError, match="Unsafe"):
        job_entrypoint.extract_resources(archive, tmp_path / "out")


def test_build_artifacts_has_python_package_and_driver_resources(tmp_path: Path) -> None:
    code_zip, resources_zip = build_artifacts.build_artifacts(ROOT, tmp_path)
    with zipfile.ZipFile(code_zip) as artifact:
        names = set(artifact.namelist())
        assert "transport_etl/jobs/run_daily_batch.py" in names
        assert "transport_etl/publish/glue_catalog.py" in names
    with zipfile.ZipFile(resources_zip) as artifact:
        names = set(artifact.namelist())
        assert "config/glue.yaml" in names
        assert "config/schemas/shipments.schema.json" in names
        assert "sql/staging/stg_shipments.sql" in names
        assert "sql/redshift/" not in names


def test_entrypoint_loads_packaged_resources(tmp_path: Path) -> None:
    _, resources_zip = build_artifacts.build_artifacts(ROOT, tmp_path)
    observed = {}

    class Job:
        def commit(self):
            observed["committed"] = True

    def daily_runner(**kwargs):
        observed["config_exists"] = (kwargs["config_dir"] / "base.yaml").is_file()
        observed["sql_exists"] = (
            Path(kwargs["overrides"]["runtime.resource_base_path"])
            / "sql"
            / "staging"
            / "stg_shipments.sql"
        ).is_file()
        return 0

    result = job_entrypoint.run_glue_job(
        [
            "--JOB_NAME",
            "test-job",
            "--run_date",
            "2026-01-01",
            "--resources_archive",
            str(resources_zip),
            *PATH_ARGS,
        ],
        runtime_factory=lambda *_: (object(), Job()),
        daily_runner=daily_runner,
    )
    assert result == 0
    assert observed == {"config_exists": True, "sql_exists": True, "committed": True}
