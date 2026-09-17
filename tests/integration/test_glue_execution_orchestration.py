"""Glue adapter calls the actual daily runner without awsglue or AWS."""

from __future__ import annotations

import pytest

from deploy.glue.job_entrypoint import run_glue_job
from transport_etl.jobs import run_daily_batch as daily


def _args() -> list[str]:
    return [
        "--JOB_NAME",
        "test-job",
        "--config",
        "base",
        "--run_date",
        "2026-01-01",
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


def test_glue_adapter_runs_shared_daily_flow_and_commits(monkeypatch) -> None:
    calls = []
    spark = object()

    class Job:
        def commit(self):
            calls.append("commit")

    monkeypatch.setattr(
        daily,
        "create_spark_session_from_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("created Spark")),
    )
    monkeypatch.setattr(
        daily,
        "stop_spark_session",
        lambda session: (_ for _ in ()).throw(AssertionError("stopped Glue Spark")),
    )

    def flow(**kwargs):
        calls.append((kwargs["spark"], kwargs["config"]["spark"]["profile"]))
        return {"outputs": {"fct_shipment": "s3://example-bucket/curated/fct_shipment"}}

    monkeypatch.setattr(daily, "_execute_daily_flow", flow)
    assert (
        run_glue_job(
            _args(),
            resource_root=daily.PROJECT_ROOT,
            runtime_factory=lambda *_: (spark, Job()),
            daily_runner=daily.run_daily_batch,
        )
        == 0
    )
    assert calls == [(spark, "glue"), "commit"]


def test_glue_adapter_does_not_commit_after_daily_failure(monkeypatch) -> None:
    commits = []

    class Job:
        def commit(self):
            commits.append(True)

    def fail_flow(**kwargs):
        raise ValueError("invalid business rule")

    monkeypatch.setattr(daily, "_execute_daily_flow", fail_flow)
    with pytest.raises(RuntimeError, match="status 1"):
        run_glue_job(
            _args(),
            resource_root=daily.PROJECT_ROOT,
            runtime_factory=lambda *_: (object(), Job()),
            daily_runner=daily.run_daily_batch,
        )
    assert commits == []
